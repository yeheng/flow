# 插件系统设计 v2：plugin runtime（executor 的身份反转）

> 状态：设计稿 v2（修订 v1 的宿主模型错误）。v1 把 executor 当固定宿主、
> 另发明了 server 宿主；正确模型是**宿主唯一**：executor 不是「顺便托管
> 插件的执行器」，它**就是 plugin runtime**——它的遗留物（task.rs 里
> 106KB 的节点语义）才是要被掏空迁出成插件的东西。
>
> 底座不变：`proc-ipc`（通道）、`plugin-proto`（协议）、`agent-plugin`
>（第一个插件，已完成迁移并 e2e 绿）。

## 0. 一句话

**master 的 v7 执行子进程，从「节点语义的唯一居所」反转为「插件运行时」：
它自己不再执行节点语义，只负责发现、拉起、供给、回收插件，并把节点
派发路由到内置快路径或插件进程。**

## 1. 三层协议栈（各层冻结边界）

```
master (backend/journal-server)
   │  v7 postcard 单流（握手/审计窗口/Permit/Transfer/心跳）── 不动
   ▼
plugin runtime（flow-executor 的演化形态，唯一插件宿主）
   │  proc-ipc 通道 + plugin-proto（initialize/config 快照/roots/取消）
   ▼
plugin 进程 ×N（flow-agent-plugin、未来的 llm-plugin、…）
```

- **v7 层一个字节不动**：master 不感知 runtime 内部变化（它派发节点，
  不关心节点由内置处理还是插件执行）。这层是 flow 的脊柱。
- **plugin-proto 层是唯一新增面**：runtime↔插件，复用既有协议与纪律
  （版本门、令牌回验、$/cancelRequest、有界队列、vectors）。
- 每个插件与 runtime 同进程组：会话击杀连带回收（agent 迁移已验证）。

## 2. runtime 的永久职责（永不插件化的清单）

这些是 runtime 的「宪法」，任何插件化提案先过这张表：

| 职责 | 为什么永久留在 runtime |
| --- | --- |
| v7 会话协议（对 master） | 脊柱；插件不可见 master |
| audit/journal 写入 | 单写者纪律；插件只产事实，不碰权威 |
| secrets 解析边界 | 明文只经通道进插件，不落任何盘面 |
| 沙箱分配（`plugins/<id>/sandbox/<dispatch>`） | 宿主分配、宿主回收，插件不猜路径 |
| 插件生命周期（spawn/进程组/宽限/reap） | 已实现（proc-ipc ChildGuard 语义） |
| 节点类型路由（dispatch → 内置 or 插件） | 注册表是 runtime 的核心状态 |
| Transfer 分块协调 | 大值进出插件走既有分块协议 |

**插件永远不**：碰 journal、看见别的插件、越过 runtime 直连 master、
持有跨 dispatch 的持久状态（执行语义的 at-least-once 纪律）。

## 3. 节点类型的三档处置（判据先行，不为插件而插件）

现有 12 种节点类型，按**同一个判据**分档——「插件化买到什么」：
**进程隔离**（跑不可信/重依赖代码）、**独立发布节奏**（运行时升级不想
动 runtime）、**依赖重量**（拖进 runtime 编译树的东西）。

| 档 | 节点类型 | 理由 |
| --- | --- | --- |
| **已插件化** | Harness | agent 运行时整体外迁（完成，e2e 绿） |
| **插件化候选**（按序） | Llm → Email → HttpCall | Llm：provider SSE/重试/凭证，独立运行时最值；Email：SMTP 栈；HttpCall：出站/SSRF 策略可独立演进 |
| **永久内置** | Start/End/Delay/Input/SubWorkflow | DAG 结构语义，不是「执行」，无隔离需求 |
| **永久内置** | Script/Condition | QuickJS 已是进程内沙箱；再套进程 = 每节点双 spawn，纯开销零隔离收益 |

「一切皆插件」是把这个架构当教条。判据是隔离/节奏/依赖，不是热情。

## 4. Manifest v2（修订：`host` 字段删除）

```toml
[plugin]
id = "harness-agent"
name = "Harness Agent"          # 调色板/配置页展示名
version = "0.1.0"
entry = "bin/flow-agent-plugin"
protocol = 1                    # plugin-proto 主版本（廉价快筛；握手权威裁决）
node_types = ["harness"]        # 本插件实现的节点类型（runtime 路由键）
                                # 内置类型名出现于此 = manifest 解析拒绝

[config]                        # 同 v1：5 类型小子集；随 run 快照冻结
[ui]                            # 仅声明消费（master 读 manifest 渲染）
config_form = "config"          # 节点/插件配置页（宿主已有表单组件）
palette = { category = "ai", icon = "spark" }   # 调色板元数据（key，非资源）
[methods]                       # runtime 可调方法（默认全拒）
invoke = "agent/runTurn"        # 节点执行入口方法（每 node_type 一个）
```

- **v1 的 `host = "server" | "executor"` 删除**：宿主唯一（runtime）。
  master 读同一份 manifest（调色板、配置表单、启用校验），**从不 spawn
  插件**——产品面没有插件进程，只有插件声明。
- `node_types` 是路由键：dispatch 到达 → 注册表查类型 → 内置处理器 or
  spawn 插件会话。内置类型名被声明 = 拒绝加载（防语义被插件悄悄顶掉）。
- 解析失败 = 该插件禁用 + 醒目日志，runtime 照常服务。

## 5. 配置 / 沙箱 / UI（相对 v1 的修订）

- **配置**：三源合一（manifest default + journal 值 + secret 引用）单函数
  解析，**随 run 快照冻结**，经 initialize params 下发最终快照——插件
  永远不见合并过程（v1 §3 全部保留；v1 的「server 在线更新
  config/changed」**删除**：执行插件没有在线配置，run 开始即冻结）。
- **沙箱**：`plugins/<id>/sandbox/<dispatch>`，宿主分配回收（v1 §4 保留；
  `data/` 对执行插件 = manifest 层拒绝，评审已定）。
- **UI 收缩为声明消费**：`config_form` + `palette` + 节点参数表单 +
  outcome 展示键。完整设计见 [2026-11-06-plugin-ui.md](2026-11-06-plugin-ui.md)
  （第一性原理：插件 UI 永远不是「和插件对话」，只有 manifest 静态声明
  + journal/nodelog 已持久事实两个来源）。

## 6. 主进程调用链（端到端）

```
master   ──v7 Execute{task}──▶  runtime
runtime  查注册表: node.type
  ├─ 内置（Script/Condition/…）──▶ 进程内快路径（现状代码）
  └─ 插件（harness/llm/…）
       ├─ 沙箱: plugins/<id>/sandbox/<dispatch>/{tmp,…}
       ├─ spawn: proc-ipc（同进程组、env 白名单、fd=3）
       ├─ initialize（版本门 + 令牌 + config 快照 + roots）
       ├─ request  <invoke 方法>（prompt/params 输入）
       │    ◀─ notification agent/event…（元信息进度 → 观测通道）
       ├─ request  InputReady/Transfer 分块（大输入，runtime 协调）
       └─ response 结果（text/outcome/exit_code）
runtime  落 audit（事实+回执，单写者）──v7 Result/AuditBatch──▶ master
回收: dispatch 完结 → 插件会话关闭（kill 进程组 + wait/reap）
```

超时/取消：master 的 v7 Cancel → runtime `$/cancelRequest` → 插件
CancelSignal → Uncertain 语义上抛（agent 路径已验证的整条链）。

## 7. 改名时机

`flow-executor` 这个名字在 task.rs 掏空之前**就是个谎言但先留着**：
master 的 `ExecutorInvocation` 定位器、CI、部署脚本都钉在它上面。当
task.rs 只剩 §2 的永久职责时，同一个 commit 改名 `flow-plugin-runtime`
（定位器 + 兄弟定位 + 文档一起），**先让代码配得上名字，再改名字**。

## 8. 落地顺序

| 步骤 | 内容 | 验证 |
| --- | --- | --- |
| R1 | manifest 解析器 + runtime 内插件注册表（node_types 路由表） | 单测：解析/拒绝/内置名冲突 |
| R2 | Harness 节点改经注册表路由到既有 agent_plugin 适配层（行为零变化，适配层降级为路由目标） | 4 个 agent e2e 套件 + backend-e2e 全量 |
| R3 | manifest config 试点：`AgentRunConfig` 改由 manifest `[config]` 声明 + run 快照冻结（替换 `install_harness_agent` 手搓 OnceCell） | 同上 + 快照冻结断言 |
| R4 | **Llm 节点插件化**（第一个真正的掏空迁移：provider 运行时外迁） | llm 相关 e2e + 新插件套件 |
| R5 | Email / HttpCall 按需跟进；task.rs 拆分收尾 + 改名 | 全量回归 |

R4 之前不碰内置节点：R1–R3 全部是「把已验证的 agent 路径一般化」，
每步 e2e 兜底；R4 起才是真正的掏空，一次迁一种类型。

## 9. 兼容性（同 wire.md 纪律）

- manifest 加字段 = 兼容（忽略+日志）；`node_types` 语义变化 = runtime
  主版本；
- runtime↔插件 wire 变化 = plugin-proto 主版本 + 全量新向量；
- v7 层零变化（master 无感）；
- 插件消失/禁用：对应节点类型在发布工作流时校验拒绝（引用了不存在
  的插件类型 = 发布失败，不静默换内置）。

## 10. remote 模式：插件系统的部署拓扑题（不是插件的设计题）

**核心结论先说：插件不需要为 remote 设计任何东西。** 插件和它的
manifest、协议、沙箱、生命周期在单机与远程完全一致——它只跟**本地
runtime** 说 plugin-proto。一个插件如果需要知道自己「跑在远端」，
分层就漏了。remote 带来的全部新问题都在宿主侧，五件事：

### R-1 部署单元：插件装在 agent 机上，操作者安装

插件是 agent 机器部署物的一部分（与 flow-agent、flow-executor 同级），
由运维方安装升级——与今天部署 agent 与执行器的方式一致（agent 本来
就是「运维方带来的受信机器」）。**master 不向 agent 分发插件二进制**：
那是插件分发/签名/原子安装/回滚的完整子系统，且「master 推代码到
agent」按设计就是远程代码执行——需要签名体系支撑，挂账为独立产品
能力，不进 runtime 协议。跨平台（不同 agent 机 OS/架构）是**打包**
问题：v1 约定「包即平台」，manifest 可选平台条目留到真实异构集群
出现再规格。

### R-2 能力上报：AgentHello 携带插件清单

agent 上联握手的能力报告（`capability` 目录已有扩展位）增加
`plugin_manifests`：本机插件的 id / version / node_types / **UI 投影**
（palette + params schema + summary_keys，几 KB/插件）。时机：上联
握手时 + 本机插件集合变更时重报；agent 重连（boot_id 变化）全量重报。
master 维护 **fleet registry**：按 agent 的活视图（仅在线 agent），
重连重建，不做持久化「最后已知」——离线 agent 本来就不可派发，
palette 显示离线机历史是元数据债。

### R-3 两级校验：master 尽力，边缘权威

异构集群里同一插件可以存在多版本，publish 时不知道 run 会落在哪台。
解法不是猜，是把权威放在事实所在地：

- **master 侧**（UX 级）：按 fleet registry 里最新 schema 做发布校验
  与早期拒绝——尽力而为，防呆不防错；
- **runtime 侧**（权威级）：派发落地后，runtime 按本地 manifest 校验
  参数，不匹配 = Business 错误，**点名插件 id+本机版本**，上抛回 master
  呈现。

这同时解决版本漂移：master 的 schema 可以滞后，边缘永远是真相。
（与 UI 文档 §1 的「类型级前移、语义在 dispatch」是同一条口径的两端。）

### R-4 路由约束：节点类型 → 持有插件的 agent 集合

派发准入在现有容量过滤上加插件能力过滤：工作流节点类型必须在目标
agent 的上报清单里（内置类型 = 全体 agent，随 runtime 二进制分发）。
AgentManager 的准入谓词从「有槽位」变为「有槽位且有插件」。无可用
agent = 明确排队/失败原因，不静默降级。

### R-5 配置与秘密：快照随派发，秘密不落 agent

配置：run 快照冻结（与单机同一条纪律），随 v7 派发经 agent 中继到
runtime，initialize 下发插件——`execution_snapshot` 携带 agent 配置
的既有路径直接推广，无新通道。秘密：master 解析（FLOW_SECRET_*），
经 OperationPermit 的内存字段过中继，runtime→插件走 initialize 参数，
agent 机上零秘密落盘。

### R-6 历史自持：冻结 UI 投影的价值兑现

remote 让 UI 文档 §4 的修正从「防呆」升为「必需」：agent 机可以消失、
插件可以卸载、集群可以重组——历史 run 的详情必须从 journal 的快照
投影自持渲染，与今天集群状态无关。单机下这是防御，remote 下这是
正确性。

### R-7 不做的事

- 插件分发/签名/自动升级（R-1 挂账）；
- per-workflow 插件版本钉死（可复现性诉求出现再设计，代价是版本矩阵
  与升级永久受限）；
- 插件感知拓扑（任何「我是不是在远端」的 API 都是抽象泄漏）；
- offline agent 的清单持久化（palette 只反映活视图）。
