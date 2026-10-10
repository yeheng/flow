# SQLite v1 → v2：切换方案（不做数据迁移）——已完成

> 终态（2026-10-10）：**v1 后端已删除**。缺省后端 = journal（JSONL v2
> 权威）；`FLOW_BACKEND=sqlite` 显式报错并指向本文。历史数据舍弃、不迁移；
> 想保全 v1 数据的用户走 `flow-journal-dev import-legacy` 只读保全。
> 本文保留为决策记录与行为差异清单。
>
> 语义参考：v1 引擎行为（Postgres 臂仍共用）见 [DESIGN.md](DESIGN.md)；
> v2 行为契约见 [JSONL_DEVELOPMENT.md](JSONL_DEVELOPMENT.md)。

## 0. 结论

- 产品服务只使用 **Journal**（JSONL v2 唯一权威）。旧 `flow-server` 与
  V1 RPC 注册层已删除；产品入口统一为 `flow-journal-server`。
- PostgreSQL 保留为库与测试，不再提供旧 PG 产品服务。backend-e2e 为 Journal
  单臂；`flow-pg` 继续保留数据库契约及容器清理测试。
- 历史数据不迁移：journal 只接受全新数据目录；v1 布局目录（有 flow.db）
  在 journal 模式下拒绝启动（防混用护栏）。

## 1. 已实现：v2 接入 v1 公共契约（M1 完成）

### 1.1 服务与执行装配

- `flow-journal-server`、backend-e2e 和 backend-perf 共用
  `flow_rpc::serve_journal_product`。桌面使用同一 V2 产品模块，复用
  `flow_backend::start_execution` 选择进程内、IPC 或远程执行模式。
- Journal 根使用 `[journal].data_dir` / `FLOW_JOURNAL_DATA_DIR`；密钥目录使用
  `[storage].data_dir` / `FLOW_DATA_DIR`。初次创建的 Journal 根必须为空。
- WS/HTTP 缺省为 9802/9803，强制 loopback。部署必须设置至少 32 字节的
  `FLOW_JOURNAL_TOKEN`；浏览器工作台在设置页可配置令牌。
- `journal_arm` 保留物化读面；`flow-store` 的 SQLite 投影可重建，均仍被 V2
  使用。`flow-engine` 内共享模型与执行能力、PG 库引用的引擎也保留。

### 1.2 RPC 接入约定

| 面 | V2 产品契约 |
|---|---|
| workflow 写命令 | `request_id` 幂等与原生回执；相同定义复用版本号；已有 run 原子拒删 |
| 原生读面 | `workflow.get/list`、`run.get/list`、事件/审计游标分页 |
| 主工作台与普通 CLI 读面 | `.get.full` / `.list.full`、`run.events.full`、`run.timeline/stats`、`workflow.versions` |
| run.cancel / signal / adjudicate | 原生提交回执；未决副作用可通过 adjudicate 或 signal 的 `payload.action` 裁决 |
| 订阅 | 指定 run 回放并追尾；`event_format=v2` 或物化 `envelope`，保留调用方显式选择 |
| schedule / webhook / template CRUD | 调用方身份贯穿权威命令；同键同参返回原回执，同键异参冲突，支持 command.status |
| Webhook | `POST /hooks/<key>`，Bearer 与稳定 `Idempotency-Key` 必填；配置检查、创建 run 与去重同提交 |
| 配置、密钥与节点类型 | `config.*`、`secrets.*`、`nodetypes.list` 已接入产品模块 |
| 观测 | 独立 `run.observations.page`，浏览器 `/journal` 展示正文与级别 |

所有 RPC 请求带 `_token`；写命令带稳定 `request_id`。`-32020` 表示提交已发生但
投影未可见，应查询原命令 `command.status`。原生 Journal 客户端保留完整回执；
主工作台与普通 CLI 解包已提交回执的 `result`。

### 1.3 v2 侧新增能力（为对齐 v1 裁决面）

- **裁决决策扩为三种**（`run.adjudicate` 增加 `decision` 参数，
  journal_v2 RPC 兼容缺省 `accept_output`）：
  - `accept_output`：人工接受产出（原语义，不伪造外部响应被捕获）；
  - `retry`：人工显式授权重发——节点回 pending、清除未决操作，驱动器以
    attempt+1 重新派发（「绝不自动重发」禁令针对机器；人工裁决就是授权
    凭据）。重放再次失败/超时仍按 v2 语义进 uncertain；
  - `failed`：人工判定失败——节点终态 failed，run 由收尾判定写 RunFailed。
- **折影保留 skip reason**（`Node.skip_reason`，serde default 兼容旧快照）：
  upstream_failed / upstream_skipped / branch_not_taken 与 v1 同词。

### 1.4 测试安全网

- `flow-backend/tests`：权威命令、Journal 恢复、投影、IPC/远程执行与安全边界。
- `flow-rpc/tests/journal_v2.rs`：RPC 删除保护、原生回执、CRUD 幂等与投影延迟。
- `flow-rpc/tests/ws_journal.rs` / `sigkill_recovery.rs`：真实 V2 进程、完整产品面、
  历史目录保护与 SIGKILL 恢复。
- backend-e2e：真实 Journal 产品进程；CLI 测试真实客户端子进程；Playwright
  构建新 V2 二进制并在隔离目录验证主工作台、原生 Journal 工作区与设置。

## 2. 阶段记录

1. ~~**默认切换**~~（2026-10-09）：`StorageConfig::default().backend = Journal`；
   v1 布局目录（有 flow.db、无 journal/ 段目录）在 journal 模式下拒绝启动
   （防混用护栏，验收测试 `ws_journal::default_backend_is_journal_on_fresh_directory`）。
2. ~~**删除 v1**~~（2026-10-09，同日完成）：`sqlite.rs`、flow-store v1 Store、`AnyBackend::Sqlite` 臂、
   v1 调度器（scheduler.rs）、schedule_fires；`FLOW_BACKEND=sqlite` 报错并
   指向本文。DESIGN.md §3 的权威描述改为 journal。
3. **产品调用方收尾**（2026-10-10）：V2 认证、幂等、回执、物化读面、桌面执行模式、
   CLI、E2E、CI 和打包入口全部迁移；原始问题与验收见仓库根 `REVIEW.md`。

## 3. 已知行为差异（journal 臂 vs v1，切换日变更面）

| 面 | v1 | journal 臂 | 说明 |
|---|---|---|---|
| 事件时间戳 | 真实墙钟 | `ts` = Unix epoch | journal 无墙钟（权威时间是 LSN/run_seq）；前端时间列显示 1970/空 |
| run ended_at / 节点耗时 | 真实值 | `null` / 0 | 同上；started_at（created_at）保留 |
| 事件 seq | 严格连续 1..N | 严格递增、允许空洞 | v2 权威序号含不映射到 v1 面的事实（WaitRegistered 等）；前端以 seq 为水位，空洞无害 |
| node_log 事件 | 走事件流 | 不发射 | v2 观测走 ObservationStore（run.observations.page）；主工作台旧控制台为空；`/journal` 可读独立观测 |
| http 等外部操作失败 | retryable 自动重试 | 一律 uncertain 等裁决 | v2 核心安全立场，**人工授权 retry 后重放再超时仍回 uncertain**（e2e 已按此断言） |
| 子 run id | 确定性派生 | uuid v7 + parent 关系 | 时间线 child_run_id 经 parent 反查保持可用 |
| run.start 幂等 | 无（双击 = 双 run） | 服务端 request_id 幂等 | 产品写调用保留显式身份，缺省自动生成键 |
| workflow.update 相同定义 | 复用版本号 | **复用版本号（已对齐）** | checksum 相同即复用最新版本 |

### 已澄清（曾有「挂起」误报，回归钉死）

- **pre-auth SIGKILL 窗口**：SIGKILL 落在 DispatchStarted 与
  OperationAuthorized 之间时，重启**安全地重新派发**（未授权 = 请求从未
  发出，重发无副作用风险）；重放请求按节点 timeout_ms 计超时后进
  uncertain。此前的「任务静默挂起」是误报：http 默认 30s 超时大于测试的
  20s 等待窗口。回归：`flow-rpc/tests/sigkill_recovery.rs`（pre-auth 杀 +
  短超时重放 + 人工裁决 failed 落终态，全链路）。e2e 的 journal 臂在杀前
  等 500ms 授权落账，对齐用例本意「请求已发出后杀」。

## 4. 运维入口

部署、执行模式、离线维护与故障处置的完整手册见 [OPS.md](OPS.md)；本节只记
切换日特有的事实：

```sh
export FLOW_JOURNAL_TOKEN="<至少32字节的部署令牌>"
export FLOW_JOURNAL_DATA_DIR=/var/lib/flow-v2/journal
export FLOW_DATA_DIR=/var/lib/flow-v2/secrets
flow-journal-server

# 打包：dist/journal/bin/ 内包含服务端、执行器、agent、CLI 与离线工具
scripts/build-journal.sh
./dist/journal/run-server.sh
```

`build-sqlite.sh` 转发到 Journal 打包入口；`build-pg.sh` 明确提示旧 PG 产品已退役。


- v1 布局目录（有 `flow.db`、无 `journal/`）在 journal 模式下**拒绝启动**。
- 想保全历史 v1 数据的用户：停写后用 `flow-journal-dev import-legacy` 做
  只读保全（[JSONL_DEVELOPMENT.md](JSONL_DEVELOPMENT.md)、[OPS.md](OPS.md) §4），
  但该基线与 journal 后端的日常服务面互相独立——切换不依赖它。
