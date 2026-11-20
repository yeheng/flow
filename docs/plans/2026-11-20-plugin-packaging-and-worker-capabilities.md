# Flow 插件打包、加载与远程 Worker 能力编排

**状态**　设计提案，待实现。基线为 `v2` 的 `1db8759`。第 1 节记录现状，第 2 节起给出目标契约。新增命令、目录和消息名供实现与评审使用。

**决策**　Worker 应向主控报告它实际能执行的节点和协议。FSM 决定哪个节点可运行，托管调度器决定交给哪一个 Worker，主控策略决定此次操作能否获得许可。插件作者交付普通 ZIP，运维人员决定包能部署到哪些 Agent。四种判断各有一个归属。

## 1. 当前可以依赖什么

| 路径 | 已有行为 | 本提案填补的缺口 |
| --- | --- | --- |
| 核心与 FSM | 独立 Dispatcher 按 `job:<handler>` 通过 IPC 派单。内存 FSM 可以只带 Script Worker 运行 | 保持这两个入口独立。产品侧在调用派发端口前完成插件选择和远程放置 |
| 安装与加载 | 管理员从主控可读目录安装。宿主封存、校验、记录回执、发布到 `plugins/<id>/`。Worker 在启动时建注册表，常驻服务显式启动 | 增加 ZIP 导入，给独立磁盘上的 Agent 同一套安装规则 |
| 内容身份 | Run、Worker 路由和授予使用 `content_hash`，覆盖 manifest 与入口。发布另有覆盖整个目录的 `bundle_digest` | 新 Run 使用整包目录摘要，覆盖库、脚本、模型和 UI 等随包内容 |
| 远程执行 | Agent 探测真实 Worker，v8 握手和容量报告携带构建画像、槽位、可选插件清单及发布代际。有清单时按节点类型、插件 ID 和内容哈希匹配 | 需要精确表达内置处理器、执行协议、包摘要、报告时效及目标节点范围 |
| 发布与就绪 | `deploy --source ... --agent A` 安装到共享目录后等待 A 报告。未指定 Agent 时返回 `published` | 独立目录需要持久部署目标、制品下载和逐节点确认。现有 `--agent` 继续表示等待名单 |

代码入口见 [独立执行与分层](../design/01-architecture.md)、[安装运行手册](../features/resident-plugin-services.md)、[现有部署命令](../../crates/flow-cli/src/journal.rs)、[v8 插件清单](../../crates/flow-engine/src/plugin_inventory.rs) 和 [远程预留](../../crates/flow-backend/src/execution/remote.rs)。

## 2. 插件目录与 ZIP

插件默认安装在 `~/.flow/plugins/<id>/`。用 `FLOW_PLUGINS_DIR=/srv/flow/plugins` 可直接指定插件根目录。只设置 `FLOW_HOME` 时，插件使用 `$FLOW_HOME/plugins/`。路径以启动 Flow 的服务账号为准，每台 Agent 可配置自己的插件根目录。[插件根路径解析](../../crates/flow-config/src/plugin_fs.rs)

```text
~/.flow/plugins/
└── ocr-plugin/
    ├── plugin.toml
    ├── bin/
    │   └── flow-ocr
    ├── web/
    │   └── index.html
    ├── lib/                  # 可选依赖库
    └── assets/               # 可选模型、字典等资源
```

`plugin.toml` 是唯一的包清单。入口和 Web UI 都填相对于插件目录的路径。示例插件提供一个 OCR 节点，并可选提供一个带 Web UI 的常驻服务。单独提供节点时可以省略 `[service]` 和 `web/`。当前 Web UI 宿主读取单个自包含 HTML，通过已有隔离页面桥访问常驻服务。[清单解析器](../../crates/flow-config/src/plugin_manifest.rs) · [常驻服务与 Web UI](../features/resident-plugin-services.md)

```toml
[plugin]
id = "ocr-plugin"
name = "OCR"
version = "1.4.0"
entry = "bin/flow-ocr"
protocol = 2
node_types = ["ocr_text"]

[methods]
invoke = "ocr/extract"

[service]
methods = ["status"]
web_ui = "web/index.html"

[permissions]
schema = 1
required_features = ["master-permit-v1"]
capabilities = []
```

### 2.1 打包与安装

用系统压缩工具或 `zip` 命令压缩插件目录。ZIP 根可直接包含 `plugin.toml`，也可以包含一层 `ocr-plugin/`。安装目录名始终取自 `plugin.id`。ZIP 文件名可附版本和平台，Flow 从清单读取插件身份。一份原生插件 ZIP 装入面向目标平台的二进制和依赖，Windows 的入口可写为 `bin/flow-ocr.exe`。以下打包命令在 `ocr-plugin/` 的父目录执行。

```sh
zip -r ocr-plugin-1.4.0-linux-amd64.zip ocr-plugin/
flow-cli journal plugins install --source /srv/flow-import/ocr-plugin-1.4.0-linux-amd64.zip
```

`install --source` 继续接收主控可读的目录路径，提案增加对 `.zip` 文件的支持。桌面或远程 CLI 的路径以运行安装服务的机器为准。现有目录安装和开发期直接放置插件的方式可以继续使用。安装完成后可在插件列表检查版本、入口、授权状态和已报告的 Worker。

## 3. 安装、加载与版本切换

### 3.1 安装与内容身份

1. 宿主在同文件系统的临时目录解压，找到包根唯一的 `plugin.toml`。忽略系统压缩工具生成的 `__MACOSX/` 和 `.DS_Store`。沿用现有大小和文件数限额，按实际解压字节计数，拒绝越界路径、链接、重名和安装器保留的元数据文件。验证 `entry` 及显式声明的 `web_ui` 指向包内普通文件，Unix 上为入口补可执行位。
2. 校验节点归属后，复用现有安装回执、发布锁和替换流程，将目录发布到 `<插件根>/<plugin.id>/`。校验失败保留已有版本。安装只准备文件，插件授权和运行仍由各自入口处理。
3. 新绑定使用整包目录摘要 `bundle_digest`，覆盖包内路径、执行标记和文件内容。实现复用现有整包摘要逻辑，ZIP 导入用一致的相对路径与权限规则生成安装目录。主控与 Agent 对同一目标平台按相同规则计算摘要。只改变 ZIP 压缩等级、文件排列或时间戳时，相同的安装树对应同一摘要。改动入口、库或 `web/index.html` 会得到新摘要。目录来源可用同一套目录摘要校验。历史 `content_hash` 绑定按原格式读取。

现有 `.ops`、staging 和发布代际标记继续作为安装器内部状态。Worker 直接从插件根扫描插件目录，以清单和入口建立注册表。用户可用普通文件工具检查已安装插件。[安装回执与恢复](../../crates/flow-backend/src/plugin_operation.rs) · [现有目录摘要](../../crates/flow-backend/src/plugin_bundle.rs)

### 3.2 从目录加载到执行

| 动作 | 时机和结果 |
| --- | --- |
| 扫描并注册 | Worker 启动或随发布代际轮换时，读取 `plugin.toml`，验证节点归属、入口及当前包身份。Agent 只有探测到实际 Worker 加载后才报告能力 |
| 执行节点 | 主控在 Run 接纳时冻结 `node_type → plugin_id、bundle_digest`，覆盖静态子流程依赖。Worker 绑定时核对精确摘要，主控在副作用前按现有流程决定是否发 Permit。插件子进程经 SDK 握手后调用 `methods.invoke`，按任务回收 |
| 启动常驻服务 | 管理员显式调用 `services.start`，宿主从 `plugins/<id>/` 启动进程并为实例读取 `web/index.html`。服务持有当前包的引用，停止后回收。纯服务包只需安装在服务宿主 |
| 确认运行 | `reported` 表示真实 Worker 已加载并通告清单。要验证包内 OCR 引擎能运行，可启动一条获准的样例 Run 检查握手与输出 |

### 3.3 升级、卸载与回滚

同一插件根内，一个插件 ID 对应一个已安装版本。替换用原安装入口加 `--replace`。有活动 Run 或未来子流程引用时先排空。有常驻实例时先停服务，再替换对应目录。安装操作列出具体阻塞项。排空后发布新目录并轮换 Worker，新 Run 按新摘要匹配。卸载用现有 `plugins uninstall`。管理员保留上一版 ZIP，需要回滚时用该 ZIP 重新替换，并核对原摘要对应的授予。部署到多个 Agent 时，对旧 ZIP 重新发布会生成新的部署代际。

## 4. Worker 需要上报哪些能力

**需要上报，而且上报单位是可共用同一组槽位的真实 Worker 构建。** 首版一个 Agent 配一种 Worker 启动配置，所有槽位共享一张能力表。若同机有两种专用 Worker，用两个受管 Agent 身份、各自的槽位预算分别登记。把两组能力取并集再配一个总槽位，会把任务派给无法执行它的进程。以后若一个 Agent 管多个池，每池须有独立 `pool_id`、能力表和配额。

| 信息 | 权威来源 | 用途 |
| --- | --- | --- |
| Agent 身份、允许执行的环境、部署目标、槽位上限 | 主控认证和管理员配置 | 限制哪些机器可以参与候选，禁止 Agent 自报身份或环境权限 |
| 可执行节点、处理器版本、执行协议、可见插件摘要 | Agent 用生产启动配置，通过 IPC 探测实际 Worker。本地池走同一报告模型 | 帮主控判断“交给这个 Worker，它能否接单” |
| 平台、当前可供本池分配的总槽位、排空、探测结果 | Agent 会话报告，受主控配置上限约束 | 调度容量、诊断。槽位按池共用，同一槽位可跑多种节点 |
| Run 需求、环境绑定、插件授予、Permit | 主控从已发布定义、Run 快照与 Journal 计算 | 约束本次执行并决定凭据。能力报告中不出现密钥值、密钥清单、主控 token 或工作流授权 |

### 4.1 从节点得到可匹配的需求

映射在托管工作流层完成。外层将需求交给派发端口，通用 Dispatcher 仍只理解 handler、约束、槽位和事件。加一个 `ocr_text` 插件无需改 FSM、通用任务枚举或 Dispatcher 的 `match`。协议或需要主控理解的新副作用种类，仍须增加对应的宿主合同。

| 执行场景 | 匹配要求 |
| --- | --- |
| 独立 Script Worker | `job:script.v1` 加流式传输特性。只实现独立任务协议的 Worker 可供内存 FSM 使用 |
| 托管 `script`、`condition`、`http_call`、`email` 等 | `managed-node-v1`、对应的内置节点及语义版本。托管脚本若选用 prepared-job 优化，还须匹配现有 prepared 特性并保留原有托管执行路径 |
| `harness` 命令模式 | 内置 `harness_command` 处理器及所需托管协议 |
| `harness` 的 `mode=agent`、`llm` 或 `ocr_text` | 托管协议、插件宿主、精确节点类型、插件 ID、Run 冻结的包摘要及 Worker 平台，以及清单要求的协议特性 |
| 等待、恢复与纯服务 | 计时器唤醒、信号和结果重放按主控状态推进。需要 Worker 完成的节点准备步骤照常匹配其处理器。已有 outcome 的恢复路径只声明实际需要的回放处理器。纯服务的 RPC 方法不进可调度节点表 |

处理器 ID 标明执行契约。`script.v1` 任务能力和托管 `script@1` 能力可在同一 Worker 出现，各自来自对应握手与实现。`harness` 模式从当前已验证的节点字面量取得。[节点与模式映射](../../crates/flow-domain/src/model.rs) · [Worker 画像与协议常量](../../crates/flow-dispatcher/src/execution_protocol/contract.rs)

例如，`ocr_text` 的一次新执行由主控在内存中形成以下需求。包摘要从 Run 快照读取，目标机器由部署和环境共同限定。运行定义只引用 `ocr_text` 及其参数，用户无须把机器名、平台摘要写进画布。机密环境允许的目标由主控另行求交。

```json
{
  "node_type": "ocr_text",
  "execution": "managed_plugin",
  "plugin_id": "ocr-plugin",
  "required_features": ["managed-node-v1", "plugin-host-v1", "master-permit-v1"],
  "bundle_digest": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
}
```

### 4.2 报告契约与刷新

以下是 `WorkerOfferV1` 的逻辑投影，便于阅读。传输沿用有界 postcard 控制帧。示例中的 `worker-b` 由连接认证确定，消息只回显 boot 和当前会话标识。

```json
{
  "agent_boot_id": "boot-b7",
  "link_session_id": "link-42",
  "offer_seq": 7,
  "capacity": {"slots_total": 2, "draining": false},
  "target": "x86_64-unknown-linux-gnu",
  "protocol_features": ["managed-node-v1", "plugin-host-v1", "master-permit-v1"],
  "jobs": [],
  "builtins": [{"handler": "script", "revision": 1}],
  "plugins": [{
    "node_type": "ocr_text",
    "plugin_id": "ocr-plugin",
    "bundle_digest": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "deployment_generation": 19
  }]
}
```

- **首次与变化。** Agent 启动、重新连接、Worker 二进制或启动配置变化、激活、撤包、排空和容量变更都触发报告。注册表变化时先撤下受影响的路由，完成一次 Worker 握手探测后按一致快照重新上报。探测前后核对激活代际，变化就丢弃结果重读。Worker 报告它实际加载的处理器与包摘要，Agent 把结果与本机激活记录连接起来，不能只读 manifest 生成“可执行”结论。
- **兼容协商。** 在 v8 `AgentHello.capabilities` 声明 `worker-offer-v1`。新主控只对声明过的 Agent 在 `AgentWelcome` 后发新控制帧 `RequestWorkerOfferV1`。收到此请求才启用新报告和绑定帧。旧主控继续收旧 `CapacityReport`，老 Agent 继续现有调度。新帧在枚举末尾追加，保持已有 postcard 判别值和结构。Agent 与 Worker 的本机 IPC 同样先协商，再请求详细报告及绑定核对。老 Worker 的 v1 清单仅用于旧绑定。
- **新鲜度。** `offer_seq` 在同一连接内从 1 递增，内容变化时加一。完整报告替换上一份。相同序号及内容是幂等刷新，同序异内容触发重同步，旧序号丢弃。协商后每个心跳周期确认当前序号，主控用自身单调时钟判断过期。超过现有心跳超时即从新派发候选中移除。新连接清除旧报告，完成 Resume 对账和首次完整报告后开放路由。旧会话发来的消息只对旧会话有效。
- **边界与背压。** 首版每份完整报告最多 256 条处理器条目（`jobs + builtins + plugins`），编码后最多 96 KiB，须小于现有 128 KiB 控制帧。检查字符串长度、摘要、唯一键、范围，拒绝超限或部分清单，显示 `offer_invalid` 并停止该池接新任务。高频变化合并为最新完整快照。普通心跳仅回显序号。受支持的旧清单仍用现有 v8 限额，超限应明确诊断。
- **绑定复核。** 主控发 `BindExecutorV2`，携带 `dispatch_id`、执行器 ID、期望会话、选中的 `offer_seq` 和该节点的精确需求。Agent 对当前完整快照复核，启动或租用真实 Worker，并让这个进程确认所选处理器及包摘要。Ack 返回 `executor_boot_id` 和实际选中项。报告序号已更新但需求、部署代际和槽位仍匹配时可确认最新序号。其余变化返回确定性拒绝，主控收回预留并重新选址。业务载荷、密钥和 Permit 均在绑定确认后走原执行协议。

新 Agent 在处理 Resume 分页时也能接收 `RequestWorkerOfferV1`，先记下请求，完成对账后再答完整报告。协商新模式且没有在途派发时，以空的最终 Resume 页显式完成对账。旧主控没有请求时，沿用旧的空清单行为。连接实现要将新报告与 Bind 管理帧限制在认证控制通道。Worker 通过业务 `Routed` 信封发出的同名帧应被拒绝。[现有路由信封与控制帧分类](../../crates/flow-dispatcher/src/execution_protocol/remote.rs)

### 4.3 Dispatcher 如何选人

1. FSM 按图和依赖选出 ready 节点。托管层用 Run 快照生成需求，包括执行模式、处理器语义版本、必要特性、允许的环境和 Agent 范围，以及插件的冻结摘要。**实际候选范围取部署目标、环境允许节点和已认证在线节点的交集。** 本地执行可用相同匹配器，候选来源换成本地 IPC 池。
2. 排除断链、报告过期、未完成 Resume、排空或代际不符的 Agent。逐项验证协议、内置处理器或精确插件、平台、部署目标。主控在接纳新 Run 前核实该包摘要对应的授予，逐次副作用仍在 Permit 阶段裁决。仅有旧画像或只知道节点名字的 Agent 不承接要求整包身份的任务。
3. 在同一临界区计算 `可用槽 = min(主控分配上限, 上报总槽) − 已预留或已绑定的槽`，下限为零。每个 `dispatch_id` 只算一次。从合格候选中优先把普通任务放到仅有内置能力的池，给插件保留容量。同级按占用比例最低优先，最后用 Agent ID 稳定决胜。若节点等待审批，实际尚被持有的远程租约继续计入占用，直到收到释放或完成对账。Agent 收缩到零槽时只停止新派发，旧租约按原生命周期结束。
4. 主控先预留，持久提交 `DispatchStarted` 与选定身份，然后发送 Bind。提交失败归还预留。绑定被拒且尚未取得业务许可时，记录本次尝试的确定结果再重新排队。迟到的 Ack 由 dispatch、会话及 boot 身份过滤。**容量变化不能产生第二张 Permit。** 已授权且结果未知时沿用 Journal 的 `uncertain`／人工裁决或已有显式可重试合同。有已提交结果时只回放。
5. 合格节点都忙时维持 ready，等待容量事件。新部署已指定正确的包但所有目标暂离线或尚未激活时，显示 `waiting_for_deployment`，由部署或连接事件唤醒，采用在 Run 接纳时记录的等待截止时间，超时给出具体缺项。配置上没有允许的平台、处理器或部署目标时返回 `unschedulable`。权限拒绝独立显示 `permission_denied`。旧工作流沿用旧的无匹配失败行为，新等待语义随调度版本启用。

**例子。** A 有 4 个内置槽，支持 `script@1`、`http_call@1`。B 有 2 个槽，报告如上，且是获准执行生产 OCR 的节点。C 有相同平台、同一 OCR 包，但仅获准测试环境。生产 Run 的脚本可给 A。两个 OCR 可给 B。第三个 OCR 等 B 归还槽。C 即使空闲也不会拿到生产数据。B 收到新版包后，主控仍按在途 Run 的冻结摘要保护已有派发。如果 B 在 Bind 之后断线，主控先按原派发对账。Dispatcher 的选择过程可记录 `run/node/dispatch`、排除原因、选定 Agent、报告序号和摘要，以便用户回答“为何还没跑”。

## 5. 把同一个 ZIP 部署到多台 Agent

共享插件根的部署继续支持。使用独立磁盘时，主控收取管理员提交的 ZIP，记录插件 ID、期望 `bundle_digest`、目标 Agent 和部署代际。主控可按部署保留一份原 ZIP 供目标 Agent 下载。本地安装成功即得到 `plugins/<id>/`，独立磁盘同步作为远程部署能力启用。

沿用 `plugins deploy` 入口的设计示例。现有 `--agent` 仍表示等待名单。新增 `--target-agent` 才声明允许获得包、参与这次插件部署的 Agent。

```sh
flow-cli journal plugins deploy \
  --source /srv/flow-import/ocr-plugin-1.4.0-linux-amd64.zip \
  --target-agent worker-a --target-agent worker-b \
  --agent worker-a --agent worker-b --timeout 60
```

1. 主控封存 ZIP，保存部署目标与代际。以 `request_id` 重试时返回原部署结果。更换目标或内容时提交新代际。同一插件的一次部署指定一个 ZIP，可发给多台兼容机器。
2. Agent 查询自身的目标，从认证连接下载 ZIP，调用和本地一样的安装流程。先解包并核对目录摘要，再检查计划是否仍为最新代际，最后发布到该 Agent 的 `<插件根>/<id>/`。下载、校验或发布失败时保留本机旧包，展示对应错误。
3. Agent 探测真实 Worker 并上报精确摘要和代际。主控只把该 ZIP 对应的任务交给目标范围内、报告匹配且有空闲槽的 Worker。目标 Agent 离线时保留待部署状态，重连只应用最新代际。已绑定任务按原派发对账。
4. `plugins.deployment` 查看期望与实际摘要、就绪、离线、下载失败或平台不兼容。CLI 等待超时可继续查询。卸载时撤回部署目标并记录待清理节点，重连后完成目录卸载。

异构 Agent 可在同一调度池处理各自支持的内置节点。原生插件按目标平台分别提供普通 ZIP，由管理员明确选择兼容的 Agent。首版一个插件部署匹配一个 ZIP 摘要。遇到一个插件确需同时覆盖多个架构时，再按平台增加部署记录，包格式和安装路径沿用本节。

## 6. 兼容、分层验收与交付顺序

| 阶段 | 改动边界 | 合入验收 |
| --- | --- | --- |
| 1　固定底层合同 | `flow-dispatcher`、`flow-workflow` 保留现有入口。在托管层抽出“节点需求 × Worker 报告”的纯匹配函数，分离本地传输与插件目录依赖 | 去掉外层能跑 IPC 多 Worker 和内存 FSM。纯内置托管组合在没有 SDK、插件目录和 Agent 时完成 Run 并从 Journal 恢复。父子策略矩阵证明子流程不能扩大父上限 |
| 2　单机包与身份 | 普通 ZIP、已有清单解析器和安装器。Run、授予及派发使用带身份类型的整包摘要 | 普通 ZIP 的两种目录布局都能安装到 `plugins/<id>/`。自定义根目录可用。改变压缩方式保持目录摘要，修改 `lib/` 内容则改变摘要。ZIP 导入失败保留旧包，崩溃后用原请求 ID 恢复 |
| 3　真实能力上报与编排 | 本地池、Agent 与托管调度接入同一需求模型。扩展握手、报告和 Bind。先用预置包验证，保持纯内置和旧协议通路 | A/B/C 例子按预期派单。内置和插件混跑不超配。旧序号、目录变化和 Bind 竞态不产生误派。断线与主控重启不重复有副作用的调用 |
| 4　独立磁盘发布 | 持久目标集、认证下载、逐机状态、离线赶上及回滚。保留当前 `deploy --source --agent` 的共享目录语义 | 两台独立目录，B 离线时发布，重连后只激活最新目标。下载中断、校验失败、磁盘满都不报告新摘要。卸载显示阻塞 Run 与服务。`--agent` 等待名单不能被误当成部署授权 |

**迁移顺序。** 先部署能同时读取旧 `content_hash` 与新 `bundle_digest` 的主控，再滚动升级 Agent 和 Worker，最后允许创建新身份的 Run。旧快照、旧授予和旧 `.ops` 按原格式读取。新旧绑定显式带身份种类。旧授予不会静默覆盖新摘要，管理员确认后才能转换。新操作、回执、执行身份使用版本化记录，下线新功能时先停止创建新运行并排空新绑定，旧二进制若不识别存储版本须在启动时拒绝写入。插件回滚与主控程序降级各走自己的步骤。

实现时以 [独立 Dispatcher 测试](../../crates/flow-dispatcher/tests/standalone.rs)、[内存工作流测试](../../crates/flow-test/e2e/tests/workflow_memory.rs)、[插件安装回归](../../crates/flow-backend/tests/plugin_lifecycle.rs)、[子流程排空](../../crates/flow-backend/tests/plugin_run_drain.rs)、[精确远程路由](../../crates/flow-test/e2e-remote/tests/journal_remote_wss.rs) 和 [双 Agent 轮换](../../crates/flow-test/e2e-remote/tests/plugin_deploy_rotation.rs) 为基线。后两项目前覆盖同机多进程。第 4 阶段另需独立文件系统上的多节点验证。
