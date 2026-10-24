# flow 工作流引擎设计文档（索引）

> 状态：**当前工作区代码的实际语义**（2026-10-24 校订）。本文件是设计文档的**索引与总纲**，
> 详细设计按章节拆分在 [`design/`](design/) 下；各分篇保留原 § 编号，跨篇引用按
> 「文档地图」的覆盖章节回查即可。本套文档不保留历史设计记录；被删除的 v1
> （SQLite/Postgres 双后端、Driver/Engine/fold 事件面、`run_tail`、NodeSlot 状态机）
> 相关内容已不在其中。
>
> 本地持久化采用 [SQLite 数据结构与提交协议](adr/0002-sqlite-persistence.md)。
> `flow.sqlite` 同事务保存接受事实、当前状态、回执和待处理动作；SQLite v3 可离线复制升级到 v4。
> 计算扩容不提供 master 高可用。
>
> **部署与运维见 [OPS.md](OPS.md)**（部署形态、执行模式、启动重启终止、离线维护、
> 升级、故障处置、容量预算与安全边界）；设计文档只描述语义与结构。
>
> 相关文档：`docs/observability-design.md`（节点可观察性）、
> `docs/opentelemetry-design.md`（已定未实现）、
> `docs/IPC_PROTOCOL_BASELINE.md`（执行协议基线——**写于 v4，未随 v7 更新**，
> 其中的帧格式与 `PROTOCOL_VERSION=4` 已过时；权威以 `contract.rs`/`message.rs`
> 为准）。

新版定义语义、固定依赖/执行上下文、业务调用、LLM、regression、证据包和格式升级的完整合同见 [EXECUTION.md](EXECUTION.md)。

## 0. 核心设计抉择

这一节是全套文档的根。后面每一条规则都从这里推导出来；读分篇前先读这里。

### 0.1 七个抉择与它们的目的

| # | 抉择 | 目的（不这样做的代价） |
| --- | --- | --- |
| **C1** | **SQLite 是唯一业务权威，事实与当前状态同事务提交** | 减少应用自行维护的分段、索引追赶、快照与恢复协议；外部副作用仍由授权、未决状态和人工裁决约束。数据库事务不等于外部操作恰好一次。 |
| **C2** | **节点执行语义只存在于 `flow-executor` 子进程；master 只做裁决与调度** | 工作流定义事实上是可执行代码（任意 JS + 任意出站 HTTP）。若在主进程内执行，一个失控脚本就能拖垮权威写者、让整个 journal 不可用。进程隔离把「用户代码」与「权威状态」分成两个故障域。进程内执行路径已全平台删除（`ExecutionMode` 只剩 `Ipc`）。 |
| **C3** | **外部副作用发生前，意图必须先落盘**（`OperationIntent` + `OperationAuthorized` 同一事务） | 崩溃窗口的本质困难是「请求发出去了吗？」。只有把「我要发这个请求」变成 journal 事实，恢复期才能区分「从未发出」（可安全重试）与「已授权、结果未知」（必须人工裁决）。没有这个事实，任何自动重试都是赌博。 |
| **C4** | **未知外部结果默认等人工裁决；显式 repeatable 节点可获持久重试授权** | 自动重试会把「至少一次」悄悄变成「可能多次」，而重复扣款/重复发信比失败更难收拾。系统选择**响亮地停下来**（run 进 `awaiting_resume`）而不是安静地猜。人工 `run.adjudicate` 是默认授权路径；`effect_policy=repeatable` 还须提交 `OperationRetryAuthorized` 并受尝试次数限制，见 [执行合同](EXECUTION.md)。 |
| **C5** | **单写者 + 数据目录排他 flock；计算容量可横向扩展，权威与提交点只有一份** | 分布式共识（Raft/多主复制）的工程与运维成本极高，而当前产品面的需求是「单机并发 + 计算扩容」。flock 让「双写者」在操作系统层不可能；`flow-agent` 只扩算力，**不提供 master 高可用**——这一点在代码与文档里都明确，不宣称 HA。 |
| **C6** | **控制帧只携带身份与元数据，用户值字节一律走 data 语义帧分块传输** | 若值能进控制帧，一个 200 MiB 的节点输出就能把会话帧撑爆（协议层只能杀会话）。v2 起 `ExecuteTask` 里的 `input`/`predecessors`/`params` 只是**声明**（`TaskValue::{Ref, Local}`），字节由 `TransferChunk` 按 256 KiB 分块送。控制帧因此有确定性上界（128 KiB）。 |
| **C7** | **每个 RPC 调用都要 token，但 token 是单用户凭据、不是多租户授权** | 系统没有用户体系，做成「多租户授权」会是假承诺。缺省只监听 loopback，对外暴露必须在外层自备 TLS + 认证。这个边界必须写在最前面，否则运维会误以为拿到 token 的不同人是不同租户。 |

### 0.2 由抉择推导出的三条铁律

1. **没有事实就没有决策**：协调器的每一次状态迁移都必须落到一条 journal 事件上
   （`journal_driver::advance_run` → `backend.internal(...)`）。内存里的推断只是
   缓存，崩溃后以 SQLite 已提交状态为准。
2. **不知道就不动**：`uncertain` 是唯一允许的「停下来」状态，它属于**平台故障面**
   （IO/断链/崩溃），不属于工作流失败面。二者绝不互相冒充（§13.3）。
3. **同一提交决定**：事实、当前状态、永久回执与后续动作一起提交到 `flow.sqlite`。
   该库及其活跃 WAL 不可作为缓存删除。`observations/` 独立且可丢弃，不参与业务裁决。

---

## 文档地图

| 分篇 | 覆盖章节 | 内容 |
| --- | --- | --- |
| [01 · 定位与总体架构](design/01-architecture.md) | §1–§2 | 定位与非目标；crate/二进制/依赖方向；三链路架构图；执行模式矩阵 |
| [02 · 权威数据模型](design/02-data-model.md) | §3 | 事务/事件/LSN、领域事件目录、State 折叠规则、写序协议与组提交、磁盘布局与索引 |
| [03 · 单机执行](design/03-single-node-execution.md) | §4–§6 | run 生命周期与数据流、dirty-run 调度、单机时序图、run/节点/等待/协调循环状态机 |
| [04 · 远程执行](design/04-remote-execution.md) | §7 | remote 架构与准入、agent 注册、远程派发、断线 Resume 对账、drain、控制平面范围 |
| [05 · 定义与执行语义](design/05-definitions-and-semantics.md) | §8–§10 | 节点类型与校验、汇合/收口/重试/输入语义、QuickJS 沙箱与模板展开 |
| [06 · RPC 层](design/06-rpc.md) | §11 | 方法面与准入、request_id 幂等、run.subscribe 订阅、错误码、cron/webhook 触发器 |
| [07 · 崩溃恢复与测试](design/07-testing-and-recovery.md) | §12、§14 | 启动序列、故障处置、观测日志边界、故障注入、测试策略 |
| [08 · 不变量与已知限制](design/08-invariants-and-limits.md) | §13、§15 | 不变量清单（验收标准，含执行协议线路 §13.6）、已知限制与未做 |

阅读顺序建议：§0 → 01（架构）→ 02（数据模型）→ 05（定义与语义）→ 03（单机执行）
→ 04（远程执行）→ 06（RPC）→ 07（恢复与测试）→ 08（不变量）。

> 分篇之间的交叉引用保留原 `§x.y` 编号；在分篇内看到 `§13.6` 之类引用时，
> 按上表的覆盖章节定位对应分篇。

## 16. 快速索引：从「我想改 X」到该读哪里

| 想改的东西 | 该读代码 | 背景章节 |
| --- | --- | --- |
| 加一种节点类型 | `flow-engine/src/model.rs` 的 `NODE_TYPE_TABLE` + `descriptor`；`flow-executor/src/task.rs` 的执行分支 | [05](design/05-definitions-and-semantics.md) |
| 改 run 状态语义 | `flow-engine/src/journal_state.rs` 的 `State::apply` | [02](design/02-data-model.md) |
| 改调度/汇合规则 | `flow-backend/src/journal_driver.rs` 的 `incoming_gate` + `advance_run` | [03](design/03-single-node-execution.md) |
| 改写盘行为/组提交 | `flow-journal/src/writer.rs` | [02](design/02-data-model.md) |
| 改协议消息 | `flow-engine/src/execution_protocol/{contract,message}.rs`（改 `contract.rs` 即需 bump `PROTOCOL_VERSION`） | [08](design/08-invariants-and-limits.md)（§13.6） |
| 加 RPC 方法 | `flow-rpc/src/journal_v2.rs` 的 `READ_METHODS`/`WRITE_METHODS` + 对应 handler | [06](design/06-rpc.md) |
| 改触发器 | `flow-rpc/src/journal_triggers.rs` | [06](design/06-rpc.md) |
| 改 agent 中继 | `flow-agent/src/{runtime,relay,pool}.rs` | [04](design/04-remote-execution.md) |
| 离线维护 | `flow-journal/src/bin/flow-journal-tool.rs`（`verify`/`rebuild-index`/`backup`）；`flow-journal-dev check-projection`（事实重放 diff） | [07](design/07-testing-and-recovery.md) |
