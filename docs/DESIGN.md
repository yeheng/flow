# flow 工作流引擎设计方案

> 状态（2026-10-10 起）：**journal（JSONL v2）是唯一后端**。v1 执行面与双后端
> 已整体删除——`flow-pg`、`flow-backend` 的 `AnyBackend`/`pg`/`journal_arm`/
> `run_tail`、`flow-engine` 的 Driver/Engine/fold 事件面、`flow-server` 二进制
> 均不存在了；执行驱动是 `journal_driver`，产品读面是 `journal_views`。
> `cargo test --workspace --all-targets --locked` 全绿。验证命令与覆盖见 §13。
> 本文 §2 / §9.4 / §13 已按现状改写；§3–§8、§12 中描述 Driver / fold /
> run_tail / Postgres 集群语义的段落**保留为历史设计记录**（V2 语义继承自其中
> 钉死的不变量，例如 §6.6 的取消与 fatal 副作用边界），阅读时以 §2 的现状
> 清单为准。
>
> **部署 / 运维见 [OPS.md](OPS.md)**（部署形态、执行模式、离线维护、升级、
> 故障处置、容量预算与安全边界）；本文只描述语义，不重复操作步骤。
>
> **容量定位（部署选型）**：journal 定位是**单机并发**——单写者 + fsync 崩溃边界，
> backend-perf 实测 ~25 runs/s（16 在飞，e2e p95 < 1s）。更高并发需求在当前
> 产品面没有替代后端（v1 的 Postgres 多节点后端已删除，见 §14）。
>
> **⚠️ 安全边界（部署前必读）**：v2 协议**每个调用都要 token**
> （`FLOW_JOURNAL_TOKEN`，≥32 字节，含订阅与分页读），但 token 是**单用户
> 凭据，不是多租户授权**——拿到 token 的任何人都是管理员。默认只监听
> loopback；对外暴露必须在外层自备 TLS + 认证（反向代理 / 内网 ACL）。
> `http_call` 节点是**任意出站 HTTP**（可打内网地址与云元数据端点
> 169.254.169.254），`script`/`condition` 是沙箱内任意 JS——工作流定义事实上是
> 可执行代码，只允许可信用户创建与发布。出站白名单/沙箱网络隔离未实现。
> 完整运维边界（journal 内含业务原文、备份介质加密）见 [OPS.md](OPS.md) §8。

## 1. 目标与边界

flow 是一个工作流执行引擎：发布不可变的流程定义（DAG），以 run 为单位执行，
进程崩溃后从完整的磁盘日志恢复。日志丢失或损坏时隔离并报告错误，不猜测副作用。
前端拖拽画布已实现：`web/`（Vue 3 + @vue-flow/core，直连本服务的 JSON-RPC WebSocket）。

明确的非目标（v1 范围决策）：

- 不做通用 DSL——表达式与脚本统一用 JavaScript。
- 多节点对等执行（v1 Postgres 后端的租约/持久 inbox/接管）已随后端删除；
  远程执行走 `flow-agent`（受信任中继，§2 的执行模式）。

节点运行可观察性（node_log 事件流、输入面快照、日志控制台）的完整设计见
`docs/observability-design.md`；本文只记它与状态机相关的契约（§3.1、§6.1、§10）。
将来接入 OpenTelemetry 的设计见 `docs/opentelemetry-design.md`——**已定未实现**，
且明确不改事件模型（该文 §2、§12），因此不改变本文任何一条不变量。

## 2. 总体架构

```
crates/
  flow-engine   执行引擎的共享能力：定义模型与校验、表达式沙箱、节点执行、
                journal_state（run 状态折叠）、观测日志、密钥、执行协议。
                （v1 的 Driver/Engine/fold 事件面已删除）
  flow-dto      领域 DTO 与状态词汇表的单一来源（零依赖叶子）：
                WorkflowVersion / WorkflowSummary / RunRecord / RunStatus。
                存储层持久化的本来就是引擎域数据，不维护第二份拷贝
  flow-store    SQLite：journal 的投影层（可随时重建，非权威）
  flow-backend  journal 唯一权威后端：journal.rs（JournalBackend）+
                journal_commands（产品 CRUD 命令，request_id 幂等）+
                journal_views（V2 产品物化视图）+ journal_driver（执行驱动）+
                journal_execution / journal_triggers / journal_import
  flow-journal  JSONL journal 库（叶子 crate）：事务日志、分页、codec 预算
  flow-rpc      jsonrpsee WebSocket 服务（bin: flow-journal-server）：
                journal_v2 协议（token 认证、写命令幂等键、回执语义）+
                journal_triggers（cron 扫描 + webhook HTTP）+ journal_download
  flow-cli      命令行客户端（bin: flow-cli，§9.3）。**纯 RPC 客户端**：只连
                flow-journal-server 的 WebSocket，不依赖 flow-backend / store，
                也不读 FLOW_BACKEND——CRUD 与触发语义唯一来源仍是 RPC 那一份
```

产品二进制按职责分属各自的 crate（不再做统一多路入口）：

| bin | 所属 crate | 职责 |
| --- | --- | --- |
| `flow-journal-server` | flow-rpc | JSONL v2 产品服务（WS RPC + cron + webhook + 下载） |
| `flow-cli` | flow-cli | 命令行客户端（纯 RPC） |
| `flow-executor` | flow-executor | 受管理执行子进程（FD 槽位由主进程 pre_exec 固定） |
| `flow-agent` | flow-agent | 受信任远程执行中继（双 TLS 上联 + 本机执行器池） |
| `flow-journal-tool` | flow-journal | journal 离线维护（verify/index/backup/repair） |
| `flow-journal-bench` | flow-journal | journal 写入基准 |
| `flow-journal-dev` | flow-backend | JSONL v2 开发运行器（run/resume/import-legacy） |

执行器定位契约（I09）：`flow-journal-server` / `flow-agent` 缺省在**自身同目录**
召唤兄弟 `flow-executor`（部署形态即「全部产品二进制同目录分发」，如
`scripts/build-journal.sh` 的 `dist/*/bin/`）；`FLOW_EXECUTOR_BIN` 显式路径
优先。找不到即报错，绝不静默回退进程内执行。测试专用脚手架
（backend-e2e 的 `flow-journal-server-e2e`、backend-perf 的 `flow-perf`）不属于
产品 bin，保持独立。

依赖方向（与 Cargo.toml 对齐的**现状**）：

```
flow-rpc ──> flow-backend ──> flow-engine ──> flow-dto
           ├─> flow-journal  （v2 值分页/下载）
           └─> flow-engine   （nodetypes.list 复用 NodeType::descriptor，单一来源）
flow-backend ──> flow-store / flow-journal / flow-dto
              └─> [仅 dev-dependencies] flow-agent（journal_remote* 测试）
flow-engine ──> flow-dto
             └─> flow-journal（只共享 StoredValue/ValueRef 词汇与 codec 预算
                  函数——引擎不依赖任何存储 I/O）
flow-journal ──> 无 flow 依赖（叶子；被 engine/backend/rpc/agent/executor 复用）
flow-agent / flow-executor ──> flow-engine, flow-journal（执行协议两侧）
flow-cli ──> flow-journal-server（纯 RPC 客户端）──> 上面的链路
flow-cli ──> flow-store ✗   ✗（CLI 不碰存储与事件日志，没有第二条写入路径）
```

后端只有 journal 一种：进程入口直接构造 `JournalBackend`，v1 的
`AnyBackend` 双后端枚举已删除。产品命令（workflow/run/schedule/webhook/template
CRUD）落在 `journal_commands`，回执带 `request_id` 幂等；产品读面
（`workflow.*.view` / `run.*.view` / triggers / templates）由 `journal_views`
物化。历史 v1 数据不迁移，仅全新数据目录；切换路线见
[SQLITE_V1_TO_V2_MIGRATION.md](SQLITE_V1_TO_V2_MIGRATION.md)。
`FLOW_BACKEND=sqlite|postgres`（v1）已删除，显式配置即报错并指向该文档。

运行：`cargo run -p flow-rpc --bin flow-journal-server`。配置分层 CLI > env >
`flow.toml` > 默认值（§9.4）；常用 env：`FLOW_JOURNAL_TOKEN`（必填，≥32 字节）、
`FLOW_JOURNAL_ADDR` / `FLOW_JOURNAL_HTTP_ADDR` / `FLOW_JOURNAL_DATA_DIR`
（§9.2）。

**单写者纪律（铁律）**：open 时对 `data_dir` 目录 fd 持排他 flock，
第二个实例立即失败。journal 用多线程 runtime（投影回放 /
值读取大量 `spawn_blocking`）。runtime 形态选择只在二进制薄壳 main 发生，
lib 内的请求处理路径不感知后端。

## 3. 核心数据结构：事件日志是唯一权威

这是整个系统最重要的设计决策，其余一切都从它推导。

> **v1 后端已删除（2026-10-09）**：本节描述的 `data_dir/runs/<id>/event.jsonl`
>
> - SQLite 元数据形态是 v1 的权威布局，已随 sqlite 后端移除。单机缺省
> 后端是 journal（`<data_dir>/journal/` JSONL 段链为唯一权威，SQLite 仅
> 可重建投影）；Postgres 臂仍共用本文描述的 v1 引擎折叠语义。
> 见 [SQLITE_V1_TO_V2_MIGRATION.md](SQLITE_V1_TO_V2_MIGRATION.md)。

**磁盘上的 `data_dir/runs/<run_id>/event.jsonl` 是 run 执行状态的唯一权威。**
SQLite `runs` 表记录初始化结果并提供查询索引，内存执行状态由日志折叠得到。
初始化必须先插入 `initializing` 元数据，再持久化 `run_started`，才可派发节点；
Driver 启动后回填 `running`。终态也必须先写事件，再更新索引。
恢复时以完整日志为准回填（`recover_unfinished`），初始化失败和日志缺失见 §7。

### 3.1 事件模型

每行一个 JSON `Envelope`：`{seq, ts, run_id, type, ...}`，`seq` 从 1 严格连续。
事件全集：

| 事件 | 载荷 | 语义 |
| --- | --- | --- |
| `run_started` | workflow_id, workflow_version, input, depth | run 创建，input 快照；depth 为嵌套深度（根 run 为 0，旧日志缺省 0） |
| `node_started` | node_id, attempt, child_run_id（可选）, input（可选） | **副作用发生前**写入；child_run_id 仅 sub_workflow 携带；input 是模板展开后的 params 脱敏快照（可观察性数据，fold 不消费） |
| `node_completed` | node_id, attempt, output, duration_ms | 节点成功 |
| `node_failed` | node_id, attempt, error, retryable | 节点失败；`retryable` 表示引擎还会重试 |
| `node_skipped` | node_id, reason | 汇合判定不满足，整节点跳过 |
| `node_log` | node_id, attempt, level, stream, message | 节点运行日志（可观察性）。level=debug/info/warn/error，stream=engine/stdout/stderr；只记日志，不改状态 |
| `signal_received` | node_id, payload | 外部信号已落盘（human_task / 裁决） |
| `run_completed` | output | run 成功 |
| `run_failed` | error | run 失败 |
| `run_cancelled` | — | run 取消 |

兼容性：旧日志的 `node_started` 可能含已废弃字段（`idempotency_key`/`params_hash`），
serde 反序列化时忽略，删除字段不破坏历史日志的可恢复性。

### 3.2 写序协议（崩溃窗口的定义）

```
append(node_started)  →  执行副作用  →  append(终态事件)
```

每个事件 `write_all` + `sync_all`，**fsync 是崩溃安全边界**——批量 fsync 不松这个
边界：`append` 返回即 durable（严格组提交），fsync 跨 run 组提交——同时在飞的
多个 run 的事件共享同一轮刷盘，但每个调用方等的是**自己文件**的 `sync_all` 完成
（`flow-engine::event::GroupCommitter`，drain 式攒批、无定时器：低负载批=1 零额外
延迟，高负载批≈并发 append 数）。崩溃语义与「每事件 fsync」逐字一致。
这界定了节点副作用不明的窗口。恢复还必须处理 `node_failed(retryable=true)` 后
尚未开始下一次 attempt、信号已记录未消费、节点失败后 run 尚未收尾以及初始化未提交。

**日志行是这条边界的例外层**：`node_log` 只 `write_all` 不进组提交（进程级持久），
搭同文件后续严格事件的 fsync 便车——终态事件 append 前 EventLog 强制先 sync 一次
（终态返回 ⇒ 终态前的日志已在盘上）。所有 `node_log`（exec 的 console 输出**与**
引擎叙事日志：重试/接管重放/等待人工/等待裁决）只走 `RunEventSink::append_log_batch`
一个出口——Postgres 上那是**两条语句一个事务**（`last_seq += N` 一次 + `jsonb_array_
elements ... WITH ORDINALITY` 整批插入 + 一次 NOTIFY），逐条则是 N 个受保护事务 +
3N 往返。`last_seq` 只取 DB 返回的批末值，不本地重算。细节见
`docs/observability-design.md`。

### 3.3 日志读写规则

- `EventLog::create`：`create_new`，run_id 已存在则报错（run_id 唯一）。
- `EventLog::open`：续写前先修复残缺尾行——崩溃可能留下无换行的半行 JSON，
  `open` 时物理截断，保证后续追加不粘连。
- 逐行解析只容忍**最后一行**残缺；中间行损坏是 `LogCorrupted` 硬错误，
  不静默吞数据。`read_events` 校验 seq 连续（防截断/损坏）。
- `Engine::read_events(run, from_seq)` 走**增量续读**（`event.rs::read_events_from`）：
  日志纯追加，因此只需解析上次之后新增的字节。护栏三条，任一不满足就整体重读、
  绝不猜：文件头指纹一致（同一路径被复用/日志重建时认出来）、文件只变长
  （截断/轮转丢弃缓存）、新块首条 `seq` 必须紧接已缓存的末尾 `seq`。
  `from_seq` 低于缓存窗口首条、或 `None`（快照）时仍是全量读 + 全序列校验，
  与加缓存之前逐字等价。缓存进程级有界（每 run 512 条事件 × 64 个 run，FIFO 淘汰）。

### 3.4 订阅的回放 + 追流状态机（flow-backend run_tail）

> 历史记录：`run_tail` 已随 v1 适配层删除；v2 订阅直接 tail journal
> （`journal_v2.rs` 的 run.subscribe），回放 + 追流 + 终态结束语义继承本节。

`run_tail` 把「从 seq=1 回放」与「追实时增量」合成一个流，`Phase` 是
`Replaying → Streaming ⇄ Filling → Done | Closed`。**只吐严格连续的前缀**：
出现缺口先补齐，补齐失败等重试定时器或下一条事件唤醒，绝不跳缺口静默丢事件。

两条不变量（都有回归测试钉住，改动前必须知道）：

- **追平后必须等唤醒，不得重读**。`caught_up` 记录「上次补读取毫无所获且无缺口」；
  没有这个标记时每次 `advance()` 都会重读一次读取面。对活着且暂时没有新事件的
  run（订阅一个长跑 run 的**常态**）这就是无限空转——每个在线观众烧满一个核，
  且每次把整份日志重读一遍。追平后由新事件或 10s 兜底定时器唤醒；
  `Phase` 装不下这个信息：它不是进度，而是「读取结果为空且无缺口」的结论。
- **缺口本身不构成退避理由**。`Filling` 只能由「一次读取毫无所获而缺口仍在」
  进入；缺口一旦在排水步就转成 `Filling`，补齐那一整步就被跳过，10s 定时器醒来
  又立刻被转回 `Filling`——读永远不发生、缺口永远不消失，订阅静默停摆
  （连终态都送不出去）。真实后端（PG 查询慢于 NOTIFY）下实时段先到是常态。

## 4. 折叠器：一个状态机，两个消费者

> 历史记录：v1 的 `fold.rs` 已删除；v2 的状态折叠在
> `flow-engine/src/journal_state.rs`，不变量继承本节。

`fold.rs` 的 `RunState::fold(Envelope)` 是**唯一**的状态转移函数。
恢复（重建驱动状态）与只读时间线（前端渲染）共用它，不存在第二份状态语义。

```
Event 流 ──fold──> RunState {
    records: HashMap<node_id, NodeRecord{state, output, started_at, ended_at,
                                          duration_ms, last_signal,
                                          child_run_id, input}>,
    phase: Running | Succeeded | Failed | Cancelled,
    fatal_error, output, workflow_id, workflow_version, input, last_seq, depth,
    started_at, ended_at,
}
```

节点状态机：`Pending → Running{attempt} → Completed | Failed{attempt,error,retryable} | Skipped{reason}`。
`Failed{retryable:true}` 是等待重试的**非终态**，时间线标签为 `retrying`；
保留事件及 NodeState 序列化格式，旧日志无需迁移。

**节点状态与输出只有一份，都在 `NodeRecord` 里**（§12.5）：已尝试次数与失败原因
**从状态派生**（`NodeState::attempt()` / `::error()`），输出是 `NodeRecord.output`
字段。驱动器读前驱输出一律经 `RunState::node_output(node_id)`，不另开 map。
输出曾经是第三种形状——`RunState::outputs` 那把与 `records` 同键的独立 map。挪走
并没有消除同步义务（四个节点终态分支照样各自要处置旧输出：`NodeStarted`/
`NodeFailed`/`NodeSkipped` 清、`NodeCompleted` 写），只是把义务搬到另一把 map 上，
并新增一个「两把 map 的键集可能不一致」的面。留在 `NodeRecord` 里，四处改的是
**同一个** `rec`，同步点数不变但不可能只改一处。
`run.timeline` 的 wire 形状（`attempts` / `error` 字段）逐字不变，只是取值改为
从 `state` 取。

关键转移：

- `node_started` **清除旧 output**（重试/重放不留脏数据），同时记下最新一次
  attempt 的输入面快照 input（展示用，终态不清除）；
- `node_log` 在 fold 中**跳过**（观察数据，不是恢复状态；`last_seq` 仍推进，
  订阅者照常收到——一条流两个用途，§3.1）；
- `node_completed` 写入 output；
- `node_failed(retryable=false)` 在 fold 中记录首个 `fatal_error`，Driver 无独立失败缓存；
- `run_cancelled` **清掉 `fatal_error` 与 `output`**：`fatal_error` 的语义是
  「本 run 终态为 failed 时的原因」，用户主动取消时终态是 Cancelled。漏清的实际
  后果是恢复回填路径（`recover_unfinished` 用 `terminal.fatal_error` 写 runs 行）
  把「节点 X 失败」当成 cancelled run 的 error 落库，而正常投影路径对 RunCancelled
  明确写 error=NULL——同一状态两条路径两个答案，正是 §9「同名字段必须同值」被破掉；
- `signal_received` 只记 `last_signal`（崩溃可能落在它与终态之间，恢复时消费它）。
  **不变量 `last_signal.is_some() ⟹ 节点非终态`**：只有非终态节点可能消费它
  （Running 节点消费，Pending 节点启动时被 `node_started` 清掉），终态节点带着
  它就是一条永远不会被消费的陈旧待办。四个节点终态分支与 `node_started` 都要清
  ——`node_skipped` 曾是唯一漏清的分支（当前不可达：信号只对 Running 节点写，
  而跳过只发生于 Pending 节点）；
- attempt 号只存在于状态里：它从 1 起、每次 +1，且 `handle_result` 丢弃非当前
  attempt 的迟到结果，所以「状态里的 attempt」恒等于「已尝试次数」，无需 max 累积。

## 5. 定义模型与校验

`Definition { nodes, edges }`（前端拖拽产物，整体作为不可变版本入库）。
节点类型：`start`、`end`、`script`、`condition`、`delay`、`http_call`、`human_task`、
`sub_workflow`、`harness`（外部 agent CLI，prompt 经 stdin 传入，stdout 作为输出）、
`email`（Resend 兼容发信）。
后两者是有外部副作用的集成节点（崩溃后不自动重放，§7 人工裁决），`has_side_effect`
与 http_call 同类。

每个类型在 `model.rs` 的 `NodeType::descriptor()` 注册一次：`NodeType::ALL` 的顺序
即 `nodetypes.list` 响应顺序（= 前端面板顺序），params_schema 之外带
`label`/`category`/`ports`/`max_instances`/`supports_retry`/`side_effect` 与
`x-widget`/`x-help`/`x-secret` 扩展键，快照测试
（`node_types_snapshot_is_stable`）钉住响应逐字节不变。

**新增节点类型只改两处**：`NodeType` enum 与 `NODE_TYPE_TABLE`
（`model.rs`，变体 ↔ 字符串名的对应表）。其余全部派生：

- `as_str` / `parse` / `ALL` / `descriptor()` 的下标 ← `NODE_TYPE_TABLE`；
- `validate_params()` 的必填清单 ← descriptor 的 `params_schema.required`；
- `secret_params()` ← descriptor 里标 `x-secret: true` 的属性；
- `opaque_params()` ← descriptor 里标 `x-opaque: true` 的属性。

`validate_params` 里剩下的 match 只放**无法表达在 `required` 里的规则**
（HTTP 方法白名单、delay 的整数/模板形态、sub_workflow 的 input_mapping 形态）；
纯参数类型没有额外规则，新类型不用碰它。`exec::dispatch` 的 match 臂仍要加一条
（编译器会逼你加）。

`x-opaque` 与 `x-widget: "code"` **不是一回事**：后者只是「前端用代码编辑器
渲染」，`harness.prompt` / `email.body` 同样是 code 组件但**要**参与 `${}` 展开；
`x-opaque` 才是「这段内容是 flow 自己的 JS 语法，不参与展开」。

`node_type_table_is_exhaustive` 钉住表与 enum 双向覆盖（数组长度由类型标注编译期
保证，内容对应关系由测试保证）；`required_params_are_actually_enforced_by_validate`
与 `opaque_params_are_exactly_the_js_bearing_fields` 钉住派生后的规则仍成立。

`Definition::validate()` 在保存与发布时强制（建图即校验，不等到运行）：

1. 节点 id 非空且唯一；类型已知；按类型校验必填参数（清单来自 descriptor）
   （script: `code`，condition: `expr`，delay: `ms`，http_call: `url`，
   sub_workflow: `workflow_id`，harness: `command`/`prompt`，
   email: `api_key`/`from`/`to`/`subject`/`body`；
   method 若给出必须属于 `HTTP_METHODS`——该白名单与 `nodetypes.list`
   共用一份，拼写错误在发布时被拒而不是运行时炸 run）；
   `${}` 模板参数（如 `ms: "${input.delay}"`、模板 method）放行到执行期判定，
   裸数字串等灰色形态仍拒绝；sub_workflow 的 `input_mapping` 若给出必须是
   对象或非空模板串；
2. 恰好一个 `start`，至少一个 `end`；
3. start 无入边，end 无出边，其余节点必须有入边（否则永不触发）；
4. 边端点存在、无自环、无重复边；**condition 出边必须带 `true`/`false` 端口，
   其余节点出边不得带端口**；**同一 condition 的多个端口不得指向同一节点**
   （见下）；
5. 拓扑必须是 DAG，且所有节点从 start 可达（防死代码）。

**为什么 condition 多端口不能指同一节点**：AND-join 语义（§6.2）下，未被选中的
那条出边判为 Unsatisfied，会把**整个**目标节点跳过——于是「走 true 分支」反而把
true 分支的下游跳掉，run 仍记 `succeeded`、输出为 null，全程无任何错误。这是错误
建模而非分支合流（真要合流应指向不同节点，或引入显式 join 策略），建图期拒掉。
实测：修复前 `s → c(condition=true) → e` 配 `c(true)→e` + `c(false)→e` 时，
condition 求值为真而 `e` 被 `branch_not_taken` 跳过。

**密钥参数（x-secret）**：params_schema 里标 `x-secret` 的参数（email 的
`api_key`）在定义里只存**名称**，真值执行前从 `FLOW_SECRET_<名称>` 环境变量注入
（`secrets.rs`）。`workflow.update` 提前校验每个名称都已配置，配置错误挡在
落库前（`missing_secrets`）；模板名称要到执行期展开才能确定，缺失时节点 fatal。
真值不参与模板展开、不随 `node_started` 落盘，`secrets.list` 只回名称列表。

## 6. 执行引擎（Driver）

> 历史记录：v1 的 Driver/Engine 已删除；v2 执行驱动是
> `flow-backend/src/journal_driver.rs`，副作用边界（§6.6 的授权语义）与
> 汇合/收集/重试语义继承本节。

每次 run 一个 tokio 任务，持有该 run 事件日志的**单写者**——没有跨 run 共享可变状态，
没有锁竞争，run 之间完全隔离。

### 6.1 调度循环

```
loop {
    内层循环推进到不动点：
        plan() → (ready, skips)
        skips 逐个 append(node_skipped)      // 跳过沿下游传播
        ready 逐个 start_node()               // 展开 params + 输入快照 → node_started + 派发执行
    if slots 为空：
        all_terminal → finalize（RunCompleted/RunFailed）
        否则 → 错误「调度停滞」（图正确性由 validate 兜底，此错误意味着语义 bug）
    select! { cancel → abort_inflight + RunCancelled;
              result_rx → 节点结果;
              signal_rx → 外部信号 }
}
```

**节点级执行状态机（`NodeSlot`）**：`slots: HashMap<node_id, NodeSlot>` 是 Driver
唯一的在途工作账本，键集 =「有在途任务或未决外部输入的节点」。三种槽位：

| 槽位 | 含义 | 持句柄 | 由谁推进 |
| --- | --- | --- | --- |
| `Running` | 节点执行中，或退避计时器在飞 | 是 | `result_rx` 的 `Done` / `RetryDue` |
| `AwaitSignal` | human_task 已落 `node_started`，挂 oneshot 等 `run.signal` | 是（等 oneshot 的任务） | `run.signal` 交付后回传 `Done` |
| `Adjudicating` | 崩溃/接管残留的副作用节点，等人工裁决 | **否** | `run.signal` 裁决（不经 `result_rx`） |

转移表（`∅` = 不在 slots 里）：

| 起始 | 事件 | 终止 |
| --- | --- | --- |
| `∅` | 派发（就绪 / 重试 / 裁决 retry） | `Running` |
| `∅` | human_task 就绪 | `AwaitSignal` |
| `∅` | 接管残留副作用节点 | `Adjudicating` |
| `Running` | `Done`（attempt 归属校验通过） | `∅` |
| `Running` | `RetryDue` | `∅` → 随即 `Running` |
| `AwaitSignal` | `run.signal` 交付 | `Running` |
| `Adjudicating` | 裁决 `retry` | `Running` |
| `Adjudicating` | 裁决 `succeeded` / `failed` | `∅` |
| 任意 | 取消 / 停机 / 失去所有权 / Driver 退出 | `∅`（`abort_inflight` 全部 abort） |

关键不变量（都有回归测试钉住）：

- **跳过必须推进到不动点**：多级下游（delay→human_task→end）单轮只处理一层
  会被误判「调度停滞」。见 `skip_propagates_through_multiple_downstream_levels`。
- **句柄不变量**：`Running` / `AwaitSignal` 各持恰好一个 `JoinHandle`，该 handle
  恰好欠一条 `DriverMsg`——每个 `spawn` 紧跟一次 `slots.insert`，每条消息的消费
  紧跟一次 `slots.remove`。human_task 的 oneshot 等待任务与重试退避计时器都必须
  计入 slots，否则终止判定提前触发。
- **`AwaitSignal → Running` 不是离开 slots**：交出 oneshot 后等待任务仍在飞、
  仍欠那条 `Done`，只有消费 `Done` 时才真正转 `∅`。提前摘除会让「信号已交付、
  结果未回传」的窗口被误判成可收尾。
- **一个节点同时只占一个槽位**：human_task 节点的 oneshot 发送端与等待任务句柄
  同处 `AwaitSignal`，由类型保证同步——旧实现把两者分放两个集合，靠约定维持
  一致，漏清一处即提前收尾或永久挂死。

**start_node**（节点执行的唯一入口：就绪派发、重试、人工裁决 retry、接管恢复
重放都经它）在写 `node_started` 之前做三件事，做完才派发执行：

1. **模板展开恰好一次**：`exec::expand_params`（§10）在写事件前完成，展开结果
   **同时**是执行期 params 与输入面快照；exec 层不再展开（双展开会把用户数据里
   合法的 `${` 再 evaluate 一遍——历史回归有测试钉住）。human_task 不执行，
   用原始 params 做快照。展开失败不破写序协议：先 `node_started` 再走节点
   失败路径（可重试失败照常调度重试）。准备阶段（`build_prep`/`build_node_prep`）
   是纯计算、不碰 sink，**同批 ready 节点并发展开**——`expand_params` 内部是
   `spawn_blocking`，在驱动循环里串行 await 会把宽扇出排成一条队，期间取消
   信号、日志排空、租约失效全部要等它跑完；派发仍逐个有序进行，事件顺序不变；
2. **输入快照脱敏后落盘**：`redact_value` 按敏感键列表（authorization/token/
   api_key…）把快照里的敏感值替换为 `***`——快照是给前端看的，事件里的原始
   output 才是下游的数据面，两者都不能动。快照另有 8KB 字节上限
   （`cap_input_snapshot`），超限降级为 `{"__truncated": true, "size": N,
   "preview": "…"}`：与日志行截断同一取法（**截断而非丢弃**，标注原始字节数
   并留下可读开头）——输入面快照存在的意义就是调试"到底传了什么进去"，
   一刀切成空壳恰好在最需要它的时候什么都不剩；
3. **密钥注入在执行期**：x-secret 参数的**名称**在快照里，真值在 exec dispatch
   前才由 `FLOW_SECRET_<名称>` 注入（只进请求头）。

**节点日志**：exec 任务持 `NodeLogger`（unbounded mpsc，发即忘，per-run 预算
`Arc<LogBudget>` 判定），driver 的 select 循环 biased 优先排空日志（发射端先发
日志后发 Done），成批转 `node_log` 落盘。攒批有 1ms 窗口、单批 256 条封顶：
产者是另一个线程（节点 exec / QuickJS console 桥），纯 `try_recv` 排空在刷屏时
每批只有 1 行——批接口形同虚设，Postgres 上等于每行一个事务。等待按**批**摊销
不是按行（窗口内到达的行一次写完）。日志接收端只活在 `drive()` 的局部变量里，
**不挂回 Driver 字段**：从 `self` 再取一次会永远拿到 `None`，攒批静默退化成
逐行落盘（这正是它一度完全不生效的原因）。日志不是恢复状态：fold 跳过 `node_log`，
重启后历史日志仍可经 `run.events` 读取（§9）。

### 6.2 汇合语义：AND-join

每条入边判定为三态：

- `Satisfied`：source Completed。condition 节点按真值选 `true`/`false` 端口，
  端口不匹配 → Unsatisfied(`branch_not_taken`)；
- `Unsatisfied(why)`：source Skipped（`upstream_skipped`）或不可重试 Failed（`upstream_failed`）；
- `Waiting`：source Pending/Running/Failed{retryable:true}。

**任一入边确定 Unsatisfied → 整节点 Skipped**；全部 Satisfied → 就绪；
否则等待。跳过的下游沿同样规则继续传播。

因此 if/else 两个互斥分支不能通过普通 AND-join 合流：未选分支会让汇合节点跳过。
当前定义保持该语义；需要分支合流时须引入显式 join 策略，不能改变旧定义的默认值。

### 6.3 输出收集：单数透传、复数映射

同一条规则（`exec::singular_or_map`），两处共用：

- **end 节点输出**：单前驱 → 透传其输出；多前驱 → `{pred_id: output}` 映射；
- **run 最终输出**：单 end → 透传；多 end → `{node_id: output}` 映射。

缺失（被跳过）的来源补 `null`。注意：单来源被跳过时输出为 null，
与「真输出了 null」不可区分——消费方需要在多 end 场景下区分时自行查时间线。

### 6.4 condition 真值判定

condition 节点输出为表达式结果经 JSON 序列化后的值；引擎用 `exec::truthy` 选出口。
对 JSON 值，与 JS `Boolean()` 一致：空数组、空对象、`"false"`、`"0"` 都为真。
表达式结果契约限定为 JSON 值；非 JSON 结果经过序列化可能改变含义
（例如 Infinity 变为 null），不能宣称任意 JS 对象的原始真值都被保留。

### 6.5 重试策略

`params.retry { max_attempts (默认 1), backoff_ms (默认 0) }`。
`NodeFailure.retryable` 决定引擎重试还是判死 run：

- 可重试：连接失败/超时、HTTP 5xx、**响应体中途断流**、429 限流（仅 email）、
  harness 超时/等待失败；
- 致命：JS 抛错、参数校验失败、HTTP 4xx（请求本身的问题）、harness 非 0 退出
  与 spawn 失败（命令不存在，重试无意义）。

这份分类 http_call / email 共用同一套习惯（后者经 `post_json_bearer`）。

退避计时器占一个 `Running` 槽位（不变量），到点后 `RetryDue` 重新派发，attempt+1。
下游在此期间等待。**恢复一律等满 `backoff_ms`**（不续算剩余时间）：事件 `ts`
由写入者时钟决定（单机 = append 时的 `Utc::now()`，Postgres = 事务内
`clock_timestamp()`），而调度进程的 `Utc::now()` 与之不同源，跨机器恢复时
这个减法不可靠——宁可多重试一次间隔，也不做会静默失效的时钟减法。

### 6.6 取消与 fatal 的副作用语义

**fatal（已决策，勿改）**：首个致命节点失败由 fold 记录在 `state.fatal_error`，
不中断其余分支：独立分支跑到自然终态再结束 run——避免中途砍掉已发出的副作用；
失败节点的下游经 `upstream_failed` 全部跳过，结果注定 `RunFailed`，
由 finalize 收尾。代价是注定失败的 run 会等最慢的无关分支跑完。
节点失败后、run 终态前崩溃，恢复也必须保留该失败结论。

**cancel**：abort 所有在途槽位。对 in-flight 的 `http_call`，请求可能已发出、
响应永远不读、run 记 `RunCancelled`——与 §7 的人工裁决是同类的副作用歧义，
取消是用户主动选择，不做裁决。

### 6.7 外部信号（human_task 与人工裁决）

`Engine::signal` 只对活着的 run 生效（registry 查找，否则明确报错）。
请求带 oneshot 回执。Driver 先校验节点等待状态和裁决 payload，非法或重复信号返回
conflict，不改变 run；有效请求写入 `signal_received` 并处理后才确认 `delivered=true`。
确认丢失后重发可以返回 conflict，调用方应查询时间线核对已提交结果。

- **human_task**：节点执行 = 等待。`node_started` 落盘后挂 oneshot 等待，
  收到 `run.signal` 先 append `signal_received` 再解除等待。
  节点输出 = 信号 payload 本身。
- **崩溃残留副作用节点裁决**：同样先记录 `signal_received`；若崩溃发生在信号与
  `node_completed` / `node_failed` / 下一次 `node_started` 之间，恢复时消费原裁决。

Driver 退出时显式 abort 剩余任务。内部错误若无法写入 `run_failed`，索引保留为
`awaiting_resume` 并记录诊断，不把未持久化的终态写进 DB。

### 6.8 子工作流（sub_workflow）

sub_workflow 节点以目标工作流的**最新已发布版本**启动一个子 run，子 run
输出透传为本节点输出。子 run 输入的缺省语义是父 run 的输入快照；节点 params
带 `input_mapping`（对象模板，值支持 `${input.x}` / `${nodes.n.y}`）时，其**展开
结果整体作为**子 run 输入——上游算出来的数据由此进入子流程（无 mapping 的存量
定义行为零变化）。父子 run 是两条完全独立的事件日志，
各自走自己的恢复与终态协议。Driver 拿不到引擎句柄（DriverSpec 只有
run_id/definition/input），启动/等待/取消经 `ChildRunLauncher` trait
（`child_run.rs`）抽象：单机 `LocalChildLauncher` 同进程复用 Engine，
Postgres `PgChildLauncher` 经 `lease::create_run` 单事务创建子 run、
轮询共享 runs 投影等终态，取消走持久 inbox（`flow-pg/src/gateway.rs`）。

- **child_run_id 确定性派生**：`{父run_id}:{节点id}:{attempt}`，在 `start_node`
  中确定并随 `node_started` 落盘——子 run 创建是副作用，其 id 先于副作用
  持久化，写序协议（§3.2）不破；
- **重放沿用已落盘 id**：崩溃恢复时 Running 残留记录带有已落盘的 child_run_id，
  重放复用同一 id 启动，`start` 撞 `RunExists` 即附着既有子 run 等待终态，
  不重复创建；**重试**（新 attempt，记录已非 Running）派生新 id，每次重试是
  独立的子 run；
- **深度上限 8**（`MAX_SUB_WORKFLOW_DEPTH`）：validate 检不了跨 definition 的
  循环引用，运行时 `depth >= 8` 直接 fatal；depth 经 `run_started` 落盘，
  恢复时读回；
- **失败分类**：子 run failed/cancelled → 父节点 fatal；launcher 错误按平台
  故障分类分流——IO/Backend/日志损坏 → 挂起 awaiting_resume（不当作工作流
  失败，§12.13），其余基础设施错误 → retryable；未配置 launcher 时执行即 fatal；
- **取消级联 best-effort**：父 run 取消时先对仍在 Running 的 sub_workflow 节点
  发起子 run 取消，再写 `RunCancelled`；取消失败只记日志，不升级。

## 7. 崩溃恢复

### 7.1 分类

进程重启时 `recover_unfinished` 扫描 `initializing`/`running`/`awaiting_resume`。
有效日志的 workflow、version、input 必须与元数据匹配。折叠后按持久状态分类：

| 节点状态/类型 | 处置 | 理由 |
| --- | --- | --- |
| Running 纯节点（start/end/script/condition/delay） | **重放**：attempt+1 | 无外部副作用 |
| Running `http_call`，无裁决信号 | **人工裁决**：`awaiting_resume` | 请求可能已发出，不猜测 |
| Running `http_call`，已有裁决信号 | **消费裁决**：retry/succeeded/failed | 裁决已经持久化 |
| Running `human_task`，已有信号 | **补终态**（output=信号） | 信号已经持久化 |
| Running `human_task`，无信号 | **继续等待**，不重复写 started | 等待无副作用 |
| Running `sub_workflow` | **重放**：沿用已落盘 child_run_id，附着既有子 run | id 确定性派生且已随 node_started 落盘，重复 start 撞 RunExists 幂等 |
「副作用节点」= `has_side_effect()` 的类型：`http_call` / `harness` / `email`——
三者恢复路径完全同构（无裁决信号一律 awaiting_resume，不自动重放）。
| Failed{retryable:true} | **重建退避计时器**，等满整段 backoff 后下一次 attempt | 内存计时器不是权威；剩余时间不可续算（§6.5） |
| Failed{retryable:false} | **保留 run 失败结论**，独立分支继续 | fold 已记录 fatal_error |

裁决信号：`run.signal {payload: {action: "retry" | "succeeded" | "failed", output?, error?}}`。

恢复循环的失败隔离：单个 run 的回填/隔离投影失败（如瞬时 SQLite 锁）记入
失败清单继续下一个 run，不中断整个恢复——一条坏行不能阻止其余 run 恢复，
更不能让服务拒绝启动。日志缺失、身份不符、或**含定义外节点**的 run 保留
awaiting_resume 并报告错误，需修复数据后重启重试。恢复分类是
**一条记录一个节点**（`RecoveryAction`，§12.20），不是分组平行集合。

### 7.2 恢复的正确性来源

- 日志已终结但 DB 未回填（崩溃在 append 与 observer 之间）→ `recover_unfinished`
  以事件为准修正 DB；
- DB 已回填但进程死在最终时刻 → 重启后 `resume_run` 发现 phase 已终态，
  返回 `AlreadyTerminal`，幂等；
- 半行残缺日志 → `EventLog::open` 物理截断；
- seq 不连续 → 硬错误，人工介入；
- 运行中基础设施故障（磁盘/数据库 IO、序列化、日志损坏）→ **不写 run_failed**，
  投影 `awaiting_resume` 挂起：平台故障不是工作流失败，SQLite 重启后恢复、
  Postgres 由 executor 自动接管；只有工作流语义失败才写 run_failed 终态；
- **引擎不变量被破坏**（`EngineError::Bug`，如调度停滞）→ 与平台故障同一条出口，
  挂 `awaiting_resume` 而非写 `run_failed`：此时定义合法、引擎却推不动，写业务
  终态会让运维照着没写错的工作流定义白查。错误信息自带卡住的节点与状态。

初始化中断：`initializing` 行配有有效 `run_started` 时按上述分类恢复；日志缺失、
空文件、首行残缺或损坏时将初始化标为 failed，保留文件与诊断，不执行节点。
这条失败记录是初始化结果，不伪造执行事件。
旧 `running`/`awaiting_resume` 的日志缺失、损坏或身份不符，则保留为
`awaiting_resume`、`live=false` 并报告恢复错误；需恢复原日志后重启，不能靠
`run.signal` 解除，也不能自动创建新日志重跑。这样兼容旧初始化窗口并避免重复副作用。
父 run 等待初始化中断的子 run 时消费 DB 投影（`LocalChildLauncher::await_terminal`
发现子日志为空时查 runs 行）：这类子 run 永远不会写出终态事件，事件侧等待会挂死。

**已知限制**：delay 崩溃后重放整段时长（不续算剩余时间）；重试退避恢复后同样
等满整段 backoff（不续算剩余时间，理由同 §6.5）。

## 8. 存储层（flow-store）

> 历史记录：v1 的 `Store`（flow.db 元数据权威）已删除；本 crate 只剩
> journal 的 SQLite 投影（`projection.rs`，可随时重建）。Postgres 后端
> 整体删除，本节 PG 相关段落为历史设计记录。

SQLite（WAL + `synchronous=NORMAL` 组提交：COMMIT 不 fsync、checkpoint 才 fsync。
SIGKILL 崩溃零丢失——页缓存归内核管；断电/内核崩溃可能丢最后几笔事务，但库
永不损坏。事件日志才是 run 状态的唯一权威，元数据只是查询索引，见 §3）。
连接池 max=1（§2 进程模型：单进程·单线程·单连接；进程内排队在 pool 上
发生，SQLITE_BUSY 在进程内结构性消失，busy_timeout 只防御进程外访问）。表：

- `workflows`：id, name, created_at；
- `workflow_versions`：`(workflow_id, version)` 主键，**不可变定义快照** + checksum。
  在 `BEGIN IMMEDIATE` 事务内检查最新 checksum 并分配版本；
  `INSERT ... RETURNING version` 返回本次写入的版本，禁止写后另查 latest。
  相同定义的并发保存也复用版本号。
  `status`: draft → published；
- `runs`：run 元数据（id, workflow_id, workflow_version, status, input, output, error,
  started_at, ended_at）+ 触发来源归因（`source`：manual/schedule/webhook/sub_workflow，
  词汇表单一来源是 flow-dto 的 `DbRunSource`，写入口校验同 DbRunStatus；
  `source_detail`：schedule id / webhook token，可空）。存量库由幂等迁移加列
  （SQLite PRAGMA 判存在性 / Postgres ADD COLUMN IF NOT EXISTS），旧行 DEFAULT
  'manual'——迁移前没有自动触发入口，归因语义正确；
- `schedules`：cron 定时调度（id, workflow_id, cron_expr, input, enabled, created_at）。
  cron 合法性校验在 RPC 边缘（-32010），存储层只持久化；
- `schedule_fires`：触发去重表，`(schedule_id, fire_at)` 主键——插入成功即赢得本次
  触发权，重复 tick 与多节点竞争都靠它去重；
- `webhooks`：webhook 触发器（token 主键，随机生成不可猜, workflow_id, enabled, created_at）。

约束：

- **只有 published 版本可执行**；显式 version 同样检查，省略时取 latest published；
- run 钉死某一版本——定义漂移在架构上不可能发生，无需额外防御；
- 有 run 记录时拒删 workflow（事件日志不能变孤儿）；
- 拒删与创建 run 互斥：SQLite 臂 delete_workflow 的计数+删除、insert_run 的
  版本存在性检查各处一个 BEGIN IMMEDIATE 写锁事务；Postgres 臂用
  FOR UPDATE / FOR SHARE 串行化（同一互斥，两臂同契约）；
- `set_run_status` 影响 0 行必须报错（静默成功会掩盖「run 行没插进去」）；
- **状态词汇表**：`runs.status` 的合法取值**单一来源**是 flow-dto 的
  `DbRunStatus`——变体清单 `ALL` 是唯一真相，`as_str()` 与之对拍，
  `is_valid_str` / `is_terminal_str` / `is_active_str` / `sql_in_list` 全部由
  `ALL` 派生。store 写入口（`insert_run`/`set_run_status`）经 `ensure_run_status`
  按 `is_valid_str` 校验；`flow-pg` 的 `CHECK` 约束由同一份 `ALL` 生成
  （排除 `initializing`——PG 的 `run.start` 单事务原子创建，该状态不可达）。
  **加一个状态变体只需改 `ALL` 与 `as_str` 两处**，其余全部跟随；
  `all_is_the_single_source_for_status_vocabulary` 钉住两者不脱节
  （数组长度由类型标注编译期保证，内容对应关系由测试保证）。
- 状态更新替换 output/error，传 None 会清空；已解决的裁决诊断不得留在成功结果里。

## 9. RPC 层（flow-rpc）

jsonrpsee WebSocket。本层只有**一份**方法实现，只依赖 `flow_backend::AnyBackend`
闭集枚举；后端差异（初始化协议、信号落账、订阅推送）全部在 flow-backend 吸收：

- `run.start`：SQLite 两段式 initializing → run_started；Postgres 单事务原子创建。
  「只有 published 可执行 + 创建前校验」在 flow-backend 的
  `resolve_runnable_definition`，`AnyBackend::create_run` 两臂共用它。
  **但它是纵深防御而非唯一副本**：`flow-pg/src/lease.rs` 的 `create_run` 与
  `flow-pg/src/child.rs` 的子 run 启动各自在事务内独立做了一遍
  「锁 workflow 行 → 校验 version 已 published → 解析定义」，
  `child.rs` 还有第三种「没有已发布版本」的错误文案。三处都改才安全。
- `run.signal` / `run.cancel`：统一 SignalAck 响应（delivered / pending / rejected）。
  `signal_id` 在 Postgres 后端必填且重试复用（真实落账的 inbox 主键，可查询）；
  SQLite 后端可省，响应**只回显客户端提供的 id、从不伪造**（没有持久 inbox，
  伪造一个查不到的 id 是欺骗客户端）；取消响应统一为 delivered 语义；
- `run.signal_status`：pg 专属能力，不进 AnyBackend 公共面——在唯一的
  方法注册点 match 枚举暴露；SQLite 返回明确的 invalid 错误（同步交付无账可查）；
- `run.subscribe`：SQLite 是进程内 broadcast（零 spawn 的流包装）；
  Postgres 按 run_id 维护 last_seq 轮询共享日志，指定 run 已终结追平后流自然结束。

引擎不依赖存储，SQLite 后端内部的 `StoreObserver` 在 flow-backend 把
`RunObserver` 状态出口适配到 runs 表。

方法：

| 方法 | 说明 |
| --- | --- |
| `workflow.create / update / publish / get / list / delete` | 定义生命周期。update 前强制 validate + x-secret 名称存在性校验（§5） |
| `workflow.versions` | 版本历史（按 version 倒序，只回 version/status/checksum/created_at 元数据列，definition 走 workflow.get 按需拉取）；workflow 不存在返回 -32011 |
| `nodetypes.list` | 前端画布能力清单：类型、端口、`params_schema`（JSON Schema draft-07 子集 type/required/properties/enum/default，另带 `x-widget`/`x-label`/`x-help`/`x-secret` 扩展键，前端据此渲染参数表单；后端校验仍以 `Definition::validate` 为准）、supports_retry、side_effect；单条描述的唯一来源是 `NodeType::descriptor`（§5） |
| `secrets.list` | 已配置的密钥清单 `{name, source}`（source=stored（界面管理）\| env（`FLOW_SECRET_<名称>`）），按名称排序，永远不含值；前端给 x-secret 参数渲染可选名称，设置页按来源决定能否删除 |
| `secrets.set / secrets.delete` | 持久化密钥管理（§9.4）：set 写入（或覆盖）一个 AES-GCM 加密落盘的密钥（真值不出进程）；delete 只删 stored 来源，env 名字回 `deleted=false`。名称限 1..=64 个字母/数字/下划线 |
| `config.get / config.update` | 统一配置（§9.4）：get 回文件级配置（默认值+文件，**不含 env 覆盖**）+ 生效的 env 覆盖名单 + 文件路径；update 深合并且服务端校验后原子写回文件。`storage.database_url` 一律脱敏为 `"<set>"`，update 侧收到 `"<set>"` 表示保持原值。**重启生效**（进程启动时一次性装配） |
| `template.create / list / get / update / delete` | 可复用节点模板（§9.5）：画布片段（节点+内部边 JSON）+ 命名信封，`node_templates` 表（两后端同构）。create/update 做逐节点校验（类型已知 + `validate_params` + 边端点在片段内），**不跑** `Definition::validate`——片段不是完整定义；重名 -32012 |
| `run.start` | 经 Backend：SQLite 校验 published → insert initializing → 持久化 run_started → 启动 Driver，初始化错误回写 failed；Postgres 单事务原子创建（§9 差异说明） |
| `run.get / run.list` | 元数据 + live 标记；list 支持 status 过滤与 source（触发来源）过滤，词汇表外 -32010 |
| `run.stats` | 精确统计（GROUP BY，非采样）：`{workflow_id?}` → `{total, by_status}`，不带过滤时附带 `by_workflow: [{workflow_id, total, by_status}]` |
| `run.timeline` | 只读时间线：定义顺序 + 折叠后的节点状态。`nodes[].input` 是 node_started 写入时已脱敏限幅的快照；`nodes[].output` 是**节点级展示值**（按敏感键列表脱敏：节点输出可能是上游 HTTP 响应、会回显凭据）；顶层 `output` 是**数据面**，与 `run.get` / `run_completed` 事件逐字一致——同名字段必须同值，否则客户端从 timeline 重建输出会拿到污染数据 |
| `run.events` | 原始事件，`from_seq` 增量拉取。`node_log` 与状态事件同流同 seq（可观察性设计：日志回放/追流不用第二条管道） |
| `run.cancel` | 统一 SignalAck：活着的 run 交付取消（SQLite 仅本进程生效）；否则 conflict |
| `run.signal` | human_task 交付 / 副作用节点裁决（§6.7）。signal_id 在 Postgres 必填且重试复用，SQLite 可省（响应只回显客户端提供的，不伪造） |
| `run.signal_status` | 持久 inbox 落账查询；pg 专属（RPC 边缘 match 暴露），SQLite 返回明确的 invalid |
| `run.subscribe` | 订阅 `run.event` 通知，可按 run_id 过滤。SQLite 订阅者消费慢时收到 Lagged 丢事件，用 `run.events`（from_seq）补齐；Postgres 按游标轮询共享日志。run_id 不存在时流立即结束（不是报错、不是永久重试） |
| `schedule.create / list / update / delete` | cron 定时调度（§9.2）。非法 cron 表达式 -32010（标准 5 字段，本地时间）；list/create 响应带服务端算好的 `next_fire_at`（RFC3339），前端不解析 cron；update 部分更新，`input` 用双 Option 区分「不改」与「清空」（显式 null） |
| `webhook.create / list / set_enabled / delete` | webhook 触发器（§9.2）。token 即 URL 凭证，随机生成不可猜 |

订阅流结束条件的完整契约（两臂一致）：终态事件转发后自然结束；run 不存在
（PG reader 回放起点报 RunNotFound）或日志为空（SQLite 臂「文件已建、
run_started 未落盘」的初始化中断窗口，恢复已按 DB 投影标终态）时立即结束流，
绝不挂起等待永远不会出现的终态事件。

### 9.2 触发器：cron 调度与 webhook

除 `run.start` 手动触发外，run 还有两个自动入口，都在 `flow-journal-server`
进程内（`flow-rpc/src/journal_triggers.rs`）：

- **cron 调度器**：`[server].scheduler_enabled`（默认开启）+
  `journal_trigger_tick_secs`（默认 20s）。每 tick 扫描全部 enabled schedule，
  触发命令带稳定身份（schedule id + 触发点）落 journal 权威——同键重试不产生
  双 run；停机期间错过的触发点不追补；目标 workflow 没有 published 版本时
  本次跳过；
- **webhook HTTP 入口**：独立于 WebSocket 端口的 HTTP 服务
  （`[journal].http_addr`，默认 `127.0.0.1:9803`，与下载路由同进程）。
  `POST /hooks/<key>`：**Bearer token（部署 `FLOW_JOURNAL_TOKEN`）+
  Idempotency-Key 头必填**；key 未知或已停用 → 404（不区分，避免探测）；
  body 须为 JSON（空 body 视为 null 输入），作为 input 启动当前 published
  版本 → 200 `{"run_id": "..."}`；无 published 版本 → 409。同一
  Idempotency-Key 重放不产生第二个 run。

四个触发入口都在 run 归因里标注：`run.start` = manual、调度器 = schedule
（detail 为 schedule id）、webhook = webhook（detail 为 key）、sub_workflow
子 run = sub_workflow。

错误码（v2 单一来源）：`-32001` unauthorized、`-32011` 不存在、`-32012` 冲突、
`-32020` COMMITTED_NOT_VISIBLE、`-32021` 超限、`-32602` 参数非法、`-32603`
内部错误。JSON-RPC 允许整体省略 params（到达 null），解析层统一归一为 `{}`。

### 9.1 wire protocol 变更记录（适配层重构）

> 历史记录：本节是 v1 时代两份 RPC 实现合并时的对齐清单，信号/id 语义已被
> v2 的 `run.signal` 原生命令回执取代，保留作考古依据。

后端适配层重构（两份 RPC 实现合一）带来的线上协议变更，客户端对齐依据：

- `run.cancel` 响应：`{"cancelling": true}` → `{"delivered": true}`（语义未变：
  取消已受理。Postgres 落账侧携带 signal_id（缺省时服务端生成）；SQLite 同步
  交付回显客户端提供的 signal_id，缺省时不伪造——两臂对「提供了 id 的调用」
  回显一致）；
- `run.signal` 请求新增可选 `signal_id`（Postgres 必填）；响应新增
  `signal_id` / `event_seq` 字段（仅在真实存在时返回）；
- `run.signal_status`：SQLite 后端从「方法不存在」变为「存在但返回 -32010
  明确错误」；Postgres 语义不变；
- 响应面：自托管 web 前端不读上述响应体，无需变更；请求面：`run.signal` 必须
  携带 `signal_id`（Postgres 必填，前端已在 `web/src/api/flow.ts` 生成）；
  外部消费者按本节对齐。

### 9.3 命令行客户端（flow-cli）

`flow-cli`（`crates/flow-cli`）是 §9 方法面的命令行封装：**纯 RPC 客户端**，
只连 flow-journal-server 的 JSON-RPC WebSocket（`--url` / `FLOW_RPC`，缺省
`ws://127.0.0.1:9802`），认证走部署 token（`FLOW_JOURNAL_TOKEN`），不依赖
任何存储 crate、不读 FLOW_BACKEND、不碰 `data_dir`。因此不存在「绕过 RPC
校验直接操纵 SQLite / 事件日志」的第二条写入路径。

```bash
flow-cli workflow list | get | versions | create | update | publish | delete
flow-cli workflow import <file> [--name N] [--no-publish]
flow-cli workflow export <id> [-o file] [--version N]
flow-cli run start <id|name> [--input JSON|@file|-] [--version N] [--detach] [--timeout S]
flow-cli run list | get | events | timeline | cancel
```

- **导入导出是客户端编排，不是新 RPC 方法**：`import` = `workflow.list` 按 name
  反查 → 不存在则 `create` → `update`（新版本）→ `publish`；`export` =
  `workflow.get`（+ `workflow.list` 反查 name）。信封格式 `{name, definition}`
  与 `examples/*.workflow.json`、`examples/run-workflow.mjs` 一致，导出的文件可
  直接再导入或喂给 run-workflow.mjs。服务端语义（不可变版本快照、只有
  published 可执行、有 run 拒删）一条不变，编排下沉成新方法只会养出第二份
  「导入」规则。失败不回滚：update/publish 被拒时留下一个 latest 0 的 workflow
  壳，重新 import 同名会复用它。web 端工作流列表页的「导入」按钮是同一份
  编排（`web/src/state/workflow-io.ts`）：信封或裸 definition、name 回落
  文件名（剥 `.json`/`.workflow`）、同名追加版本、默认发布；
- **手动触发**：`run start` 接受 workflow_id 或 name（本地解析），默认轮询
  `run.get` 到终态并把 run 输出打到 stdout；`--detach` 只打印 run_id。等待用
  轮询不用 `run.subscribe`：一次性等待没有推送优化的收益，反而多一条会断开
  的通道要兜底。`awaiting_resume`（human_task / 待裁决）不是终态，CLI 不提供
  `run.signal`——信号仍走 web 前端或 RPC，CLI 不引入第二条写入路径；
- **输出纪律**：文档（definition / run 输出 / run 记录）走 stdout，说明与进度
  走 stderr，于是 `flow-cli workflow get X > def.json` 拿到的是纯 JSON；
  `--json` 打印服务端原始结果（供 jq），人类可读表格（CJK 对齐、id 截断
  12 字符、时间转本地时区）为缺省形态；
- **退出码契约**（测试钉住）：0 成功；1 本地错误（文件/JSON/连不上/超时/
  非交互环境缺 `-y`）；2 服务端 RPC 错误（错误码原样透出）；4 触发的 run
  终态为 failed/cancelled——CI 据此区分「命令打错」与「工作流失败」；
- **破坏性操作要确认**：`workflow delete` 在非交互 stdin 下必须显式 `-y`
  （绝不挂起等输入），服务端仍会对有 run 记录的工作流返回 -32012。

### 9.4 统一配置（flow-config）与持久化密钥

进程配置曾全部散落在 30+ 个 `FLOW_*` 环境变量 + CLI 参数里。现收敛为一份
TOML（`flow.toml`，`crates/flow-config`，叶子 crate），分层加载
（高覆盖低）：**CLI `--config` > 环境变量 > 配置文件 > 内置默认值**。
搜索顺序：显式路径 → `FLOW_CONFIG` → `./flow.toml` → 平台配置目录。
分区：`[server]`（调度器开关与 journal 触发器 tick）、`[storage]`
（backend/data_dir；backend 只有 `journal`）、`[execution]`
（mode/executor_bin/x_max/[remote]）、`[agent]`、`[journal]`。
v1 的 `[server].rpc_addr/http_addr/scheduler_tick_secs`、`[storage].database/
database_url` 与整个 `[pg]` 段已删除（deny_unknown_fields：存量旧键显式报错）。

- **加载必须发生在构造 tokio runtime 之前**。`flow-journal-server` /
  `flow-agent` / `flow-journal-dev` 在 main 里同步 `Config::load`；
- **存量 `FLOW_*` 环境变量保持原语义**（v1 的 `FLOW_ADDR` / `FLOW_HTTP_ADDR` /
  `FLOW_DATABASE_URL` / `FLOW_ROLE` / `FLOW_*_MS` / `FLOW_MAX_RUNS` 已随后端
  删除）；env 覆盖按分区手写合并（显式可 grep），生效名单记录在
  `Loaded.env_overrides` 供设置页展示「这些项重启后仍会被 env 覆盖」；
- **明确不收敛**：`RUST_LOG`、`FLOW_SECRET_*`（见下）、journal token（每工作区
  凭据）、执行协议冻结常量（contract.rs，改即协议变更）、executor FD 槽位、
  `FLOW_RUN_LOG_BUDGET` 等 engine 内部预算（读取点在 driver 深处，维持 env）；
- **config.get / config.update**：工作副本是**文件级**视图（默认值+文件，env
  不进副本）——文件才是用户可编辑的真相；update 深合并（分区级替换）→
  `validate` → 原子写回（tmp+rename），坏配置永远不落盘。全部字段重启生效；
- **持久化密钥**（`flow-engine/src/secrets_store.rs`）：`secrets.set/delete`
  写 AES-256-GCM 加密的 `<data_dir>/secrets.json`，主密钥 `<data_dir>/secret.key`
  （随机 32B，unix 0600）。进程入口把 `SecretFileStore` 安装为进程级
  `SecretSource`；`get_secret` / `list_secrets` **stored 优先、env 兜底**——
  dispatch 前注入、真值不落盘不展开的既有边界不变（§5 x-secret）。主密钥
  丢失（被换/被删）表现为解密失败=未配置，不炸进程。

### 9.5 可复用节点模板

画布片段（若干节点 + 内部边）的命名持久化，跨流程、跨设备复用：

- **存储**：`node_templates` 表（id/name UNIQUE/category/nodes JSON/edges
  JSON/时间戳），journal 权威事件 + SQLite 投影，`journal_views::template_*`
  物化读面；
- **校验是逐节点的**：类型已知（`NodeType::parse`）+ `validate_params`
  （与整图 `Definition::validate` 同源的参数规则，为模板开放为 pub）+ 边端点
  在片段内 + condition 出边端口合法。**不跑整图校验**——片段没有
  start/end 约束，这是「模板≠隐藏工作流」的根本原因（绕开 `workflow.update`
  的整图不变量，而不是破坏它）；
- **前端只做一份片段落地逻辑**：`editor.ts` 的 `insertFragment`（id 重生成、
  偏移、内部边重连、max_instances 检查、入 undo 栈）同时服务内存剪贴板
  （粘贴）与模板插入；模板拖放用独立 MIME（`application/flow-template-id`），
  面板按名列出、可重命名/删除。未知类型沿用 `unknownTypeDesc` 占位降级。

## 10. JS 沙箱（expr.rs）

rquickjs：无 IO、CPU 同步执行（放 `spawn_blocking`，不占死 tokio worker）、
interrupt handler 超时中断（默认 2000ms，`timeout_ms` 可调）。

沙箱边界的证据有两条，都不靠人读：

1. **行为测试**（`expr.rs` 的 `sandbox_has_no_std_os_or_module_loader`，随
   `cargo test` 常驻）：`typeof std/os/quickjs` 必须是 `"undefined"`，
   `globalThis` 上的 `require/process/fetch/XMLHttpRequest` 全部不可用，
   静态 `import` 必须被拒。升级 rquickjs 后测试自动重新验证，
   不需要任何人记得去考古；
2. **构建证据**（2026-09 对 rquickjs 0.14 重新核对）：`rquickjs-sys` 的 build.rs
   仅编译 libregexp.c / libunicode.c / quickjs.c / dtoa.c，编译产物不含
   quickjs-libc.o——`std`/`os` 模块的唯一来源没有被编进引擎。
   引擎也未注册任何模块加载器：脚本里连 `import` 都不可用。

- `eval_body`：script 节点，函数体带 `return`，可用 `input` 与 `nodes`（前驱输出快照）；
- `eval_expr`：condition 节点；
- `expand_templates`：**所有节点 params 的统一前置展开**（`exec::expand_params`，
  唯一例外是 `NodeType::opaque_params`——script 的 `code` / condition 的 `expr`
  等用户 JS 字段，其中的 `${}` 是 JS 模板字面量，展开即破坏用户代码）。
  http_call 的 url/headers/body 只是这条规则的头号用户，delay 的 `ms`、
  sub_workflow 的 `input_mapping`、harness 的 prompt、email 的 subject/body 等同规则生效。
  展开由 driver 的 `start_node` 在**写 `node_started` 前**调用一次（展开结果 =
  输入面快照，§6.1），exec 层不再展开。展开的数据面与 `nodes` 一致：
  只读 `input`（run 输入）与直接前驱输出快照。
  **限制**：`${}` 内不能包含 `}`（按第一个 `}` 截断），不支持嵌套对象
  字面量等复杂表达式。两条规则按「整个值是否恰为一个模板」分流：
  **整值模板**（`"${expr}"` 独占整个字符串）按求值结果的**原类型**替换
  （数字/对象/数组穿透，`undefined` 归一 `null`）——`input_mapping` 的
  结构化传参依赖这条；**插值模板**（模板嵌在更长文本里）保持字符串语义，
  非字符串结果 `JSON.stringify` 后拼回原位。整值字符串模板在两条规则下
  结果相同，存量定义零行为变化。

脚本 `console.*` 经宿主函数桥接到日志发射器（log/info→stdout，warn/error→
stderr，格式化在 JS 侧完成）：console 输出就是普通节点日志，走同一 budget/
通道/`node_log` 落盘路径——condition 求值路径上没有 logger（disabled，输出丢弃）。
预算：per-run 默认 10000 条 debug/info（`FLOW_RUN_LOG_BUDGET` 可调），超限
丢弃并写 Warn 摘要行；单行 8KB 截断。**上限只管 debug/info 合计**，
warn/error 永久放行且**不消耗 debug/info 预算**——否则一个刷 warn 的节点
能把整条 run 的 info 日志预算吃光。

数据契约：进出沙箱的整数经 **BigInt 边界**精确无损——|v| > 2^53 的 i64/u64
（雪花 ID、高精度金额）注入沙箱时转 BigInt，出口按十进制精确还原 i64/u64，
透传、比较（用 `123n` 字面量）、模板展开均不再静默舍入；插值模板对 BigInt
用 `toString()` 拿精确数字。代价是响亮失败取代静默错值：BigInt 与 Number
混算抛 TypeError、`JSON.stringify`（含 BigInt）抛错、超出 i64/u64 的 BigInt
结果（如 `2n ** 64n`）在出口被拒。注意：脚本里的 Number 字面量仍是
ECMAScript 语义（`9007199254740993` 字面量本身已舍入为 2^53），需要精确请
写 n 后缀或从 input 传入；非整数浮点仍走 f64。纯 Rust 路径（start/delay/
human_task/子 run 透传、事件日志 i64/u64 序列化）本就无损，重试/重放同样
无损，决定论不变。

## 11. 外部 HTTP 调用语义

http_call params 经 `${}` 统一展开后发请求（默认超时 30s；method 取自
`HTTP_METHODS` 白名单，定义层校验后执行层再验一次）。可选 `proxy`
（`http(s)://[user:pass@]host:port`）按请求建独立 client，缺省用共享 client；
代理 URL 非法是配置错误（fatal）。
输出 `{status, headers, body}`（body 能解析为 JSON 则解析，否则原样字符串）。
失败分类（email 经 `post_json_bearer` 共用同一份，唯一补充是 429 限流
也可重试）：

- 连接失败/超时 → retryable（副作用不明确或未发生，交给重试策略）；
- 5xx → retryable；4xx → fatal（请求本身的问题）；email 另认 429 → retryable；
- **响应体读取失败（连接中途断开）→ retryable**——绝不带着 200 + 空 body 记成功。

harness 节点（外部 agent CLI：`command` + `args`，可选 `workdir`/`timeout_ms`
默认 300s）：prompt 经 stdin 写入，捕获 stdout/stderr。退出码 0 → 输出
`{exit_code, stdout, stderr, result?}`（stdout 去空白后能解析为 JSON 对象/数组
时附 `result`）；非 0 → fatal，错误消息带 stderr 尾部；超时 → 杀进程
（kill_on_drop）、retryable；spawn 失败（命令不存在）→ fatal。
email 节点（Resend 兼容 `POST {endpoint}`，默认 `api.resend.com/emails`）：
body 为 `{from, to, subject, text}`（纯文本正文），输出 `{status, id}`。
email 的 `api_key` 是 x-secret 名称，执行前注入真值，只进 Authorization 头，
不进输出/错误消息/事件。

自动重试不保证外部副作用只发生一次：POST 超时或断流时下游仍可能已提交。
启用多次重试需要调用方接受重复风险，或在 headers/body 中使用下游支持的稳定幂等键。
人工裁决只解决崩溃后结果不明，不把任意 HTTP 请求变为恰好一次执行。

## 12. 不变量清单（改动前必须知道）

1. 执行状态由完整日志重建；初始化结果和日志缺失诊断按 §7 单独处理；
2. 副作用之前必写 `node_started`；恢复还覆盖等待重试、信号消费与收尾窗口；
3. `RunState::fold` 是唯一状态转移函数，恢复与时间线共用；
4. 跳过必须推进到不动点；每个持句柄的 slot 恰欠一条 DriverMsg；
   **节点级执行状态机只有一处**——`driver::NodeSlot` + `slots` map，终止判定
   （`slots.is_empty()`）与 abort 清理（`abort_inflight`）同源；一个节点同时只
   占一个槽位（§6.1）；
5. **节点状态与输出只有一份，都在 `NodeRecord` 里**（§4）：输出是
   `NodeRecord.output` 字段，attempt 与 error 从 `state` 派生
   （`NodeState::attempt` / `::error`），三者都不另存副本——此前 `error`/
   `attempts` 各有一份影子字段，每个事件分支都要手工同步，`error` 副本连
   `NodeSkipped` 都没覆盖；输出曾被挪进 `RunState::outputs` 那把与 `records`
   同键的独立 map，同步义务并未消失（四个节点终态分支照样各自处置旧输出），
   只是多出「两把 map 键集可能不一致」的面。四个分支改的是同一个 `rec`，
   不可能只改一处。**四个节点终态分支的字段处置对称**：`node_completed` /
   `node_failed` / `node_skipped` 都清 `output` 与待消费的 `last_signal`
   （`node_skipped` 曾是唯一漏清 `last_signal` 的分支）。节点执行输入面
   `nodes` 只暴露直接前驱的输出（重放决定论，见 §10），经 `RunState::node_output` 读；
6. 端口规则：condition 必须 true/false，其余必须无端口；**同一 condition 的
   多个端口不得指向同一节点**（否则 AND-join 会把被选中分支的下游整个跳过，
   §5 规则 4）；
7. end/run 输出共用「单数透传、复数映射」规则（`singular_or_map`）；
8. **状态词汇表的唯一真相是 `DbRunStatus::ALL`**：`as_str` 与它对拍，
   `is_valid_str` / `is_terminal_str` / `is_active_str` / `sql_in_list` 全部派生，
   `flow-pg` 的 `CHECK` 列表也由它生成（排除 PG 不可达的 `initializing`）。
   此前 `is_valid_str` 是与 enum 不联动的手写 `matches!` 表、`CHECK` 列表是第三份
   手写拷贝——加变体编译器不提醒，症状是该状态写入时当场被拒、run 从恢复扫描里
   静默消失。**折叠相位到落库状态的映射同样只有一份**：`RunPhase::as_db_status`
   （`flow-engine`），两个后端的恢复回填（`recover_unfinished` 与
   `reconcile_terminal`）共用，不各写一遍 match。
9. 可重试失败非终态；不可重试失败由 fold 持久推导，恢复不会变为成功。
   **`run_cancelled` 清掉 `fatal_error` 与 `output`**——取消不是失败，漏清会让
   恢复回填给 cancelled 的 run 行带上节点失败原因（§4）。
10. 信号先校验，再持久化、消费和确认；无效请求不改变 run 终态。
11. sub_workflow 的 child_run_id 确定性派生（`{父run}:{节点}:{attempt}`）并随
    node_started 落盘；重放沿用已落盘 id 附着既有子 run，重试派生新 id。
12. 父子 run 各有独立事件日志；嵌套深度上限 8，depth 经 run_started 持久化；
    子 run failed/cancelled 传导为父节点 fatal，取消级联是 best-effort。
13. 平台故障（IO/Backend/日志损坏/引擎 Bug）与工作流失败分离：前者投影
    `awaiting_resume` 等待恢复/人工，只有工作流语义失败才写 run_failed 终态；
    所有权丢失（LeaseLost）则静默退出，三者互不冒充。
14. 一个 run 至多一个活 Driver（engine registry 原子占位 reserve_run）：
    重复 start/resume 是无操作，绝不允许第二个写者——EventLog 各自计数 seq，
    双写者必然产生重复 seq 与重复副作用。「活着的判据只有一条」：`reserve_run`
    与 `is_live` 都以「注册位存在**且**未 exited」为准（清理任务分两步：先置
    `exited` 再摘 key），两处不得对同一个 run 给出不同答案。跨进程维度由
    data_dir 排他 flock 强制（`SqliteBackend::open`，LOCK_NB 立即失败）：
    registry 管不住另一个进程的启动恢复，flock 管；同进程二次 open（独立 fd）
    同样被拒，flock 按 open file description 判定。
15. 节点 params 执行前统一 `${}` 展开（`exec::expand_params`），唯一例外是
    `NodeType::opaque_params`（用户 JS 字段）；展开数据面 = `input` +
    直接前驱输出快照，不扩大决定论边界。sub_workflow 的 `input_mapping`
    展开结果整体作为子 run 输入，缺省保持「父 run 输入」旧语义。
16. 展开恰好一次：driver 的 `start_node` 展开，exec 层不再展开——展开结果
    同时是执行期 params 与输入面快照（脱敏后随 `node_started` 落盘）。
17. `node_log` 不是恢复状态：fold 跳过它、恢复不重建它；日志是进程级持久
    （搭同文件严格事件的 fsync，终态事件前强制兜底 sync），订阅与
    `run.events` 照常送达。per-run 预算超限只丢 debug/info，warn/error 放行。
18. x-secret 参数在定义里只存名称，真值在 dispatch 前从 `FLOW_SECRET_<名称>`
    注入：不进模板展开、不进事件（`node_started.input` 写入时已按敏感键
    列表脱敏）、`secrets.list` 只回名称。真值缺失 = 节点 fatal。
19. **日志里出现定义外 node_id = 日志损坏**，恢复路径（单机 `resume_run` 与
    PG `drive_after_acquire` 同一处校验）按 `LogCorrupted` 拒，绝不驱动。漏掉
    的话那条幽灵记录停在非终态时，终止判定读的两个键集不重合（`plan()` 遍历
    `definition.nodes` 看不见它，`all_terminal()` 遍历 `records` 看得见）→
    slots 空而 `all_terminal()` 恒假 → 每次恢复都挂 `awaiting_resume`，run
    永久卡死、只能人工改数据。只读路径（`Engine::snapshot`、订阅回放）不校验，
    语义仍是「原样呈现磁盘」。
20. **恢复分类一条记录一个节点**（`RecoveryPlan` 是 `Vec<RecoveryAction>`，不是
    分组平行集合）：「进入人工裁决」与「既有裁决已落盘」合成 `Adjudicate
    { received }` 一条。拆成两个 Vec 时两者一致性只靠 `classify` 的 push 顺序
    维持，一旦只写一半，消费端查不到 `Adjudicating` 槽位 → `InvalidSignal`
    冒泡 → 被判成工作流失败 → **把引擎内部记账不一致写成用户的 run_failed 终态**。
    已落盘但载荷非法的裁决按 `LogCorrupted` 归平台故障挂起，同属这条出口。
    `apply_recovery_plan` 分**显式两趟**（先登记全部 `Adjudicating` 槽位、后消费
    全部裁决），不依赖 `classify` 的 push 顺序——`actions` 来自 HashMap 迭代，
    顺序不确定。混成一趟会多投影一次 `Running`：`apply_signal` 消费掉一个裁决后
    要看「还有没有 `Adjudicating` 槽位」，而另一个待裁决节点此时尚未登记，引擎
    误判「已无待裁决」把仍挂在人工裁决上的 run 投影成 running。
21. **订阅追平后等唤醒、缺口只在读不到新数据时才退避**（run_tail，§3.4）。
    前者没有标记时会无限重读读取面——订阅一个长跑 run 就把一个核烧满，且每次
    重读整份日志；后者把「有缺口」直接当成退避理由会让补齐那一整步被跳过，
    10s 定时器醒来又被转回退避，读永远不发生、订阅静默停摆（终态都送不出去）。
    两条分别由 `live_run_at_tail_blocks_instead_of_spinning` 与
    `gap_created_by_live_segment_is_filled_by_retry` 钉住。

## 13. 测试策略

运行 `cargo test --workspace --all-targets --locked` 与
`cargo clippy --workspace --all-targets --locked -- -D warnings`。覆盖约定：

### 测试代码怎么摆（Rust 的四层位置约定）

| 层 | 位置 | 规则 |
| --- | --- | --- |
| 单测 | `src/<模块>.rs` 末尾 `#[cfg(test)] mod tests;` | 能碰 `super::*` 私有项；模块超过 ~150 行就拆到 `src/<模块>/tests.rs` |
| 集成测试基建 | `flow-test-support`（`io`：TempDir / free_port） | 跨 crate 共享的测试代码只能放这里（v1 的 `pg` docker 基建已随后端删除） |
| 集成测试 | `tests/<主题>.rs` + `tests/common/mod.rs` | 只走 public API；共享夹具放 `common`（放子目录，cargo 才不会把它当成独立测试 target） |
| 端到端 | `backend-e2e/tests/*.rs`（`e2e_test!` 宏） | 真起进程；`common` 里只有 e2e 专属部分 |

三条硬规则：

- **不许为了让 `tests/` 通过而把内部 API `pub` 出去**——测试事实不能在公开
  API 上开洞。
- **共享夹具不逐文件复制**：backend-e2e 的 harness 在 `src/common`，
  flow-engine 的执行夹具在 `tests/common`。
- 被测进程与产品进程同一份装配（backend-e2e 的 `flow-journal-server-e2e`
  与 flow-rpc 的 `flow-journal-server` 同一 `serve_journal_product`）——
  「被测的就是生产的」。

### 具体覆盖

- **backend-e2e（`crates/backend-e2e`）**：v2 产品面的完整端到端矩阵。每个用例
  独占一个临时 journal 目录（v2 唯一权威），真起被测服务进程
  （`CARGO_BIN_EXE_flow-journal-server-e2e`——本 crate 自己的薄壳，与
  flow-rpc 的 `flow-journal-server` 是同一份 `serve_journal_product` 实现，
  只是改名避免两个 bin 写同一个 `target/debug/flow-journal-server`；含
  SIGKILL 崩溃恢复与重启），HTTP stub 全部本地 TcpListener（确定性，不依赖
  外部网络）。覆盖：workflow 生命周期与「只有 published 可执行」、run 执行
  （script/condition/delay/输出收集/skip 传播/重试/JS 沙箱/harness/email）、
  human_task 信号与取消、http_call 分类与模板、sub_workflow（透传/失败传导/
  取消级联/深度上限）、schedule 真触发与 webhook HTTP 全分支、订阅（回放/
  增量/未知 run 即结束）、SIGKILL 恢复与人工裁决、错误码映射、触发器 CRUD 的
  幂等与回执恢复。入口：`cargo test -p backend-e2e`（无外部依赖）。
- **flow-rpc**：`tests/contracts.rs`（进程内 v2 协议契约：token 认证、写命令
  幂等键、-32020 回执、config.get/update 落盘、secrets、模板与触发器 CRUD、
  错误码分层）+ `tests/ws_journal.rs`（真起 flow-journal-server 进程的 WS
  连接/订阅/SIGKILL 恢复）+ `journal_v2.rs` 内联单测。
- **flow-backend**：`tests/journal*.rs` 覆盖权威命令语义（删除保护、版本复用）、
  验收用例、容量上限、import-legacy 历史保全、IPC 执行装配与 remote 执行
  （ops/reconnect/TLS）；`journal_driver.rs` / `journal_commands.rs` /
  `journal_views.rs` 带内联单测。
- **flow-engine**：定义模型与校验（`model.rs` 表驱动单测：表 ↔ enum 双向覆盖、
  必填清单真的在校验、opaque/secret 参数清单）、JS 沙箱边界行为测试
  （`sandbox_has_no_std_os_or_module_loader`，§10）、表达式 BigInt 边界、
  journal_state 折叠语义、执行协议帧/传输、密钥存储；`tests/observation.rs`
  集成验证观测日志。
- **flow-journal**：`tests/{journal,maintenance,page}.rs` 钉住日志写读、
  离线维护与值分页契约。
- **flow-cli（`crates/flow-cli`）**：CLI 自己的契约。`flow-cli` 二进制按
  `CARGO_BIN_EXE_flow-cli` 作为**子进程**跑，服务端真起
  `flow-journal-server`（隔离目录、随机端口）。钉住：import→list→get/versions
  →run（等待终态/stdout 输出）→export→roundtrip 全链路、`--input` 三种形态、
  `--detach`、退出码契约（本地 1 / RPC 2 / run 失败 4）、非交互环境 delete
  缺 `-y` 必须报错而非挂起。入口：`cargo test -p flow-cli`。
- **flow-executor / flow-agent**：执行侧协议契约（audit window、relay、
  runtime）随 crate 单测。
- **backend-perf（`crates/backend-perf`）**：性能压测 harness（黑盒，真起被测
  进程）。被测进程是 `flow-perf` 二进制的自举服务模式（`FLOW_PERF_SERVE=1` →
  `serve_journal_product`，与 flow-journal-server 同一装配）。场景：run 执行
  吞吐（chain/fanout）、RPC 读路径延迟、订阅推送延迟、崩溃恢复耗时。报告为
  最近秩分位数（p50/p90/p95/p99）文本 + 可选 JSON。入口：
  `cargo run -p backend-perf -- --help`。
- **前端（`web/`）**：`npm test`（vitest 单测：状态机、labels、journal-log、
  SchemaField 等）+ `npm run test:e2e`（Playwright：global-setup 自行构建并
  拉起隔离的 flow-journal-server，跑编辑器/run/触发器/设置页/journal 页）。

## 14. 未做

- delay 剩余时间恢复（当前崩溃/恢复后整段重放）；
- 多 end 被跳过时与真 null 输出的显式区分；
- `run.start` 客户端幂等键（当前双击 = 两个 run，服务端 uuid 生成）；
- 跨进程 SIGSTOP 场景下的真实副作用计数验收（副作用准入已用确定性前缀测试钉住）；
- ~~多节点对等执行~~：v1 Postgres 后端（租约/inbox/接管）已整体删除；
  远程执行走 `flow-agent`，多后端能力不再提供（见本文头部状态说明）。

### v1/v2 已知语义差异（切换日的行为变更面）

- **retry 范围**：v1 对 retryable 的 http/harness/email 失败（5xx/超时/429）自动
  重试（§6.5）；v2 的重试只覆盖纯计算节点（script/condition），外部操作
  失败一律进 uncertain 等待人工裁决——**绝不自动重发已授权的外部操作**
  （JSONL_DEVELOPMENT「不会自动重发未知外部操作」，v2 核心安全立场）。
- **子 run id 派生**：v1 确定性派生 `{父run}:{节点}:{attempt}`（不变量 11）；
  v2 用 uuid v7 + 「WaitRegistered 与子 RunStarted 同事务」保证原子性与幂等，
  不依赖派生式。
- v2 的 run.start 已要求稳定 `request_id`（写命令幂等键），v1 的
  `run.start` 客户端幂等键仍未做（见上）。

v1（SQLite 权威）→ v2（journal 权威）的切换已完成（2026-10-10 起唯一后端；
历史数据舍弃、不迁移），全程记录见
[SQLITE_V1_TO_V2_MIGRATION.md](SQLITE_V1_TO_V2_MIGRATION.md)。
