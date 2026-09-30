# flow 工作流引擎设计方案

> 状态：单机实现 + 对等模式多节点执行（Postgres 后端；租约 / inbox / 接管的设计
> 记录在 `flow-pg` 各模块的头注释里）；
> `cargo test --workspace --all-targets --locked` 全绿（含 backend-e2e 的
> Postgres 变体，需 docker；缺 Postgres 时对应用例跳过，其余全绿）。验证命令与覆盖范围见 §13。
> 本文描述当前代码的实际语义，是后续开发的权威参考。分布式子系统的设计不再单列
> 文档（曾有一份 `DISTRIBUTED.md`，删除时已有 40+ 处引用指向不存在的章节）——
> 逐条落在 `flow-pg` 各模块的头注释里，改代码时顺带就在改设计。
> 架构焊点：**SQLite + events.jsonl 是默认与权威后端**（canonical）；Postgres 是
> 可替代后端，两者统一在 `flow-backend` 的 `AnyBackend` 闭集枚举之后（§2）。
>
> **容量定位（部署选型）**：SQLite 定位是**单机并发**——单写者 + fsync 崩溃边界，
> backend-perf 实测 ~25 runs/s（16 在飞，e2e p95 < 1s）；**并发再高就换 Postgres**
> （`FLOW_BACKEND=postgres`，同一套 API，实测 ~60-70 runs/s @16 在飞，且可多节点
> 水平扩展 executor）。两者语义等价（backend-e2e 双后端同契约），选型只看容量。

> **⚠️ 安全边界（部署前必读）**：本服务**没有任何认证/授权**——JSON-RPC
> WebSocket 谁连上谁就是管理员。默认只监听 `127.0.0.1:9800`；对外暴露
> （如多节点部署的 gateway）必须在外层自备 TLS + 认证（反向代理 / 内网 ACL）。
> `http_call` 节点是**任意出站 HTTP**（可打内网地址与云元数据端点
> 169.254.169.254），`script`/`condition` 是沙箱内任意 JS——工作流定义事实上是
> 可执行代码，只允许可信用户创建与发布。出站白名单/沙箱网络隔离未实现。

## 1. 目标与边界

flow 是一个工作流执行引擎：发布不可变的流程定义（DAG），以 run 为单位执行，
进程崩溃后从完整的磁盘日志恢复。日志丢失或损坏时隔离并报告错误，不猜测副作用。
前端拖拽画布已实现：`web/`（Vue 3 + @vue-flow/core，直连本服务的 JSON-RPC WebSocket）。

明确的非目标（v1 范围决策）：

- 不做通用 DSL——表达式与脚本统一用 JavaScript。
- 多节点执行：对等抢占模式（peer）已在 Postgres 后端实现（租约、持久 inbox、
  副作用边界；设计记录在 `flow-pg` 各模块的头注释里）；中心指派模式未实现（见 §14）。

节点运行可观察性（node_log 事件流、输入面快照、日志控制台）的完整设计见
`docs/observability-design.md`；本文只记它与状态机相关的契约（§3.1、§6.1、§10）。

## 2. 总体架构

```
crates/
  flow-engine   执行引擎。Driver 只依赖 RunEventSink 后端边界（Phase 0），
                单机后端走 event.jsonl，不依赖任何存储实现
  flow-dto      领域 DTO 与状态词汇表的单一来源（零依赖叶子）：
                WorkflowVersion / WorkflowSummary / RunRecord / DbRunStatus。
                存储层持久化的本来就是引擎域数据，不维护第二份拷贝
  flow-store    SQLite：workflow / workflow_versions / runs 元数据（单机后端）
  flow-backend  后端适配层：`AnyBackend` 闭集枚举（**不是 dyn trait**）
                屏蔽两种架构选择。SQLite + event.jsonl 是默认与权威实现
                （`sqlite.rs`，即本文档描述的全部语义）；Postgres 是可替代实现
                （`pg.rs`；分布式设计记录在 `flow-pg` 各模块的头注释里）。公共面只含两个
                后端都诚实实现的方法；
                初始化协议、信号落账、订阅推送的差异在边界内吸收；
                「只有 published 可执行 + 创建前校验」单点在 resolve_runnable_definition
  flow-pg       Postgres 后端实现：共享日志、epoch 租约、持久 inbox、executor
  flow-rpc      jsonrpsee WebSocket 服务（bin: flow-server）；**只依赖
                `AnyBackend` 枚举**，不感知 flow-store / flow-pg，也不读 FLOW_BACKEND；
                进程内还跑 cron 调度器与 webhook HTTP 入口（§9.2）
  flow-cli      命令行客户端（bin: flow-cli，§9.3）。**纯 RPC 客户端**：只连
                flow-server 的 WebSocket，不依赖 flow-backend / store / pg，
                也不读 FLOW_BACKEND——CRUD 与触发语义唯一来源仍是 RPC 那一份
```

依赖方向（不可反转）：

```
flow-rpc ──> flow-backend ──> flow-engine ──> flow-dto
                         ├──> flow-store ──> flow-dto
                         └──> flow-pg ────> flow-engine, flow-dto
flow-pg ──> flow-store   ✗（两个后端互相独立，互不感知）
flow-engine ✗ flow-*     （引擎不依赖存储；状态出口走 RunEventSink trait）
flow-rpc ──> flow-store / flow-pg   ✗（上层不感知具体后端）
flow-cli ──> flow-server（WebSocket 客户端）──> 上面的整条链路
flow-cli ──> flow-store / flow-pg ✗   ✗（CLI 不碰存储与事件日志，
                没有第二条写入路径；SQLite / Postgres 对 CLI 行为一致）
```

后端选择只在进程入口发生一次：`flow_backend::open_from_env()` 按
`FLOW_BACKEND=sqlite（缺省）| postgres` 构造 `AnyBackend`，之后整条 RPC 链路
只看枚举。闭集枚举而非 trait 对象：每加一个方法编译器逼着两个臂都写完，
不存在某个后端静默继承错误默认实现的坑。

运行：`cargo run --bin flow-server`。环境变量 `FLOW_ADDR`（默认 `127.0.0.1:9800`）、
`FLOW_DB`、`FLOW_DATA_DIR`、`FLOW_HTTP_ADDR` 与 `FLOW_SCHEDULER`（§9.2）；
Postgres 模式另见 `flow-pg/src/config.rs`
（`FLOW_BACKEND`、`FLOW_DATABASE_URL`、`FLOW_ROLE`、`FLOW_LEASE_TTL_MS` 等）。

**SQLite 模式的进程模型（焊死三件套）**：单进程（open 时对 `data_dir` 目录
fd 持排他 flock，第二个实例立即失败——多节点清用 postgres 后端）·
单线程（flow-server 跑 current_thread runtime，一个 OS 线程；异步任务仍
并发，JS 求值/文件 IO 经 spawn_blocking 走独立阻塞线程）·单连接
（SQLite 连接池 max=1，进程内 DB 访问全串行，SQLITE_BUSY 结构性消失）。
runtime 形态选择只在二进制薄壳 main 发生（`flow_backend::
prefer_current_thread_runtime`），lib 内的请求处理路径依旧不感知
FLOW_BACKEND。

## 3. 核心数据结构：事件日志是唯一权威

这是整个系统最重要的设计决策，其余一切都从它推导。

**磁盘上的 `data_dir/runs/<run_id>/event.jsonl` 是 run 执行状态的唯一权威。**
SQLite `runs` 表记录初始化结果并提供查询索引，内存执行状态由日志折叠得到。
初始化必须先插入 `initializing` 元数据，再持久化 `run_started`，才可派发节点；
Driver 启动后回填 `running`。终态也必须先写事件，再更新索引。
恢复时以完整日志为准回填（`recover_unfinished`），初始化失败和日志缺失见 §7。

### 3.1 事件模型

每行一个 JSON `Envelope`：`{seq, ts, run_id, type, ...}`，`seq` 从 1 严格连续。
事件全集：

| 事件 | 载荷 | 语义 |
|---|---|---|
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

## 4. 折叠器：一个状态机，两个消费者

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
`sub_workflow`、`llm`（OpenAI 兼容 chat/completions）、`email`（Resend 兼容发信）。
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
渲染」，`llm.prompt` / `email.body` 同样是 code 组件但**要**参与 `${}` 展开；
`x-opaque` 才是「这段内容是 flow 自己的 JS 语法，不参与展开」。

`node_type_table_is_exhaustive` 钉住表与 enum 双向覆盖（数组长度由类型标注编译期
保证，内容对应关系由测试保证）；`required_params_are_actually_enforced_by_validate`
与 `opaque_params_are_exactly_the_js_bearing_fields` 钉住派生后的规则仍成立。

`Definition::validate()` 在保存与发布时强制（建图即校验，不等到运行）：

1. 节点 id 非空且唯一；类型已知；按类型校验必填参数（清单来自 descriptor）
   （script: `code`，condition: `expr`，delay: `ms`，http_call: `url`，
   sub_workflow: `workflow_id`，llm: `api_key`/`model`/`prompt`，
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

**密钥参数（x-secret）**：params_schema 里标 `x-secret` 的参数（llm / email 的
`api_key`）在定义里只存**名称**，真值执行前从 `FLOW_SECRET_<名称>` 环境变量注入
（`secrets.rs`）。`workflow.update` 提前校验每个名称都已配置，配置错误挡在
落库前（`missing_secrets`）；模板名称要到执行期展开才能确定，缺失时节点 fatal。
真值不参与模板展开、不随 `node_started` 落盘，`secrets.list` 只回名称列表。

## 6. 执行引擎（Driver）

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
|---|---|---|---|
| `Running` | 节点执行中，或退避计时器在飞 | 是 | `result_rx` 的 `Done` / `RetryDue` |
| `AwaitSignal` | human_task 已落 `node_started`，挂 oneshot 等 `run.signal` | 是（等 oneshot 的任务） | `run.signal` 交付后回传 `Done` |
| `Adjudicating` | 崩溃/接管残留的副作用节点，等人工裁决 | **否** | `run.signal` 裁决（不经 `result_rx`） |

转移表（`∅` = 不在 slots 里）：

| 起始 | 事件 | 终止 |
|---|---|---|
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

- 可重试：连接失败/超时、HTTP 5xx、**响应体中途断流**、429 限流（仅 llm/email）；
- 致命：JS 抛错、参数校验失败、HTTP 4xx（请求本身的问题）。

这份分类 http_call / llm / email 共用同一套习惯（后者经 `post_json_bearer`）。

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
|---|---|---|
| Running 纯节点（start/end/script/condition/delay） | **重放**：attempt+1 | 无外部副作用 |
| Running `http_call`，无裁决信号 | **人工裁决**：`awaiting_resume` | 请求可能已发出，不猜测 |
| Running `http_call`，已有裁决信号 | **消费裁决**：retry/succeeded/failed | 裁决已经持久化 |
| Running `human_task`，已有信号 | **补终态**（output=信号） | 信号已经持久化 |
| Running `human_task`，无信号 | **继续等待**，不重复写 started | 等待无副作用 |
| Running `sub_workflow` | **重放**：沿用已落盘 child_run_id，附着既有子 run | id 确定性派生且已随 node_started 落盘，重复 start 撞 RunExists 幂等 |
「副作用节点」= `has_side_effect()` 的类型：`http_call` / `llm` / `email`——
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
|---|---|
| `workflow.create / update / publish / get / list / delete` | 定义生命周期。update 前强制 validate + x-secret 名称存在性校验（§5） |
| `workflow.versions` | 版本历史（按 version 倒序，只回 version/status/checksum/created_at 元数据列，definition 走 workflow.get 按需拉取）；workflow 不存在返回 -32011 |
| `nodetypes.list` | 前端画布能力清单：类型、端口、`params_schema`（JSON Schema draft-07 子集 type/required/properties/enum/default，另带 `x-widget`/`x-label`/`x-help`/`x-secret` 扩展键，前端据此渲染参数表单；后端校验仍以 `Definition::validate` 为准）、supports_retry、side_effect；单条描述的唯一来源是 `NodeType::descriptor`（§5） |
| `secrets.list` | 已配置的密钥**名称**列表（`FLOW_SECRET_<名称>` 存在性），永远不含值；前端给 x-secret 参数渲染可选名称 |
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

除 `run.start` 手动触发外，run 还有两个自动入口，都在 flow-server 进程内：

- **cron 调度器**：默认开启，`FLOW_SCHEDULER=off` 禁用。每 20s tick 扫一次
  全部 enabled schedule，取「最近一次 ≤ now 的整分触发点」（cron 标准 5 字段，
  本地时间，解析用 cron-parser crate），先 `try_insert_fire`（schedule_fires
  主键去重，多节点下谁先插入谁触发）再 `create_run`（当前 published 版本，
  输入取 schedule.input）。每个 tick 每个 schedule 最多补一次火——停机期间
  错过的触发点不追补；目标 workflow 没有 published 版本时本次跳过；
  **`create_run` 遇瞬时故障（磁盘满 / DB 不可达）时撤销去重行**（`delete_fire`），
  让下一个 tick 重试同一触发点——去重键含 `fire_at`，下一轮算的是新一分钟的键，
  不撤销则这一分钟的火永久丢失。配置类失败（无 published 版本 / workflow 不存在）
  不撤销：重试也不会变好。「不追补」只针对停机期间错过的点，不是静默吞掉磁盘错误；
- **webhook HTTP 入口**：独立于 WebSocket 端口的 HTTP 服务，`FLOW_HTTP_ADDR`
  （默认 `127.0.0.1:9801`）。`POST /hook/<token>`：token 未知或已停用 → 404
  （不区分，避免探测）；body 须为 JSON（空 body 视为 null 输入），作为 input
  启动当前 published 版本 → 200 `{"run_id": "..."}`；无 published 版本 → 409。
  webhook 与 RPC 共用同一个安全边界：无认证，只允许绑定可信地址（见文首警告）。

四个触发入口都在 runs 表写入归因：`run.start` = manual、调度器 = schedule
（detail 为 schedule id）、webhook = webhook（detail 为 token）、sub_workflow
子 run = sub_workflow（两臂的 launcher 直插路径各自标注，不经 run.start）。

错误码：`-32010` 参数非法、`-32011` 不存在、`-32012` 冲突、`-32603` 内部错误。
JSON-RPC 允许整体省略 params（到达 null），解析层统一归一为 `{}`。

### 9.1 wire protocol 变更记录（适配层重构）

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
只连 flow-server 的 JSON-RPC WebSocket（`--url` / `FLOW_RPC`，缺省
`ws://127.0.0.1:9800`），不依赖任何存储 crate、不读 FLOW_BACKEND、不碰
`data_dir`。因此两个后端对它行为一致，也不存在「绕过 RPC 校验直接操纵
SQLite / 事件日志」的第二条写入路径。

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
  壳，重新 import 同名会复用它；
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
  sub_workflow 的 `input_mapping`、llm/email 的 prompt/subject/body 等同规则生效。
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
`HTTP_METHODS` 白名单，定义层校验后执行层再验一次）。
输出 `{status, headers, body}`（body 能解析为 JSON 则解析，否则原样字符串）。
失败分类（llm / email 经 `post_json_bearer` 共用同一份，唯一补充是 429 限流
也可重试）：

- 连接失败/超时 → retryable（副作用不明确或未发生，交给重试策略）；
- 5xx → retryable；4xx → fatal（请求本身的问题）；llm/email 另认 429 → retryable；
- **响应体读取失败（连接中途断开）→ retryable**——绝不带着 200 + 空 body 记成功。

llm 节点（OpenAI 兼容 `{base_url}/chat/completions`，默认 `api.openai.com`）：
输出 `{content, model, usage}`；`json_mode` 时请求带
`response_format: {type: json_object}`，`system` 非空时进 messages[0]。
email 节点（Resend 兼容 `POST {endpoint}`，默认 `api.resend.com/emails`）：
body 为 `{from, to, subject, text}`（纯文本正文），输出 `{status, id}`。
两者的 `api_key` 是 x-secret 名称，执行前注入真值，只进 Authorization 头，
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

## 13. 测试策略

运行 `cargo test --workspace --all-targets --locked` 与
`cargo clippy --workspace --all-targets --locked -- -D warnings`。覆盖约定：

### 测试代码怎么摆（Rust 的四层位置约定）

| 层 | 位置 | 规则 |
|---|---|---|
| 单测 | `src/<模块>.rs` 末尾 `#[cfg(test)] mod tests;` | 能碰 `super::*` 私有项；模块超过 ~150 行就拆到 `src/<模块>/tests.rs` |
| 集成测试基建 | `flow-test-support`（`io`：TempDir / free_port；`pg`：docker 容器 + 独占测试库 + 孤儿 volume 回收） | 跨 crate 共享的测试代码只能放这里；只用到 `io` 的 crate 用 `default-features = false` 关掉 `pg` |
| 集成测试 | `tests/<主题>.rs` + `tests/common/mod.rs` | 只走 public API；共享夹具放 `common`（放子目录，cargo 才不会把它当成独立测试 target） |
| 端到端 | `backend-e2e/tests/*.rs`（`e2e_test!` 双后端展开） | 真起进程；`common` 里只有 e2e 专属部分 |

三条硬规则：

- **不许为了让 `tests/` 通过而把内部 API `pub` 出去**。曾经为了一份
  `tests/group_commit.rs` 把 `commit_stats()` / `read_events()` 导出到
  `flow_engine`——一个测试事实在公开 API 上开了两个洞。这条断言现在住在
  `src/event.rs` 的 `#[cfg(test)] mod group_commit_tests`。
- **共享夹具不逐文件复制**。`flow-engine/tests/{engine_recovery,
  recovery_regressions,sub_workflow}.rs` 曾经各有一份几乎同构的 `Harness`；
  现在统一在 `tests/common`。同理 `flow-rpc/tests/ws_{rpc,pg}.rs` 的两份
  `ServerProc`。
- **docker / PG 测试库的生命周期只在 `flow-test-support::pg` 一处**
  （历史上有三份各自漂移的实现，其中 flow-pg 那份的残留库清扫因为取错了
  时间戳长度，从来没删掉过任何库）。

### docker 卫生（backend-e2e / flow-test-support::pg）

PG 镜像声明了 `VOLUME /var/lib/postgresql/data`，不带挂载启动就落到一个**匿名**
volume 上。Docker 只在容器**自己退出**时（`--rm`）回收匿名 volume；测试清理走
的是 `docker rm -f`（atexit 与残留清扫都走这条），它只删容器，把 volume 留在
`/var/lib/docker/volumes` 里变成孤儿——一个 e2e 运行漏一个。三层都堵住：

1. **不创建**：数据目录挂 `--tmpfs`，匿名 volume 根本不出现（临时库的数据
   本来就没有跨容器的意义，tmpfs 还更快）；
2. **不留**：删除容器一律 `docker rm -f -v`；
3. **回收历史遗留**：启动时（删完残留容器之后，否则它们还不算 dangling）
   清扫 dangling 匿名 volume。只认 64 位十六进制的匿名名——命名 volume
   是开发者显式建的，绝不动。

`backend-e2e/tests/docker_hygiene.rs` 把这三层各钉一条断言（外加
「cleanup 必须真的 DROP 测试库」）。每条都验证过：对应的修复被回退，测试就红。

测试库的清理同样有讲究：`DROP DATABASE ... WITH (FORCE)`（PG 13+）原子踢掉
所有会话再删库，且删库必须排在「关自己的池」之前——用例手里常有第二个池
（PgEngine / PgBackend / 自建 EventHub），先关池会把删库拖到超时之后。

### 具体覆盖

- RPC 测试用 `env!("CARGO_BIN_EXE_flow-server")`（flow-rpc 自己的 bin）真起进程，
  `child.kill()`(SIGKILL) + 同 data_dir 重启证明恢复；
  就绪用 TCP connect 轮询，重启换新端口避免 EADDRINUSE；
- http_call 卡住用「本地 TcpListener 接受连接后持有不响应」；
  断流用「声明 Content-Length 但发一半即关闭」——确定性，不依赖不可路由地址；
- 客户端用 `ObjectParams` 具名参数；`subscribe` 三参
  `(subscribe_method, params, unsubscribe_method)`；
- 状态词汇表（§8）：`flow-dto` 的 `all_is_the_single_source_for_status_vocabulary`
  钉住 `ALL` 与 `as_str` 不脱节；`flow-pg` 的 `generated_run_status_check_*` 钉住
  派生出的 `CHECK` 列表排除 `initializing` 且与迁移前手写版逐字一致；
  `flow-backend` 的契约测试钉住 store 写入口真的在校验。
  「只有 published 可执行 + 创建前校验」的规则断言单点钉在 flow-backend 的
  `resolve_runnable_definition` 测试；
- JS 沙箱边界由行为测试钉住（§10），随每次 `cargo test` 重新验证；
- 图校验规则由 `model.rs` 的表驱动单测逐条钉住（端口/环/不可达/参数必填/
  模板参数放行与灰色形态拒绝；condition 多端口指同一节点的拒绝见
  `condition_ports_must_target_distinct_nodes`），不为了一句「工作流存在环」起进程；
- 节点类型注册表（§5）由 `model.rs` 钉住：`node_type_table_is_exhaustive`（表 ↔
  enum 双向覆盖）、`required_params_are_actually_enforced_by_validate`（派生出的
  必填清单真的在校验）、`opaque_params_are_exactly_the_js_bearing_fields`
  （只有 script.code / condition.expr 不参与展开）、`secret_params_are_the_expected_keys`；
- `nodetypes.list` 响应由 `node_types_snapshot_is_stable` 快照测试钉住
  （descriptor 注册表收敛是纯重构，响应必须逐字节不变）；
- `recovery_regressions.rs` 验证失败恢复、重试下游、剩余退避、非法/重复信号和裁决恢复；
  另钉三条：定义外节点（幽灵记录停在非终态时终止判定永远不成立）在恢复时被判
  `LogCorrupted` 拒（`node_outside_definition_is_rejected_on_recovery`）；已落盘
  但载荷非法的裁决归平台故障挂起、**不写 run_failed 业务终态**
  （`corrupt_durable_adjudication_is_a_platform_fault_not_a_run_failure`）；还有
  待裁决节点时 run 不得被投影回 `running`
  （`awaiting_adjudication_never_projects_back_to_running`，用状态投影记录器断言
  `Running` 只被投影过一次——钉住 §12.20 的两趟顺序）；
- `group_commit.rs`（flow-engine）钉严格组提交（§3.2）：append 返回即完整可读、
  seq 连续、并发多日志不串扰，且 fsync 必须真被组批（批数 ≤ 事件数的一半）；
  fsync 级持久化与写序协议由 backend-e2e 的 SIGKILL 用例钉住；
- `contracts.rs` 验证显式 draft 拒绝、初始化中断和旧日志缺失隔离；另钉恢复回填
  不得给 cancelled 的 run 行带上节点失败原因（正常投影路径写 NULL，回填必须一致，
  §12.9 `terminal_recovery_backfill_does_not_put_a_node_failure_on_a_cancelled_run`）；
- `sub_workflow.rs` 验证子 run 输出透传、子失败 fatal、RunExists 附着、深度上限、
  取消级联，以及崩溃重放沿用同一 child_run_id；
- `engine_recovery.rs` 钉住节点输入面语义：`nodes` 只暴露直接前驱输出，
  非前驱引用深层访问即 fatal（`nodes_scope_*`）；终态 run 的重复恢复幂等且如实
  报 `AlreadyTerminal`（`resume_after_terminal_run_reports_terminal_not_fake_resumed`
  / `repeated_resume_after_terminal_is_idempotent`）；
- `driver::slot_tests` 钉住节点级执行状态机（§6.1）：三个变体各自都阻止终止、
  `AwaitSignal → Running` 交付后仍在飞、只有 `Adjudicating` 无句柄、清理幂等；
- `fold.rs` 的 `attempt_and_error_are_derived_from_state_on_every_path` 钉住
  「attempt/error 从状态派生、无影子副本」（§12.5）——走一遍全部事件类型，
  每步断言展示值与状态恒等；`node_output_is_cleared_on_every_non_completed_path`
  钉住输出住在 `NodeRecord` 且每个非 completed 分支都清它，
  `terminal_node_never_carries_a_stale_pending_signal` 钉住「终态节点不携带待消费
  信号」（`node_skipped` 曾是四个终态分支里唯一漏清 `last_signal` 的）；
  `run_cancelled_clears_pending_fatal_error` 钉住取消清 `fatal_error`，
  `node_outside_definition_is_log_corruption` 钉住定义外节点判损坏，
  `run_phase_maps_to_db_status_on_every_phase` 钉住落库状态映射；
- 调度器触发去重的撤销语义由 `scheduler::tests` 钉住：撤销后同一分钟可重取
  触发权、撤销不影响别的分钟、重复撤销幂等；
- `child_await.rs` 钉住父 run 等待初始化中断子 run 不挂死（空日志 → DB 投影）；
- 回归护栏：重复恢复不产生第二个写者（engine_recovery）、共享 data_dir 的第二个
  实例被排他 flock 拒绝且锁释放后可重开（flow-backend exclusive_lock）、平台故障挂起不写
  run_failed（sub_workflow）、订阅缺口补齐与失败重试（flow-backend run_tail）、
  子 run 重放沿用钉版本（flow-backend flow-store 的 child_version_pin + flow-pg 的
  `replayed_child_run_keeps_pinned_version`，两臂同一条契约）、信号错误码与订阅
  回放契约（contracts）、空日志订阅立即结束（run_tail）、PG reader 回放起点报
  RunNotFound（flow-pg protocol）、身份不符隔离且不重扫——以 lease_epoch
  停止攀升断言（flow-pg recovery）、删除后 insert_run 拒绝孤儿 run（flow-store）、
  run_started 进全局广播两臂一致（flow-engine global_subscribe）；
- store 并发测试核对每个返回版本对应的定义及相同定义的版本复用；
- 测试数据库必须放在独占目录的 `flow.db`，只清理独占目录，禁止删除系统临时目录；
- **Postgres 后端**：租约 fencing、接管窗口、恢复分类与 inbox 幂等由
  `flow-pg/tests/{protocol,recovery}.rs` 覆盖，集群级 smoke/SIGKILL/订阅由
  `flow-rpc/tests/ws_pg.rs` 覆盖。每个测试在独立数据库中运行
  （名称含时间戳，启动时清理残留）；测试库通过 `FLOW_TEST_DATABASE_URL`
  指定（默认 `postgres://flow:flow@127.0.0.1:54329/flow`），不可达时自动跳过，
  没有可用 Postgres 时 `cargo test` 仍必须全绿；
- **backend-e2e（`crates/backend-e2e`）**：后端契约的完整端到端矩阵。每个用例
  对 SQLite（独占临时目录 `flow.db`）与 Postgres 两个后端各跑一遍，钉死
  「两臂同契约」；真起被测服务进程（`CARGO_BIN_EXE_flow-server-e2e`——本 crate
  自己的 `flow-server-e2e` 薄壳，与 flow-rpc 的 `flow-server` 是同一份
  `run_from_env` 实现，只是改名以免两个 bin 写同一个 `target/debug/flow-server`；
  含 SIGKILL 崩溃恢复与重启），HTTP stub 全部本地 TcpListener（确定性，不依赖
  外部网络）。容器与测试库由 `flow-test-support::pg` 用 docker CLI 自管
  （`postgres:16-alpine`，随机端口，label `com.flow.e2e=1`）：进程退出
  （atexit）与下次启动（按 label 清扫容器、按 `e2e_%` 前缀清扫测试库）双层
  清理，每个用例独占一个数据库、用完即 DROP，panic 路径也先清理再 unwind；
  数据目录 tmpfs + `rm -f -v` + 启动回收孤儿匿名 volume，磁盘上不留
  容器/测试库/volume 垃圾（见上面「docker 卫生」与 `tests/docker_hygiene.rs`）。
  需要 docker；`FLOW_E2E_PG_IMAGE` 可换镜像。覆盖：workflow 生命周期与
  「只有 published 可执行」、run 执行（script/condition/delay/输出收集/skip
  传播/重试/JS 沙箱/大整数 BigInt 边界）、human_task 信号与取消、http_call 分类与
  模板、sub_workflow（透传/失败传导/取消级联/深度上限）、schedule 真触发与
  webhook HTTP 全分支、订阅（回放/增量/未知 run 即结束）、SIGKILL 恢复与
  人工裁决、错误码映射。入口：`cargo test -p backend-e2e`。
- **flow-test-support（`crates/flow-test-support`）**：测试基建 crate，
  publish = false、不进产品二进制。`io`：独占临时目录 `TempDir`（Drop 即删，
  断言失败也不漏）、`free_port`、`wait_ready`；`pg`（特性，默认开）：docker
  容器、每用例独占测试库、残留清扫、孤儿 volume 回收。flow-pg / flow-rpc 的
  测试、backend-e2e、backend-perf 都指着这一份。
- **flow-cli（`crates/flow-cli`）**：CLI 自己的契约（测试里不直连 JSON-RPC——
  那测的是服务端）。真起两个进程/实例：`flow-cli` 二进制按
  `CARGO_BIN_EXE_flow-cli` 作为**子进程**跑，服务端用
  `flow_rpc::serve` + `SqliteBackend::open`（显式路径、随机端口、不碰环境
  变量，用例可并行）在进程内监听。钉住：import→list→get/versions→run
  （等待终态/stdout 输出）→events/timeline→export→roundtrip import 全链路，
  name 与 workflow_id 等价解析，`--input` 三种形态（内联/@文件/stdin），
  `--detach` 的 stdout 只剩 run_id，退出码契约（本地 1 / RPC 2 / run 失败 4），
  未发布即执行与非法定义分别由服务端 -32010 与本地 JSON 错误覆盖，非交互
  环境 delete 缺 `-y` 必须报错而非挂起，`--url` 覆盖 `FLOW_RPC`，服务不可达
  的错误文案带起服务提示。入口：`cargo test -p flow-cli`。
- **backend-perf（`crates/backend-perf`）**：后端性能压测 harness（黑盒，真起
  被测进程，双后端矩阵）。与 backend-e2e 分工：e2e 钉行为契约，这里量性能。
  被测进程是 `flow-perf` 二进制的自举服务模式（`FLOW_PERF_SERVE=1` →
  `flow_rpc::run_from_env`，与 flow-server 逐字同一份实现）；存储上下文复用
  backend-e2e 的 harness（独占临时目录 / docker 测试库、SIGKILL、panic 路径
  清理）。四个场景：run 执行吞吐（chain/fanout 两形态，完成检测走订阅流，
  测量本身不给读路径加压）、RPC 读路径延迟（run.get/list/stats/timeline/
  events 轮转交错）、订阅推送延迟（run.start 返回 → 事件到达）、崩溃恢复
  耗时（空存储重启基线 vs 停驻 N 个 human_task run 的重启，差值即恢复代价，
  附恢复后推进耗时）。「并发 N」= 同时在飞 N 个 run（提交侧限流，k6 的 VU
  模型），被测进程的 FLOW_MAX_RUNS 随之放大；提交失败（如 SQLite 高并发
  `database is locked`）不炸进程，计入 `submit_errors` 计数器。报告为最近秩
  分位数（p50/p90/p95/p99）文本 + 可选 JSON（`--json`）。默认轻量回归规模
  （数分钟）；`--runs/--concurrency/--prefill/--iterations` 放大做重负载。
  入口：`cargo run -p backend-perf -- --help`。

## 14. 未做

- delay 剩余时间恢复（当前崩溃/接管后整段重放）；
- 多 end 被跳过时与真 null 输出的显式区分；
- `run.start` 客户端幂等键（当前双击 = 两个 run，服务端 uuid 生成）；
- 中心指派模式（中心指派模式未实施（见 DESIGN §14）；对等模式已实现（设计见 `flow-pg` 各模块头注释），
  未做多节点压测）；
- 跨进程 SIGSTOP 场景下的真实副作用计数验收（副作用准入已用确定性前缀测试钉住）；
- 不指定 run_id 的全局订阅仍有后端差异：SQLite 只推本进程事件、Postgres 推
  全集群增量；指定 run_id 的回放 + 追流 + 终态结束语义两后端已统一
  （flow-backend 的 run_tail 状态机）。
