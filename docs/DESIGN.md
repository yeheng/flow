# flow 工作流引擎设计方案

> 状态：单机实现；验证命令与覆盖范围见 §13。
> 本文描述当前代码的实际语义，是后续开发的权威参考。

## 1. 目标与边界

flow 是一个工作流执行引擎：发布不可变的流程定义（DAG），以 run 为单位执行，
进程崩溃后从完整的磁盘日志恢复。日志丢失或损坏时隔离并报告错误，不猜测副作用。
前端拖拽画布已实现：`web/`（Vue 3 + @vue-flow/core，直连本服务的 JSON-RPC WebSocket）。

明确的非目标（v1 范围决策）：

- 单机单进程；多节点设计稿见 `DISTRIBUTED.md`。fold 和图语义可复用，
  租约、信号交付和外部副作用边界需要额外协议，尚未实现；
- 不做通用 DSL——表达式与脚本统一用 JavaScript。

## 2. 总体架构

```
crates/
  flow-engine   执行引擎。只依赖事件日志文件，不依赖任何存储实现
  flow-store    SQLite：workflow / workflow_versions / runs 元数据
  flow-rpc      jsonrpsee WebSocket 服务（bin: flow-server），适配层
```

依赖方向（不可反转）：

```
flow-rpc ──> flow-engine
flow-rpc ──> flow-store
flow-engine ✗ flow-store   （引擎不依赖存储；状态出口走 RunObserver trait）
```

运行：`cargo run --bin flow-server`。环境变量 `FLOW_ADDR`（默认 `127.0.0.1:9800`）、
`FLOW_DB`、`FLOW_DATA_DIR`。

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
| `run_started` | workflow_id, workflow_version, input | run 创建，input 快照 |
| `node_started` | node_id, attempt | **副作用发生前**写入 |
| `node_completed` | node_id, attempt, output, duration_ms | 节点成功 |
| `node_failed` | node_id, attempt, error, retryable | 节点失败；`retryable` 表示引擎还会重试 |
| `node_skipped` | node_id, reason | 汇合判定不满足，整节点跳过 |
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

每个事件 `write_all` + `sync_all`（fsync 是崩溃安全边界）。
这界定了节点副作用不明的窗口。恢复还必须处理 `node_failed(retryable=true)` 后
尚未开始下一次 attempt、信号已记录未消费、节点失败后 run 尚未收尾以及初始化未提交。

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
    records: HashMap<node_id, NodeRecord{state, attempts, started_at, ended_at,
                                          duration_ms, output, error, last_signal}>,
    outputs: HashMap<node_id, Value>,     // 唯一所有者：Driver 直接读它
    phase: Running | Succeeded | Failed | Cancelled,
    fatal_error, output, workflow_id, workflow_version, input, last_seq,
    started_at, ended_at,
}
```

节点状态机：`Pending → Running{attempt} → Completed | Failed{retryable} | Skipped{reason}`。
`Failed{retryable:true}` 是等待重试的**非终态**，时间线标签为 `retrying`；
保留事件及 NodeState 序列化格式，旧日志无需迁移。
关键转移：

- `node_started` **清除旧 output**（重试/重放不留脏数据）；
- `node_completed` 写入 `outputs`；
- `node_failed(retryable=false)` 在 fold 中记录首个 `fatal_error`，Driver 无独立失败缓存；
- `signal_received` 只记 `last_signal`（崩溃可能落在它与终态之间，恢复时消费它）；
- 终态事件携带的 `attempt` 与 `records.attempts` 取 max，重试计数不丢。

## 5. 定义模型与校验

`Definition { nodes, edges }`（前端拖拽产物，整体作为不可变版本入库）。
节点类型：`start`、`end`、`script`、`condition`、`delay`、`http_call`、`human_task`。

`Definition::validate()` 在保存与发布时强制（建图即校验，不等到运行）：

1. 节点 id 非空且唯一；类型已知；按类型校验必填参数
   （script: `code`，condition: `expr`，delay: `ms`，http_call: `url`；
   method 若给出必须属于 `HTTP_METHODS`——该白名单与 `nodetypes.list`
   共用一份，拼写错误在发布时被拒而不是运行时炸 run）；
2. 恰好一个 `start`，至少一个 `end`；
3. start 无入边，end 无出边，其余节点必须有入边（否则永不触发）；
4. 边端点存在、无自环、无重复边；**condition 出边必须带 `true`/`false` 端口，
   其余节点出边不得带端口**；
5. 拓扑必须是 DAG，且所有节点从 start 可达（防死代码）。

## 6. 执行引擎（Driver）

每次 run 一个 tokio 任务，持有该 run 事件日志的**单写者**——没有跨 run 共享可变状态，
没有锁竞争，run 之间完全隔离。

### 6.1 调度循环

```
loop {
    内层循环推进到不动点：
        plan() → (ready, skips)
        skips 逐个 append(node_skipped)      // 跳过沿下游传播
        ready 逐个 start_node()               // 写 node_started + 派发执行
    if inflight、human_waiting、adjudicating 全空：
        all_terminal → finalize（RunCompleted/RunFailed）
        否则 → 错误「调度停滞」（图正确性由 validate 兜底，此错误意味着语义 bug）
    select! { cancel → abort_inflight + RunCancelled;
              result_rx → 节点结果;
              signal_rx → 外部信号 }
}
```

关键不变量（都有回归测试钉住）：

- **跳过必须推进到不动点**：多级下游（delay→human_task→end）单轮只处理一层
  会被误判「调度停滞」。见 `skip_propagates_through_multiple_downstream_levels`。
- **inflight 不变量**：inflight 里每个 handle 都还欠一条 `DriverMsg`。
  human_task 的 oneshot 等待任务、重试退避计时器都必须计入，
  否则终止判定提前触发。

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

- 可重试：连接失败/超时、HTTP 5xx、**响应体中途断流**；
- 致命：JS 抛错、参数校验失败、HTTP 4xx（请求本身的问题）。

退避计时器计入 inflight（不变量），到点后 `RetryDue` 重新派发，attempt+1。
下游在此期间等待。恢复从 `NodeFailed` 的时间戳与固定版本的 backoff_ms
计算剩余等待时间；已过期则立即调度，墙钟回退最多重新等待整段 backoff。

### 6.6 取消与 fatal 的副作用语义

**fatal（已决策，勿改）**：首个致命节点失败由 fold 记录在 `state.fatal_error`，
不中断其余分支：独立分支跑到自然终态再结束 run——避免中途砍掉已发出的副作用；
失败节点的下游经 `upstream_failed` 全部跳过，结果注定 `RunFailed`，
由 finalize 收尾。代价是注定失败的 run 会等最慢的无关分支跑完。
节点失败后、run 终态前崩溃，恢复也必须保留该失败结论。

**cancel**：abort 所有 inflight。对 in-flight 的 `http_call`，请求可能已发出、
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
| Failed{retryable:true} | **重建退避计时器**，下一次 attempt | 内存计时器不是权威 |
| Failed{retryable:false} | **保留 run 失败结论**，独立分支继续 | fold 已记录 fatal_error |

裁决信号：`run.signal {payload: {action: "retry" | "succeeded" | "failed", output?, error?}}`。

### 7.2 恢复的正确性来源

- 日志已终结但 DB 未回填（崩溃在 append 与 observer 之间）→ `recover_unfinished`
  以事件为准修正 DB；
- DB 已回填但进程死在最终时刻 → 重启后 `resume_run` 发现 phase 已终态，
  返回 `AlreadyTerminal`，幂等；
- 半行残缺日志 → `EventLog::open` 物理截断；
- seq 不连续 → 硬错误，人工介入。

初始化中断：`initializing` 行配有有效 `run_started` 时按上述分类恢复；日志缺失、
空文件、首行残缺或损坏时将初始化标为 failed，保留文件与诊断，不执行节点。
这条失败记录是初始化结果，不伪造执行事件。
旧 `running`/`awaiting_resume` 的日志缺失、损坏或身份不符，则保留为
`awaiting_resume`、`live=false` 并报告恢复错误；需恢复原日志后重启，不能靠
`run.signal` 解除，也不能自动创建新日志重跑。这样兼容旧初始化窗口并避免重复副作用。

**已知限制**：delay 崩溃后重放整段时长（不续算剩余时间）；fsync 每事件一次，
组提交未做（正确性优先，这是后续优化点）。

## 8. 存储层（flow-store）

SQLite（WAL）。表：

- `workflows`：id, name, created_at；
- `workflow_versions`：`(workflow_id, version)` 主键，**不可变定义快照** + checksum。
  在 `BEGIN IMMEDIATE` 事务内检查最新 checksum 并分配版本；
  `INSERT ... RETURNING version` 返回本次写入的版本，禁止写后另查 latest。
  相同定义的并发保存也复用版本号。
  `status`: draft → published；
- `runs`：run 元数据（id, workflow_id, workflow_version, status, input, output, error, started_at, ended_at）。

约束：

- **只有 published 版本可执行**；显式 version 同样检查，省略时取 latest published；
- run 钉死某一版本——定义漂移在架构上不可能发生，无需额外防御；
- 有 run 记录时拒删 workflow（事件日志不能变孤儿）；
- `set_run_status` 影响 0 行必须报错（静默成功会掩盖「run 行没插进去」）；
- **状态词汇表**：`runs.status` 的合法取值
  （initializing/running/awaiting_resume/succeeded/failed/cancelled）由 store 私有常量定义，
  写入口（`insert_run`/`set_run_status`）经 `ensure_run_status` 校验；
  公共词汇表是引擎的 `DbRunStatus`（`as_str()` 生成同样字符串）。
  两边脱钩会在写入时当场报错（跨 crate 契约测试钉住），
  而不是让 run 从恢复扫描里静默消失。
- 状态更新替换 output/error，传 None 会清空；已解决的裁决诊断不得留在成功结果里。

## 9. RPC 层（flow-rpc）

jsonrpsee WebSocket。引擎不依赖存储，`StoreObserver` 在这一层把
`RunObserver` 状态出口适配到 runs 表。

方法：

| 方法 | 说明 |
|---|---|
| `workflow.create / update / publish / get / list / delete` | 定义生命周期。update 前强制 validate |
| `nodetypes.list` | 前端画布能力清单：类型、端口、参数 schema、supports_retry、side_effect |
| `run.start` | 校验 published → insert initializing → 持久化 run_started → 启动 Driver；初始化错误回写 failed |
| `run.get / run.list` | 元数据 + live 标记 |
| `run.timeline` | 只读时间线：定义顺序 + 折叠后的节点状态 |
| `run.events` | 原始事件，`from_seq` 增量拉取 |
| `run.cancel` | 活着的 run → cancelling；否则 conflict |
| `run.signal` | human_task 交付 / 副作用节点裁决（§6.7） |
| `run.subscribe` | 订阅 `run.event` 通知，可按 run_id 过滤。订阅者消费慢时收到 Lagged 丢事件，用 `run.events`（from_seq）补齐 |

错误码：`-32010` 参数非法、`-32011` 不存在、`-32012` 冲突、`-32603` 内部错误。
JSON-RPC 允许整体省略 params（到达 null），解析层统一归一为 `{}`。

## 10. JS 沙箱（expr.rs）

rquickjs：无 IO、CPU 同步执行（放 `spawn_blocking`，不占死 tokio worker）、
interrupt handler 超时中断（默认 2000ms，`timeout_ms` 可调）。

沙箱边界的证据（升级 rquickjs 时必须重新查证）：`rquickjs-sys` 只编译
`quickjs.c`，**不含 `quickjs-libc.c`**（`std`/`os` 模块的唯一来源），
引擎也未注册任何模块加载器——脚本里连 `import` 都不可用。

- `eval_body`：script 节点，函数体带 `return`，可用 `input` 与 `nodes`（前驱输出快照）；
- `eval_expr`：condition 节点；
- `expand_templates`：http_call 的 url/headers/body 中 `${expr}` 展开。
  **限制**：`${...}` 内不能包含 `}`（按第一个 `}` 截断），
  不支持嵌套对象字面量等复杂表达式。

## 11. http_call 语义

参数经 `${}` 模板展开后发请求（默认超时 30s；method 取自 `HTTP_METHODS` 白名单）。
输出 `{status, headers, body}`（body 能解析为 JSON 则解析，否则原样字符串）。失败分类：

- 连接失败/超时 → retryable（副作用不明确或未发生，交给重试策略）；
- 5xx → retryable；4xx → fatal（请求本身的问题）；
- **响应体读取失败（连接中途断开）→ retryable**——绝不带着 200 + 空 body 记成功。

自动重试不保证外部副作用只发生一次：POST 超时或断流时下游仍可能已提交。
启用多次重试需要调用方接受重复风险，或在 headers/body 中使用下游支持的稳定幂等键。
人工裁决只解决崩溃后结果不明，不把任意 HTTP 请求变为恰好一次执行。

## 12. 不变量清单（改动前必须知道）

1. 执行状态由完整日志重建；初始化结果和日志缺失诊断按 §7 单独处理；
2. 副作用之前必写 `node_started`；恢复还覆盖等待重试、信号消费与收尾窗口；
3. `RunState::fold` 是唯一状态转移函数，恢复与时间线共用；
4. 跳过必须推进到不动点；inflight 每个 handle 恰欠一条 DriverMsg；
5. `state.outputs` 是唯一输出所有者（无第二份手工同步）；
6. 端口规则：condition 必须 true/false，其余必须无端口；
7. end/run 输出共用「单数透传、复数映射」规则（`singular_or_map`）；
8. 状态词汇表单一来源：`DbRunStatus`，store 写入口校验。
9. 可重试失败非终态；不可重试失败由 fold 持久推导，恢复不会变为成功。
10. 信号先校验，再持久化、消费和确认；无效请求不改变 run 终态。

## 13. 测试策略

运行 `cargo test --workspace --all-targets --locked` 与
`cargo clippy --workspace --all-targets --locked -- -D warnings`。覆盖约定：

- RPC 测试用 `env!("CARGO_BIN_EXE_flow-server")` 真起进程，
  `child.kill()`(SIGKILL) + 同 data_dir 重启证明恢复；
  就绪用 TCP connect 轮询，重启换新端口避免 EADDRINUSE；
- http_call 卡住用「本地 TcpListener 接受连接后持有不响应」；
  断流用「声明 Content-Length 但发一半即关闭」——确定性，不依赖不可路由地址；
- 客户端用 `ObjectParams` 具名参数；`subscribe` 三参
  `(subscribe_method, params, unsubscribe_method)`；
- 契约测试钉住跨 crate 状态词汇表（§8）。
- `recovery_regressions.rs` 验证失败恢复、重试下游、剩余退避、非法/重复信号和裁决恢复；
- `contracts.rs` 验证显式 draft 拒绝、初始化中断和旧日志缺失隔离；
- store 并发测试核对每个返回版本对应的定义及相同定义的版本复用；
- 测试数据库必须放在独占目录的 `flow.db`，只清理独占目录，禁止删除系统临时目录。

## 14. 未做

- `sub_workflow` 节点；
- delay 剩余时间恢复（当前崩溃后整段重放）；
- fsync 组提交（吞吐优化，不动语义）；
- 多 end 被跳过时与真 null 输出的显式区分；
- `run.start` 客户端幂等键（当前双击 = 两个 run，服务端 uuid 生成）；
- 多节点部署（`DISTRIBUTED.md` / `SCHEDULER.md` 是待实施设计，尚未通过完整集群验收）。
