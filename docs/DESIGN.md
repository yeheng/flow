# flow 工作流引擎设计方案

> 状态：单机实现 + 对等模式多节点执行（`DISTRIBUTED.md`，Postgres 后端）；
> `cargo test --workspace --all-targets --locked` 94 个测试全绿；验证命令与覆盖范围见 §13。
> 本文描述当前代码的实际语义，是后续开发的权威参考。
> 架构焊点：**SQLite + events.jsonl 是默认与权威后端**（canonical）；Postgres 是
> 可替代后端，两者统一在 `flow-backend` 的 `AnyBackend` 闭集枚举之后（§2）。

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
- 多节点执行：对等抢占模式（peer）已按 `DISTRIBUTED.md` 实现（Postgres 后端，
  租约、持久 inbox、副作用边界）；中心指派模式见 `SCHEDULER.md`，仍未实现。

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
                （`pg.rs`，DISTRIBUTED.md）。公共面只含两个后端都诚实实现的方法；
                初始化协议、信号落账、订阅推送的差异在边界内吸收；
                「只有 published 可执行 + 创建前校验」单点在 resolve_runnable_definition
  flow-pg       Postgres 后端实现：共享日志、epoch 租约、持久 inbox、executor
  flow-rpc      jsonrpsee WebSocket 服务（bin: flow-server）；**只依赖
                `AnyBackend` 枚举**，不感知 flow-store / flow-pg，也不读 FLOW_BACKEND
```

依赖方向（不可反转）：

```
flow-rpc ──> flow-backend ──> flow-engine ──> flow-dto
                         ├──> flow-store ──> flow-dto
                         └──> flow-pg ────> flow-engine, flow-dto
flow-pg ──> flow-store   ✗（两个后端互相独立，互不感知）
flow-engine ✗ flow-*     （引擎不依赖存储；状态出口走 RunEventSink trait）
flow-rpc ──> flow-store / flow-pg   ✗（上层不感知具体后端）
```

后端选择只在进程入口发生一次：`flow_backend::open_from_env()` 按
`FLOW_BACKEND=sqlite（缺省）| postgres` 构造 `AnyBackend`，之后整条 RPC 链路
只看枚举。闭集枚举而非 trait 对象：每加一个方法编译器逼着两个臂都写完，
不存在某个后端静默继承错误默认实现的坑。

运行：`cargo run --bin flow-server`。环境变量 `FLOW_ADDR`（默认 `127.0.0.1:9800`）、
`FLOW_DB`、`FLOW_DATA_DIR`；Postgres 模式另见 `DISTRIBUTED.md` §10
（`FLOW_BACKEND`、`FLOW_DATABASE_URL`、`FLOW_ROLE`、`FLOW_LEASE_TTL_MS` 等）。

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
| `node_started` | node_id, attempt, child_run_id（可选） | **副作用发生前**写入；child_run_id 仅 sub_workflow 携带 |
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
                                          duration_ms, output, error, last_signal,
                                          child_run_id}>,
    outputs: HashMap<node_id, Value>,     // 唯一所有者：Driver 直接读它
    phase: Running | Succeeded | Failed | Cancelled,
    fatal_error, output, workflow_id, workflow_version, input, last_seq, depth,
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
节点类型：`start`、`end`、`script`、`condition`、`delay`、`http_call`、`human_task`、
`sub_workflow`。

`Definition::validate()` 在保存与发布时强制（建图即校验，不等到运行）：

1. 节点 id 非空且唯一；类型已知；按类型校验必填参数
   （script: `code`，condition: `expr`，delay: `ms`，http_call: `url`，
   sub_workflow: `workflow_id`；method 若给出必须属于 `HTTP_METHODS`——该白名单
   与 `nodetypes.list` 共用一份，拼写错误在发布时被拒而不是运行时炸 run）；
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

### 6.8 子工作流（sub_workflow）

sub_workflow 节点以目标工作流的**最新已发布版本**启动一个子 run，输入为父 run
的输入快照，子 run 输出透传为本节点输出。父子 run 是两条完全独立的事件日志，
各自走自己的恢复与终态协议。Driver 拿不到引擎句柄（DriverSpec 只有
run_id/definition/input），启动/等待/取消经 `ChildRunLauncher` trait
（`child_run.rs`）抽象：单机 `LocalChildLauncher` 同进程复用 Engine，
Postgres `PgChildLauncher` 经 `lease::create_run` 单事务创建子 run、
轮询共享 runs 投影等终态，取消走持久 inbox（DISTRIBUTED.md §6）。

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
| Failed{retryable:true} | **重建退避计时器**，等满整段 backoff 后下一次 attempt | 内存计时器不是权威；剩余时间不可续算（§6.5） |
| Failed{retryable:false} | **保留 run 失败结论**，独立分支继续 | fold 已记录 fatal_error |

裁决信号：`run.signal {payload: {action: "retry" | "succeeded" | "failed", output?, error?}}`。

### 7.2 恢复的正确性来源

- 日志已终结但 DB 未回填（崩溃在 append 与 observer 之间）→ `recover_unfinished`
  以事件为准修正 DB；
- DB 已回填但进程死在最终时刻 → 重启后 `resume_run` 发现 phase 已终态，
  返回 `AlreadyTerminal`，幂等；
- 半行残缺日志 → `EventLog::open` 物理截断；
- seq 不连续 → 硬错误，人工介入；
- 运行中基础设施故障（磁盘/数据库 IO、序列化、日志损坏）→ **不写 run_failed**，
  投影 `awaiting_resume` 挂起：平台故障不是工作流失败，SQLite 重启后恢复、
  Postgres 由 executor 自动接管；只有工作流语义失败才写 run_failed 终态。

初始化中断：`initializing` 行配有有效 `run_started` 时按上述分类恢复；日志缺失、
空文件、首行残缺或损坏时将初始化标为 failed，保留文件与诊断，不执行节点。
这条失败记录是初始化结果，不伪造执行事件。
旧 `running`/`awaiting_resume` 的日志缺失、损坏或身份不符，则保留为
`awaiting_resume`、`live=false` 并报告恢复错误；需恢复原日志后重启，不能靠
`run.signal` 解除，也不能自动创建新日志重跑。这样兼容旧初始化窗口并避免重复副作用。
父 run 等待初始化中断的子 run 时消费 DB 投影（`LocalChildLauncher::await_terminal`
发现子日志为空时查 runs 行）：这类子 run 永远不会写出终态事件，事件侧等待会挂死。

**已知限制**：delay 崩溃后重放整段时长（不续算剩余时间）；重试退避恢复后同样
等满整段 backoff（不续算剩余时间，理由同 §6.5）；fsync 每事件一次，
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
  （initializing/running/awaiting_resume/succeeded/failed/cancelled）**单一来源**是
  flow-dto 的 `DbRunStatus`（`as_str()` 生成这些字符串）；store 写入口
  （`insert_run`/`set_run_status`）经 `ensure_run_status` 按 `DbRunStatus::is_valid_str`
  校验，不维护第二份私有常量。契约测试钉住 store 写入口真的在校验，
  词汇表外的字符串在写入时当场报错，而不是让 run 从恢复扫描里静默消失。
- 状态更新替换 output/error，传 None 会清空；已解决的裁决诊断不得留在成功结果里。

## 9. RPC 层（flow-rpc）

jsonrpsee WebSocket。本层只有**一份**方法实现，只依赖 `flow_backend::AnyBackend`
闭集枚举；后端差异（初始化协议、信号落账、订阅推送）全部在 flow-backend 吸收：

- `run.start`：SQLite 两段式 initializing → run_started；Postgres 单事务原子创建。
  「只有 published 可执行 + 创建前校验」单点在 flow-backend 的
  `resolve_runnable_definition`，两个后端 create_run 共用，不存在第二份实现；
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
| `workflow.create / update / publish / get / list / delete` | 定义生命周期。update 前强制 validate |
| `workflow.versions` | 版本历史（按 version 倒序，只回 version/status/checksum/created_at 元数据列，definition 走 workflow.get 按需拉取）；workflow 不存在返回 -32011 |
| `nodetypes.list` | 前端画布能力清单：类型、端口、`params_schema`（JSON Schema draft-07 子集 type/required/properties/enum/default，另带 `x-widget`/`x-label`/`x-help` 扩展键，前端据此渲染参数表单；后端校验仍以 `Definition::validate` 为准）、supports_retry、side_effect |
| `run.start` | 经 Backend：SQLite 校验 published → insert initializing → 持久化 run_started → 启动 Driver，初始化错误回写 failed；Postgres 单事务原子创建（§9 差异说明） |
| `run.get / run.list` | 元数据 + live 标记 |
| `run.timeline` | 只读时间线：定义顺序 + 折叠后的节点状态 |
| `run.events` | 原始事件，`from_seq` 增量拉取 |
| `run.cancel` | 统一 SignalAck：活着的 run 交付取消（SQLite 仅本进程生效）；否则 conflict |
| `run.signal` | human_task 交付 / 副作用节点裁决（§6.7）。signal_id 在 Postgres 必填且重试复用，SQLite 可省（响应只回显客户端提供的，不伪造） |
| `run.signal_status` | 持久 inbox 落账查询；pg 专属（RPC 边缘 match 暴露），SQLite 返回明确的 invalid |
| `run.subscribe` | 订阅 `run.event` 通知，可按 run_id 过滤。SQLite 订阅者消费慢时收到 Lagged 丢事件，用 `run.events`（from_seq）补齐；Postgres 按游标轮询共享日志。run_id 不存在时流立即结束（不是报错、不是永久重试） |

错误码：`-32010` 参数非法、`-32011` 不存在、`-32012` 冲突、`-32603` 内部错误。
JSON-RPC 允许整体省略 params（到达 null），解析层统一归一为 `{}`。

### 9.1 wire protocol 变更记录（适配层重构）

后端适配层重构（两份 RPC 实现合一）带来的线上协议变更，客户端对齐依据：

- `run.cancel` 响应：`{"cancelling": true}` → `{"delivered": true}`（语义未变：
  取消已受理。若 Postgres 侧携带 signal_id，则一并返回）；
- `run.signal` 请求新增可选 `signal_id`（Postgres 必填）；响应新增
  `signal_id` / `event_seq` 字段（仅在真实存在时返回）；
- `run.signal_status`：SQLite 后端从「方法不存在」变为「存在但返回 -32010
  明确错误」；Postgres 语义不变；
- 响应面：自托管 web 前端不读上述响应体，无需变更；请求面：`run.signal` 必须
  携带 `signal_id`（Postgres 必填，前端已在 `web/src/api/flow.ts` 生成）；
  外部消费者按本节对齐。

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
- `expand_templates`：http_call 的 url/headers/body 中 `${expr}` 展开。
  **限制**：`${...}` 内不能包含 `}`（按第一个 `}` 截断），
  不支持嵌套对象字面量等复杂表达式。

数据契约限制：进出沙箱的数据经 JS Number（f64）——绝对值大于 2^53 的整数
（雪花 ID、高精度金额）会被静默舍入（9007199254740993 → …0992）并沿节点
输出向下游传播。大整数必须以字符串传递；重试/重放同样舍入，不影响决定论。

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
5. `state.outputs` 是唯一输出所有者（无第二份手工同步）；节点执行输入面
    `nodes` 只暴露直接前驱的输出（重放决定论，见 §10）；
6. 端口规则：condition 必须 true/false，其余必须无端口；
7. end/run 输出共用「单数透传、复数映射」规则（`singular_or_map`）；
8. 状态词汇表单一来源：`DbRunStatus`，store 写入口校验。
9. 可重试失败非终态；不可重试失败由 fold 持久推导，恢复不会变为成功。
10. 信号先校验，再持久化、消费和确认；无效请求不改变 run 终态。
11. sub_workflow 的 child_run_id 确定性派生（`{父run}:{节点}:{attempt}`）并随
    node_started 落盘；重放沿用已落盘 id 附着既有子 run，重试派生新 id。
12. 父子 run 各有独立事件日志；嵌套深度上限 8，depth 经 run_started 持久化；
    子 run failed/cancelled 传导为父节点 fatal，取消级联是 best-effort。
13. 平台故障（IO/Backend/日志损坏）与工作流失败分离：前者投影
    `awaiting_resume` 等待恢复/人工，只有工作流语义失败才写 run_failed 终态；
    所有权丢失（LeaseLost）则静默退出，三者互不冒充。
14. 一个 run 至多一个活 Driver（engine registry 原子占位 reserve_run）：
    重复 start/resume 是无操作，绝不允许第二个写者——EventLog 各自计数 seq，
    双写者必然产生重复 seq 与重复副作用。

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
- 契约测试钉住状态词汇表（§8；位于 flow-backend：store 写入口校验
  DbRunStatus 单一词汇表）；「只有 published 可执行 + 创建前校验」的规则断言
  单点钉在 flow-backend 的 `resolve_runnable_definition` 测试；
- JS 沙箱边界由行为测试钉住（§10），随每次 `cargo test` 重新验证；
- `recovery_regressions.rs` 验证失败恢复、重试下游、剩余退避、非法/重复信号和裁决恢复；
- `contracts.rs` 验证显式 draft 拒绝、初始化中断和旧日志缺失隔离；
- `sub_workflow.rs` 验证子 run 输出透传、子失败 fatal、RunExists 附着、深度上限、
  取消级联，以及崩溃重放沿用同一 child_run_id；
- `engine_recovery.rs` 钉住节点输入面语义：`nodes` 只暴露直接前驱输出，
  非前驱引用深层访问即 fatal（`nodes_scope_*`）；
- `child_await.rs` 钉住父 run 等待初始化中断子 run 不挂死（空日志 → DB 投影）；
- 回归护栏：重复恢复不产生第二个写者（engine_recovery）、平台故障挂起不写
  run_failed（sub_workflow）、订阅缺口补齐与失败重试（flow-backend run_tail）、
  子 run 重放沿用钉版本（flow-backend flow-store 的 child_version_pin + flow-pg 的
  `replayed_child_run_keeps_pinned_version`，两臂同一条契约）、信号错误码与订阅
  回放契约（contracts）；
- store 并发测试核对每个返回版本对应的定义及相同定义的版本复用；
- 测试数据库必须放在独占目录的 `flow.db`，只清理独占目录，禁止删除系统临时目录；
- **Postgres 后端**：租约 fencing、接管窗口、恢复分类与 inbox 幂等由
  `flow-pg/tests/{protocol,recovery}.rs` 覆盖，集群级 smoke/SIGKILL/订阅由
  `flow-rpc/tests/ws_pg.rs` 覆盖。每个测试在独立数据库中运行
  （名称含时间戳，启动时清理残留）；测试库通过 `FLOW_TEST_DATABASE_URL`
  指定（默认 `postgres://flow:flow@127.0.0.1:54329/flow`），不可达时自动跳过，
  没有可用 Postgres 时 `cargo test` 仍必须全绿。

## 14. 未做

- delay 剩余时间恢复（当前崩溃/接管后整段重放）；
- fsync 组提交（吞吐优化，不动语义）；
- 多 end 被跳过时与真 null 输出的显式区分；
- `run.start` 客户端幂等键（当前双击 = 两个 run，服务端 uuid 生成）；
- 中心指派模式（`SCHEDULER.md` 待实施；对等模式已按 `DISTRIBUTED.md` 实现，
  未做多节点压测）；
- 跨进程 SIGSTOP 场景下的真实副作用计数验收（副作用准入已用确定性前缀测试钉住）；
- 不指定 run_id 的全局订阅仍有后端差异：SQLite 只推本进程事件、Postgres 推
  全集群增量；指定 run_id 的回放 + 追流 + 终态结束语义两后端已统一
  （flow-backend 的 run_tail 状态机）。
