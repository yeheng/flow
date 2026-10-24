# 03 · 单机执行：数据流、时序与状态机（§4–§6）

> 本文是 [DESIGN.md](../DESIGN.md) 的分篇。章节编号与全文一致；总索引与核心抉择见 [DESIGN.md](../DESIGN.md)。

## 4. 单机数据流

### 4.1 一条 run 的生命周期

```mermaid
flowchart LR
    A["run.start<br/>RPC"] --> B["RunStarted<br/>(journal 落盘)"]
    B --> C["dirty run 队列"]
    C --> D["advance_run<br/>遍历节点"]
    D --> E{"入边 gate<br/>判定"}
    E -->|"未就绪"| D
    E -->|"可跳过"| F["NodeSkipped"]
    E -->|"就绪"| G["Attempt::begin<br/>DispatchStarted"]
    G --> H["execute_node<br/>→ ExecutionPort"]
    H --> I["flow-executor"]
    I -->|"InputPrepared"| D
    I -->|"审计流"| J["AuditBatch"]
    I -->|"Result"| K["NodeCompleted/<br/>WaitRegistered"]
    K --> D
    D -->|"全部终态"| L["settle_run"]
    L --> M["RunCompleted/<br/>RunFailed"]

    style B fill:#f9f,stroke:#333
    style I fill:#bbf,stroke:#333
```

### 4.2 数据流动图：值从哪来，到哪去

```mermaid
flowchart TB
    subgraph INPUT["输入面（三层来源，优先级从高到低）"]
        direction LR
        I1["run.input<br/>StoredValue"]
        I2["前驱输出<br/>predecessors[node_id]"]
        I3["节点 params<br/>模板展开后"]
    end

    subgraph EXEC["flow-executor 进程"]
        T["模板展开<br/>exec.rs"]
        JS["QuickJS 沙箱<br/>32MiB heap"]
        HC["HTTP 客户端<br/>reqwest"]
        T --> JS
        T --> HC
    end

    subgraph OUT["输出面"]
        O1["值 ≤64KiB<br/>→ Inline 进事件"]
        O2["值 >64KiB<br/>→ ValueChunk×N → ValuePublished"]
    end

    I1 --> T
    I2 --> T
    I3 --> T
    JS --> O1
    JS --> O2
    HC --> O1
    HC --> O2
    O2 --> J["journal 段文件<br/>(权威副本)"]
    O1 --> J
    J --> V["journal_views<br/>物化读面"]
    J --> RPC["RPC / 下载端点"]

    style J fill:#f9f,stroke:#333,stroke-width:2px
```

**关键点**：`execute_node` 传给出站执行器的不是字节，是**引用**。
`predecessors` 是 `BTreeMap<String, StoredValue>`，其中 `StoredValue::Ref` 携带
`ValueRef{journal_id, output_id, codec, version, chunk_count, total_bytes, digest}`
——digest、总字节与值存储位置的语义载体。真正的字节由 `transfer_inputs` 从
journal 值存储读出、按 256 KiB 切块、经 data 帧送过去。执行器侧按
`INPUT_BUDGET = 8 MiB` 硬限聚合。

### 4.3 dirty-run 调度模型（不是工作窃取，不是线程池）

```mermaid
stateDiagram-v2
    [*] --> Idle
    Idle --> Draining: take_dirty_runs()
    Draining --> Draining: advance_run(run_id)
    note right of Draining
        每次只推进「已提交事实允许」的步数
        写事件 → 状态变新鲜 → 继续下一节点
    end note
    Draining --> Throttled: tasks.len() >= limit
    Throttled --> Idle: defer_run(剩余 run)
    Draining --> Idle: 队列空
    Idle --> Idle: notify / wakes 到期 / sleep(delay)
```

- 唤醒源共四个：`cancel.cancelled()`、`tasks.join_next()`、`notify.notified()`、
  `sleep(delay)`（`wakes` 里最近的到期时间）。
- `wakes: BTreeMap<i64, BTreeSet<String>>` 是**持久等待的定时器堆**：delay/retry
  到期时把 run 重新塞回 dirty 队列。
- 在飞任务数达到 `limit` 时，**整个剩余 dirty 队列被 defer 回去**（不是只跳过
  当前 run）——避免同一批里靠前的 run 反复抢占靠后 run 的机会。
- 失败与 panic 分三层处理：
  - **节点任务内 panic**：`execute_node` 用 `catch_unwind` 捕获并转成已提交证据
    （NodeFailed / WaitRegistered），run 随后经 finished → `defer_run` **立即**
    重回队列，不是延迟重试。
  - **任务被 abort / panic 逃出任务边界**（`join_next` 返回 Err）：放弃本轮
    协调器，整个循环重启。
  - **`advance_run` 自身 panic 或返回 Err**：外层 `catch_unwind` 捕获，run 以
    1000ms 延迟重试（`wakes[now+1000ms]`）。
  - **协调循环自身 panic**：整个循环重启，把所有非终态 run 重新 defer，从已提交
    证据重建（`inspect` 折叠态）后继续，睡 1s。

---

## 5. 时序图（单机）

### 5.1 一次普通节点派发（无外部操作）

```mermaid
sequenceDiagram
    autonumber
    participant C as Coordinator
    participant B as JournalBackend
    participant W as Journal 写者
    participant P as ExecutionPort(Ipc)
    participant X as flow-executor

    C->>B: take_dirty_runs()
    C->>B: inspect(折叠态)
    C->>B: Attempt::begin → DispatchStarted
    B->>W: append(事务)
    W-->>B: fsync, Commit{lsn}
    C->>P: execute(attempt, node, predecessors)
    P->>X: Execute(ExecuteTask)
    X->>X: 模板展开 + JS/HTTP 执行
    X->>P: AuditBatch(观测行)
    X->>P: AuditBatch{[input_prepared]}
    P->>B: append(input_prepared, audit_seq=1)
    X->>P: AuditBatch(结果摘要)
    P->>B: append(audit_seq=2..N)
    P->>X: AuditAck(acked_seq=N)
    X->>P: Result(outcome)
    P->>B: append(NodeCompleted, sealed_through=N)
    B->>W: append → fsync
    W-->>P: Commit
    P->>X: ResultCommitted
    X-->>P: 通道释放
    P-->>C: Ok(()) → defer_run
    C->>B: advance_run(下一遍)
```

**要点**：

- `ResultCommitted` 是**执行器退出的许可**。执行器在看到它之前不敢退出，否则
  「结果已算但未落盘」的窗口会让崩溃恢复无法判断该不该重算。
- `AuditAck` 之前执行器不会认为审计安全。**2s 超时后原序号原内容重传只是
  remote 中继模式的行为**（`--remote-relay` 启用的重放通道，`ACK_RETRANSMIT_MS`）；
  本地 IPC 只等待 ack 通知、不按超时重传。重传安全的前提是审计记录幂等
  （`audit_seq` 严格 +1 已在折叠层去重）。
- `InputPrepared` 的 `audit_seq` 从 1 开始。结果事件的 `audit_seq` 按 kind 区分：
  `NodeCompleted` 自身作为审计条目计费，`audit_seq = attempt.audit_seq + 1`
  （与 I17 一致）；`NodeFailed` / `WaitRegistered` 的 `audit_seq = 0`。屏障条件
  统一写「事件过账后 `sealed_through == attempt.audit_seq`」。

### 5.2 外部操作授权（C3/C4 的落点）

```mermaid
sequenceDiagram
    autonumber
    participant X as flow-executor
    participant P as ExecutionPort
    participant B as JournalBackend
    participant W as Journal 写者
    participant EXT as 外部系统

    X->>X: 计算请求体（fingerprint = sha256）
    X->>P: RequestOperation{operation_id, fingerprint, 请求声明}
    P->>B: append(OperationIntent) + append(OperationAuthorized)
    Note over P,B: 同一事务、两条事件，audit_seq 均为 0
    B->>W: append → fsync
    W-->>P: Commit{lsn}
    P->>X: OperationPermit{permit_id}
    X->>EXT: 真实 HTTP 请求（此刻才发生副作用）
    alt 成功拿到响应
        EXT-->>X: 2xx 响应体
        X->>P: Result(outcome=Ok)
        P->>B: append(NodeCompleted, sealed_through)
        P->>X: ResultCommitted
    else 网络中断 / 超时 / 进程被杀
        X--xP: 通道断开
        P->>B: 补写 WaitRegistered{Uncertain}
        Note over B: run.status = AwaitingResume
        P->>B: 无 OperationOutcome，run 卡住
    end
```

**为什么不能自动重发**：`OperationAuthorized` 落盘后，「请求可能已到达外部系统」
成为既成事实。此时任何重发都是把「至少一次」升级成「可能多次」。所以
`advance_committed_node` 看到节点携带 operation 时，唯一动作是写
`WaitRegistered{Uncertain}` 然后返回——**连 gate 都不再评估**。

触发条件是「**携带 operation 且本机没有已提交的结果**」，而不只 `outcome.is_none()`：
`OperationOutcome` 已捕获、但输出/结果未达完成形态的节点同样折入 Uncertain
（integrity 分别记 `unknown` / `outcome_retained`）。Uncertain 由 master 侧
（dispatch / driver）补写，不是执行器自报。

### 5.3 人工信号与裁决

```mermaid
sequenceDiagram
    autonumber
    participant U as 运维 / 用户
    participant R as RPC 层
    participant B as JournalBackend
    participant C as Coordinator

    Note over B: run.status = AwaitingResume<br/>节点 Waiting{Uncertain}

    alt 情况 A：人工确认外部操作结果
        U->>R: run.adjudicate{run_id,node_id,<br/>decision:"accept_output", output}
        R->>B: 校验 reason ≤4096 字节、decision 合法
        B->>B: append(Adjudicated{accept_output, output})
        Note over B: Node → Succeeded，run → Running
    else 情况 B：人工确认未执行，要求重试
        U->>R: run.adjudicate{decision:"retry"}
        B->>B: append(Adjudicated{retry})
        Note over B: Node → Pending（清 operation/error/output）<br/>run → Running
        C->>C: 下一遍 advance_run 以 attempt+1 重新派发
    else 情况 C：人工判定失败
        U->>R: run.adjudicate{decision:"failed", reason}
        B->>B: append(Adjudicated{failed})
        Note over B: Node → Failed，run → Running<br/>独立分支继续，收尾写 RunFailed
    end

    Note over U,R: 另一种形态：delay/signal/child 等待
    U->>R: run.signal{run_id, node_id, payload}
    R->>B: append(SignalReceived)
    B->>B: append(WaitResolved)
    Note over B: run.signal 同时充当 uncertain → Adjudicated 的桥
```

**裁决三种决策的语义差异**：

| decision | 节点终态 | 是否重新派发 | 适用场景 |
| --- | --- | --- | --- |
| `accept_output` | `Succeeded` | 否 | 运维从外部系统查到了真实结果，手工补录 |
| `retry` | `Pending` | 是（attempt+1） | 确认外部操作**没有**产生副作用 |
| `failed` | `Failed` | 否 | 确认失败，放弃该分支 |

### 5.4 崩溃恢复时序

```mermaid
sequenceDiagram
    participant N as 新协调器
    participant S as flow.sqlite
    N->>S: flock 数据目录，打开 SQLite/WAL
    Note over S: SQLite 恢复完整已提交事务，未提交事务不可见
    N->>S: 读取提交水位和控制元数据
    N->>S: 加载非终态 run、node、当前 attempt
    N->>N: dirty_runs 播种，持久化新 master epoch
    N->>N: 安全工作继续；未知授权结果进入 uncertain
```

未发布值不能被业务引用；正文按需读取并核验。事实账本的全量检查属于显式 verify，
不是每次启动的前置扫描。损坏状态/正文报错，不自动以历史前缀冒充完整执行状态。

---

## 6. 状态图

### 6.1 run 状态机

```mermaid
stateDiagram-v2
    [*] --> Running: RunStarted

    Running --> Running: 节点推进 / WaitRegistered{delay,signal,child,retry}
    Running --> AwaitingResume: WaitRegistered{Uncertain}
    Running --> Succeeded: RunCompleted
    Running --> Failed: RunFailed
    Running --> Cancelled: RunCancelled

    AwaitingResume --> Running: Adjudicated{accept_output|retry|failed}
    AwaitingResume --> Running: run.signal（桥接）
    AwaitingResume --> Cancelled: RunCancelled

    Failed --> [*]
    Succeeded --> [*]
    Cancelled --> [*]
```

**`AwaitingResume` 是全系统唯一的人工介入点**。它只由 `WaitKind::Uncertain` 触发，
而 `Uncertain` 只由「已授权外部操作结果不明」产生。看到这个状态，运维必须去外部
系统查明真相，然后用 `run.adjudicate` 明确告诉引擎发生了什么。

### 6.2 节点状态机

```mermaid
stateDiagram-v2
    [*] --> Pending: RunStarted

    Pending --> Running: DispatchStarted（attempt+1）
    Running --> Succeeded: NodeCompleted
    Running --> Skipped: NodeSkipped
    Running --> Failed: NodeFailed（无 retry_wake_at）
    Running --> Waiting: NodeFailed（带 retry_wake_at，仅 script/condition）
    Running --> Waiting: WaitRegistered{delay|signal|child|uncertain}

    Waiting --> Running: Retry 到期（不经 WaitResolved，<br/>直接以 attempt+1 重新 DispatchStarted）
    Waiting --> Succeeded: WaitResolved（signal 解除 / delay 到期 / child 成功——<br/>折叠层 WaitResolved 恒置 Succeeded）
    Waiting --> Failed: child 失败（驱动层写 NodeFailed，非 WaitResolved）
    Waiting --> Succeeded: Adjudicated{accept_output}
    Waiting --> Failed: Adjudicated{failed}
    Waiting --> Pending: Adjudicated{retry}

    note right of Pending
        retry 决策清空 operation/error/output，
        下一遍 advance_run 以 attempt+1 重新走 gate
    end note

    Failed --> [*]
    Succeeded --> [*]
    Skipped --> [*]
```

**WaitResolved 的折叠层语义只有一种终态**：非终态 run 上的 `WaitResolved` 一律把
节点置为 `Succeeded`（signal 解除、delay 到期、child 成功都走这里）。child 失败
不产生 `WaitResolved`——驱动层直接写 `NodeFailed`；Retry 等待到期也不经
`WaitResolved`，直接进入新一轮 `DispatchStarted`。「run 回到 Running」是事件之后
`refresh_status` 的效果，不是节点的 WaitResolved 转移。

### 6.3 等待类型

| WaitKind | 触发者 | run.status | 解除方式 | 可自动恢复 |
| --- | --- | --- | --- | --- |
| `Delay` | delay 节点 / 重试 backoff | Running | `wake_at` 到期 → `WaitResolved` | 是 |
| `Retry` | script/condition 失败 | Running | `wake_at` 到期 → 重新派发（不经 WaitResolved） | 是 |
| `Signal` | human_task / input 节点 | Running | `run.signal` → `WaitResolved` | 否（等外部） |
| `Child` | sub_workflow | Running | 子 run 成功 → `WaitResolved`；失败 → 驱动层写 `NodeFailed` | 是 |
| `Uncertain` | 外部操作结果不明 | **AwaitingResume** | `run.adjudicate` → `Adjudicated` | **否（C4）** |

### 6.4 协调循环状态机

```mermaid
stateDiagram-v2
    [*] --> Select: spawn

    Select --> Select: cancel / join_next / notify / sleep(delay)
    Select --> JournalFailed: journal.stats().failure.is_some()
    JournalFailed --> Select: 限频报错后 continue（空转不停摆）

    Select --> HandleFinished: 任务结束
    HandleFinished --> DrainDirty: defer_run(run_id)

    Select --> FireWakes: wakes 到期
    FireWakes --> DrainDirty: defer_run(每个到期 run)

    Select --> DrainDirty: notify
    DrainDirty --> DrainDirty: advance_run(run_id)
    DrainDirty --> Throttle: tasks.len() >= limit
    Throttle --> Select: defer_runs(剩余全部)
    DrainDirty --> Select: 队列空

    DrainDirty --> RetryLater: advance_run 返回 Err
    RetryLater --> Select: wakes[now+1000ms] += run_id

    Select --> Restart: 循环自身 panic
    Restart --> Select: 非终态 run 全部 defer，睡 1s
```
