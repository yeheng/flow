# 02 · 权威数据模型（§3）

> 本文是 [DESIGN.md](../DESIGN.md) 的分篇。章节编号与全文一致；总索引与核心抉择见 [DESIGN.md](../DESIGN.md)。

## 3. 权威数据模型：一个业务数据库

结构、生命周期与验收的完整决定见 [ADR 0002](../adr/0002-sqlite-persistence.md)。

### 3.1 事务、事件与 LSN

- `Transaction { v: 2, journal_id, lsn, tx_id, events: Vec<Event> }`。
- `Event { v: 1, kind, run_id?, run_seq, dispatch_id?, node_id?, audit_seq, payload }`。
- 首次业务提交为 LSN 1；提交序号由唯一写者分配，失败事务不占序号。
- 事实存为 transactions/events 关系记录；保存完整事务摘要并在读取事实时校验。
- CLI 与运行证据包可导出 JSONL；磁盘权威是 SQLite 行，提交时只序列化一次。
- `lsn` 是事务序号，`run_seq` 是 run 内业务序号，`audit_seq` 是 dispatch 内审计序号。

### 3.2 领域事件

| 事件 | 归属 | 语义 |
| --- | --- | --- |
| `SegmentStarted` / `SegmentSealed` | 保留编码枚举 | SQLite 不生成分段事件，外部提交仍拒绝 |
| `Command` | 命令 | 写命令回执落账（scope / request_id / fingerprint / result / lsn） |
| `WorkflowCreated` / `WorkflowUpdated` / `WorkflowPublished` / `WorkflowDeleted` | 产品 | 定义生命周期；版本号必须连续（`non-contiguous definition version`） |
| `ScheduleChanged` / `WebhookChanged` / `TemplateChanged` | 产品 | 触发器与节点模板（UI 资产也随权威走） |
| `RunStarted` | run | run 创建，input 快照 + depth + source 归因 |
| `RunCancelled` / `RunCompleted` / `RunFailed` | run | 三种终态 |
| `DispatchStarted` | 节点 | 一次派发开始：`node_execution_id` + `attempt` + `dispatch_id` |
| `InputPrepared` | 节点 | 模板展开后的输入面已提交（含 input / predecessors / params） |
| `ValueChunk` / `ValuePublished` | 值 | 大值分块与发布描述符（走 `QueueClass::Audit(output_id)` / Control） |
| `OperationIntent` + `OperationAuthorized` | 节点 | **同一事务两条**：外部操作授权事实（C3） |
| `OperationOutcome` | 节点 | 外部操作结果已捕获 |
| `OperationRetryAuthorized` | 节点 | 显式 repeatable 外部操作的持久机器重试授权：清除未决操作、进入持久 backoff（见 [EXECUTION.md](../EXECUTION.md)） |
| `NodeCompleted` / `NodeFailed` / `NodeSkipped` | 节点 | 节点终态 |
| `WaitRegistered` / `WaitResolved` | 节点 | 持久等待登记/解除（delay/signal/child/retry/uncertain） |
| `SignalReceived` | 节点 | 外部信号已落盘 |
| `Adjudicated` | 节点 | 人工裁决已落盘 |
| `LateAudit` | 节点 | 迟到的审计证据（不得推进 run_seq） |
| `MasterEpochStarted` | 数据集 | 写者任期开始；必须 dataset 级、epoch 恰好 +1 |
| `LegacyImport` | 数据集 | v1 历史只读保全基线 |

**协议层的两条结构性校验**（`State::apply_mode`，重放与提交同路）：

- `OperationIntent` 的下一条必须是 `OperationAuthorized`，且 `run_id`/`dispatch_id`/
  `node_id`/`payload` 全同、两者 `audit_seq` 都为 0；
- `OperationAuthorized` 的前一条必须是 `OperationIntent`。

### 3.3 State：唯一领域转移规则

`flow-engine/src/journal_state.rs` 的 `State::apply(&Transaction)` 是**唯一**的状态
转移函数。提交前用同一规则预演并生成当前实体变更，预演始终撤销；COMMIT 后才更新内存。
启动加载当前实体，正常启动不重放事实账本。离线事实核验仍可以使用 reducer。

```
Transaction 流 ──apply──> State {
    journal_id, applied_lsn, master_epoch, version,
    values: Publications,              // 已发布值描述符缓存（flow_journal::value::Publications，
                                       // 尾部保留 PUBLISHED_TAIL = 8192 条）
    workflows: BTreeMap<String, Workflow>,
    runs:     BTreeMap<String, Run>,
    schedules / webhooks / templates / legacy: BTreeMap<String, Value>,
    commands:  BTreeMap<String, CommandRecord>,   // 幂等窗口，见 §11.2
    // 另有 run_workflows / schedule_fires / compact_audits 等内部簿记字段，略
}
```

注意 `State` 上的 u64 字段（`applied_lsn` / `master_epoch` / 各 `audit_seq` /
`run_seq`）在序列化时**写成字符串**（`snapshot_decimal`），反序列化时两种形态都接受
——避免 JSON number 在 JS 客户端丢精度。

```
Run {
    run_id, workflow_id, workflow_version, definition: StoredValue, input: StoredValue,
    source, source_detail, parent: Option<Parent{run_id,node_id}>, depth,
    status: RunStatus, output, error, last_run_seq,
    nodes: BTreeMap<String, Node>,
}

Node {
    node_execution_id, attempt, dispatch_id, status: NodeStatus,
    output, branch, prepared: Option<Prepared>, wait: Option<Wait>,
    operation: Option<Operation>, attempts: BTreeMap<dispatch_id, Attempt>,
    error, skip_reason,
}

Attempt {          // 一次 dispatch 的账本：结果屏障记在这里
    audit_seq, audit_digests, audit_lsns, result: Option<Event>,
    result_lsn, sealed, integrity,
}
Operation { operation_id, fingerprint, permit_id, request, outcome: Option<StoredValue> }
Prepared   { input, predecessors, params, engine_build, node_semantics }
Wait       { kind: WaitKind, wake_at, child_run_id, output }

```

**关键折叠规则**（都有单测钉住）：

- `DispatchStarted`：清 `wait`、`status = Running`、`attempt = 上次 + 1`（必须恰好
  +1，否则 `unsafe dispatch retry`）、新建 `attempts[dispatch_id]`。
  **带未决 `operation`（outcome 为 None）时拒绝再次派发**——这是 C4 的折叠层闸门。
- `NodeSkipped`：整条替换记录为 `Skipped` + `skip_reason`，不带 attempts 账本。
- `InputPrepared`：校验 `prepared.input == run.input`、`node_semantics ∈ {1,2}`、
  每个前驱必须已 `Succeeded` 且输出逐字相同——**准备事实不得与已提交事实漂移**。
- 任何 `audit_seq > 0` 的事件：必须 `audit_seq == attempt.audit_seq + 1`，且
  dispatch 已 sealed 时只接受 `LateAudit`（`audit gap/duplicate or sealed dispatch`）。
- `NodeCompleted | WaitRegistered | NodeFailed`：**结果审计屏障**——
  `sealed_through` 必须等于 `attempt.audit_seq`；`prepared` 缺失时载荷必须带
  `preparation_failed: true`。通过后 `attempt.sealed = true`、记下原事件。
- `OperationAuthorized`：run 终态或已有 operation 即拒；`fingerprint` 必须等于
  请求字节的摘要（`StoredValue::Ref` 取 `value.digest`）。
- `WaitRegistered`：`kind == Uncertain` → `run.status = AwaitingResume`；
  其余（delay/signal/child/retry）→ `run.status = Running`。**同词同义**：
  运维看到 `awaiting_resume` 即代表需要人工介入。
- `NodeFailed` 带 `retry_wake_at` 且无 operation：节点转 `Waiting{Retry}` 而非
  Failed；**有 operation 时拒绝**（`external operation cannot use pure retry`）。
- `Adjudicated`：`accept_output` → Succeeded；`retry` → **Pending**（清 operation/
  error/output，等驱动器以 attempt+1 重新派发）；`failed` → Failed。三种都把
  `run.status` 拉回 `Running`。
- `Command`：幂等窗口 `COMMAND_WINDOW = 8192` 条，超窗按 lsn 序淘汰最旧一半。
  淘汰规则只依赖事件序，重放确定性不受影响。
- `MasterEpochStarted`：epoch 必须恰好 +1，且事件不得带 run/dispatch/node 身份。

### 3.4 写序协议与组提交

协调器串行决定 → reducer 预演并撤销 → 有界写入 owner → 一个 SQLite 事务：
事实、当前实体、永久回执、值正文/描述符及提交水位 → COMMIT → 内存 apply → 确认与调度。

- 专用线程独占写连接；`WAL + synchronous=FULL`，同时执行 `PRAGMA fullfsync=ON`
  （由 SQLite 在不支持的平台自行忽略）。
- 控制与审计队列继续按字节和请求数背压、轮转；当前实体变更也计入字节预算。
- 批次最多等待 5 ms，SQLite 事务提交整批；失败时整批回滚。平台故障（介质/IO 类）
  冻结 writer（fail closed，不能确认半套数据）；输入类错误（Invalid/Limit/Conflict）
  只拒绝当前批，writer 继续服务。
- 调用方取消不取消 owner 已持有的提交；后续请求和协调循环会接续其结果。
- 查询连接只读，读取相同数据库；`committed` 与 `visible` 对应同一提交，无独立 projector。
- 外部副作用只能在授权提交后发送；未知结果继续等待人工裁决。

### 3.5 磁盘布局

```text
<data_dir>/
  flow.sqlite              # 唯一业务数据库
  flow.sqlite-wal          # SQLite 管理；运行中不可单独删除
  flow.sqlite-shm           # SQLite 管理
  observations/            # 独立、有界、允许丢弃的日志
```

目录 0700、主文件 0600，拒绝数据库符号链接。小值 ≤64 KiB 内联；大值按 ≤256 KiB
分块保存在 value_chunks 的 BLOB，单值上限 256 MiB。事实记录只保留 chunk 元数据，
读取/导出时恢复编码，正文不在事件 JSON 内重复保存。

### 3.6 表与索引

权威 DDL：[`flow-journal/src/schema.sql`](../../crates/flow-journal/src/schema.sql)。

| 表 | 职责 |
| --- | --- |
| store_meta | 数据集身份、提交水位和领域提交模式 |
| transactions / events | 接受事实、完整事务摘要与提交字节数、run/dispatch 关联 |
| current_entities | 每个 `(kind,key)` 唯一当前对象；`parent/slot/extra` 是写时解析的键列，`workflow_id/status/source/name/dispatch_id` 是由 body 生成的查询列；覆盖更新 |
| value_chunks | output/chunk 索引下的原始正文与摘要 |

当前行仍以 JSON body 作为 wire/事实投影的载体，但所有查询用显式列和 `WHERE kind=...` 分区索引，
不再对 `key`/`body` 做 `json_extract` 表达式索引。workflow 元数据与每个 definition version 分开；
run 与 node 分开；node 与 attempt 分开。
command/audit 回执、值发布描述符、父子关系和 dispatch 所有权使用有明确用途的当前记录。
未完成流的进度不落盘：值连续性、总字节和摘要由写者在提交时校验一次。
不保留每次修改后的完整实体版本，不另建 execution index、projection、应用 checkpoint 或位置文件。

列表过滤、排序、分页与计数在 SQLite 执行，`WHERE kind=...` 分区索引覆盖 workflow/status/source/name，
另有 run-node、attempt、child、definition-version 和 run-event 索引。正常启动只取活跃 run。
