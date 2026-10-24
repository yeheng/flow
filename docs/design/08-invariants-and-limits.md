# 08 · 不变量清单与已知限制（§13、§15）

> 本文是 [DESIGN.md](../DESIGN.md) 的分篇。章节编号与全文一致；总索引与核心抉择见 [DESIGN.md](../DESIGN.md)。

## 13. 不变量清单

这份清单是代码审查与测试设计的验收标准。每条都应有对应用例；违反任何一条
都是 bug，不是「设计选择」。

### 13.1 权威性

| # | 不变量 |
| --- | --- |
| I1 | flow.sqlite 是业务权威；事实、当前状态、回执和待处理动作原子提交 |
| I2 | `lsn` 全局无缺口；唯一写者分配，失败事务不占号 |
| I3 | 同一 `data_dir` 同时只有一个写者（flock 排他，进程内 + OS 双层） |
| I4 | 接受事实与查询状态同时提交，不存在独立投影进度 |
| I5 | 所有 run 状态迁移都是 `State::apply` 折叠的结果，不存在第二份状态语义 |

### 13.2 副作用安全（C3/C4）

| # | 不变量 |
| --- | --- |
| I6 | 任何外部副作用发生前，`OperationIntent` + `OperationAuthorized` 必须已作为**同一事务**落盘 |
| I7 | `operation.outcome` 为 None 的节点不得再次派发（`unsafe dispatch retry`） |
| I8 | 未知结果默认等待人工裁决；仅显式 repeatable 且持久提交机器重试授权时可自动重试 |
| I9 | `retry_wake_at` 与 `operation` 互斥（`external operation cannot use pure retry`） |
| I10 | `OperationIntent` 与 `OperationAuthorized` 在事务内相邻、载荷全同、`audit_seq` 均为 0 |
| I11 | `OperationAuthorized` 的 `fingerprint` 必须等于请求字节摘要 |

### 13.3 平台故障 ≠ 工作流失败

| # | 不变量 |
| --- | --- |
| I12 | `RunStatus::AwaitingResume` 只由 `WaitKind::Uncertain` 触发，只表示平台故障 |
| I13 | 节点失败不立即终结 run；独立分支跑完，下游跳过，`settle_run` 收口 |
| I14 | `delay`/`signal`/`child`/`retry` 等待期间 `run.status` 恒为 `Running` |
| I15 | 协调循环 panic → 整个循环重启，从折叠态重建，不丢已提交证据 |

### 13.4 审计与屏障

| # | 不变量 |
| --- | --- |
| I16 | `audit_seq` 在 dispatch 内严格 +1；重复序号必须内容相同（`audit retransmit with different content` 是错误） |
| I17 | 结果事件的 `sealed_through` 必须等于 `attempt.audit_seq`（`NodeCompleted` 额外 +1） |
| I18 | dispatch 已 sealed 后只接受 `LateAudit`，且不得推进 `run_seq` |
| I19 | `InputPrepared` 的 `prepared.input` 必须等于 `run.input`，`node_semantics` 必须为 1 或 2 |
| I20 | `Attempt::finish` 幂等：同 dispatch 已有结果时，比较 kind 与去掉 `sealed_through` 的载荷 |

> **I17 的读法**：写侧 `Attempt::finish` 把 `NodeCompleted` 自身作为审计条目计费，
> 其 `audit_seq = sealed_through = attempt.audit_seq + 1`；`NodeFailed` /
> `WaitRegistered` 的 `audit_seq` 为 0、`sealed_through` 记当前值。折叠层统一判
> 「事件过账后 `sealed_through == attempt.audit_seq`」，两种情形自洽。

### 13.5 进程与协议边界

| # | 不变量 |
| --- | --- |
| I21 | 节点执行语义只存在于 `flow-executor`；master 不在进程内运行用户代码 |
| I22 | 执行器缺失或协议版本不符 → 明确失败，**不**静默回退进程内执行 |
| I23 | 控制帧 ≤ 128 KiB 且不含用户值字节；变长值走 data 语义帧 256 KiB 分块 |
| I24 | `ResultCommitted` 之前执行器不得退出（否则「已算未落盘」无法判别） |
| I25 | `in_process` 执行模式在任何平台都不可配置 |

### 13.6 执行协议线路（v7 postcard 单流）

**唯一帧格式**：`u32 body_len（大端）| postcard(Message)`。整条线只有这一种帧，
没有第二种物理通道；postcard 的**变体索引即帧类型**。

```
┌─────────────────────┬──────────────────────────────────────────────┐
│ u32 body_len (BE)   │ postcard(Message)                            │
└─────────────────────┴──────────────────────────────────────────────┘
 读端一见长度头就校验上限，分配任何载荷缓冲之前拒绝非法长度
```

**帧预算按消息类型分类**（`frame.rs::frame_limit`）——分类只服务上限，不构成通道：

| 预算 | 常量 | 覆盖的消息 |
| --- | --- | --- |
| data 语义 | `DATA_MAX_FRAME` = 1 MiB | `TransferChunk`、`AuditBatch`、`ObservabilityBatch` |
| control 语义 | `CONTROL_MAX_FRAME` = 128 KiB | 其余全部（含 `Hello`/`Execute`/`Result`） |
| 递归 | — | `Routed` 取**内层消息**的预算：内层是 control 语义（如 `Reject`/`Heartbeat`）时按 128 KiB 受限，内层是 data 语义（如 `TransferChunk`）时按 1 MiB 受限 |

`Execute` 是控制帧里**唯一承载大块用户可控内容**的消息：`ExecuteTask.node` 是
未展开的节点定义（`Value`）。dispatch 侧（`flow-backend` 派发路径）对**整个
`Execute` 消息的真实 wire 编码**按 `CONTROL_MAX_FRAME` 预检，超限拒绝派发而不是
杀会话——分别预检 node/previous_outcome 会漏掉两者相加超限，所以按整条消息校验。
其余控制帧只带短身份串（`IDENTITY_MAX_BYTES` = 128）与能力列表——`Hello.capabilities`
同样可变长，因此 `encode_frame` 对它一样吃 `CONTROL_MAX_FRAME` 上限（`frame.rs`
用 200 KiB 的能力列表钉住这条）。

**版本门集中在握手**：v5 起每帧不再自带版本号，`Hello`/`Welcome` 携带
`version`（= `PROTOCOL_VERSION` = 7），任一侧不匹配即明确拒绝。跨版本配对在握手
处收口，不依赖逐帧校验。

**审计计费与 wire 编码解耦**：`record_bytes()` 是主进程与执行器共用的确定性
**JSON** 编码长度函数（窗口预算与幂等内容比对用），与 postcard wire 编码无关。
同一个 `audit_seq` 在两条线路上有两种编码，但只有 JSON 那一种参与计费。

**消息目录（28 个变体）**，按方向：

| 方向 | 消息 |
| --- | --- |
| 子/agent → 主（单向） | `Hello`、`Ready`、`Accepted`、`AuditBatch`、`RequestOperation`、`Result`、`Stopped`、`ObservabilityBatch`、`Heartbeat` |
| 主 → 子（单向） | `Welcome`、`Execute`、`AuditAck`、`OperationPermit`、`ResultCommitted`、`Cancel` |
| 双向 | `TransferChunk`、`InputReady`、`Reject`、`Routed` |
| agent ↔ 主（上联管理） | `AgentHello`、`AgentWelcome`、`CapacityReport`、`BindExecutor`、`BindExecutorAck`、`Resume`、`ResumeReply`、`Drain`、`DrainComplete` |

四行合计 9 + 6 + 4 + 9 = **28 个变体**，与 `message.rs` 的 `type_name()` 分支一一
对应（`postcard_decode_rejects_unknown_variant_index` 用索引 28 越界钉住这个数）。
`Heartbeat` 实际只有子/agent 一侧发送（master 只消费），故归入上行单向。

**`Routed` 是远程中继的唯一包络**：`Routed{envelope, inner: Box<Message>}`。
v5 起内层 `TransferChunk` 的字节由嵌套枚举原生承载，v4 那个专用
`KIND_ROUTED_TRANSFER_CHUNK` 帧与其三处 `dispatch_id` 一致性校验随枚举嵌套自然
消失。`dispatch_id` 仍是路由键：远程中继按它把帧包进信封发往归属执行器。

`routable()` 规定内层禁止嵌套 `Routed`、禁止上联管理帧与握手帧；`Reject` 与
`Heartbeat` 例外——执行器的任务失败通知与心跳必须能经 agent 中继给主进程。
这条规则在**出站 wrap 侧与入站解包侧双向执行**（master 解包 `Routed` 时同样
校验内层）；`Reject` 自身没有派发归属字段，由 agent 的 fair_mux 桥按来源句柄
注入归属标签后再包装——否则上联包装器按「无归属」静默丢弃，master 的终态
错误入口就收不到远程基础设施故障。取消后的排空有截止时间
（`CANCEL_REAP_TIMEOUT_MS` = 30s）：持续心跳但不响应 Cancel 的执行器到点由
master 派发方放弃等待、agent 侧强制收割，不能无限占用槽位。

**对抗性输入的防线**（`message.rs`/`frame.rs` 的单测各钉一条）：

- 帧内尾随垃圾 → postcard 未恰好消费完帧体 → 协议错误（防「合法消息 + 附加字节」走私）；
- 巨型长度声明（`String` len varint = `u64::MAX` 量级）→ 先做边界检查再取字节，
  小帧内立即报错，**不先按声明长度分配**；
- 变体索引越界（目录 28 个）→ 明确拒绝；
- 半帧/粘帧 → `FrameDecoder` 累积缓冲，仅在帧完整、预算通过后才从缓冲摘除；
- **`Routed` 嵌套深度**（`ROUTED_MAX_DEPTH` = 8）→ `Routed.inner` 的反序列化带
  深度计数，超限在解码层拒绝。没有这道门，每层仅约 7 字节的嵌套可在 1 MiB 合法
  帧内塞约 15 万层递归，postcard 递归下降解码（及派生 Drop/Debug）栈溢出 abort，
  且发生在握手/令牌校验之前——对等端一发帧即可打崩单写者权威进程。深度门同时
  让成功解码值的派生递归有界。

**为什么删掉 `Channel`**：v4 用一字节 `kind` + JSON meta 区分三种帧、另设
`Channel` 概念表达 data 归属；v5 把整个 `Message` 枚举交给 postcard 后，变体索引
天然承载类型、嵌套枚举天然承载层级，这两个概念都只剩冗余。线路两端都是 Rust，
用户 JS 摸到的是节点输入值而不是协议帧——v4 为 JS 安全整数设的十进制字符串戒律
也随之删除。

---

## 15. 已知限制与未做

**明确未实现**（不要按已实现规划）：

| 项 | 状态 |
| --- | --- |
| master 高可用 / journal 复制 / 多写者 | 未实现，与 C5 冲突 |
| 多 master 调度与接管 | 非当前产品范围（§7.7） |
| 多租户授权与用户体系 | 未实现（C7） |
| 恰好一次外部调用 | **不可能实现**（§11.4） |
| OpenTelemetry 链路追踪 | 已定未实现（`docs/opentelemetry-design.md`） |
| `in_process` 执行模式 | 已全平台删除 |

**已知取舍**：

- **单写者与 WAL 成本**：写入由唯一 owner 串行决定，长读、大正文与维护会影响 WAL 和尾延迟；不提供多协调器 HA。
- **订阅是轮询**：固定 50ms，无 gap-fill 状态机（§11.3）。
- **历史保留未裁剪**：事实、永久回执、旧 attempt 与正文继续增长；本次没有引入 TTL 或 GC。
- **审计吞吐受 W_BYTES 约束**：单 dispatch 在飞审计 ≤ 2 MiB，是协议硬上限。
- **drain 运维入口未接线**：`AgentManager::drain` 目前只有测试/e2e 调用（§7.6）。
