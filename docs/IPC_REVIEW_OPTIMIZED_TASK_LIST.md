# IPC 方案复核、优化方案与实现任务清单

复核日期：2026-10-13。依据当前工作区源码和本次定向测试。原始输入：`review.md`。本文交付方案与任务清单；实现进度见下方【实现进度】（2026-10-13 汇总；推荐范围 T01–T13 全部完成，仅剩条件任务 T14）

| 任务 | 状态 | 关键落点 |
| --- | --- | --- |
| T01 基线 | ✅ 完成 | `examples/ipc_baseline` + `docs/IPC_PROTOCOL_BASELINE.md`（含分配计数） |
| T02 握手同帧读取 | ✅ 完成 | agent/master 握手统一经 `FrameTransport`；双侧粘连帧回归 |
| T03 背压与关闭 | ✅ 完成 | 会话故障状态直达 recv；三个服务循环有界出站积压 `SendBacklog` |
| T04 收敛单流 | ✅ 完成 | 删除 `spawn_streams`/双流编解码/DataBind |
| T05 类型化编解码 | ✅ 完成 | 解码 `decode_envelope`（RawValue 借用直析）；编码直写帧缓冲 |
| T06 审计计量缓存 | ✅ 完成 | 队列项缓存规范编码大小，push/flush/重放共用 |
| T07 连续 transfer 缓冲 | ✅ 完成 | 执行器与 backend 均改连续前缀缓冲，去块树与二次拷贝 |
| T08 中继所有权与预算 | ✅ 完成 | `unwrap_from_master_owned` 移动；上联字节预算 + 按字节轮转 |
| T09 故障矩阵 | ✅ 完成 | `dispatch::fault` 确定性检查点 + `journal_fault_matrix` 三场景 |
| T10 打包/启动/兼容 | ✅ 完成 | 回收无孤儿、取消重建不泄漏、env_clear + FD 隔离、loopback 原语 |
| T11 端到端复测 | ✅ 完成 | 全量回归 + 基线复跑 |
| T12 二进制 wire | ✅ 完成 | v4 是唯一协议（删除 v3）；transfer raw blob；≈8× + 25% 线字节 |
| T13 WSS 上联 | ✅ 完成 | `WsByteStream` + `listen_wss` 认证 + agent 客户端 + 集成测试 |
| **T14 Windows Agent** | ⛔ **未做（条件未触发）** | 无 Windows 产品要求；交叉检查被 rquickjs-sys/aws-lc-sys 原生依赖与缺失 MSVC 工具链阻断，需真实 Windows CI |

补充说明：
- T12 最终形态是「v4 唯一协议」：v3 帧编解码、base64 `TransferChunk` 路径、`protocol` 配置开关与 `spawn_v4` 双轨全部删除，`PROTOCOL_VERSION = 4`；Journal schema 不变（仍 base64 ValueChunk），WSS 复用同一帧格式。
- 期间修复的存量缺陷：`listen_tls` 启动阻塞（v3 收敛时改为直跑 accept 死循环）；`data_addr` 存量配置兼容回归。
- 唯一已知抖动：`journal_ipc::killing_executor_mid_script_recovers_with_new_process`（SIGKILL 时序）在并行负载下偶发失败，单独运行稳定通过。

【品味评分】

凑合。单流和减少编码开销的方向正确；现状判断过时，把可靠传输、持久确认、外部操作授权混为一谈。按原文整体重写会删掉正确性机制，还会引入新的协议和部署复杂度。

【致命问题】

1. **批判的双连接架构已经被改掉。** `execution_protocol/contract.rs:18` 当前版本为 v3；Unix 使用单 socketpair，非 Unix 本地执行使用单 loopback 连接；`FrameTransport::spawn` 使用单发送队列、单读任务和单写任务。Agent 上联也已使用单条主动建立的 mTLS/TCP 连接。`control/data` 目前主要是消息分类和预算上限，不是两条生产连接。
2. **删除持久窗口会破坏有界生产。** `AuditStream` 的窗口约束待发送及已发送未持久确认的数据。socket 写成功不证明 Journal 已经提交。逐条请求应答只能用串行等待替代窗口，会牺牲批处理能力；仍然需要生产队列、字节预算和取消处理。
3. **本地不重传已经实现；远程重放有真实用途。** `audit.rs` 默认 `remote_replay=false`，只有 Agent 启动的执行器通过 `--remote-relay` 开启重放。远程重连后需要补齐尚未得到持久确认的审计，TCP/WSS 都不能证明断线前的最后一条消息是否提交。
4. **授权请求应答也已经实现。** `dispatch.rs:938` 的路径校验请求字节与指纹，持久提交 Intent/Authorized，随后发送 Permit；`task.rs:1025` 才接收许可并继续。不能把 `operation_id`、指纹、派发身份、取消裁决压成一个通用 `Seq`。
5. **“全部去掉 Base64”和“零分配帧中继”都不成立。** 输出 `ValueChunk` 是既有 Journal JSON 记录，`flow-journal/src/value.rs` 的 `Chunk.data` 为 Base64 字符串。改变传输编码不等于改变持久格式。Agent 还必须处理绑定、会话归属、重连对账、容量和进程回收，不能退化为无状态字节泵。
6. **WS 分片不能让应用 Cancel 任意插队。** WS 的 Close/Ping/Pong 可以插入分片消息，普通应用消息不能随意交错。应用 Cancel 必须作为独立消息，在完整消息之间调度；已经进入 TCP/TLS 缓冲的数据不能被抢占。64 KiB 是待测的调度参数，不是协议规定，也不是延迟保证。

【改进方向】

删除过时的双流内部实现与重复编码，保留持久确认和恢复；让消息所有权穿过中继而不是深拷贝；先测量，再决定是否升级大载荷 wire 和增加 WSS。

【核心判断】

值得做：清理 v3 之后留下的双流实现，维护准确的协议说明，减少重复 JSON 构造、复制与审计计量，有界化中继的物理缓冲，修正握手与关闭路径。

不值得按原文直接做：重新实现已完成的单连接、删除持久窗口和远程补传、增加统一序号、全面替换小消息编码、为“统一”而引入 WSS 或 HTTP 大文件通道。现有本地与远程已经共用 `FrameTransport` 和业务消息。

【关键洞察】

- 数据结构：连接负责有序传输；派发负责身份与归属；审计序号负责连续持久前缀；操作身份与请求指纹负责外部调用授权。四件事不能塞进一个序号。
- 复杂度：真正该删的是中间 `serde_json::Value` 树、重复编码计量、重复大载荷复制和双流测试路径。窗口和授权屏障是业务不变量。
- 风险点：静默丢帧、旧会话越权、未提交就 ACK/Permit、结果超过持久前缀、重连重复外部调用，以及把有界帧数误当成完整内存预算。

## 1. 原文逐项核验

| 原文主张 | 核验结论 | 优化后的处理 |
| --- | --- | --- |
| 当前有本地 Control/Data 两个 socket | 不属实，生产路径已经是 v3 单流 | 验收已有单流，清理遗留分支 |
| 单任务就必然只能使用单流 | 结论过强；单流是本项目合适的简化，不是逻辑必然 | 保持单流，同时处理共享队列的背压 |
| OS FIFO 永久消除命令/数据乱序 | 只保证同方向已写字节的顺序；生产者提交顺序仍由应用保证 | 单写者；保留 Execute→输入、请求块→Ready→请求的依赖 |
| 当前 IPC 很慢 | 缺少可用的当前性能证据 | 先建立编码和端到端基线，再承诺收益 |
| 本地滑动窗口是模拟丢包 | 错误，它限制尚未持久确认的审计生产 | 保留持久信用和批量 ACK |
| 本地可以去掉重传 | 正确，但当前已经关闭本地重放 | 回归覆盖，不重复实现 |
| 请求应答代替网络栈 | 外部授权当前就是请求应答 | 保留操作身份与持久裁决，不新增通用 Barrier |
| 去掉分块校验 | 只能讨论冗余的每块哈希；最终内容指纹和值摘要必须保留 | 先简化装配，测量哈希成本；首轮保留现有校验 |
| 二进制 JSON 更简单 | 未指定格式、类型语义和版本，不能作为实现契约 | 首轮保留 JSON；优化类型化编解码 |
| WSS 可以消除双 TLS 和 DataBind | 双 TLS/DataBind 已从生产连接握手移除 | WSS 只服务 HTTP 代理/部署接入需求 |
| Agent 是纯 Frame Pump | 不完整；它是受信任、有状态的路由和进程管理者 | 只精简载荷处理，不删归属和生命周期状态 |
| DispatchID 固定为 16 字节 UUID | 现有 wire 类型为 String，fixtures 有 `dispatch-09` 等值；不能直接收窄契约 | 保留字符串 ID，不新增映射表 |
| Binary WS 等于零 Base64、零分配 | 错误；载荷仍可能是 JSON/Base64，客户端 WS 还需要 masking | 指标写成减少复制与编码，实际测量 |
| 每帧 64 KiB 保证 Cancel 立即到达 | 错误；受发送队列、消息边界、接收队列与网络速度约束 | 先测取消延迟，按完整消息边界调度 |

证据入口：

- [本地单流与版本](../crates/flow-engine/src/execution_protocol/contract.rs)、[spawn](../crates/flow-engine/src/execution_protocol/spawn.rs)、[loopback](../crates/flow-engine/src/execution_protocol/loopback.rs)。
- [单流与遗留双流实现](../crates/flow-engine/src/execution_protocol/transport.rs)、[现有帧编解码](../crates/flow-engine/src/execution_protocol/frame.rs)。
- [Agent 主动单 TLS 上联](../crates/flow-agent/src/runtime.rs)、[Master 单 TLS 接入](../crates/flow-backend/src/execution/remote.rs)。
- [本地/远程审计重放区别](../crates/flow-executor/src/audit.rs)、[Agent 启动参数](../crates/flow-agent/src/pool.rs)。
- [授权、持久确认与结果屏障](../crates/flow-backend/src/execution/dispatch.rs)、[Journal 持久提交](../crates/flow-journal/src/writer.rs)。
- [输入装配与输出分块](../crates/flow-executor/src/transfer.rs)、[持久值格式](../crates/flow-journal/src/value.rs)。
- [RFC 6455 §5.4、§5.5：分片与控制帧](https://www.rfc-editor.org/rfc/rfc6455.html#section-5.4)、[§5.1：客户端 masking](https://www.rfc-editor.org/rfc/rfc6455.html#section-5.1)。

## 2. 优化后的实施方案

【Linus式方案】

1. **沿用现有架构。** Backend/Agent→Executor：单 socketpair 或既有 loopback；Backend↔Agent：现有单 mTLS 连接。每个方向一个串行写者，读循环独立运行。
2. **把传输事实与持久事实分开。** `send` 只表示入队；`AuditAck` 只表示连续持久前缀；`OperationPermit` 只表示已提交的外部操作授权；`ResultCommitted` 只表示结果已提交。Heartbeat/InputReady 不推进持久游标。
3. **首轮不改 v3 wire。** 保持 `u32 BE length + u8 channel + JSON`，明确 length 包括标签和 JSON。类型化序列化直接写目标缓冲；解码使用借用信封和 `RawValue`，避免完整 JSON 树反复 clone。
4. **保留窗口，减少窗口维护成本。** 记录生成时缓存确定性编码大小；本地只保存必要的计量元数据，远程保留有界未确认数据。网络排队、重传副本和解码扩张另计物理预算。
5. **输入装配采用连续缓冲。** 同一 transfer 只追加下一段；同内容的已接收前缀补发作为幂等重复接受，缺口或冲突立即拒绝。保留最终长度、摘要、总预算和引用身份。
6. **只在无业务依赖的消息之间调度。** 可合并/丢弃观测，Heartbeat 可合并；Cancel 不得重新打开或改变已提交事实。不同 dispatch 按字节公平仲裁，同 dispatch 的审计和传输依赖保持原顺序。
7. **二进制大载荷与 WSS 分别设置进入条件。** 编码开销确为瓶颈才做二进制 transfer；确需 HTTP 代理接入才做 WSS。两者不互相绑定，不把 Journal 升级塞进传输升级。

首轮不引入：Named Pipe 新承载、通用 Seq、CBOR/MessagePack、HTTP blob 服务、持久 Agent spool、透明重复外部操作。保留既有 `audit_seq`、`operation_id`、`result_id` 和会话身份。

## 3. 实现 Task List

所有任务均为待实施。T01–T11 为推荐范围；T12–T14 为明确有条件的扩展。每项先完成其依赖，再进入下一项。

### T01 / P0：冻结 v3 现状、不变量与性能基线

- **状态：已完成（2026-10-13）**。`examples/ipc_baseline`（可复现命令 + 分配计数器）与 `docs/IPC_PROTOCOL_BASELINE.md`（环境、原始数值、口径与边界）已交付；二/三期历史文档已在 v3 收敛时删除（历史保留为历史）。
- **目标**：让优化针对当前实现，并建立能够复测的收益证据。
- **背景**：原文仍描述双连接；二、三期历史正文也有双流内容，最新工作区已补充 v3 现状说明，需要验收历史与当前实现的边界。现有未跟踪 `examples/ipc_design_probe.rs` 的解码基准把含长度头的整个 frame 交给 JSON parser，且新路径硬编码字段、并非完整等价实现，不能用于收益结论。
- **范围**：本文件；`docs/JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md`、`PHASE3_PLAN.md` 的当前状态说明；`crates/backend-perf/src/` 或独立可复用的协议 benchmark。原始探针不纳入生产实现。
- **方法**：列出实际生产调用链；记录协议版本、帧和输入上限、窗口公式；对小控制消息、审计批次、最大合法传输、大输出、远程断网逐项测量编码 CPU、分配次数/字节、吞吐、ACK/Permit/Cancel 延迟、RSS、Journal sync 时间。记录机器、负载、构建参数与原始结果。
- **测试**：基准先通过完整消息 roundtrip/语义等价断言；同时跑 release 微基准和端到端场景，区分编码、进程启动与磁盘提交耗时。
- **验收**：可重复的基线命令和报告齐全；单流已完成的事项不进入重写任务；性能结论有实测支持。
- **约束**：不把残缺探针的结果当产品指标；不覆盖用户已有未跟踪文件；不承诺未经测量的倍数提升。

### T02 / P0：握手全程使用同一个帧读取器

- **状态：已完成（2026-10-13）**。两侧握手均先建 transport 再收发 AgentHello/AgentWelcome；新增 master 侧（attach_inprocess 粘连 Hello+尾帧）与 agent 侧（假 mTLS master 粘连 Welcome+尾帧）集成回归，尾帧必须交付（表现为整会话拒绝）。
- **目标**：消除 AgentHello/Welcome 握手交接时的尾部字节丢失。
- **背景**：Agent `connect` 和 Master `run_session` 都创建临时 decoder，取出首帧后丢弃 decoder，再创建新的 transport。一次 read 若包含 Welcome 和首条业务消息，临时 decoder 中的后续字节会丢失。这是源码可确认的丢字节路径；本次没有执行该路径的专门故障复现。
- **范围**：`crates/flow-agent/src/runtime.rs::connect`；`crates/flow-backend/src/execution/remote.rs::run_session`；`execution_protocol/transport.rs`；`journal_remote_tls.rs`。
- **方法**：完成 TLS 后直接创建 transport；AgentHello/AgentWelcome 也经同一个 transport 收发。握手增加整体 deadline，保留帧上限、版本与能力校验；不以重新 split/unsplit 重置解码状态。
- **测试**：Welcome+首条 BindExecutor 同一次写入；首帧与下一帧半截粘连；按字节拆分 Hello；EOF、超时、错误版本和错位握手消息。
- **验收**：交接不丢完整帧或半帧，后续帧恰好交付一次；认证完成前不注册 Agent；所有异常都回收连接资源。
- **约束**：不依赖 sleep 让帧分开；不额外再造握手专用 decoder；不绕过 mTLS 身份校验。

### T03 / P0：写失败和队列背压不能卡住关闭与接收

- **状态：已完成（2026-10-13）**。传输层（会话故障状态：错误槽 + Notify 许可 + 终止信号）与三个服务循环（executor 主循环、agent serve、backend pool 会话）全部落地有界出站积压（`SendBacklog`）；满发送队列期间入站 Cancel/ACK/心跳/关闭均可处理，阻塞中的发送方随写任务退出释放。
- **目标**：让 I/O 故障、取消和 shutdown 在发送拥塞时仍然可处理。
- **背景**：当前 writer 失败主要置 `broken`，不直接唤醒阻塞的 `recv`；若 reader 仍然挂起，故障传播依赖后续活动。多个服务循环在 select 分支中直接等待 `transport.send`，满发送队列期间不再选择接收/关闭分支。
- **范围**：`execution_protocol/transport.rs`；`flow-executor/src/runtime.rs`；`flow-agent/src/runtime.rs`、`pool.rs`；Backend `execution/pool.rs`、`remote.rs` 的相同发送点。
- **方法**：会话共用终止信号，读写任一失败即触发，recv 能观察真实错误并随后稳定关闭；发送等待纳入主 select 或由有界出站任务承载；显式保留待发送消息的所有权；关闭采用既有宽限期并终止读写任务。
- **测试**：伪 AsyncWrite 报错而 AsyncRead 永久 pending；发送队列满时收到 ACK、Cancel、shutdown；对端停读；任务 panic；close 与发送竞态；正常 drain 写完最后确认。
- **验收**：等待 recv/send 的调用者均被唤醒；无 CPU 自旋、无丢失业务帧、无残留 I/O 任务；宽限期结束后必然退出。
- **约束**：不以无界后台队列“解决”阻塞；不将 Full 等同 Closed；不因观测丢弃拆掉正确性会话。

### T04 / P1：把内部传输与测试收敛到一套单流

- **状态：已完成（094034a）**。`spawn_streams`、双流帧编解码与 DataBind 握手已删，测试全部迁移单流；本轮复核无残留。
- **目标**：删除仍在维护的内部双流路径，保留消息预算分类。
- **背景**：生产已调用 `spawn`；`spawn_streams`、旧帧函数仍主要存在于测试。最新注释已标明它们是遗留路径；继续测试旧承载不能证明生产单流关闭语义正确。
- **范围**：`execution_protocol/{transport.rs,frame.rs,mod.rs}`；`flow-agent/src/runtime.rs` 测试；`flow-executor/src/observe.rs` 测试；相应设计文档。
- **方法**：单流内部状态只保留一个 outbound；把双流关闭/错误测试改成单流；逐一确认旧函数外部兼容边界。公开旧函数若仍需兼容，保留明确废弃的薄适配入口，内部不再依赖它。DataBind 和 data_credential 的旧 wire 字段在 v3 中保留兼容；运行时不再消费。
- **测试**：所有 fixture 单流 roundtrip；Execute→TransferChunk→InputReady；请求块→Ready→RequestOperation；graceful close；畸形帧；不同消息分类的上限。
- **验收**：生产和核心测试共用一套 I/O；旧描述有历史/现状边界；控制帧不会获得数据帧的大上限。
- **约束**：不能直接删除公开 API 或 v3 wire 字段；不能删掉 control/data 的独立预算语义；不能宣称全局生产者自动有序。

### T05 / P1：保持 wire 的类型化单遍编解码

- **状态：已完成（2026-10-13）**。解码：借用信封 + RawValue 类型化单遍解析（`Message::decode_envelope`），与旧 `from_envelope` 共用同一套 Wire 结构与拒绝行为（全 fixtures 等价测试）。编码：JSON 直写预留长度头的帧缓冲（消除中间 Vec 拷贝）。encode 侧信封 Value 树仍在（to_envelope），进一步类型化 Serialize 待复测证明必要后推进。
- **目标**：减少中间 Value 树与大 body clone，不改变 v3 消息契约。
- **背景**：`encode_tagged_frame` 先构造信封 Value、再 JSON 编码、再复制到帧；`from_envelope` clone body 后再次转换；解码器也有 payload 和去标签的重复复制。
- **范围**：`execution_protocol/{message.rs,frame.rs,transport.rs,fixtures.rs}`；远程握手统一后的编解码调用；必要的 Cargo 依赖配置。
- **方法**：使用借用的类型化 wire 结构和 serde，将 JSON 直接写入预留头部的帧缓冲并回填长度；解码信封借用 RawValue，再解析目标消息。调整 decoder 的缓冲消费方式，减少 drain 搬移和去标签复制。保留 decimal 字符串、字段规则、嵌套 Routed 校验与版本拒绝。
- **测试**：全部消息 fixtures；新旧编码的 wire 内容比较；现有确定性要求对应字节级比较；字段顺序变化、转义 Unicode、非法 decimal、未知类型、Routed 嵌套和最大合法帧；基线分配对比。
- **验收**：同样消息被旧/新 v3 codec 双向接受；JSON 数值和拒绝行为保持契约；大 payload 的复制/分配下降有测量记录。
- **约束**：不能用字符串 find 或手写拼接解析 JSON；不能依靠字段固定顺序；不能为省分配放松长度和身份校验。

### T06 / P1：缓存审计编码大小，保留持久信用与远程重放

- **状态：已完成（2026-10-13）**。队列项缓存规范编码大小；push_wait 等待与入队共用一次计量；flush/重放全部用缓存；窗口/ACK/重放语义不变（audit_window 套件全绿）。
- **目标**：去掉 push/flush/replay 的重复计量序列化，保证持久窗口不变。
- **背景**：同一 AuditRecord 的 encoded_bytes 被多次调用；本地已不保留重放副本，仍需要 sent/durable 累计量。
- **范围**：`flow-executor/src/audit.rs`；Backend `execution/dispatch.rs` 审计计量和去重处；`flow-executor/tests/audit_window.rs`。
- **方法**：私有队列项缓存记录的规范编码大小和必要摘要；本地保存有界计量元数据，远程保存未确认内容；预算按现有规范编码计算。校验 ACK 不超出已发送前缀，过期 ACK 不改变信用；批大小与帧上限统一计算。后端使用同一规范计算，不能信对端自报字节数。
- **测试**：窗口满后生产暂停、ACK 后恢复；输出大于窗口；取消等待；伪造未来 ACK/错误累计字节；本地不重放、远程同内容补传；同序号异内容拒绝。
- **验收**：逻辑 pending+unacked 不超过 W；InputReady/Heartbeat/socket write 不释放信用；AuditRecord wire 和 Journal 事实不变；重复编码次数下降。
- **约束**：不改成每条审计同步往返；不让 ACK 授权 HTTP；不能删远程未确认记录；不能用非规范 JSON 大小改变原计量口径。

### T07 / P1：连续 transfer 缓冲替代每块 BTreeMap

- **状态：已完成（2026-10-13）**。执行器输入装配与 backend op_transfers 均改为连续前缀缓冲：追加式接收、幂等前缀补发、缺口/重叠/异内容立即拒绝；整值摘要、每块校验、输入总预算与操作指纹保留；消除排序与二次拷贝。
- **目标**：消除按 offset 排序再整体复制的装配成本，保留重连补发。
- **背景**：单流同 transfer 正常有序；当前装配器仍维护块树和额外 assembled 缓冲。Backend 操作请求装配也维护块树。
- **范围**：`flow-executor/src/transfer.rs`；Backend `execution/dispatch.rs` 的 op_transfers；`flow-executor/tests/transfer_bounds.rs`；远程 reconnect 测试。
- **方法**：外层按 transfer_id 定位；内层使用连续 Vec、已接收长度和最终描述。offset==len 时追加；完全落在已接收前缀且字节相同时接受幂等重复；缺口、跨边界重叠或异内容拒绝。保留整值摘要、每块现有校验、输入总预算、操作请求指纹；计入保留和装配后字节，消费/取消时释放。
- **测试**：连续多块、同内容补发、异内容补发、缺口、offset 溢出、空值、过大声明、重复 Ready、Ready 冲突、跨多 transfer 总预算、重连后全输入重发。
- **验收**：无需排序或另拼一份完整值；结果与现有实现相同；远程重复输入不导致死等或重执行；预算在分配/追加之前检查。
- **约束**：不把所有非连续 offset 都当网络乱序暂存；不删最终摘要；不能只计算新块而漏掉已装配/仍持有缓冲；不能改变输入引用身份。

### T08 / P1：中继拥有消息，按字节预算与 dispatch 公平发送

- **状态：已完成（2026-10-13）**。`unwrap_from_master_owned` 移动内层（深拷贝消除）；上联共享出站字节预算（8 MiB，估算×2 安全系数，随消息所有权 acquire/release）；轮转桥按字节额度（64 KiB/轮且≤4 帧/轮）。估算口径（非精确序列化计量）已在代码与基线报告标注，精确化属 T12/复测后续。
- **目标**：减少 Agent 深拷贝，并防止洪泛和重复补传放大物理缓冲。
- **背景**：`unwrap_from_master(&Message)` 克隆整个 inner；中继多层 mpsc 主要按条数限制，一层 256 个最大帧就有约 256 MiB 载荷上界。持久窗口不能替代重复帧和所有中间队列的内存预算；目前按帧轮转也不等于按字节公平。
- **范围**：`flow-agent/src/{relay.rs,runtime.rs,pool.rs}`；必要的 transport 出站预算入口；Agent 相关测试及 backend-perf 远程负载。
- **方法**：中继解包消费 Message，移动 Box 内消息；保留借用公开入口的兼容适配。为 executor、共享中继及 transport 队列设硬字节预算，预算票据随消息所有权流转并在实际释放后归还；采用按字节额度的 dispatch 轮转。旧会话包装在实际发送时重新校验，未确认数据不丢；可丢观测优先舍弃。取消和收包不会因一个满 inbox 永久停止服务所有 dispatch。
- **测试**：大帧洪泛+小帧安静任务；未确认重放洪泛；断网期间队列增长；恢复后的旧 envelope；单 executor 公平性；慢 inbox 与另一个 dispatch 的 Cancel；取消时预算归还。
- **验收**：每层队列的字节上限及合计 RSS 峰值有记录；安静任务和取消有可测的服务上界；同 dispatch 依赖不被调度打乱；Agent 不产生权威确认。
- **约束**：不能用无限队列、深拷贝备用通道或提前 ACK 化解背压；不能未经裁决丢弃审计；不能为了公平重排同一 transfer/audit 的依赖。

### T09 / P0：冻结授权、结果提交与重连的故障矩阵

- **状态：已完成（2026-10-13）**。`dispatch::fault` 确定性检查点（未注册零开销）+ `tests/journal_fault_matrix.rs` 三场景：授权持久化前断线→HTTP 副作用为 0；持久化后/Permit 发送前取消→不发 Permit 且外部调用为 0；ResultCommitted 前断线→外部调用恰好一次且不误报成功。Resume 五裁决的既有 reconnect 套件继续覆盖。
- **目标**：使所有优化都经过同一组业务正确性验收。
- **背景**：单流消除跨连接乱序，不能消除 Journal 提交、取消与外部调用之间的竞态。远程已经有 Resume 五种裁决，应复用。
- **范围**：`journal_ipc.rs`、`journal_remote.rs`、`journal_remote_tls.rs`、`journal_remote_reconnect.rs`、`journal_remote_ops.rs`；必要的测试故障注入点。
- **方法**：复用现有基建加入可控暂停点，在持久化前后、Permit 发送前后、ResultCommitted 前后断线/取消；用 HTTP harness 计数实际副作用，用 Journal 回放核对事实。确定性注入代替依赖 sleep 的竞态复现。
- **测试**：未持久化不得 HTTP；取消先提交不发 Permit；授权先提交但 Outcome 未知不透明重跑；结果最后审计未提交不得 ResultCommitted；ACK/Permit/结果确认丢失后按原身份恢复；boot/epoch 变化与缺数据走 ReconcileRequired；同身份异内容拒绝。
- **验收**：本地、远程优化前后副作用次数和权威事件语义相同；外部调用不存在额外重复；所有 Resume 裁决和不可恢复场景有覆盖。
- **约束**：不改变恢复政策；不把重连当重新执行授权；不把 at-least-once 传输宣传成外部调用 exactly-once。

### T10 / P1：进程启动、旧配置和协议兼容验收

- **状态：已完成（2026-10-13）**。打包/启动/兼容全量验收落地：缺二进制与子进程秒退明确失败、握手拒绝、兄弟执行器定位（含空格路径）、旧配置（data_addr）解析、父端关闭 kill+wait/reap 无孤儿、连续取消与重建不泄漏、env_clear 与 FD 隔离（含通道 FD 阳性对照）、loopback 回连原语。`--channel-fd`/`--remote-relay`/旧地址配置保留。
- **目标**：让用户现有打包、启动参数和配置继续工作。
- **背景**：Unix Backend 和 Agent 已共用 spawn_executor_process；stdin 并非当前协议 FD。data_addr 已作为旧配置兼容字段忽略，不应重新变成必填或突然拒绝。
- **范围**：`execution_protocol/{spawn.rs,unix.rs,loopback.rs,contract.rs}`；Backend `execution/pool.rs`；Agent `pool.rs`、Executor `main.rs`；`flow-config/src/lib.rs`；`journal_ipc_packaging.rs`；配置示例与 OPS。
- **方法**：验证已有公共 spawn 路径；测试 env_clear、FD 隔离、父/子端关闭、kill+wait/reap、失败握手回收和兄弟二进制定位。保留 --channel-fd、回连 env、--remote-relay 和旧地址配置；明确废弃字段。首轮 wire 不变，无需 bump。
- **测试**：正常/缺二进制/子进程秒退；握手拒绝；父进程退出；连续取消与重建；真实产品打包启动；旧配置解析；本地 loopback 路径。
- **验收**：部署无需改启动入口；无孤儿进程/FD 泄漏；旧配置仍能启动；协议契约和 public API 的变更有明确版本边界。
- **约束**：不改成 stdin/stdout 承载以打破当前日志和 FD 契约；不重复实现已经共享的 Unix spawn；不宣称已验证 Windows Agent。

### T11 / P1：端到端复测、文档更新与最终验收

- **状态：已完成（2026-10-13）**。第三轮全量回归：flow-engine lib 83/83；Executor 11/11；Agent 6/6；flow-config 8/8；backend 十套件 50 通过/10 ignored（含 T02 粘连帧、T09 故障矩阵、既有多流取消/重连）；`cargo check --workspace --all-targets` 零告警；基线复跑数值稳定。性能收益的定量结论仍需同机同命令前后对比（当前只有一组基线）。
- **目标**：完成推荐范围，交付有证据的简化和性能结果。
- **背景**：微基准减少分配不保证端到端提速，磁盘提交和进程成本也可能占主导。
- **范围**：T01 的 benchmark/report；相关 crate 检查与测试；二、三期设计/任务文档和 OPS 的当前状态说明。
- **方法**：复测与基线完全相同的负载；核对单流路径与所有队列预算；报告吞吐、CPU、RSS、ACK/Permit/Cancel 分位延迟；复核本地与远程故障矩阵。把历史设计保留为历史，新增当前 v3 说明。
- **测试**：本文件第 4 节的 crate 与集成套件；真实单 TLS 进程测试；16 活跃派发、持续大输出、反复断网和 drain；原有进程内/IPC 事实等价测试。
- **验收**：T01–T10 的必要项完成；权威事实和兼容性不退化；性能收益/成本如实记录；峰值内存符合重新核算的预算；没有以降低可靠性换取吞吐。
- **约束**：不以“测试通过”替代性能证据；不把本地模拟测试当真实 WAN soak；不留下没有消费者的兼容补丁。

### T12 / P2、有条件：增加二进制大载荷 wire

- **状态：已完成，v4 为唯一协议（2026-10-13）**。帧 `u32 body_len | u8 kind | u32 meta_len | JSON meta | raw`；`TransferChunk` 载荷走 raw blob，内存表示 `bytes: Vec<u8>`（不再 base64）。**已彻底删除 v3 帧编解码与 base64 TransferChunk 路径，无 v3/v4 双轨**；`PROTOCOL_VERSION = 4`。Journal schema 不变（仍 base64 ValueChunk），WSS 复用同一帧格式。相对 v3 实测：transfer 编解码 **≈8×**、线字节 **−25%**、满队列 Cancel 排队延迟 **−78%**（详见 `docs/IPC_PROTOCOL_BASELINE.md`）。测试：全 fixtures 往返、raw 不入 base64、未知 kind/meta 溢出/malformed 拒绝、半帧粘帧、本地 IPC 与远程 agent 大输入 raw 往返。
- **进入条件/目标**：T11 证明 Base64/大载荷编码是主要成本，且收益值得一次 wire 升级；只优化 transfer 和必要的大值审计载荷。
- **背景**：Base64 增加线字节和编解码；小控制消息无需全面换二进制对象格式。
- **范围**：`execution_protocol/{contract.rs,message.rs,frame.rs,fixtures.rs}`；Executor transfer/audit；Backend dispatch；Agent relay；新旧协议兼容测试及打包参数。
- **方法**：冻结 v4 格式为 `u32 BE body_len + u8 kind + u32 BE meta_len + UTF-8 JSON metadata + raw bytes`；metadata 保留业务序号/字符串 ID/最终摘要，raw 仅给有二进制体的类型，不新增统一 Seq。kind 映射、长度是否含头、端序、允许组合、硬上限和错误处理写入 fixtures。v4 通过显式配置与新接入地址启动，v3 默认入口保持可用；每个会话始终单连接。大值审计在 Master 边界转换为既有 Journal ValueChunk，规范计量与摘要沿用原口径。
- **测试**：v3↔v3、v4↔v4；错误版本明确拒绝；滚动升级两类接入共存；raw/meta 长度溢出与错配；所有大值和审计事实与 v3 等价；重连补发；端到端收益复测。
- **验收**：传输 raw 不再 Base64；旧 Journal 可读、新 Journal 仍是原持久格式；v3 用户保留启动路径；线字节/CPU 收益有报告。
- **约束**：不能静默改变 v3；不能变更 Journal schema/version；不能删除最终内容摘要；不能把 string dispatch 收窄成 UUID；不能保留无期限的两套业务执行逻辑。

### T13 / P2、有条件：增加 WSS Agent 上联

- **状态：已完成（2026-10-13，按需求直接实施）**。新增 WSS 附加承载，不替换直连 mTLS：
  - `flow-engine/execution_protocol/ws.rs`：`WsByteStream` 把 WS Binary 消息适配为 v3 字节流（一条完整帧=一条 Binary；读侧有界重组；Text/超限拒绝；Ping 自动 Pong；Close=EOF；读写缓冲硬上限）。
  - 后端 `AgentManager::listen_wss(addr, token)`：明文 TCP（代理终止 TLS），显式认证映射（`Authorization: Bearer <token>` 常量时间比较 + `x-flow-agent-id`，不信任客户端自报以外的身份）；协议级 `max_message_size` 上限。
  - Agent `connect_wss`：`--wss-url`/`--token`（env `FLOW_AGENT_WSS_URL`/`FLOW_AGENT_TOKEN` + 配置文件 `[agent].wss_url/token`），与 mTLS 路径共用同一套 AgentHello/AgentWelcome 握手与 `FrameTransport`。
  - 配置 `[execution.remote].wss_addr/agent_token`：`start_execution_remote` 配置即附加启动 WSS 监听。
  - 测试 `journal_remote_wss.rs`：真实 flow-agent 进程经 WSS 跑通业务；缺/错 token 与缺身份头拒绝；Text 消息拒绝关闭；适配器单元测试（roundtrip/Text/Ping→Pong/Close→EOF）。
  - 约束遵守：执行协议未混入现有 JSON-RPC；Executor 不感知 WS；复用同一裁决与故障测试；未降低现有认证（直连 mTLS 保留）。
- **进入条件/目标**：有明确的 HTTP Upgrade/反向代理/统一入口部署需求；把 WSS 作为附加承载，不作为性能优化必做项。
- **背景**：当前 Agent 已主动连接单 mTLS 上联；WSS 不提供额外的持久确认或恢复能力。
- **范围**：Agent runtime/tls/config；Backend AgentManager 接入；`flow-rpc` HTTP 服务装配（若复用现有入口）；OPS、配置样例及新的 WSS 集成测试。
- **方法**：增加独立 `/ws/agent` Upgrade 路由，不能把执行协议混入现有 JSON-RPC。复用选定 wire：一个完整协议帧对应一个 WS Binary message；先沿用已验证上限，拒绝 Text 和过大 message。读循环持续处理 WS Ping/Pong/Close；保持应用 Heartbeat/Resume 与归属校验。认证结果映射允许的 agent_id；直连保留 mTLS，代理终止 TLS 场景明确认证映射，不能信任客户端自报 ID。限定 WS 库、应用队列和重组的总预算。
- **测试**：真实 WSS 和代理 Upgrade；未认证/错误身份；超限消息和畸形 wire；Ping/Close；慢链路与 Cancel；断线补传、重复结果、drain；旧 mTLS 配置继续工作。
- **验收**：Executor 无需理解 WS；两种远程承载复用同一裁决与故障测试；部署需求得到满足；没有新的事实存储或消息确认规则。
- **约束**：不降低现有认证；不承诺零分配；不能将 WS fragmentation 当应用调度；不能仅把 WS frame 设置为 64 KiB 就宣称 message 和内存也有界。若实测要求更小完整消息，先单独设计有界应用分段和重组并验收，不能偷偷破坏现有 128 KiB 控制帧/512 KiB 审计记录契约。

### T14 / P2、有条件：Windows Agent 启动支持

- **状态：进入条件未触发且本机不可验证，暂缓（2026-10-13）**。无 Windows 产品要求；`cargo check --target x86_64-pc-windows-msvc` 被 rquickjs-sys / aws-lc-sys 原生依赖与缺失的 MSVC 工具链阻断，无法按「禁止以跨目标 check 代替真实运行」的验收标准推进，需真实 Windows CI。
- **进入条件/目标**：产品要求远程 Agent 支持 Windows；复用已存在的 loopback 承载。
- **背景**：Backend 非 Unix 路径已经存在，Agent pool 仍直接导入 Unix spawn。原文提出 Windows 并不代表当前 Agent 已跨平台。
- **范围**：Agent `pool.rs`；`execution_protocol` 的公共进程启动入口；Windows CI 和打包；必要的路径定位代码。
- **方法**：抽出唯一跨平台 spawn 入口，Unix 返回 socketpair，非 Unix 返回既有 loopback；统一子进程句柄、bootstrap 和环境隔离；实际修复 Windows 二进制定位和回收差异。不增加 Named Pipe。
- **测试**：Windows 原生 build、真实 spawn/握手/任务执行、超时与取消、父死亡回收、兄弟 exe 定位、端口清理。
- **验收**：Windows 原生 CI 通过并跑通真实 Agent→Executor；操作授权与结果屏障不变。
- **约束**：不以跨目标 cargo check 代替真实运行；不宣称本次 macOS 测试覆盖 Windows；不引入第二套业务协议。

## 4. 顺序、验证与交付边界

推荐顺序：T01 → T02/T03 → T04 → T05/T06/T07 → T08 → T10 → T11。T09 的故障矩阵从 T01 后建立，每个实现任务都按相关用例回归；T11 完成整体验收。T12、T13、T14 分别按其进入条件启动，不能因为“看起来统一”自动打包进首轮。

实施后验证命令（相应二进制与真实进程测试环境须先按仓库现有基建准备）：

```sh
cargo test -p flow-engine --lib execution_protocol --locked
cargo test -p flow-executor --lib --tests --locked
cargo test -p flow-agent --lib --locked
cargo test -p flow-backend --test journal_ipc --test journal_ipc_packaging --locked
cargo test -p flow-backend --test journal_remote --test journal_remote_tls --locked
cargo test -p flow-backend --test journal_remote_reconnect --test journal_remote_ops --locked
cargo check --workspace --all-targets --locked
```

本次已执行（2026-10-13 第六轮，全量）：flow-engine lib 86/86；Executor 11/11；Agent lib 6/6；flow-config 9/9；backend 十二套件（含新增 journal_raw_transfer）通过（journal_ipc 的 killing_executor_mid_script 为既有 SIGKILL 时序抖动，单跑稳定通过）；`cargo check --workspace --all-targets` 与 clippy 零告警；基线复跑（v4 唯一协议）。

本次已执行（2026-10-13 第五轮，全量）：flow-engine lib 88/88（含 WSS 适配器 3 用例）；Executor 11/11；Agent lib 6/6；flow-config 8/8；backend 十一套件 56 通过/10 ignored（含 journal_remote_wss 3 用例）；`cargo check --workspace --all-targets` 通过。

本次已执行（2026-10-13 第四轮，全量）：flow-engine lib 85/85（含 loopback T10 新增用例）；Executor 11/11；Agent lib 6/6；flow-config 8/8；backend journal_ipc/packaging/remote/remote_tls/remote_reconnect/remote_ops/fault_matrix/acceptance/execution/capacity 共 53 通过、10 ignored（packaging 含 T10 新增回收/隔离用例）；`cargo check --workspace --all-targets` 通过；`examples/ipc_baseline` 复跑稳定（含 T12 分解）。第一至三轮记录见 git 历史。仍不覆盖：真实 WAN soak、非 Unix 完整 IPC 装配（需对应平台运行环境，T14 条件）、T12/T13/T14 条件任务（已给出进入条件判定）。

最终交付应包含：实现 diff、兼容说明、上述正确性结果、基线/复测报告、真实队列预算、最新部署说明。验收必须同时证明事实不变、资源有界和收益真实。
