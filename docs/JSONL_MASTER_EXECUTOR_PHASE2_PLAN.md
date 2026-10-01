# JSONL 重构二期：本地 IPC 与执行子进程

日期：2026-09-30；更新：2026-10-02。状态：**I01–I10 已实现并通过开发验收（等价性/故障矩阵/打包回归；生产切换与 release 性能矩阵未执行，见各任务证据）。**本文件承接 [一期基础](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md)，任务独立见 [I01–I10](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md)。原远程方案已迁至 [三期](JSONL_MASTER_EXECUTOR_PHASE3_PLAN.md)。

<a id="phase2-s1"></a>
## 1. 二期范围

二期只把一期进程内节点执行替换为受管理的本地执行子进程：flow-executor、有界池、双 Unix socketpair、FD/进程树管理、帧与会话、审计确认窗口、结果屏障及取消回收。主进程仍独占 journal、DAG 与业务命令，SQLite 仍为投影。业务输入/输出、单行事务、固定 ValueRef、Intent/Authorized 同事务和 projected 写入语义复用一期，不重建另一套存储。

普通节点一次派发，在同一子进程内准备和执行；持久等待登记后释放进程。模板、script、condition 全部迁移，不能留下用户表达式在主进程。二期不含 TLS、远程路由、Resume 或 agent，相关能力属于三期。

<a id="phase2-s2"></a>
## 2. 一期交接与实现边界

I01 的输入是 [F17](JSONL_MASTER_EXECUTOR_TASK_LIST.md#task-f17) 已验收的 journal/reducer/StoredValue、内部提交/授权/结果门面、基础回归和负载基线。先冻结 IPC DTO、capabilities、X_max 与窗口预算，再实现传输；这些内容不反向阻塞一期。

一期 dispatch_id 是节点尝试的持久业务身份，二期将其绑定到 executor/session，不能因为更换通道就改变 operation_id 或重新求值。新增会话/任期事件按版本扩展，旧读者无法解释时明确拒绝，不能跳过。内部提交返回值分别封装成 AuditAck、OperationPermit、ResultCommitted；socket 收到数据不等于一期持久回执。

子进程不打开 store/journal、不持有 ChildRunLauncher。IPC DTO 可放 flow-engine::execution_protocol，进程池放 backend，新增 flow-executor 二进制；只抽象收/发/关闭，不引入通用 RPC 框架。进程隔离是故障隔离，不自动构成恶意代码沙箱。

<a id="phase2-s3"></a>
## 3. 执行进程与 IPC

执行任务：[I02–I08](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md#task-index)。

<a id="phase2-s3-1"></a>
### 3.1 本地双 socketpair 与传输边界

主进程创建两组 Unix `SOCK_STREAM` socketpair，通过 `tokio::process::Command` 启动执行器，仅将各组的子端文件描述符交给目标子进程：

```text
主进程                      执行子进程
  control endpoint <------> control endpoint
  data endpoint    <------> data endpoint
```

- control：握手、任务派发、取消、持久确认、心跳、结果描述；禁止大载荷。
- data：输入分块、审计批次、业务输出分块、**观测记录（独立预算类别，可丢弃、永不反压 golden source）**；接受窗口限流。
- 不绑定 `.sock` 路径，不起本地 listener，不实现子进程服务发现。
- 只映射目标 FD，其他 FD 默认 close-on-exec；父关闭子端，子关闭父端，其他子进程不能继承这些连接。否则父死后 EOF 可能永远不出现。
- FD 映射必须按 Linux/macOS 的 spawn 约束实现和测试；如使用 pre_exec，回调只能做允许的低层 FD 操作，禁止分配内存、加锁或调用普通异步代码。
- stdin 不作为协议通道；stdout/stderr 不携带协议帧。业务输出必须经审计适配器转成记录，原生输出若需纳入完整性保证，要有明确的捕获、排序和封口路径，不能仅重定向到无人读取的管道。
- IPC 读写由持续运行的 I/O 任务负责，不能等节点执行返回才读取审计。每条连接一个串行帧写入器，避免并发写导致帧交错。

两条连接消除控制和数据之间的流内队头阻塞，但不消除磁盘写入等待。取消可以及时收到，取消的 durable ack 仍然必须等待权威提交。

业务层依赖小型 Transport 边界（按 control/data 发帧、收帧、关闭），不依赖 Unix FD、PID 或机器路径。远程适配在三期定义，二期不冻结远程认证、路由或重连协议。

<a id="phase2-s3-2"></a>
### 3.2 会话身份与本地启动检查

| 字段 | 生命周期和用途 |
| --- | --- |
| journal_id | 权威数据集身份；不同数据集之间禁止续接游标 |
| master_epoch | 主进程任期；取得独占日志写权后持久提交新任期，再派发任务；不能代替存储端 fencing |
| executor_id | 主进程分配的执行槽身份，不使用 PID 作为协议身份 |
| executor_boot_id | 子进程每次启动生成的新随机身份；同槽位重启必须变化 |
| session_id | 完成握手后主进程分配的会话身份；断线后旧 session 不自动复活 |
| dispatch_id | 持久节点派发身份；内部准备与执行共用审计序列，准备期间不允许外部副作用 |
| operation_id | 外部逻辑操作身份；网络重试不自动改变 |
| command_id | 稳定控制命令身份，重复 Execute/Cancel 返回原处理状态 |
| audit_seq | dispatch 内审计顺序，跨会话补传仍不变 |

本地启动检查：父进程创建并按槽位保存两条 socketpair 的端点归属，不再用 nonce/BindData 重新证明父进程已经建立的连接关系。

1. 子进程 control 发 Hello：协议版本、boot_id、build 和能力；父进程按该端点查找自己创建的槽位。
2. 主进程回 Welcome：选定版本、会话身份、帧上限、固定窗口和启动超时。双方完成本地 I/O 循环初始化后以 control Ready 表示可接任务；任一通道故障按 §3.7 回收。
3. journal_id/master_epoch/executor_id/boot_id/session_id 保存在会话上下文，本地任务帧只携带类型、dispatch_id 及必要业务字段；接收端根据连接上下文校验派发归属。启动检查不消耗业务派发。

远程认证与连接绑定见三期文档，二期只定义父进程已建立的 socketpair 归属。

版本不兼容、缺失所需能力或身份不匹配：明确拒绝，不降级成无法解释的执行行为。未知必需消息类型是协议错误；可忽略的扩展字段由协议版本契约声明。

PID 只用于本机 wait/reap 与诊断。不得把 PID 当作派发或会话身份。

<a id="phase2-s3-3"></a>
### 3.3 物理帧与统一消息格式

每帧为 `u32 大端长度 + UTF-8 JSON`，长度只计算 JSON 部分；读端处理半帧/粘帧，先校验长度再分配。建议初值：control 最大 64 KiB、data 最大 1 MiB；握手只可在本端硬上限以内协商。初版不做传输压缩，避免无界解压；大内容使用分块。

示例为 Execute 后发送的一批审计，省略真实内容：

```json
{
  "v": 1,
  "type": "AuditBatch",
  "dispatch_id": "dispatch-09",
  "body": {
    "first_seq": "41",
    "records": [
      {"audit_seq": "41", "kind": "business_audit", "payload": {"message": "example"}}
    ]
  }
}
```

示例为本地业务帧；会话身份由接收端上下文提供，二期不携带远程路由包络。dispatch 必须解析到固定 run_id/node_id/attempt/node_execution_id，不允许收到消息后修改这层绑定。

epoch、序号、字节累计位置等可能超过 JavaScript 安全整数范围的值统一使用十进制字符串；初版 fixture 固定这一编码，避免将来 RPC/工具接入时精度变化。

消息中不使用本机文件路径充当输入/输出。大输入用 TransferChunk(transfer_id, offset, bytes, digest) 加 InputReady；二进制 bytes 初版使用 base64，计费包含编码后的体积。大输出用已定义的输出审计 chunk 和结果描述。输入 InputReady 只表示接收器校验并准备完成，不表示新的权威提交。

<a id="phase2-s3-4"></a>
### 3.4 本地消息目录与确认语义

| 消息 | 通道/方向 | 含义与限制 |
| --- | --- | --- |
| Hello / Welcome / Ready | control | 本地启动与能力检查，不授予任务执行权 |
| TransferChunk / InputReady | data / control | 输入传输、校验；不释放审计持久窗口 |
| Execute / Accepted | control，主→子 / 子→主 | 一次节点派发，内部准备与执行；已提交 Execute 授权纯计算，Accepted 仅表示接收 |
| AuditBatch / AuditAck | data，子→主 / control，主→子 | 包含 InputPrepared、输出 chunk、OperationOutcome 等执行事实；ACK 仅确认持久性 |
| RequestOperation / OperationPermit | control，子→主 / 主→子 | 请求携带已提交实际请求的有限描述/引用；Intent 与 Authorized 同事务提交后才发专用许可 |
| ObservabilityBatch | data，子→主，单向 | 独立预算，无确认、不占 audit_seq，可采样丢弃，不反压 golden source |
| Result / ResultCommitted | control，子→主 / 主→子 | Result 含固定结果描述和最后审计序号；屏障满足、业务结果或持久等待登记提交后确认 |
| Cancel / Stopped | control，主→子 / 子→主 | 持久取消意图驱动停止；Stopped 不证明外部操作撤销或审计持久 |
| Heartbeat | control，双向 | 存活与进度提示，不是执行结果或持久提交证明 |

InputPrepared 是 AuditBatch 中的事实类型，不新增独立 prepare 派发或 Prepared 握手。它持久后可继续已被 Execute 授权的纯计算；外部调用仍必须等待 OperationPermit。内部准备进度不产生另一套结果确认。

收到消息、输入可用、审计持久、操作获准、结果已提交分别表达。control 命令按 command_id/dispatch_id 幂等；重复 Execute 不重新求值或调用。子进程只接收空闲槽位的新派发或当前派发的重复消息，正常 ResultCommitted 前不复用槽位；旧 dispatch 在复用后拒绝。本地不包含 Resume/ResumeReply，远程消息目录在三期冻结。

<a id="phase2-s3-5"></a>
### 3.5 按持久确认计费的发送窗口

**并发面符号（全文唯一，不得互相代用）。** 旧版本把“在飞 run 数”“活跃派发数”“日志游标”都写成 `C`，导致预算公式按错误的量推导；现拆为：

| 符号 | 含义 | 定值方 | 影响的资源 |
| --- | --- | --- | --- |
| `R_max` | 在飞 run 状态数（in-flight run，含等待中的） | 继承 F02/F17 基线，I01 确认 **≥1000**（[一期 §1](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s1)） | 主进程 run 状态内存、调度表、可卸载索引 |
| `A_max` | 活跃派发上界（同时持有 dispatch 事实、占窗口预算的派发数） | I01 独立定值并写明推导 | 全局窗口预算 = `A_max × W × k` |
| `X_max` | 执行槽位数（同时运行任务子进程数） | I01 独立定值并写明推导 | 进程数 × 每进程 RSS |
| `W` | 单派发持久确认窗口 | I01 由内存上界反推 | 审计在途字节 |
| `k` | 每派发实际预算对 `W` 的倍数（生产队列 + 输入 transfer + 重传副本 + 解码扩张 + 待落盘） | I01 预算估算，I10 实测复核 | 内存峰值 |
| `D` / `S` | 单派发的已持久前缀 / 已发送前缀（审计序号） | 运行时量 | 判定未确认字节 `B(S)-B(D)` |

三者的关系不是恒等式：`R_max` 大于 `A_max` 是正常形态（§3.8 规定等待不占执行进程，一个 `delay`/`human_task` 的 run 在飞但无活跃派发）。**`R_max` 不得代入窗口预算公式，`A_max` 不得用来论证主进程状态内存，`X_max` 不得用来论证审计吞吐。** 三个上限与实测峰值各自构成验收项（§7）；任一未定值即视为协议未冻结（§2）。

每个 dispatch 维护从 1 开始的审计序列。协议固定单条 audit record 的确定性编码规则；发送端保存原始编码用于重传，主进程按同一规则校验字节数和摘要，不能接受未经校验的自报体积。

令 `size(i)` 为第 i 条记录的协议编码字节数，`B(n)=sum(size(1)..size(n))`。主进程确认最高连续持久序号 D；执行器已发送最高连续序号 S，则：

```text
未持久确认字节 = B(S) - B(D)
发送新记录的条件：未持久确认字节 + 新记录字节 <= 当前窗口 W
```

- AuditAck 同时带 durable_audit_seq、durable_bytes、commit_lsn；只单调前进，且不能超过主进程已提交位置。执行器核对边界后才释放对应缓冲。
- socket write/receive、InputReady、Heartbeat 和 agent 暂存都不改变 D。
- 重传不重复占用逻辑窗口，但受物理发送预算和重传次数/退避限制，防止重复流量淹没接收端；内容必须一致。
- 本地 v1 使用固定 W，无 WindowUpdate；执行阶段存续期间不缩窗。改变配置只影响之后准入的新派发。停止接新任务与数据背压负责降载，不撤回已授予的预算。
- 握手确保最大单条编码记录不超过可发送窗口。大记录先分块；否则会出现一条记录永远无法发送的死锁。
- 派发前从全局预算预留完整 W，终止/排空后释放；无法预留就不派发。预算另外计入未发送生产队列、输入 transfer、重传副本、解码扩张及待落盘队列，不能仅每任务有界。固定窗口与最大记录配合保证每个准入任务可发送至少一条记录；读入后落盘不得等待同一任务先释放持久窗口。
- data 读入必须经过预算准入；不能不断读 socket 到无界队列以绕过背压。
- control 和 data 读写任务独立，控制帧有独立有限预算；禁止把 heartbeat 或大批命令变成另一条无界通道。

审计持久确认窗口与输入传输窗口分开计算。输入是主进程已知的权威内容，接收完成可以释放输入传输缓冲，但不能借它确认执行器新产生的审计。

**全局预算按活跃派发上界反推，不按在飞 run 数反推。** “无法预留就不派发”使全局预算直接等于并发派发上限：`A_max` 个活跃派发对应的常驻预算 ≈ `A_max × (W + 未发送生产队列 + 输入 transfer + 重传副本 + 解码扩张 + 待落盘队列) = A_max × W × k`。因此设计顺序必须是：**先由 I01 分别固定 `A_max`（并说明它与 `R_max ≥ 1000` 的关系）与允许的常驻内存上界，再反推 W 与 k**，而不是先定 W 再看能跑多少并发。`R_max = 1000` 只保证主进程能同时持有 1000 个 run 状态，不保证 1000 个派发在飞——后者需要另外论证 `A_max`、`X_max` 与进程内存，本方案不默认 `A_max = R_max`。停止接新任务、背压数据是降载手段，不能用来掩盖“预算本身不足以支撑目标并发”。

<a id="phase2-s3-6"></a>
### 3.6 结果屏障与双通道乱序

Result 包含 dispatch_id、固定的 result_id、最后审计序号 N、outcome 及小结果/大值固定描述。即使 Result 从 control 先到，主进程也必须同时满足以下条件：

1. dispatch 与当前执行身份匹配，未被已提交取消/替代派发废止。
2. 审计 1..N 连续且全部持久化；Result 的 N 不得小于该派发已接受的执行审计范围。
3. 固定描述所指的连续 chunk 已全部持久化，块数、总长度与摘要验证通过；候选描述的发布与结果接受可同事务提交。
4. 结果 schema、节点 attempt 和固定执行输入关系合法。正常执行结果须绑定已提交 InputPrepared；准备失败可提交失败结果及已知原始输入/异常证据，不要求伪造不存在的求值结果。
5. 主进程以一个事务提交派发结果接受及节点终态，或持久等待登记，并完成 fsync；InputPrepared 不终结派发，等待登记不伪装成节点成功，见 §3.8。

只有第 5 步成功，才能发 ResultCommitted；是否推进依赖节点由已提交节点状态决定。日志未齐时最多保留一个有界的小 Result 描述，等待 data 继续到达，不能阻塞 data 读取线程。

发送 Result 前执行器停止该任务所有审计生产者，并完成输出捕获封口，再确定 N；Result 后不得继续产生该任务的普通执行审计。封口后的运行时诊断需另行归类，不能假装属于已确认的 1..N。

ResultCommitted 丢失时，重传同一 result_id 和内容返回原提交。相同身份不同内容是错误；本地同会话重传只做确认，不再次执行；断线按 §3.7 回收。已取消/旧派发的迟到审计可进入 LateAudit 记录供审计查询，但不重新打开 run、不延后已经结束的兼容订阅，也不能偷偷改终态。

取消和完成按 [一期 §6.3](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s6-3) 对同一派发串行裁决：完成先提交则后续取消不能撤销完成；取消先提交则拒绝业务结果。后者立即进入 Draining，仍接收并持久确认允许的审计；执行器不再等待 ResultCommitted 复用槽位，而是尽力发送封口状态后退出，该进程统一回收重建。宽限期到达即 kill/reap，未封口记 unknown/incomplete；存储故障不能为了发停止确认而伪造提交。本地 v1 不增加另一套取消后可复用任务的握手协议。

Stopped 可携带 sealed、最后 N、未决外部操作信息；sealed 只有在全部生产者停止后可为 true。主进程只有确认 1..N 持久且封口有效，才能记录取消阶段审计完整；Stopped 本身不是持久确认，缺少封口绝不推断完整。

普通 Audit 与 LateAudit 共用 (dispatch_id, audit_seq) 去重键与原始内容校验；LateAudit 只改变业务用途，不另起序列。相同原始记录只存一次；异内容拒绝。连续前缀 D 仅随连续持久记录增长，迟到的非连续证据可保存但不能越过缺口发 AuditAck。默认补传按序；存在永久缺口时转入显式证据导入，单独预留预算且不授权执行。摘要/定位索引可从日志重建，重传校验按需查磁盘，不把全部历史哈希常驻内存。

<a id="phase2-s3-7"></a>
### 3.7 断线状态机：连接状态与任务状态分开

连接状态不能直接覆盖业务状态。已提交完成的任务即使连接断开也仍然完成；未提交结果的任务不能仅因连接断开就判定外部操作失败。

| 连接状态 | 进入条件 | 允许动作 | 下一状态 |
| --- | --- | --- | --- |
| Starting | 已 spawn，持有两个预期端点 | Hello/启动检查，无任务执行 | Handshaking 或 Closed |
| Handshaking | 启动检查尚未完成 | 完成版本/限额与 I/O 初始化 | Ready 或 Closed |
| Ready | 两通道初始化完成且健康 | 派发、审计、确认、结果、取消 | Suspect 或 Draining |
| Suspect | 任一通道 EOF/错误，或心跳超时/进度策略触发 | 冻结新派发与新的外部操作；保留未确认数据和未决状态 | Draining |
| Draining | 本地失联、计划关闭或取消回收 | 尽力封口/停止，宽限期后 kill，最后 wait/reap | Closed |
| Closed | 端点关闭、进程已回收或会话废止 | 保留持久事实，按任务恢复策略处理 | 新进程/新会话重新 Starting |

本地 v1 不实现自动重连。同一 socketpair 断开不能通过换一组 FD 就恢复旧执行权；主进程回收该子进程，再从已提交事实决定是否新派发。任一通道失效，整个会话停止接受新任务；尚健康的通道可用于停止通知和有界审计收尾，不能无限等待。

任务状态独立表达为 `Dispatched → Preparing → Executing → WaitingForCommit → Committed`（已有 InputPrepared 时跳过 Preparing），并允许转入 `CancelRequested / Uncertain`。控制连接/心跳丢失只提供失联证据，不证明进程已停止。执行器在安全点检查失联标志，停止新的外部调用；进行中的请求可能仍然生效。无法及时停在安全点时由监督者在宽限期后终止进程，未决副作用保留 Uncertain。

主进程在取得独占写权并恢复日志后持久增加 master_epoch；旧会话不能继续执行。新主进程可以在显式对账后接收旧 dispatch 的审计证据，但不凭重连消息自动应用旧业务结果。任期只是协议身份，不是共享文件 fencing。

<a id="phase2-s3-8"></a>
### 3.8 一次派发内隔离准备与执行

二期所有用户 JS（模板、script、condition）都移入执行子进程运行；这是对一期进程内适配器的替换。普通节点只创建一次持久 dispatch；准备与执行是内部步骤，共用一套 audit_seq、窗口、取消与结果协议，不再为 stage=prepare/execute 分别派发。

1. Execute 携带节点定义、run 输入与前驱 StoredValue 引用；若已有已提交 InputPrepared，则直接携带其引用并跳过求值。实际输入记录包含 run 输入、可访问的前驱输出、展开后的参数及必要版本/默认值，凭证只使用声明的引用。
2. 子进程在预算内准备参数，产生 InputPrepared 审计事实；无模板节点也记录实际输入。准备上下文没有外部 I/O。该事实按 node_execution_id 固定，一旦持久便不可在重试时换值或重新求时间/随机模板。
3. 等 InputPrepared 对应 AuditAck 后继续已被 Execute 授权的纯计算；外部节点经 §4.1 RequestOperation 获得专用许可才可调用。AuditAck 是数据屏障，不产生外部调用权限。
4. 执行完成后封口并发送唯一 Result，主进程提交结果再回 ResultCommitted，随后复用槽位。准备失败走同一派发的失败 Result，不发送第二个 Execute；取消按同一 dispatch 排空回收。

短暂等待 InputPrepared/OperationPermit/ResultCommitted 时可以占用当前进程；一期 F07 测提交等待时延，I08/I10 测真实占槽成本。delay、human_task、sub_workflow 只借用子进程完成必要参数准备；随后以封口 Result 的等待请求交给主进程，在同一结果接受事务登记 wake_at/信号等待/父子关系，确认后释放槽位。长期业务等待由主进程恢复与唤醒，不持有子进程。无表达式的 start/end、依赖判断与固定等待可由主进程直接处理，按同一输入/输出 schema 持久记录实际值，用事务 lsn 定位，无需伪造执行器 dispatch/audit_seq。

崩溃后已提交 InputPrepared 按 StoredValue 恢复；未提交准备结果可按纯计算策略用新 dispatch 重做，旧派发审计与缺口保留。已有操作授权却无 Outcome 时仍按未知副作用核对，不因准备已完成而重发请求。重试可换进程，但正常准备完成不会主动再排一次执行队列。

准备、执行、解析及 JS↔JSON 转换均执行总输入、heap/进程和输出预算；多前驱累计计量，主进程不物化大值。InputPrepared 不是节点终态，持久等待登记也不是节点完成；只有实际节点结果或失败裁决才能推进相应 DAG 状态。

<a id="phase2-s3-9"></a>
### 3.9 生命周期

- `R_max`（在飞 run 数，§3.5）、`A_max`（活跃 dispatch 数）、`X_max`（执行槽位数）、单任务内存、运行时长与待提交字节均有显式上限。`R_max` 覆盖 ≥1000 在飞 run；`A_max`、`X_max` 由 I01 独立定值，初始进程/内存预算注明估算或现有测量依据。F07 验证提交器，I10 以真实节点核对进程数 × RSS 与三类并发资源；不得将预估冒充实测，或用 `R_max` 代替 `A_max`/`X_max` 交差。
- 子进程退出：回收槽位；根据已提交派发/结果分类，不直接将所有任务标记成功或自动重试。
- 取消先持久记录意图，再发 Cancel；按 §3.6 进入有界排空并回收进程，不复用已取消任务的执行器；宽限期后 terminate/kill，随后必须 wait/reap。
- 主进程退出时停止派发并管理子进程树；子进程感知 control socket EOF 后按 §3.7 停止执行。平台上必要时实现进程组/父死亡机制并分别测试，不能只靠 Drop 或 PID 查杀。
- 主进程重启后拒绝旧会话结果；历史审计的补录与业务结果应用明确区分。
- 初版执行器不允许任意脱离管理的后台进程；若以后增加 shell 节点，必须补整棵进程树取消与审计收集契约。


<a id="phase2-s4"></a>
## 4. IPC 中的持久确认与操作授权

执行任务：[I05](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md#task-i05)、[I07](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md#task-i07)。

<a id="phase2-s4-1"></a>
### 4.1 明确能保证的边界

保证：主进程已返回 durable ack 的业务/审计记录，在约定故障模型内可恢复；节点完成事件提交时，其声明的审计前缀已经完整提交。

不能保证：任意执行进程在生成数据的同一瞬间被杀死，尚未传给主进程的数据仍然存在。对必须先留痕后继续的操作，执行器必须同步等待审计持久确认。

故障使未确认尾部丢失或无法证明封口时，记录该执行阶段审计完整性为 unknown/incomplete，保留已有记录，不能报告正常成功或静默补造原始审计内容。业务状态与审计完整性分字段保存：业务取消/失败不证明审计完整；后续重试成功也不覆盖旧尝试缺口。run 汇总包含所有派发尝试及其准备过程的完整性，有缺口不得标为完整成功。

本节所称“不丢弃”只约束 [一期 §5.0](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s5-0) 定义的 golden source 记录（一期完整业务事实要求），不约束可观察性记录（一期观测边界要求）——后者按 [一期 §5.0](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s5-0) 允许采样与丢弃，其丢失不影响本节任何结论。这实现“正常路径无丢弃、已确认数据耐久、成功阶段审计封口”，不等于“任何故障下所有已生成数据都能保存”。用户要求的完整历史不能被本文擅自降级：I01 必须逐类明确允许的故障模型。若要求失败/强杀任务也绝无原始数据缺口，当前单主提交与易失执行器不足以兑现，相关场景不得验收或上线，必须另行引入来源端可靠保留/结果查询等能力。

二期外部调用遵循：执行器准备实际输入并提交 InputPrepared → control 发 RequestOperation（小型请求描述/引用、操作身份与指纹）→ 主进程校验并与取消串行裁决 → **同一个事务写 OperationIntent 与 OperationAuthorized** → fsync 成功后发送 OperationPermit → 执行器调用 → 持久 OperationOutcome → 发布节点结果。Intent 与 Authorized 是两类事实，但不要求两次提交或两轮 fsync。完整请求来自已提交输入/引用，主进程不得只校验未经核实的摘要；原始响应仍按 [一期 §6.1](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s6-1) 流式捕获。

AuditAck 仅证明数据持久化，不授予外部调用权。普通 AuditBatch 中的历史意图或证据不会触发授权；唯一授权入口是有效当前派发的 RequestOperation。主进程按 [一期 §6.3](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s6-3) 串行裁决请求、取消和派发废止：取消先提交或身份无效时，只保存适用的请求/拒绝证据，不写 Authorized、不发 Permit；许可先提交时保留在途不确定性。同请求重传附着原裁决，同身份异指纹拒绝。

授权记录和 OperationPermit 绑定 journal_id、master_epoch、session_id、dispatch_id、operation_id、request_fingerprint、permit_id。本地会话字段由连接上下文取得。执行器只有在当前任务仍可执行且指纹一致时消费许可；重复 permit_id 不再调用，已停止或旧会话的许可失效。新会话不重发旧许可恢复执行；授权已提交而无 Outcome 时按 [一期 §9.1](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s9-1) 核对，不能推断操作尚未发生。取消前已授权而执行器尚未获知取消的操作仍可能生效，不承诺瞬间撤销外部效果。

外部成功、响应未持久化即崩溃仍可能丢失原始响应；通过原 operation_id 查询或幂等接口核对，仅能恢复外部系统承诺保留的事实。没有这些能力时保持 Uncertain 并人工裁决，禁止盲重发。同步审计或本地 spool 都不能消除这个跨系统窗口。

<a id="phase2-s4-2"></a>
### 4.2 协议

- 执行器发 `AuditBatch(dispatch_id, first_seq, records)`。
- 主进程连续校验并提交，回 `AuditAck(dispatch_id, durable_audit_seq)`。
- 执行器保留尚未确认的有界窗口；窗口满则等待。重传同序号同内容幂等，同序号不同内容是协议错误。
- 执行器回 `Result(dispatch_id, last_audit_seq, outcome)`。
- 主进程只有在审计 1..last_audit_seq 已完整持久化、结果身份仍有效时，才提交本次派发结果与相应状态变更；InputPrepared 不是结果封口或节点完成。
- 正常执行器等待 `ResultCommitted` 后方可清理任务状态并接受下一个任务；取消/废止走 §3.6 的有界排空与进程回收，不无限等待被拒绝结果的确认。

AuditAck 是主进程权威日志的持久确认，不是 socket 接收确认，也不是未来 agent 的暂存确认。窗口计算和结果屏障的精确规则分别见 §3.5、§3.6。

取消/超时可能需要强制杀进程；杀死前未确认数据无法被承诺完整。取消终态必须携带是否正常封口和最后 durable_audit_seq，不能把不完整任务标记为正常完成。收到取消后的迟到结果不推进 DAG；其中可验证的审计记录仍作为迟到审计证据保存，不能因业务结果无效就全部丢弃。

<a id="phase2-s4-3"></a>
### 4.3 资源与通道

全部队列按字节限制，不能只限制消息条数；大记录走 chunk，不静默截断。控制消息/ack/cancel 必须能独立推进，不得被满审计队列堵死。主进程提交器公平调度各派发的有限批次，不允许一个日志洪泛任务独占全部提交机会。

**可观察性通道与权威通道必须分开记账（[一期 §5.0](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s5-0)）。** `nodelog`（console / stdout / stderr / 引擎叙事）按 [一期 §5.0](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s5-0) 是可丢弃的可观察性记录：它的发送不得阻塞 JS 执行、不占用审计序号、不占用持久窗口、不参与 §4.2 的确认协议；量由有界预算封顶，超限丢弃并计数，且必须在展示层标明被丢弃的量。节点实际输入/输出与数据流动是 golden source，走 §4.2 的持久确认路径，允许阻塞与背压，不允许丢弃或截断。两者**共用同一通道传输是允许的（data 通道上的独立预算类别，优先级低于 golden source、永不反压它）；但共用同一套审计序号或同一套持久窗口计费则视为实现错误**。若选择不传输（在源端按采样丢弃），也必须记录丢弃计数。

主进程不能在等执行结果时停止读取审计。JS 计算超时和“等待审计持久化”的存储等待分别记账，不能把数据库慢误报为脚本死循环。等待持久 ACK 必须可被取消/失联唤醒；唤醒后停止业务执行并进入收尾，不丢弃后继续执行。无法封口时保留 unknown/incomplete，由监督者在宽限期后回收。

磁盘水位过低时停止接收新业务任务，并背压既有任务；硬故障时停止执行许可。所有权威数据默认不自动清理以腾空间。


<a id="phase2-s5"></a>
## 5. 接入、打包与从一期升级

I09 在 I03 骨架后提供受控 executor 定位和开发启动入口；后续 I08 走通真实子进程 HTTP → script → delay → 重启。基础与 IPC 执行模式显式配置，任何一个 run 只绑定一种执行模式；启用 IPC 时二进制缺失/版本不兼容直接失败，不静默落回进程内执行。

切换前停止新派发、排空可安全完成任务并按原操作身份处理未决请求；日志 writer 始终只有一个，不复制/丢弃一期历史。已有等待与准备结果按已提交事实恢复，不重发已完成 HTTP。schema/事件能力变化记录兼容矩阵；二期已写新事件后，只能回退到能完整读取这些事件的兼容二进制，不能直接用一期旧版本或旧备份继续写入。

公开 API、分页/引用与客户端成功语义保持一期契约，新增执行诊断不得改变业务 run_seq。I10 验收基础回归与本期故障矩阵后形成独立部署评审；三期不构成本期依赖。

<a id="phase2-s6"></a>
## 6. 二期故障矩阵

| 故障/场景 | 必须结果 |
| --- | --- |
| 半帧/粘帧、非法长度、旧 boot/session、错槽 dispatch | 分配前限长，拒绝错身份，不串任务 |
| 兄弟进程继承 FD、父退出、单通道断开 | FD 归属正确，EOF 可发现；停止执行并有界回收，不等待永远不可达的 ACK |
| 审计满窗口、control/data 乱序、ACK 丢失/同序号异内容 | 控制可推进，按连续持久前缀确认；幂等重传，异内容拒绝 |
| Result 先于最后审计、候选值缺块、结果回执丢失 | 不提前完成，持续读 data，返回原提交不重复推进 |
| RequestOperation 与取消竞争、许可重复/迟到 | 与一期同样串行裁决；AuditAck 不授权，旧会话不恢复执行权 |
| 模板死循环、内存超限、强杀、取消排空超时 | 影响限定到目标进程；kill 后 wait/reap，缺口/未知副作用保留 |
| 已提交 InputPrepared 后换进程重试 | 不重新求值；原 operation_id 不改变，未决操作先核对 |
| 单槽父→子→孙、delay/signal、大量等待 | 等待不占进程，不丢唤醒，不靠扩大进程池绕过死锁 |
| 观测洪泛/独立存储失效、输入传输与输出同时满额 | 不占审计窗口或权威完整性，所有层预算有界 |
| 一期等待/结果升级到二期、二进制缺失/版本错误、回退 | 基础数据和 API 等价，失败明确，不双写或静默回退执行 |

I10 同时回归 [一期故障矩阵](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s14)。SIGKILL 与真实断电的测试范围分别报告。

<a id="phase2-s7"></a>
## 7. 二期容量与验收

在一期相同机器、耐久级别和代表性负载下对比进程内与 IPC 模式，保留 R_max ≥ 1000 及吞吐/p99/内存目标。I01 独立固定 X_max、A_max、W、输入传输及全局/进程预算；I10 测主/子进程 RSS、进程池等待、等 ACK/Permit/ResultCommitted 占槽时间、帧与队列字节、进程回收时间、持久吞吐和 projected p99。

不能用 IPC 缓冲稳定代替 HTTP→分块→扇出/JS→重启的全链路验证。指定高提交率下测试摊销回归，低负载不强制凑批。单进程故障不得拖死主调度器，所有成功结果满足一期完整性，等待不占进程。没有满足窗口/进程资源、内容正确性与延迟目标时，不宣称二期可用。

<a id="phase2-approval"></a>
## 8. 二期实施与交接状态

I01–I10 于 2026-10-02 实现完成（开发验收口径）：本地进程池/IPC fixtures/稳定业务身份/端到端持久语义已交付；等价性与二期故障矩阵回归通过，debug 与 release 口径回归均绿。本机 release 曾被 Xcode 21 ld 的 LINKEDIT 未对齐 bug 阻塞（sqlx-macros dylib 无法 dlopen，机器级问题、与本代码无关），已以 scripts/release.sh（rust-lld + 26.5 SDK）修复并取得 release 混合负载数字（48 run 7.8s、主进程 RSS 40 MiB、执行器峰值 12 MiB，见 I10 证据）。未执行项：生产切换、真实断电（沿用一期排除口径）。三期在此基础上增加中继。I10 向三期交付本地进程池、IPC fixtures、稳定业务身份和端到端持久语义；三期在此基础上增加中继，不能提前返回权威 ACK。
