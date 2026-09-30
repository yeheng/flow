# JSONL 重构三期：远程 agent

日期：2026-09-30。状态：**设计已迁入，产品实现未开始**。承接 [二期本地 IPC](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md)，任务独立见 [R0–R3 清单](JSONL_MASTER_EXECUTOR_PHASE3_TASK_LIST.md)。基础数据契约继续引用 [一期](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md)。

三期从二期 I10 的进程池、IPC 与验收证据出发，增加远程连接/认证、路由、中继、机器资源和断线对账。它不回到一期重新实现存储，也不成为一期/二期上线前置条件。

<a id="phase3-s1"></a>
## 1. 远程 agent 与跨机器执行

执行任务：[R0-01](JSONL_MASTER_EXECUTOR_PHASE3_TASK_LIST.md#task-r0-01)、[R1-01](JSONL_MASTER_EXECUTOR_PHASE3_TASK_LIST.md#task-r1-01)、[R2-01](JSONL_MASTER_EXECUTOR_PHASE3_TASK_LIST.md#task-r2-01)、[R3-01](JSONL_MASTER_EXECUTOR_PHASE3_TASK_LIST.md#task-r3-01)、[R3-02](JSONL_MASTER_EXECUTOR_PHASE3_TASK_LIST.md#task-r3-02)。

本节是三期候选设计，须基于二期最终协议复核后实施。首步保留一个权威提交主进程，通过远程 agent 扩展计算容量；agent 自己不推进 DAG、不写权威运行状态。

<a id="phase3-s1-1"></a>
### 1.1 拓扑与职责

```text
主进程 flow-server
  唯一 JournalWriter + 调度 + SQLite 投影
         |
         | control TLS 连接 + data TLS 连接
         v
机器 A：flow-agent                  机器 B：flow-agent
  资源/进程池管理                     资源/进程池管理
  路由与有界转发                     路由与有界转发
    | 双 socketpair                   | 双 socketpair
    +--> flow-executor                +--> flow-executor
    +--> flow-executor                +--> flow-executor
```

每个 agent 主动连接主进程，管理本机受控执行器、报告可用资源与 build/capabilities、执行主进程分配的任务、转发审计/结果/确认、处理进程树取消。无需开放每个执行子进程的网络端口。

主进程负责选择机器、固定 dispatch→agent→executor 的归属、所有业务重试与恢复决策。agent 只允许在派发记录约束内选择空闲本地槽位；归属通过 BindExecutor/确认固定后才能 Execute。不能因本地超时自行换一个子进程重复执行同一派发。

一台机器的 agent 崩溃影响该机执行能力，不改变已经写入主进程日志的事实。它不是日志副本、数据库代理或第二个调度主节点。

<a id="phase3-s1-2"></a>
### 1.2 远程传输、身份与路由

- 主↔agent 使用两条 TCP + TLS 连接，初版采用 mTLS 验证双方身份；无需立即引入 HTTP/gRPC/QUIC。TLS 身份映射到允许的 agent_id，不能信客户端自报字段。
- 新增稳定 agent_id、每次启动变化的 agent_boot_id、上联 link_session_id。executor_id/boot_id/dispatch_id 继续标识实际执行来源，agent 的会话不能替代子进程身份。
- control 完成握手后发一次性 data 绑定凭据，data 连接校验同一认证主体、agent_boot_id 和 link_session_id，防止连接串配。握手/凭据有有效期；连接替换后旧绑定作废。
- 本地 socketpair 仍由 agent 管理；agent 是受信任的协议中继，主进程持久登记或确认的派发归属限定它可上报哪些任务。
- 外层增加路由包络 `agent_id / agent_boot_id / link_session_id / executor_id / executor_boot_id`，内层保留任务消息及审计原始编码；journal_id/master_epoch/link_session_id 由经认证的上联上下文校验，agent 将本地 dispatch 归属映射到已确认路由，不能信执行器自报机器身份。重建本地/上联会话时可替换经验证的连接包络，不能修改 dispatch、audit_seq 或记录内容。
- 协议能力分层协商：主↔agent 和 agent↔executor 各自握手，agent 汇报真实能力交集，不冒充子进程支持更高版本。
- 多个 executor 共享 agent 的 control/data 上联时，data 采用按 dispatch 的有界公平发送；帧仍有大小上限，不能将一个任务整个历史拼成一帧。control 只发小消息，避免同一机器内的日志洪泛饿死其他任务。

远程 transport 复用 [二期 §3](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s3) 的长度帧和业务消息；增加 AgentHello/AgentWelcome、CapacityReport、BindExecutor、Resume/ResumeReply。没有必要将所有本机进程管理动作都暴露成远程 RPC。

<a id="phase3-s1-3"></a>
### 1.3 端到端确认，不允许 agent 提前确认

```text
executor 发 AuditBatch
  → agent 有界转发
  → 主进程校验并写 JSONL
  → 主进程 fsync 成功
  → AuditAck 经 agent 返回 executor
```

agent 不能因为已经收包、写本地缓存或发到 TCP 就生成 AuditAck。ResultCommitted 同样只能来自主进程的权威提交。OperationPermit 也只能转发主进程已持久授权的许可；agent 必须核对当前路由/会话，不能将 AuditAck 转为许可，不能在对账或补传时重发旧会话许可。确认绑定 journal_id、当前授权会话与 dispatch，agent 不得替换 durable_audit_seq 或提升 commit_lsn。

执行器的未持久窗口 D/S 与固定 W 规则不变；主进程准入时同时预留全局和机器级 C_agent 预算，agent 再为本地派发预留对应转发空间。存续阶段不动态缩窗，对账补传同样需要预算准入。转发层重复持有缓冲要计入实际 RSS 预算，不能以“各层分别有界”掩盖叠加内存。

大输入由主进程按内容标识和摘要传输，agent 可建立可丢弃缓存。InputReady 只证明输入可用；缺输入必须重传，不能用路径名假定两台机器共享磁盘。大输出沿审计 chunk 路径进入权威日志，不能只把 agent 本地路径放进完成结果。

<a id="phase3-s1-4"></a>
### 1.4 上联断线、重连与未决任务对账

三期在二期本地会话之外增加上联状态：`Ready → Suspect → Reconnecting → Reconciling → Ready/Closed`。Suspect 冻结新任务与新外部操作；Reconnecting 只负责退避认证；Reconciling 只允许主进程批准的证据补传、固定结果提交与取消收尾。进入 Ready 只开放新派发，不恢复旧执行权。这两个重连状态不进入二期本地 IPC 状态机。

agent 上联任一通道失效后进入 [二期 §3.7](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s3-7) 的 Suspect：立即停止新任务分配，并通过本地 control 通知各执行器冻结新的外部操作/受控停止。安全点和强制终止规则与本地一致。主进程将相关未决任务标为连接丢失，不把所有任务立即当作已失败并重新执行。

上联重连只恢复通信和对账能力，**不自动恢复旧派发的执行权**。初版远程恢复不实现任意程序指令位置续跑；需要重执行时由主进程记录新的派发，并沿用正确的外部 operation_id。

agent 退避重连、重新认证、建立全新 link_session_id 后发送 Resume 清单（分页），每项至少包含：

- 原 journal_id/master_epoch、agent_boot_id、executor_boot_id 和 dispatch_id；
- 本地任务状态：仍执行/受控停止/已生成结果/进程已退出；
- 最后已知 durable_audit_seq、仍保有的数据范围、是否有固定 result_id/最后审计序号；
- 是否丢失未确认数据、是否曾强制终止，以及可用固定结果描述的摘要。

主进程依据权威日志逐项回复：

| ResumeReply 动作 | 条件与处理 |
| --- | --- |
| AlreadyCommitted | 结果已在权威日志；返回原提交确认，不再执行 |
| UploadOnly | 主进程给出真实 durable_audit_seq，agent/执行器只补传其后仍保有的记录；主进程预留补传窗口后才能发送，不授权新外部操作 |
| SubmitExistingResult | 同一有效派发尚未被取消/替代，执行器已经生成并封口结果；允许提交该固定结果，仍必须过 [二期 §3.6](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s3-6) 屏障 |
| CancelAndDrain | 持久取消/派发废止；终止旧执行，只收允许的迟到审计 |
| ReconcileRequired | boot 变化、任期变化、缺数据或外部操作未知；记录事实并进入恢复/人工核对流程，不透明重跑 |

同一 boot 且尚未提交的完成结果可以在明确 SubmitExistingResult 后进入正常屏障；更换执行器进程不能声称仍握有原任务内存。agent_boot 变化后先盘点并回收旧进程，不凭旧 PID 收养未知执行器。

任何会话/任期变化的历史数据提交都必须先完成显式归属核对与补传准入；不能以严格身份拒收为由丢弃历史证据，也不能绕过授权。主进程重启导致 master_epoch 变化时，默认只补录审计/对账，不接受重连隐式恢复业务提交；若需采用旧结果，必须通过显式恢复裁决在新任期记录依据。主进程返回的权威游标优先于 agent 的“最后收到 ack”；agent 声称的位置领先于主日志时视为完整性/数据集问题，停止自动处理。

<a id="phase3-s1-5"></a>
### 1.5 暂存与完整性的限制

首版远程使用有界内存转发，断网即背压并停止新的外部操作。若业务后来要求短暂断网仍能保留更多未确认数据，可增加按 dispatch 分段的本地 spool，但须单独验收磁盘限额、重启恢复与重传。

spool 是传输暂存，不是第二份权威日志：写入 spool 不释放端到端持久确认窗口，也不允许 agent 返回业务成功。启用 spool 时可将经协商的未确认窗口存放在磁盘而非全部内存，但仍有明确总字节上限，满后必须停止生产。

agent 机器或磁盘一起丢失时，尚未到达主进程的数据仍可能丢失。此时主进程保留已确认前缀并明确标记未完成/不完整，不生成成功结果。必须先可靠留痕的操作依然要等主进程持久确认，不能承诺在任意断网下既持续执行又零数据缺口。

<a id="phase3-s1-6"></a>
### 1.6 资源、部署与故障隔离

agent 上报 CPU/内存/可用槽位/待确认字节/本地存储水位；主进程按已分配资源扣减，报告有延迟也不能重复超配。初版只做能力匹配与有界槽位分配，不提前引入复杂公平调度器。

进程池隔离以每子进程一任务为起点。agent drain 时停止接新派发、等待或按策略终止旧任务、转发最后提交确认，再退出；单任务取消按 [二期 §3.6](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s3-6) 回收目标子进程，不关闭整台 agent 的上联。agent 自身诊断与业务审计分开处理；业务记录仍遵守完整性契约。

远程执行器版本、可用节点类型、凭证引用与资源要求在派发前校验。凭证通过既定秘密管理途径按任务提供，不能把主进程整个环境复制到远程进程。此处是执行边界约束，不新建秘密管理产品。

<a id="phase3-s1-7"></a>
### 1.7 未来实施阶段与故障验收

| 阶段 | 工作 | 验收 |
| --- | --- | --- |
| R0 协议复用 | 用本地模拟中继实现外层路由、agent/executor 双层身份和能力协商；不改变审计内层编码 | 同一组任务 fixtures 在直接 socketpair 和中继路径得到相同权威事实；错路由拒绝 |
| R1 远程连接 | flow-agent、本机进程池、双 TLS 连接、认证绑定、全局/机器/任务窗口 | 多机器执行与大审计流；不允许 agent 暂存提前 ack；无无界缓冲 |
| R2 失联对账 | Resume/ResumeReply、断线停止、重复帧/ack、boot 与 epoch 变化 | 断线后不双重执行；已提交结果回原确认；数据缺口阻止成功；迟到审计可追溯 |
| R3 运维与容量 | agent drain/升级、资源报告、备份检查、真实网络与存储压力测试；仅有需求时加入 spool | 单 agent 故障不影响其他机器任务；主日志满足所有已确认数据；明确主进程仍是单提交与可用性边界 |

必须注入：control 单独断开、data 单独断开、网络持续阻塞、结果已提交但 ack 丢失、补传意图不得产生许可、旧会话许可重放拒绝、agent 在转发前/后崩溃、executor 在审计封口前/后崩溃、重连期间取消、旧 session 注入、新 boot 伪装旧派发、多 executor 公平性、spool 满/损坏（若启用）。网络故障用可控代理/测试 transport 模拟，不依赖偶然断网。

<a id="phase3-s1-8"></a>
### 1.8 主进程高可用是另一项设计

远程 agent 扩展计算能力，不自动扩展主进程提交吞吐或提供主进程高可用。真正多主或跨机器接管必须另行证明：

1. 新 owner 能读取全部已确认日志；异步上传本地文件不能提供零缺口接管。
2. 实际接收追加的一方拒绝旧 owner；在 PG 表中更新 epoch 不会自动保护普通文件写入。
3. 旧派发迟到结果不能改变新状态；审计证据保留规则仍成立。
4. 跨分区父子命令有稳定身份、可靠交付与重试；不能沿用单日志事务的原子性结论。

本次不实现共识协议，不声称共享目录自动具备 fencing。集群阶段可选择可靠日志服务或受控共享持久存储，但在选定并验证前，不发布高可用承诺。


<a id="phase3-deferred"></a>
## 2. 按需求另行评审的扩展

以下能力从基础范围移出，不自动成为任何一期或 R0–R3 的依赖。三期继续继承一期的单外部操作、projected 成功和固定 ValueRef 契约，只有另行评审通过才扩展：

- **单节点多外部调用**：一期只支持一个外部逻辑操作。若未来需要多调用适配器，须先定义稳定 operation_key、逐操作请求指纹、Outcome 复用与未决操作核对；A 已成功、B 未知时不得从头盲重跑，调用路径变化必须能被检测。不得仅用调用次序或随机 ID 猜测操作身份，不实现任意脚本指令快照续跑。
- **公开 durable 写入模式**：一期所有写入成功均等投影可见。只有出现明确延迟需求后，才另行设计 `wait_for=durable`、`min_commit_cursor` 和有界读取等待；继续保留已提交超时查询与向后兼容行为。此模式不是本地 IPC 或远程执行的前置条件。
- **复杂分块清单**：一期使用连续 chunk 与固定 ValueRef，不构建清单树。仅在离散分块复用等实际需求出现后，重新定义格式版本与流式校验，不反向扩大一期引用模型。
- **提交域扩展与高可用**：目标负载实测不足时先定位批处理、编码、磁盘和投影瓶颈；分片、额外 WAL、复制与接管均须另行设计，不能依据单次 fsync 微基准预选。任何方案都必须证明已确认的完整事实可从权威 JSONL 恢复。

<a id="phase3-approval"></a>
## 3. 三期实施与验收状态

本轮仅迁移设计与任务，R0–R3 尚未执行。后续单独记录实际用户授权、网络/机器负载、协议版本与验收证据。R0-01 的前置交付是 [二期 I10](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md#task-i10)。

R2/R3 覆盖 §1.7 的远程故障矩阵，并回归一期持久业务事实和二期进程/IPC 语义。主进程 HA、日志复制、跨分区事务与 §2 按需扩展都不是默认三期范围。计划、验收和生产切换分别记录。
