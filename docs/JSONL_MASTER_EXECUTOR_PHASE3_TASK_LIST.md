# JSONL 重构三期任务清单：远程 agent

日期：2026-09-30；更新：2026-10-02。状态：**R0-01/R1-01/R2-01/R3-01 实现完成（开发验收口径；生产切换未执行），R3-02 按约定 skipped。证据见 docs/refactor-evidence/R0-01–R3-02.md。**

<a id="agent-entry"></a>
## 执行入口与完成定义

先读本段、所选任务和引用设计；保留工作区既有编辑，不自动启动并行 agent。R0-01 依赖已验收的二期 I10，不能用一期基础完成代替本地 IPC 已完成。agent 仅中继/资源管理，不产生权威 ACK 或独立副作用许可。

按实际用户授权范围实施，不重复请求已获授权；每个任务附 `docs/refactor-evidence/<ID>.md` 和真实测试/未覆盖项再标 done。R3-02 仅有明确需求时实施，否则记录 skipped；其他三期任务不得隐含要求 spool、高可用或新的提交域。

<a id="task-index"></a>
## 本期任务与依赖

| 任务 | 工作 | 前置任务 |
| --- | --- | --- |
| [R0-01](#task-r0-01) | 模拟中继与远程复用契约 | [I10](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md#task-i10) |
| [R1-01](#task-r1-01) | agent 双 TLS 与机器资源准入 | [R0-01](#task-r0-01) |
| [R2-01](#task-r2-01) | 断线对账与证据补传 | [R1-01](#task-r1-01) |
| [R3-01](#task-r3-01) | 远程运维与容量验收 | [R2-01](#task-r2-01) |
| [R3-02](#task-r3-02) | 可选有界 spool | [R2-01](#task-r2-01) |

<a id="remote-tasks"></a>
## 三期 R0–R3 任务

以下只属于三期远程实施范围。主进程 HA、日志复制、跨分区事务另行设计，不在任务中暗含实现；R3-02 为可选项。

<a id="task-r0-01"></a>
### R0-01 通过模拟中继冻结远程复用契约

- **状态**：done（2026-10-02）；证据：`docs/refactor-evidence/R0-01.md`。
- **前置输入**：[I10](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md#task-i10) 的交付证据。
- **设计引用**：[§1.1](JSONL_MASTER_EXECUTOR_PHASE3_PLAN.md#phase3-s1-1)、[§1.2](JSONL_MASTER_EXECUTOR_PHASE3_PLAN.md#phase3-s1-2)、[§1.7](JSONL_MASTER_EXECUTOR_PHASE3_PLAN.md#phase3-s1-7)。只按需读取这些小节。
- **目标与原因**：通过模拟中继冻结远程复用契约；解决下述契约对应的正确性或交付问题。
- **就地契约**：三期需实际实施授权；agent 仅中继/资源管理，不推进 DAG 或产生权威 ACK。
- **改动范围**：未来 agent DTO/模拟 relay；本地协议 fixtures。

**执行步骤**：

1. 增加 agent/boot/link/executor 路由身份和能力交集。
2. 定义 BindExecutor/确认与归属持久化，确认后才 Execute。
3. 保留审计原始编码，复用本地消息。
4. 模拟错路由/旧会话验证。

**验证场景**：

- [ ] 直接 socketpair 与 relay fixtures 得到相同事实。
- [ ] 错归属/伪造能力拒绝。
- [ ] ACK/Permit 不被中继生成。

**验收出口**：业务协议一致、远程边界明确且未引入第二调度主节点。

**交付物与交接**：远程 fixtures、模拟中继与归属测试。在本任务证据报告写明实际位置、命令和结果，供下游直接消费。

**禁止事项**：未获远程授权不启动；不暴露所有本机管理为远程 RPC。

<!-- END R0-01 -->

<a id="task-r1-01"></a>
### R1-01 实现远程 agent 双 TLS 与资源准入

- **状态**：done（2026-10-02）；证据：`docs/refactor-evidence/R1-01.md`。
- **前置输入**：[R0-01](#task-r0-01) 的交付证据。
- **设计引用**：[§1.1](JSONL_MASTER_EXECUTOR_PHASE3_PLAN.md#phase3-s1-1)、[§1.2](JSONL_MASTER_EXECUTOR_PHASE3_PLAN.md#phase3-s1-2)、[§1.3](JSONL_MASTER_EXECUTOR_PHASE3_PLAN.md#phase3-s1-3)、[§1.6](JSONL_MASTER_EXECUTOR_PHASE3_PLAN.md#phase3-s1-6)。只按需读取这些小节。
- **目标与原因**：实现远程 agent 双 TLS 与资源准入；解决下述契约对应的正确性或交付问题。
- **就地契约**：mTLS 主体映射 agent_id；data 一次性绑定 control 会话；全局/agent/dispatch 分层预算；ACK/Permit 端到端来自主进程。
- **改动范围**：拟新增 flow-agent；server agent manager；本地 executor 池复用。

**执行步骤**：

1. agent 主动连接与身份认证，绑定双通道。
2. 实现容量报告及主进程分配扣减，BindExecutor 后执行。
3. 按 dispatch 公平转发并计入所有缓冲复制。
4. 按内容标识传输入，最小凭证引用。
5. 取消只回收目标子进程。

**验证场景**：

- [ ] 连接串配/过期凭据/旧 session。
- [ ] 多 executor 洪泛公平性。
- [ ] 报告延迟不超配。
- [ ] 缓存不提前 ACK。
- [ ] 单任务取消不关闭上联。

**验收出口**：多机可执行且总资源有界，错误身份/路由不进入权威日志。

**交付物与交接**：agent 二进制、认证/绑定/准入实现及网络集成测试。在本任务证据报告写明实际位置、命令和结果，供下游直接消费。

**禁止事项**：不通过 IP 信任身份；不复制主进程完整环境或把本地缓存当权威。

<!-- END R1-01 -->

<a id="task-r2-01"></a>
### R2-01 实现断线对账与历史证据补传

- **状态**：done（2026-10-02）；证据：`docs/refactor-evidence/R2-01.md`。
- **前置输入**：[R1-01](#task-r1-01) 的交付证据。
- **设计引用**：[§1.4](JSONL_MASTER_EXECUTOR_PHASE3_PLAN.md#phase3-s1-4)、[§1.3](JSONL_MASTER_EXECUTOR_PHASE3_PLAN.md#phase3-s1-3)、[二期 §3.7](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s3-7)。只按需读取这些小节。
- **目标与原因**：实现断线对账与历史证据补传；解决下述契约对应的正确性或交付问题。
- **就地契约**：任一上联失效即停止新派发/外部操作；重连只恢复通信，不恢复旧执行权。Permit 不跨会话重用。
- **改动范围**：agent reconnect；server Resume 裁决；executor 受控停止。

**执行步骤**：

1. 实现退避认证、新 link session、分页 Resume。
2. 按日志输出 AlreadyCommitted/UploadOnly/SubmitExistingResult/CancelAndDrain/ReconcileRequired。
3. 补传前预留预算并验证 dispatch 归属。
4. boot 变化回收旧进程，epoch 变化默认只补证据。
5. 旧结果采用需显式持久恢复裁决。

**验证场景**：

- [ ] 任一通道断开/持续阻塞。
- [ ] 结果 ACK 丢失。
- [ ] 重连期间取消。
- [ ] 旧 boot/session 注入。
- [ ] agent 游标领先主日志。
- [ ] 补传 Intent 不产生 Permit。

**验收出口**：断线不双重执行、不丢已确认事实；缺口阻止成功，不隐式采用旧结果。

**交付物与交接**：对账状态机、受控代理故障测试与恢复说明。在本任务证据报告写明实际位置、命令和结果，供下游直接消费。

**禁止事项**：不收养仅凭 PID 的未知进程；不将重连当任意程序续跑。

<!-- END R2-01 -->

<a id="task-r3-01"></a>
### R3-01 验收远程运维与容量

- **状态**：done（2026-10-02，开发验收口径）；证据：`docs/refactor-evidence/R3-01.md`。
- **前置输入**：[R2-01](#task-r2-01) 的交付证据。
- **设计引用**：[§1.6](JSONL_MASTER_EXECUTOR_PHASE3_PLAN.md#phase3-s1-6)、[§1.7](JSONL_MASTER_EXECUTOR_PHASE3_PLAN.md#phase3-s1-7)、[§1.8](JSONL_MASTER_EXECUTOR_PHASE3_PLAN.md#phase3-s1-8)、[一期 §15](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s15)。只按需读取这些小节。
- **目标与原因**：验收远程运维与容量；解决下述契约对应的正确性或交付问题。
- **就地契约**：多 agent 扩计算，不扩单主耐久吞吐，也不提供 HA。
- **改动范围**：agent drain/升级；backend-perf；远程部署/备份说明。

**执行步骤**：

1. 实现 drain/升级期间停止新派发和确认收尾。
2. 执行多机器网络/磁盘压力与公平性测试。
3. 验证单 agent 故障隔离及主日志容量。
4. 记录部署/版本/凭证边界和主进程故障处理。

**验证场景**：

- [ ] agent 转发前后崩溃。
- [ ] drain 超时回收。
- [ ] 多机日志洪泛下控制 p99。
- [ ] 一机掉线其他机器继续。
- [ ] 原始审计计数和摘要。

**验收出口**：达到独立远程验收目标，单主限制明确；未通过容量门槛不扩发布范围。

**交付物与交接**：远程验收报告、运维手册和部署证据。在本任务证据报告写明实际位置、命令和结果，供下游直接消费。

**禁止事项**：不宣称 HA；不实现未经设计的共享目录 fencing。

<!-- END R3-01 -->

<a id="task-r3-02"></a>
### R3-02 可选：有界磁盘 spool

- **状态**：skipped（2026-10-02，无明确需求，按清单约定记录）；证据：`docs/refactor-evidence/R3-02.md`。
- **前置输入**：[R2-01](#task-r2-01) 的交付证据。
- **设计引用**：[§1.5](JSONL_MASTER_EXECUTOR_PHASE3_PLAN.md#phase3-s1-5)、[§1.7](JSONL_MASTER_EXECUTOR_PHASE3_PLAN.md#phase3-s1-7)。只按需读取这些小节。
- **目标与原因**：可选：有界磁盘 spool；解决下述契约对应的正确性或交付问题。
- **就地契约**：默认跳过；仅业务明确要求断网保留更多未确认数据且单独批准时启用。spool 不产生权威确认，仍受端到端总窗口限制。
- **改动范围**：agent 按 dispatch 暂存与恢复；仅新增需求成立时实现。

**执行步骤**：

1. 固定磁盘配额/文件格式/重启发现与校验。
2. 按 dispatch 保留原始编码并在对账后重传。
3. 仅收到主 durable ACK 后回收已确认暂存。
4. 满盘停止生产，损坏记录缺口与证据。

**验证场景**：

- [ ] spool 满/损坏/重启。
- [ ] 主 ACK 丢失重传。
- [ ] agent 整机丢失。
- [ ] 计费包括磁盘未确认窗口和内存副本。

**验收出口**：不提前 ACK、不无界积压，无法保留时停止执行并明确完整性。

**交付物与交接**：经批准的 spool 契约、实现与故障证据；无需求则记录不适用。在本任务证据报告写明实际位置、命令和结果，供下游直接消费。

**禁止事项**：不能默认纳入 MVP；不能承诺断网持续执行且任意故障零缺口。

<!-- END R3-02 -->


远程故障逐项映射至 [三期 §1.7](JSONL_MASTER_EXECUTOR_PHASE3_PLAN.md#phase3-s1-7)，并回归一期基础及二期 IPC；实现完成、验收通过与生产切换分别记录。
