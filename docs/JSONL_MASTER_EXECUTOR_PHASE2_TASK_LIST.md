# JSONL 重构二期任务清单：本地 IPC 与子进程

日期：2026-09-30；更新：2026-10-02。状态：**I01–I10 实现完成（开发验收口径；生产切换未执行），证据见 docs/refactor-evidence/I01–I10.md。**设计依据：[本期方案](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md)。本轮用户要求仅修订文档，不表示已开始实现或允许生产切换。

<a id="agent-entry"></a>
## 执行入口与完成定义

本期入口依赖一期 F17；I09 只依赖 I03，需在 I08 集成前完成开发启动链。最终验收 I10 不依赖任何 R 任务。

1. 读取本段、所选任务及引用小节；先核对工作区/源码/依赖证据，保留既有编辑，不 reset，不把旧结论当最新事实。
2. 在实际用户授权的阶段范围内自主推进，不重复索要已获授权；任务清单不授权自动启动并行 agent。
3. JSONL 权威与完整业务事实、独立可丢弃观测、单行事务、固定 ValueRef、单操作裁决和 projected 成功语义不因分期而削弱。
4. 每任务产出接口/实现/必要回归和 `docs/refactor-evidence/<ID>.md` 证据，记录真实命令、结果与未覆盖项后才能标 done；计划测试不等于已执行。
5. 只引用已验收依赖；前一期不依赖后一期。编号用于定位，执行顺序以依赖为准。实际实现、验收和生产切换分别记录在本期设计状态节。

<a id="task-index"></a>
## 本期任务与依赖

| 任务 | 工作 | 前置任务 |
| --- | --- | --- |
| [I01](#task-i01) | 冻结本地 IPC 与资源契约 | [F17](JSONL_MASTER_EXECUTOR_TASK_LIST.md#task-f17) |
| [I02](#task-i02) | 实现双通道帧与会话校验 | [I01](#task-i01) |
| [I03](#task-i03) | 实现子进程池与可靠回收 | [I02](#task-i02) |
| [I04](#task-i04) | 迁移模板与节点执行到子进程 | [I03](#task-i03) |
| [I05](#task-i05) | 实现持久窗口与观测传输 | [I04](#task-i04) |
| [I06](#task-i06) | 实现输入传输与输出分块衔接 | [I05](#task-i05) |
| [I07](#task-i07) | 接通操作许可、结果屏障与取消排空 | [I06](#task-i06) |
| [I08](#task-i08) | 接入 IPC Driver 与持久等待恢复 | [I07](#task-i07)、[I09](#task-i09) |
| [I09](#task-i09) | 提前交付 IPC 开发打包与升级入口 | [I03](#task-i03) |
| [I10](#task-i10) | 验收本地 IPC 并交接远程阶段 | [I08](#task-i08)、[I09](#task-i09) |

<a id="task-i01"></a>
### I01 冻结本地 IPC 与资源契约

- **状态**：done（2026-10-02）；证据：`docs/refactor-evidence/I01.md`。
- **前置输入**：[F17](JSONL_MASTER_EXECUTOR_TASK_LIST.md#task-f17)。
- **设计引用**：[二期 §1](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s1)、[二期 §2](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s2)、[二期 §3.5](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s3-5)。
- **改动范围**：IPC fixtures、能力/会话、窗口与进程预算。

**执行步骤**：

1. 消费一期接口和基线，冻结一次 Execute、AuditBatch/Ack、RequestOperation/Permit、Result/Committed、Cancel/Stopped 与观测传输 DTO。
2. 固定 u32 长度 JSON、control/data 上限、会话字段、X_max/A_max/W/输入传输/全局预算及期望 p99；与一期行/值上限配合。
3. 固定新增任期/归属事件的版本兼容与升级边界；不重定义一期基础事实，不增加远程消息。

**验收出口**：

- [ ] 合法/非法/异身份/重复消息 fixtures 齐全，单条记录可被窗口容纳。
- [ ] 三类并发与实际进程预算独立，资源/指标有单位，无远程前置项。

<!-- END I01 -->

<a id="task-i02"></a>
### I02 实现双通道帧与会话校验

- **状态**：done（2026-10-02）；证据：`docs/refactor-evidence/I02.md`。
- **前置输入**：[I01](#task-i01)。
- **设计引用**：[二期 §3.1](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s3-1)、[二期 §3.2](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s3-2)、[二期 §3.3](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s3-3)。
- **改动范围**：execution_protocol、帧 codec、父端点归属、任期事件接入。

**执行步骤**：

1. 实现独立 control/data I/O、长度校验和串行写帧；Hello/Welcome/Ready 按父端点绑定槽位，PID 只作诊断。
2. 绑定 journal_id/epoch/boot/session/dispatch，初始化持久任期接口；禁止本地 listener、nonce 连接重绑和透明重连。

**验收出口**：

- [ ] 半帧/粘帧、非法长度、错版本/槽位/旧 boot 明确拒绝。
- [ ] data 满时 control 仍能推进，未持久授权不能因握手而执行。

<!-- END I02 -->

<a id="task-i03"></a>
### I03 实现子进程池与可靠回收

- **状态**：done（2026-10-02）；证据：`docs/refactor-evidence/I03.md`。
- **前置输入**：[I02](#task-i02)。
- **设计引用**：[二期 §3.1](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s3-1)、[二期 §3.7](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s3-7)、[二期 §3.9](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s3-9)。
- **改动范围**：flow-executor 二进制、spawn/FD/进程树与有界槽位。

**执行步骤**：

1. 每进程一个任务，仅映射目标 socketpair FD；其余 close-on-exec，父子各关对端。
2. 实现本地状态机、任一通道故障后的停止/排空、超时 kill/wait/reap 和父死亡检测；骨架阶段只运行受控测试任务。

**验收出口**：

- [ ] Linux/macOS 的启动失败、FD 泄漏、兄弟继承、父退出与卡死回收正确。
- [ ] 无无限 spawn/僵尸/失管进程，已取消任务的进程不直接复用。

<!-- END I03 -->

<a id="task-i04"></a>
### I04 迁移模板与节点执行到子进程

- **状态**：done（2026-10-02）；证据：`docs/refactor-evidence/I04.md`。
- **前置输入**：[I03](#task-i03)。
- **设计引用**：[二期 §3.8](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s3-8)。
- **改动范围**：exec/expr 子进程适配与每任务 JS 上下文。

**执行步骤**：

1. 迁移全部模板、script/condition 与固定适配器，主进程不留用户表达式求值。
2. 同一 dispatch 内准备和执行，接入 InputPrepared 提交端口及总输入/heap/进程限额；已有输入跳过求值，待 I07 接通真实副作用。

**验收出口**：

- [ ] 死循环/内存超限局限于目标进程，任务之间 JS 状态隔离。
- [ ] prepare 无网络，语义与一期一致，不产生第二个 prepare 派发。

<!-- END I04 -->

<a id="task-i05"></a>
### I05 实现持久窗口与观测传输

- **状态**：done（2026-10-02）；证据：`docs/refactor-evidence/I05.md`。
- **前置输入**：[I04](#task-i04)。
- **设计引用**：[二期 §3.4](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s3-4)、[二期 §3.5](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s3-5)、[二期 §4](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s4)。
- **改动范围**：AuditBatch/Ack、发送/接收预算、ObservabilityBatch。

**执行步骤**：

1. 把一期持久回执包装为 AuditAck，按 B(S)-B(D) 计算未确认字节，派发前预留 W；计入生产/解码/待落盘/重传副本。
2. 同 audit_seq 同原始内容重传幂等，异内容拒绝；ACK 仅随连续持久前缀前进，满窗口可取消。
3. ObservabilityBatch 接一期独立存储，无 ACK、不占审计窗口、不阻塞 JS 或权威事实。

**验收出口**：

- [ ] 最大记录、全部槽位满窗口、ACK 丢失、取消阻塞与磁盘暂停均有界推进。
- [ ] socket 收到但未 sync 不确认；观测丢弃不影响完整性。

<!-- END I05 -->

<a id="task-i06"></a>
### I06 实现输入传输与输出分块衔接

- **状态**：done（2026-10-02）；证据：`docs/refactor-evidence/I06.md`。
- **前置输入**：[I05](#task-i05)。
- **设计引用**：[二期 §3.3](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s3-3)、[二期 §3.6](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s3-6)、[一期 §6.1](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s6-1)。
- **改动范围**：TransferChunk/InputReady 与一期 StoredValue 流接口。

**执行步骤**：

1. 大输入按 transfer/offset/digest 校验，传输窗口独立于审计窗口；InputReady 只证明输入可用。
2. 输出复用一期连续 chunk 与固定候选描述，Result 有界；接通 HTTP 生产、引用扇出/汇合和受限 JS 物化。

**验收出口**：

- [ ] 输入/输出同时满额无循环等待，缺块/乱序/异内容/超限明确失败。
- [ ] 全链路 RSS 有界，重读已存值不重发网络请求，不使用本机路径当数据。

<!-- END I06 -->

<a id="task-i07"></a>
### I07 接通操作许可、结果屏障与取消排空

- **状态**：done（2026-10-02）；证据：`docs/refactor-evidence/I07.md`。
- **前置输入**：[I06](#task-i06)。
- **设计引用**：[二期 §3.6](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s3-6)、[二期 §4.1](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s4-1)、[一期 §7](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s7)。
- **改动范围**：主进程 IPC 适配到一期授权/结果门面。

**执行步骤**：

1. RequestOperation 经一期单操作裁决，同事务 Intent/Authorized 持久后再发绑定会话的 Permit；AuditAck 永不授权。
2. Result 与 data 乱序时仍读 data，只有完整前缀/值且身份有效才回 ResultCommitted；重复返回原提交。
3. 取消胜出后收迟到证据并 Stopped/封口，超时回收进程；缺口与未知副作用按一期规则保留。

**验收出口**：

- [ ] 许可重复/迟到/跨会话、取消前后两顺序、已提交许可丢失均不重复调用。
- [ ] Result 先到、缺块、结果 ACK 丢失、强杀与旧尝试缺口正确处理。

<!-- END I07 -->

<a id="task-i08"></a>
### I08 接入 IPC Driver 与持久等待恢复

- **状态**：done（2026-10-02）；证据：`docs/refactor-evidence/I08.md`。
- **前置输入**：[I07](#task-i07)、[I09](#task-i09)。
- **设计引用**：[二期 §3.8](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s3-8)、[二期 §5](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s5)、[一期 §9](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s9)。
- **改动范围**：执行端口切换、父子/等待/恢复与一期等价回归。

**执行步骤**：

1. 将一期进程内适配器替换为 IPC 派发；固定一个 run 的执行模式，一次尝试只有一个 dispatch。
2. 等待请求在主进程同事务登记，确认后释放槽位；主进程处理取消/信号，不等待空闲子进程。
3. 用真实子进程走通 HTTP→script→delay→重启及父→子→孙，复用已提交输入/Outcome，不自动续跑未知操作。

**验收出口**：

- [ ] 单槽父子等待无死锁，重启不延长 delay、不重求输入或重发已完成 HTTP。
- [ ] 一期/二期的状态、输出与公开事件语义等价，只有执行诊断差异。

<!-- END I08 -->

<a id="task-i09"></a>
### I09 提前交付 IPC 开发打包与升级入口

- **状态**：done（2026-10-02）；证据：`docs/refactor-evidence/I09.md`。
- **前置输入**：[I03](#task-i03)。
- **设计引用**：[二期 §5](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s5)。
- **改动范围**：server/executor 打包、模式配置与测试启动链。

**执行步骤**：

1. 提供受控 executor 定位和明确的 IPC opt-in 模式，为 I08 集成提前交付启动链；缺失/不兼容二进制启动失败，不静默回退。
2. 定义一期→二期停止派发/排空/未决处理与 schema 能力检查；已写新增事件后的回退必须能解释全部事实。

**验收出口**：

- [ ] 打包产物、路径带空格、错误版本、无遗留进程和模式配置通过。
- [ ] 同数据集单写者、同 run 单执行模式，未接通能力明确报错。

<!-- END I09 -->

<a id="task-i10"></a>
### I10 验收本地 IPC 并交接远程阶段

- **状态**：done（2026-10-02，开发验收口径；生产切换未执行）；证据：`docs/refactor-evidence/I10.md`。
- **前置输入**：[I08](#task-i08)、[I09](#task-i09)。
- **设计引用**：[二期 §6](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s6)、[二期 §7](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-s7)、[二期状态](JSONL_MASTER_EXECUTOR_PHASE2_PLAN.md#phase2-approval)。
- **改动范围**：二期故障/e2e/perf 与升级验收报告。

**执行步骤**：

1. 逐行落实二期故障矩阵并回归一期基础、权限/客户端与迁移；真实断电覆盖与 SIGKILL 分别报告。
2. 按同等负载测主/子进程 RSS、X_max/A_max/W、等确认占槽、回收时延、吞吐与 projected p99，核验全部内容与旧缺口。
3. 形成二期独立部署/回退评审，向三期交付进程池/IPC fixtures/身份与持久语义，记录实际批准与验收状态。

**验收出口**：

- [ ] 所有 I 任务和二期能力有证据；没有 agent/TLS/Resume 的前置依赖。
- [ ] 隔离、预算与基础语义同时通过，不把中继或未执行测试标为已完成。

<!-- END I10 -->

旧编号迁移见 [一期映射表](JSONL_MASTER_EXECUTOR_TASK_LIST.md#task-migration)，只用于追溯来源，不构成跨期依赖。
