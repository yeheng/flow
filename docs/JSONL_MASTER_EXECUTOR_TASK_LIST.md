# JSONL 重构一期任务清单：基础存储与进程内执行

日期：2026-09-30。状态：**仅设计与任务已拆分，所有实现任务待执行**。设计依据：[本期方案](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md)。本轮用户要求仅修订文档，不表示已开始实现或允许生产切换。

<a id="agent-entry"></a>
## 执行入口与完成定义

一期不包含 IPC 帧、会话、进程池或远程中继；基础执行保留在当前进程。F08 提供开发入口，F17 独立验收，不等待二期或三期。

1. 读取本段、所选任务及引用小节；先核对工作区/源码/依赖证据，保留既有编辑，不 reset，不把旧结论当最新事实。
2. 在实际用户授权的阶段范围内自主推进，不重复索要已获授权；任务清单不授权自动启动并行 agent。
3. JSONL 权威与完整业务事实、独立可丢弃观测、单行事务、固定 ValueRef、单操作裁决和 projected 成功语义不因分期而削弱。
4. 每任务产出接口/实现/必要回归和 `docs/refactor-evidence/<ID>.md` 证据，记录真实命令、结果与未覆盖项后才能标 done；计划测试不等于已执行。
5. 只引用已验收依赖；前一期不依赖后一期。编号用于定位，执行顺序以依赖为准。实际实现、验收和生产切换分别记录在本期设计状态节。

<a id="task-index"></a>
## 本期任务与依赖

| 任务 | 工作 | 前置任务 |
| --- | --- | --- |
| [F01](#task-f01) | 记录现状与工作区基线 | 本期实际实施授权 |
| [F02](#task-f02) | 冻结基础数据契约与一期负载 | [F01](#task-f01) |
| [F03](#task-f03) | 实现单行有界事务格式 | [F02](#task-f02) |
| [F04](#task-f04) | 实现唯一 writer 与批量持久提交 | [F03](#task-f03) |
| [F05](#task-f05) | 实现锁、分段与保守恢复 | [F04](#task-f04) |
| [F06](#task-f06) | 实现派生索引、检查点与流式值读取 | [F05](#task-f05) |
| [F07](#task-f07) | 验收基础提交器容量 | [F06](#task-f06) |
| [F08](#task-f08) | 实现 reducer、命令裁决与基础开发入口 | [F07](#task-f07) |
| [F09](#task-f09) | 实现完整投影与统一公开命令入口 | [F08](#task-f08) |
| [F10](#task-f10) | 接通完整值生产、状态与受限消费 | [F06](#task-f06)、[F08](#task-f08) |
| [F11](#task-f11) | 实现进程内输入、授权与结果提交 | [F09](#task-f09)、[F10](#task-f10) |
| [F12](#task-f12) | 接通进程内 Driver、持久等待和恢复 | [F11](#task-f11) |
| [F13](#task-f13) | 交付独立有界观测存储 | [F02](#task-f02) |
| [F14](#task-f14) | 迁移分页、引用与客户端读取 | [F09](#task-f09)、[F12](#task-f12)、[F13](#task-f13) |
| [F15](#task-f15) | 交付 verify、backup、repair 与 rebuild 工具 | [F06](#task-f06)、[F09](#task-f09) |
| [F16](#task-f16) | 实现历史迁移与回退规则 | [F14](#task-f14)、[F15](#task-f15) |
| [F17](#task-f17) | 验收一期基础并交接二期 | [F16](#task-f16)、[F07](#task-f07) |

<a id="task-f01"></a>
### F01 记录现状与工作区基线

- **状态**：todo；证据：`docs/refactor-evidence/F01.md`。
- **前置输入**：无前置实现任务；需本期实际实施授权。
- **设计引用**：[一期 §3](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s3)、[一期 §12](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s12)。
- **改动范围**：工作区、engine/store/backend/RPC 与现有测试。

**执行步骤**：

1. 记录 git 状态和已有编辑，盘点权威写路径、API、父子/信号/重试及旧 PG 独立模式；保留所有既有工作。
2. 核实按文件组批与每 run 文件的 fsync 行为；区分本地路径与 FLOW_MAX_RUNS 所在 PG 路径，不用测试计数推断全部生产吞吐。
3. 记录旧日志配置预览、实际输入缺失/截断及不可证明完整的历史；运行相关原生回归，区分代码与环境失败。

**验收出口**：

- [ ] 基线命令可复现，脏文件、现有失败和历史缺失均有证据。
- [ ] 未把配置预览或推断值记为完整实际输入。

<!-- END F01 -->

<a id="task-f02"></a>
### F02 冻结基础数据契约与一期负载

- **状态**：todo；证据：`docs/refactor-evidence/F02.md`。
- **前置输入**：[F01](#task-f01)。
- **设计引用**：[一期 §5](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s5)、[一期 §6](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s6)、[一期 §7](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s7)、[一期 §8](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s8)、[一期 §10](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s10)、[一期 §15](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s15)。
- **改动范围**：journal/StoredValue/内部提交接口与验收 fixtures；不包含 IPC DTO。

**执行步骤**：

1. 固定单行事务包络、原始字节摘要、LF/重复键/版本规则、事务 lsn 与事件位置；固定实际输入、audit_seq、单操作授权、结果与等待的基础事件。
2. 固定 StoredValue 和连续 chunk 描述、总物化/解析/输出预算、projected/command.status、分页/引用 DTO 及错误码；声明凭证与读取/下载/备份授权。
3. 固定 R_max ≥ 1000、A_max、事务/s、编码 MB/s、大小分布、等待比例、突发量、p99、RSS、队列/写批、恢复与保留目标；fsync 微基准仅作参考，高提交率摊销阈值与低负载场景分开。
4. 准备合法/损坏/异身份/超限 fixtures；一期不定 X_max/W、socket 帧或会话协议。

**验收出口**：

- [ ] 每个数值有单位、负载、通过条件和估算/实测标注。
- [ ] fixtures 覆盖同身份异参、半行/错误摘要、固定值描述、已提交不可见和分页空页。
- [ ] 完整性故障边界与读取权限明确；没有后期协议前置要求。

<!-- END F02 -->

<a id="task-f03"></a>
### F03 实现单行有界事务格式

- **状态**：todo；证据：`docs/refactor-evidence/F03.md`。
- **前置输入**：[F02](#task-f02)。
- **设计引用**：[一期 §6.2](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s6-2)、[一期 §6.4](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s6-4)。
- **改动范围**：新增 flow-journal codec/流式读取。

**执行步骤**：

1. 一行一个事务，事件数组内原子归约；行/事务同一编码上限，起点 1 MiB，事务 lsn 单调，事件用 (lsn,event_index) 定位。
2. 校验原始 data 字节摘要、LF、schema/版本与重复键；固定上限内缓冲一行，不随全日志增长。

**验收出口**：

- [ ] 半行、缺 LF、非法/重复键、错误摘要、未知版本和边界大小正确拒绝。
- [ ] 多事件事务不能暴露部分事实，多事务读取 RSS 有界。

<!-- END F03 -->

<a id="task-f04"></a>
### F04 实现唯一 writer 与批量持久提交

- **状态**：todo；证据：`docs/refactor-evidence/F04.md`。
- **前置输入**：[F03](#task-f03)。
- **设计引用**：[一期 §6.2](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s6-2)、[一期 §6.3](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s6-3)、[一期 §8](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s8)。
- **改动范围**：JournalWriter、有界命令/审计队列、内部持久回执。

**执行步骤**：

1. 只有 writer 持有权威写句柄；批量完整事务共用 fsync，成功后推进 durable_lsn 并返回内部回执。
2. 按字节限制生产/排队/写批，控制与审计公平调度；实现确定性短写/同步错误注入。
3. 同步失败冻结追加、成功确认和新授权，重新读取确定边界后才恢复；不实现网络 ACK。

**验收出口**：

- [ ] 任意写入/fsync 故障均不提前成功，回执丢失可恢复原提交。
- [ ] 满队列和洪泛有界且控制/其他任务不饥饿，低负载及时提交。

<!-- END F04 -->

<a id="task-f05"></a>
### F05 实现锁、分段与保守恢复

- **状态**：todo；证据：`docs/refactor-evidence/F05.md`。
- **前置输入**：[F04](#task-f04)。
- **设计引用**：[一期 §6.4](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s6-4)、[一期 §11](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s11)。
- **改动范围**：排他锁、journal_id、段链、轮转与尾部恢复。

**执行步骤**：

1. 取得独占写权后校验数据集与段顺序，在事务行边界封口/轮转，包含文件与目录同步。
2. 完整有效事务恢复，歧义尾部保留并只读，缺段/封口损坏停止；输出离线 repair 定位。

**验收出口**：

- [ ] 双写者争锁、轮转各故障窗口、尾部半行/损坏均有确定结果。
- [ ] 不自动删歧义尾部，不声称单副本可检测全部历史回退。

<!-- END F05 -->

<a id="task-f06"></a>
### F06 实现派生索引、检查点与流式值读取

- **状态**：todo；证据：`docs/refactor-evidence/F06.md`。
- **前置输入**：[F05](#task-f05)。
- **设计引用**：[一期 §6.1](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s6-1)、[一期 §11](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s11)。
- **改动范围**：journal 索引/检查点、verify 核心、按值/提交边界读取。

**执行步骤**：

1. 按 run/dispatch 和 (output_id,chunk_index) 建立可重建定位，检查点记录版本/数据集/提交边界/状态摘要。
2. 实现临时写入→sync→替换→目录同步，以及快恢复与逐段完整 verify；读取固定上界、不拼全历史/大对象。

**验收出口**：

- [ ] 删除或损坏派生数据仍可由 JSONL 重建；缺段/游标领先不能被检查点掩盖。
- [ ] 大值固定描述和全部 chunk 闭包可验证，流式读与 verify RSS 有界。

<!-- END F06 -->

<a id="task-f07"></a>
### F07 验收基础提交器容量

- **状态**：todo；证据：`docs/refactor-evidence/F07.md`。
- **前置输入**：[F06](#task-f06)。
- **设计引用**：[一期 §15](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s15)。
- **改动范围**：真实 journal benchmark 与一期容量证据。

**执行步骤**：

1. 用受控生产者按 F02 的低提交率、持续混合和突发负载驱动真实 writer；校验实际事务/事件/字节与摘要。
2. 测持久吞吐、控制/审计 p99、队列/RSS、每批量及数据/目录 fsync 分布；预算内调参后同时复验。
3. 失败先定位实现/编码/磁盘/公平调度瓶颈，仍不达标再评审提交域；不预选分片或额外 WAL。

**验收出口**：

- [ ] 吞吐/p99/内存共同达标；高提交率摊销回归和低负载及时提交分别通过。
- [ ] 慢盘/满盘不无界积压；微基准不能独立决定通过，未声称已验收业务或 IPC。

<!-- END F07 -->

<a id="task-f08"></a>
### F08 实现 reducer、命令裁决与基础开发入口

- **状态**：todo；证据：`docs/refactor-evidence/F08.md`。
- **前置输入**：[F07](#task-f07)。
- **设计引用**：[一期 §4](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s4)、[一期 §5](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s5)、[一期 §6.3](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s6-3)、[一期 §7](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s7)。
- **改动范围**：engine/backend 的事实状态与内部命令门面。

**执行步骤**：

1. 实现版本化 reducer，统一定义/配置/请求/执行/等待/裁决事实；相同业务对象及幂等键的在途冲突串行裁决。
2. 状态只持 StoredValue，冲突校验基于已提交权威状态；内部函数返回持久回执，执行代码不能绕过 writer。
3. 提供显式选择基础 v2 数据目录/测试路径的开发入口；旧 PG 模式独立，未接通能力明确未就绪，不与旧权威写者混用。

**验收出口**：

- [ ] 同键同参附着原结果，异参拒绝；同批冲突不会读取旧状态同时通过。
- [ ] 纯日志重建不运行 JS/HTTP，未知事件版本拒绝，基础入口无 IPC 依赖。

<!-- END F08 -->

<a id="task-f09"></a>
### F09 实现完整投影与统一公开命令入口

- **状态**：todo；证据：`docs/refactor-evidence/F09.md`。
- **前置输入**：[F08](#task-f08)。
- **设计引用**：[一期 §10](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s10)。
- **改动范围**：store/backend/RPC 的 Projector、写入口与查询边界。

**执行步骤**：

1. 完整日志事务与 applied_lsn 在同一 SQLite 事务应用；同读事务固定 L，组合读取 journal 也截到 L。
2. 接通定义/config/run.start/signal/cancel/cron/webhook 的统一权威命令、请求指纹与触发去重。
3. 所有公开写成功等投影可见；提交后超时明确 COMMITTED_NOT_VISIBLE，command.status 从 journal 返回原提交；内部等待限时限量。

**验收出口**：

- [ ] 逐记录退出、批次限制、删除投影与并发查询不产生半笔状态。
- [ ] 暂停投影不提前成功或假 404，重试不重复写入；没有公开 durable 模式。

<!-- END F09 -->

<a id="task-f10"></a>
### F10 接通完整值生产、状态与受限消费

- **状态**：todo；证据：`docs/refactor-evidence/F10.md`。
- **前置输入**：[F06](#task-f06)、[F08](#task-f08)。
- **设计引用**：[一期 §6.1](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s6-1)、[一期 §8](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s8)。
- **改动范围**：exec/expr 与 StoredValue 流接口；先用受控响应流验证。

**执行步骤**：

1. InputPrepared、节点/最终输出与 Outcome 使用 Inline/Ref；HTTP 流增量计量，连续 chunk 校验后发布固定描述，不先完整读取再分块。
2. 扇出复制引用、汇合流式生成新对象；无可信长度、非法 JSON 文本回退、解码扩张均有预算，重读已存内容不能重发请求。
3. 一期 JS 留在受限进程内上下文，累计输入/heap/解析/输出限额并计入总 RSS；真实外部调用须待 F11 的内部授权接通。

**验收出口**：

- [ ] 缺块、异内容重复、长度/摘要/超限不得发布可用结果；没有清单树。
- [ ] 多前驱合计超限在运行代码前拒绝；流式路径内存不随对象总量增长。

<!-- END F10 -->

<a id="task-f11"></a>
### F11 实现进程内输入、授权与结果提交

- **状态**：todo；证据：`docs/refactor-evidence/F11.md`。
- **前置输入**：[F09](#task-f09)、[F10](#task-f10)。
- **设计引用**：[一期 §7](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s7)。
- **改动范围**：内部事实/授权/结果门面与现有节点适配器。

**执行步骤**：

1. 同一 dispatch 内准备并执行，实际输入持久后继续；基础 audit_seq 连续去重，完成结果核验封口和引用，纯审计回执不授予副作用。
2. authorize_operation 与取消串行核验单操作身份/实际请求指纹，Intent/Authorized 同事务持久后返回内部许可；适配器消费一次，再提交真实响应及 Outcome。
3. 取消胜出后结果不推进 DAG，允许的迟到证据保留；已授权无 Outcome 保留未知状态，失败/强杀缺口跨重试保留。

**验收出口**：

- [ ] 输入提交前后崩溃、同操作异参、授权半写/回执丢失及取消竞争正确处理。
- [ ] 外部成功无 Outcome 不盲重发，结果缺审计/块不能完成；不实现 IPC 发送窗口。

<!-- END F11 -->

<a id="task-f12"></a>
### F12 接通进程内 Driver、持久等待和恢复

- **状态**：todo；证据：`docs/refactor-evidence/F12.md`。
- **前置输入**：[F11](#task-f11)。
- **设计引用**：[一期 §9](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s9)。
- **改动范围**：driver/engine/child、等待索引、取消树。

**执行步骤**：

1. 由已提交状态准入 A_max 内任务；保留现有 exec/expr 调用方式，一次尝试内准备并执行，已有输入不重求值。
2. 父子创建/等待与子完成/唤醒分别原子提交；固定 wake_at、幂等 signal、卸载等待状态，有界取消树可重启续做。
3. 恢复按纯计算/已完成操作/未知操作分类；进程内 HTTP→script→delay→重启链路使用 F08 开发入口验证，不要求 executor 二进制。

**验收出口**：

- [ ] 父→子→孙在小活跃额度下可完成，不丢唤醒或重启延长 delay。
- [ ] 已完成 HTTP 不重发，大取消树/1000 在飞可有界恢复；不假设可强杀单个进程内任务。

<!-- END F12 -->

<a id="task-f13"></a>
### F13 交付独立有界观测存储

- **状态**：todo；证据：`docs/refactor-evidence/F13.md`。
- **前置输入**：[F02](#task-f02)。
- **设计引用**：[一期 §5.0](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s5-0)、[一期 §8](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s8)。
- **改动范围**：nodelog、独立观测段与有界读取。

**执行步骤**：

1. 从权威事实中拆出 console/stdout/stderr/引擎叙事；进程内直接写独立有界队列/观测段，支持轮转和整段丢弃。
2. 提供按 run/dispatch 的有界读取与丢弃计数；可跨重启读回，但不进入权威 verify/备份/完整性。二期再加 ObservabilityBatch 传输。

**验收出口**：

- [ ] 损坏/删除/写满观测存储不改变 journal、audit_seq、业务输出或完整性。
- [ ] 观测洪泛不阻塞 JS/权威提交，展示可识别观测丢弃。

<!-- END F13 -->

<a id="task-f14"></a>
### F14 迁移分页、引用与客户端读取

- **状态**：todo；证据：`docs/refactor-evidence/F14.md`。
- **前置输入**：[F09](#task-f09)、[F12](#task-f12)、[F13](#task-f13)。
- **设计引用**：[一期 §10.2](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s10-2)、[一期 §10.3](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s10-3)。
- **改动范围**：RPC/run_tail、web API/monitor、CLI。

**执行步骤**：

1. 实现固定上界分页、参数绑定游标、ValueRef 查询/流式下载与 v2 订阅；旧接口限额内形状不变，超限明确报错。
2. 订阅缓冲后取 timeline 并按 seq 对齐，有界缓存/溢出恢复，空页结束不当作 run 终态；console 来自独立观测存储。
3. 客户端识别 COMMITTED_NOT_VISIBLE 并查询原提交；下载与 run 权限同源，不把游标当授权。

**验收出口**：

- [ ] 并发写、多页/空页/断点、错游标、慢消费、大值与终态后迟到证据无重复漏项。
- [ ] 已提交超时不重复写，越权拒绝；服务端、浏览器与 CLI 内存均有界。

<!-- END F14 -->

<a id="task-f15"></a>
### F15 交付 verify、backup、repair 与 rebuild 工具

- **状态**：todo；证据：`docs/refactor-evidence/F15.md`。
- **前置输入**：[F06](#task-f06)、[F09](#task-f09)。
- **设计引用**：[一期 §11](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s11)、[一期 §6.4](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s6-4)。
- **改动范围**：CLI 与恢复/备份工具。

**执行步骤**：

1. 在确定提交边界复制完整段与活跃段前缀并校验；投影/检查点可省略，观测存储可丢弃。
2. repair 离线保留证据，列出候选前缀/影响，经显式运维确认写新目录；不改原日志或宣称丢失后缀从未被确认。
3. 接通完整 verify、派生索引/检查点及 SQLite rebuild 命令，复用 F06/F09；工具记录锁、目标目录与失败退出码，重建不执行用户代码或外部请求。

**验收出口**：

- [ ] 复制中轮转、缺段/失败、仅 JSONL 备份恢复均可复现。
- [ ] repair 未确认不写、确认后原证据不变；快恢复与完整 verify 范围分开。

<!-- END F15 -->

<a id="task-f16"></a>
### F16 实现历史迁移与回退规则

- **状态**：todo；证据：`docs/refactor-evidence/F16.md`。
- **前置输入**：[F14](#task-f14)、[F15](#task-f15)。
- **设计引用**：[一期 §12](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s12)。
- **改动范围**：离线 LegacyImport、差异报告与切换流程。

**执行步骤**：

1. 停止旧写者后备份并导出一致清单；在新目录可重入导入定义/元数据和旧事件，保留原身份/时间/seq。
2. 逐 run 报告实际输入缺失、截断/冲突与不可证明完整性；不得从配置或执行代码推断填充。
3. 仅由新 journal 重建比对；新写入后只允许兼容新日志的二进制回退或显式迁移，不直接恢复旧库。

**验收出口**：

- [ ] 中断重入、冲突、缺失报告与原备份保护通过，重建无任何外部调用。
- [ ] 切换结果可审查；不在本任务自动进行未经授权的生产切换。

<!-- END F16 -->

<a id="task-f17"></a>
### F17 验收一期基础并交接二期

- **状态**：todo；证据：`docs/refactor-evidence/F17.md`。
- **前置输入**：[F16](#task-f16)、[F07](#task-f07)。
- **设计引用**：[一期 §13](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s13)、[一期 §14](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s14)、[一期 §15](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s15)、[一期 §17](JSONL_MASTER_EXECUTOR_REFACTOR_PLAN.md#plan-s17)。
- **改动范围**：基础端到端/e2e/perf、打包、阶段验收报告。

**执行步骤**：

1. 逐行落实一期故障矩阵；从基础打包产物跑真实进程内 HTTP→脚本→等待→重启、迁移/重建、长历史客户端读取。
2. 分别验证 R_max/A_max、吞吐/p99/projected/RSS/恢复，核对实际内容，说明真实断电未覆盖项；保持旧 PG 独立回归。
3. 记录基础执行无子进程隔离的边界，整理可独立部署的配置/运维与回退结果；向二期交付 journal/reducer/StoredValue、内部提交接口、fixtures 和基线。

**验收出口**：

- [ ] 全部一期任务与故障/容量门槛有证据，不依赖 I/R 任务或 executor 二进制。
- [ ] 实现、验收、生产切换状态分别记录，未授权切换不执行。

<!-- END F17 -->

<a id="task-migration"></a>
## 旧任务编号迁移与覆盖

旧 P0–P7 是原单机方案工作包，已按职责拆分为 F/I 两期；下表不是执行依赖。R0–R3 编号保留并整体归三期，见 [三期任务](JSONL_MASTER_EXECUTOR_PHASE3_TASK_LIST.md)。

| 旧任务 | 新责任任务 |
| --- | --- |
| P0-01 | [F01](#task-f01) |
| P0-02 | [F02](#task-f02)、[I01](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md#task-i01) |
| P0-03a | [F02](#task-f02)、[I01](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md#task-i01) |
| P0-03b | [F02](#task-f02)、[I01](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md#task-i01) |
| P1-01 | [F03](#task-f03) |
| P1-02 | [F04](#task-f04) |
| P1-03 | [F05](#task-f05)、[I02](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md#task-i02) |
| P1-04 | [F06](#task-f06) |
| P1-05 | [F07](#task-f07) |
| P2-01 | [F08](#task-f08) |
| P2-02 | [F09](#task-f09) |
| P2-03 | [F09](#task-f09) |
| P3-01 | [I02](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md#task-i02) |
| P3-02 | [I03](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md#task-i03) |
| P3-03 | [I04](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md#task-i04) |
| P4-01 | [F11](#task-f11)、[I05](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md#task-i05) |
| P4-02 | [F10](#task-f10)、[I06](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md#task-i06) |
| P4-03 | [F11](#task-f11)、[I07](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md#task-i07) |
| P4-04 | [F11](#task-f11)、[I07](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md#task-i07) |
| P4-05 | [F13](#task-f13)、[I05](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md#task-i05) |
| P5-01 | [F12](#task-f12)、[I08](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md#task-i08) |
| P5-02 | [F12](#task-f12)、[I08](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md#task-i08) |
| P5-03 | [F12](#task-f12)、[I08](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md#task-i08) |
| P6-01 | [F14](#task-f14)、[I10](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md#task-i10) |
| P6-02 | [F15](#task-f15) |
| P6-03 | [F16](#task-f16) |
| P6-04 | [F08](#task-f08)、[F17](#task-f17)、[I09](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md#task-i09) |
| P7-01 | [F17](#task-f17)、[I10](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md#task-i10) |
| P7-02 | [F17](#task-f17)、[I10](JSONL_MASTER_EXECUTOR_PHASE2_TASK_LIST.md#task-i10) |

基础的业务保证在 F 任务验收，传输/进程保证在 I 任务验收；不因旧任务跨期而要求一期完成 IPC。修改编号、接口或范围时同步三份任务清单的依赖与设计引用。
