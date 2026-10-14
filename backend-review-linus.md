# 后端审查：数据结构、可靠性与复杂度

审查日期：2026-10-14。代码基准：`e58b9ab`。采用 `review-linus` 的五层方法：数据结构、特殊情况、复杂度、兼容性、实际需求。

## 核心判断

**品味评分：凑合。可靠性边界有好设计，运行状态的数据布局有明显问题。**

值得做的是修正状态所有权、限制资源、缩小事务变更面；不值得做的是在当前结构上继续叠加更多集群、队列和恢复抽象。

最严重的设计债不是文件太长，而是把调度状态、完整历史、审计回执和产品读模型装进同一棵可克隆对象树。然后用锁、整对象回滚、第二份 reducer 状态和全量投影去维持一致性。历史越多，正常请求越贵。

本次已将审查范围扩展到 `crates/` 下全部 **13 个 crate**：生产实现、入口、Cargo 配置、迁移、测试契约与性能工具。文件清单包含 157 个 Rust/TOML/SQL 文件，约 5 万行。下面按 crate 列出检查面；测试部分结合用例与断言核对，并执行默认 workspace 测试，不宣称每一行测试代码都经过同等深度的人工推演，也不宣称完成形式化验证或所有部署矩阵。

**扩大范围后的判断：先修数据结构和资源所有权，再谈平台扩展。** 部分代码写着“有界”“原子”“关闭可唤醒”，实现却只覆盖正常路径。注释不能充当不变量。新增 F09–F19，保留 F01–F08 的编号以便追踪；新增任务 R09–R16。

| Crate | 检查重点 | 判断 / 对应发现 |
|---|---|---|
| `flow-engine` | 图校验/索引、reducer、JS、secret、协议编解码、传输、平台进程入口 | 核心结构与关闭契约存在缺口；F04–F08、F09、F11、F12、F18 |
| `flow-journal` | codec、writer、队列、segment、值地址、checkpoint、离线维护/工具 | 持久提交与拒绝含糊尾部值得保留；历史布局拖累其容量；F06/F07 |
| `flow-backend` | 命令、调度、执行池、结果/副作用屏障、remote、legacy import、PG migration | 不变量跨层重复；共享上联仍有阻塞路径；F06/F07、F14–F17、F19 |
| `flow-agent` | 连接、分页对账、relay 预算、公平转发、spawn/回收、TLS | 裸额度和串行等待破坏故障恢复；F03、F13 |
| `flow-executor` | input/script/HTTP/harness、审计窗口、传输装配、观测桥、runtime | 输入输出边界不一致、日志入口无界；F01/F02、F10、F12 |
| `flow-rpc` | 认证、回执/可见性、订阅、触发器、下载、config/secrets、产品入口 | RPC 大小限制不能保护物化阶段；配置读改写不串行；F18/F19 |
| `flow-store` | SQLite 投影事务、游标、分页/读上限、重建 | 可重建定位正确；整 run JSON 更新单元错误；F07 |
| `flow-config` | 文件/env 层次、默认值、校验、merge/save | 固定 tmp、缺 sync 和并发更新契约；F18 |
| `flow-dto` | 状态词汇、版本/运行 DTO、序列化契约 | 保持薄 DTO；清理状态重复定义，见 R07 |
| `flow-cli` | HTTP/WS client、错误/退出码、workflow import/export、receipt 恢复、config | 没有新增独立 P1；版本字段风险核对后排除：命令结果的 version 是数字 |
| `flow-test-support` | 子进程、ready 标记、端口重试、executor 定位、PG 测试辅助 | 基础设施保持薄；测试结果须区分平台、feature 与 ignored case |
| `backend-e2e` | 真实 server/WS/HTTP、CRUD、执行、信号、子流程、触发/订阅、crash | 默认用例全部通过；大 input 节点、保留键、阻塞关闭等组合仍漏测 |
| `backend-perf` | 并发 harness、throughput/read/subscribe/recovery、计数与分位数 | 是可用的基线工具；不足以证明历史增长、日志洪泛和 remote 故障下的资源上界，见 R16 |

## Findings：保留追踪编号，逐项标注优先级

### F01 / P1：harness 的超时没有覆盖 stdin 写入，存在管道死锁

证据：`crates/flow-executor/src/task.rs:1228` 先等待 `stdin.write_all(prompt)`，到 `:1235` 才对 `wait_with_output()` 开始 timeout。

触发条件：子进程不读 stdin，大 prompt 填满管道；或者子进程先输出大量 stdout/stderr 再读 stdin，输出管道填满，而父进程仍在写 stdin、尚未读取输出。

后果：配置的 timeout_ms 不覆盖这段等待。执行器心跳仍可正常发送，不能依赖心跳超时释放卡住的槽位。

改进：从进程执行开始建立统一 deadline；并发写 stdin、读 stdout/stderr、等待退出；timeout 和取消覆盖整个操作；所有路径明确 kill/reap。授权后的超时继续遵守 uncertain 契约，不能自动重跑。

验证状态：静态路径确认；本次未执行完整 harness 黑盒复现。

### F02 / P1：harness 输出先无界收集，事后才应用值大小限制

证据：`crates/flow-executor/src/task.rs:1235` 使用 `wait_with_output()`；`:1266` 才对 stdout 调用 `store_bytes()`；stderr 到 `:1282` 才截取尾部。

后果：stdout/stderr 在运行期间持续积累在内存中。后面的值大小限制与 stderr_tail 不能保护捕获阶段。进程隔离限制了故障传播方式，但没有给同机内存资源提供上限。

改进：stdout 流式、有界捕获；stderr 使用固定容量尾部缓冲。超过完整结果捕获上限时停止执行并保留“不完整结果”的事实，不能把截断内容当完整成功结果。磁盘 spool 也必须有总量上限。

验证状态：静态路径确认；本次未进行内存压力测试。

### F03 / P1：agent 上联预算在发送失败路径泄漏，重连不会重置

证据：`crates/flow-agent/src/runtime.rs:503` 从队列拿出 `(message, budgeted_bytes)`；`:519` 的 `queue_uplink()` 失败后直接 return，跳过 `:524` 的 release。`:516` 的包装错误返回也没有释放预算。生产 `run_agent()` 在循环外创建 pipeline，跨重连复用其 budget。

后果：出队消息已丢失，但字节额度仍计为占用。反复故障会耗尽预算，wrapper 停在 acquire；agent 可以完成重连但无法继续正常转发。

根因：资源所有权是裸 `u64` 加手工 release，控制流增加一个 return 就会漏账。

改进：队列元素持有 RAII budget permit；失败、丢弃、取消自动归还。优先用 Tokio `Semaphore` / `OwnedSemaphorePermit`，不要再补若干 release 分支。

验证状态：静态路径确认；尚未执行反复重连的进程级复现。

### F04 / P1：run 状态按最后一个节点事件覆盖，无法表示多个并行分支

证据：`crates/flow-engine/src/journal_state.rs:732`，任意 DispatchStarted 写 `run.status = Running`；`:897` 按本次 WaitRegistered 的类型决定整个 run 状态；人工裁决结束也直接写 Running。

触发条件：A 分支处于 uncertain wait，B 分支启动或进入普通等待；或 A/B 均 uncertain，只裁决其中一个。

后果：还有节点需要人工介入，run 却变为 running，影响状态筛选、统计和运维判断。不是“是否继续运行其他分支”的问题，而是聚合状态失真。

改进：终态由 run 事实决定；非终态的 awaiting_resume 由所有节点的未决等待聚合。起步用一个统一的纯函数扫描节点，确认成本后再加计数；不要让每个分支各写一份聚合状态规则。

验证状态：临时 reducer 探针复现。预置一个 uncertain 节点，再应用另一节点 DispatchStarted，原等待保留、run 状态变为 Running。此验证覆盖 reducer 行为，不宣称已经完成故障 E2E。

### F05 / P2：图声称不可变，公共字段却允许修改，缓存可返回错误或 panic

证据：`crates/flow-engine/src/model.rs:448` 的 nodes/edges 为 pub，adj 为 OnceLock；`:493` 的注释承认写字段后索引会过期；`:528` 直接按旧下标访问 edges。

复现：先 `incoming_count("e")` 建索引，再 `edges.clear()`；count 仍为 1，`incoming("e")` 越界 panic。重命名节点也会留下过期的 by_id。

当前生产调用未发现修改这些字段，因此这是确定的公共数据结构缺陷，不等同于已经发生线上崩溃。

改进：短期提供明确修改入口并失效缓存；长期分为可编辑 Definition DTO 与私有不可变 CompiledDefinition，校验编译后生成索引。不能靠“现在没人改”维持不变量。

验证状态：临时 Rust 探针已复现旧索引与越界，panic 由 catch_unwind 捕获。

### F06 / P1（容量设计）：完整历史常驻两份内存，R_MAX 没有限制这一部分

证据：`crates/flow-engine/src/journal_state.rs:235` 的 State 持有所有 runs；Run 持有全部 nodes/attempts；Attempt 的 audit_digests/audit_lsns 随每条审计增长。`crates/flow-backend/src/journal.rs:261` 将整个 committed.state 克隆给 projector，并独立继续 apply。`journal_commands.rs:558` 的 R_MAX 仅统计非终态 run。

后果：总历史和审计量决定常驻内存，不是当前工作量。终态运行结束并不释放其完整历史。读一个 run 的若干路径还调用 `state()` 深克隆所有历史。

补充：`journal.rs:718` 序列化整个 State 后收集所有 ValueRef；终态 runs 仍在 State，因此其引用仍进入 live 集。`flow-journal/src/value.rs:165` 对 published 快照设置 8 MiB 限制。历史引用增长到超过该限制会使 checkpoint 失败，并回退到下次启动回放；不能把注释中的“历史逐出”视为已实现。

改进：区分活跃执行状态、终态摘要和按需历史。保留 journal 权威与历史可查询契约；把 audit 回执/历史索引放入可重建磁盘索引，不能随意删除去重信息。projector 只持有它需要的折叠状态，而不是第二棵完整执行历史。

验证状态：存储和所有权路径确认；具体可承载历史量、RSS 曲线与 checkpoint 失败规模需要性能实测。

### F07 / P1（性能设计）：每条事件克隆并重写完整 run，累积成本呈二次增长

证据：`journal_state.rs:380` 的回滚单元是整个 Run；`journal.rs:529` 先 State.check，再在 `:552` State.apply，projector 还做一次 apply；`journal.rs:776` 任何 run-scoped 事件都选择 run 投影，`:818` 序列化完整 Run。

后果：每追加一条审计，先克隆越来越大的 attempts/audit maps，再把越来越大的 JSON 写入 SQLite。单次 run 若累积 k 条审计，累积复制和编码成本可达 O(k²)。投影 batch 限制按原始 journal 字节计算，不能约束展开后的完整 run 行字节。

改进：事务 staging 缩小到修改的节点/attempt/字段；保持“验证失败完全不可见”的原子性。投影拆成 run 摘要、节点状态和审计索引，审计新增不重写整份 run；同批次中相同实体的更新可在保持游标语义的前提下合并。不要直接取消 check，也不要提交后忽略 reducer 错误。

验证状态：算法和调用路径确认；未报告未经测量的毫秒值或吞吐提升。

### F08 / P2：Node/Wait 的结构允许大量非法组合，恢复逻辑靠特殊分支修补

证据：`journal_state.rs:160` 的 Node 同时持有 status、wait、operation、prepared、output、error、attempts；`:205` 的 Wait 允许任意 kind 加任意 wake_at/child_run_id。`journal_driver.rs:456` 需要额外检查 Waiting 却缺少 wait 的组合。

后果：真正约束散落在 reducer、driver、dispatcher、executor 中；每增加一个等待或恢复状态都增加交叉条件。

改进：先将 Wait 改为内部 tagged enum：Delay/Signal/Child/Retry/Uncertain 各携带必要字段。把当前执行状态与历史账本分开。用兼容序列化适配现有 journal/checkpoint，不进行一次性全状态机重写。

验证状态：设计债，不作为独立已发生的业务 bug。

### F09 / P1：传输键不是一一映射，合法节点名可以撞上协议保留键

证据：`crates/flow-engine/src/execution_protocol/transfer_key.rs:35`：RunInput 编码为 `in:run-input`，NodeInput(id) 编码为 `in:{id}`，PreparedParams 编码为 `in:prepared-params`。`model.rs` 没有拒绝这两个节点名；transfer_key 的测试注释却声称“定义校验另有节点名约束”。

触发：前驱名为 `run-input`，run 输入和前驱输出均走引用/分块传输，且内容不同。`dispatch.rs:1259` 使用同一 transfer_id 发送两份值，`flow-executor/src/transfer.rs:49` 按该字符串聚合，重复偏移的异内容被拒。`prepared-params` 在需要重传准备参数时同样冲突。小 Inline 值可能绕过这个问题，不能用小值 happy flow 证明安全。

另一个边界：接收端限制 transfer_id 长度为 256，图校验却不限制 node id 长度；长节点名在发布时通过，到大值传输才失败。

改进：传输标识应表达值的种类与节点身份。短期对新定义校验保留名及 wire 长度；旧定义必须给出明确兼容策略，不能悄悄改名。长期改为不可混淆的 tagged key，并通过协议版本迁移。

验证：临时探针证明该图通过 validate，且 NodeInput 的编码与 RunInput 完全相同、parse 无法往返。未把该探针称为完整大值 E2E。

### F10 / P1：input 节点把超过 Inline 上限的合法结果塞进控制帧

证据：`crates/flow-executor/src/task.rs:660` 直接构造 `StoredValue::Inline(Value::Object(resolved))`，没有调用现有分块存储路径。run 输入允许 8 MiB，StoredValue Inline 的上限是 64 KiB。

复现：start → input(fields=[blob]) → end，输入 blob 为 100 KiB。run.start 接受；运行最终 Failed，错误为 `protocol frame malformed: postcard: Serde Deserialization Error`。这是业务允许的输入引发协议断链，而不是正常的用户参数错误。

改进：所有节点输出经过同一个有界 store_value 边界：小值 Inline，大值 ValueChunk/ValuePublished + Ref。默认字段值与 signal 路径也要对齐，禁止各节点手工决定 Inline。

验证：真实 executor 子进程 + JournalBackend 的临时集成探针已复现，不是纯序列化单测。

### F11 / P1：FrameTransport.close 无法中断正在阻塞的写入或入站投递

证据：`crates/flow-engine/src/execution_protocol/transport.rs:390` 的 write_all、`:395` 的 flush 不监听 terminate；`:325` 的 `events.send(...).await` 也不监听取消。close 只设 token、替换 sender，join 无 deadline。写循环在等待队列时监听取消，已经进入 I/O 后就失效。

后果：对端保持连接却停读时，close 后 join 仍永久等待；入站队列满时，读任务也可能无法退出。丢弃 JoinHandle 不会 abort 任务，缺少 Drop 回收进一步延长连接与任务生命周期。`flush_and_close` 有超时后 abort，是部分正确实现，但其他 close/join 路径没有同一保证。

改进：取消覆盖 read/write/flush/入站投递所有等待点。部分帧已写后取消，应直接销毁会话，不能复用该流；任务由结构化生命周期持有，关闭宽限后 abort 并等待回收。

验证：duplex 容量 1 字节、对端停读，发送 Ready 后 close，100 ms 内 join 未返回，临时探针确认。未测真实 TCP 的不同缓冲容量。

### F12 / P1：日志“可丢弃”的入口是无界队列，预算被 usize::MAX 关闭

证据：`crates/flow-executor/src/observe.rs:66` 创建 unbounded_channel；`:67` 使用 `LogBudget::new(usize::MAX)`。`flow-engine/src/nodelog.rs:110` 的 hard_max 为 max(max, 100000)，因此这里软硬预算都近似无限。后端批次发送的 try_send 只限制下游，不能限制这个入口。

后果：JS 大量 console.info/error 时，Rust 侧队列可持续增长；JS heap 限制覆盖不了 Rust 日志缓冲。单线程/转发滞后或 CPU 竞争尤其容易放大积压。日志不能反压执行，不意味着可以无限占内存。

附带生命周期缺口：ObservationClose 的 timeout 消费 JoinHandle，超时后任务被 detach，没有 abort；稀疏日志只有满批/关闭时才发，长任务的少量日志无法及时显示。现有测试还明确断言“容量未满时不得提前发批”，没有验证实时观测需求。

改进：入口使用按条数与字节双重限制的非阻塞队列，满即计数丢弃；有限预算；按短周期 flush；close 超时明确终止转发任务。观测失败不能拆除审计/结果会话。

验证：静态所有权/排队路径确认；没有给出未经实测的 RSS 数值。

### F13 / P1：agent 的对账和 executor 启动等待阻塞整个会话生命周期

证据：`crates/flow-agent/src/runtime.rs:86` 在 connect 的 15 秒 timeout 之外直接 await resume；`:315` 等 ResumeReply 无 deadline、无 shutdown 分支；页号一致性只由 debug_assert 校验。serve 中 BindExecutor 分支同步等待 pool.bind_executor 的启动握手。

触发：master 发送 Welcome 后保持连接但不回复 Resume；或者一个新 executor 启动迟缓。前者使 agent 停在 Reconciling，无法正常响应 shutdown；后者让该上联暂停心跳和其他 executor 帧，启动故障可以牵连已有任务。

改进：每页及整轮对账都有 deadline、取消和正式页号/dispatch 集校验；启动任务移出共享会话循环，用完成事件回送，限制并行启动数。取消/超时后的执行事实仍由 master 对账裁决。

验证：静态控制流确认；没有执行“静默 master + 多 executor”的完整网络故障复现。

### F14 / P1（准入）+ P2（生命周期）：remote 握手没有期限，listener 不随 shutdown 退出

证据：`crates/flow-backend/src/execution/remote.rs:291` 的 accept 循环无限运行，没有 shutdown select，也没有保存 JoinHandle；`:311` 的 TLS accept 无 timeout。WSS accept_hdr 同样缺少期限。active 计数限制等待握手的连接，但 inject_stream 启动 session 后即返回，不能将该计数描述为所有活跃 session 的上限。

后果：未认证客户端占满 256 个等待握手名额即可阻止合法 agent 接入。manager.shutdown 取消 session，却不关闭 listener；后台任务强持 manager/backend，端口和资源生命周期不受公开关闭契约控制。启动第二个 listener 失败时，已启动的第一个也缺少统一回滚。

改进：用握手 permit + deadline；单独定义已认证 agent/session 上限与替换规则。manager 持有 listener/session task 集，shutdown 取消并 join；启动要么全部成功，要么回收已经创建的资源。

验证：静态确认；未执行 256 连接压力测试。P1 指实际配置为对外监听时的准入风险。

### F15 / P1：master 上联把单个 dispatch 的背压扩散给所有 executor

证据：`crates/flow-backend/src/execution/remote.rs:683` 在 run_session 的共享入站循环中等待 `routing.events.send(...).await`。目标 runner 正在等磁盘提交或下游操作时，事件队列能填满。

后果：一个 dispatch 阻塞后，其他任务的审计、Cancel、Resume、Heartbeat 以及会话 watchdog 分支都无法继续处理。已有“出站饱和仍处理入站”的测试不能覆盖这个相反方向的堵点。

改进：每 dispatch 保留有限入站积压并公平投递；控制事件保持可处理；超额时显式失效相应会话并进入恢复裁决。不能通过无界队列掩盖慢消费者，也不能丢弃正确性帧。

验证：静态 await 路径确认，故障矩阵需新增一慢多快的 remote 场景。

### F16 / P1：legacy WAL 恢复修改了被用作原始证据的副本，重复导入失败

证据：`crates/flow-backend/src/journal_import.rs:113` 要求既有备份逐字节匹配源清单；`:254` 却直接在同一 `legacy-source` 数据库副本执行 `wal_checkpoint(TRUNCATE)`。数据库/WAL 字节变了，下一次 copy_evidence 自己拒绝自己的备份。

复现：构造带未 checkpoint WAL 的停写快照，首次 import 成功，再导入相同未改变的源，返回 `existing legacy backup differs`。源文件未被修改，但目标中宣称保全的原始证据已被修改；正常重试/中断恢复契约失效。

改进：legacy-source 作为不可变原始证据；恢复/查询另用工作副本，或生成独立派生 SQLite 快照。分别记录原始清单和派生快照身份，不用同一个目录承担两种角色。

验证：临时 SQLx WAL 集成探针已复现两次导入的差异。

### F17 / P1：导入永久进度错误依赖 8192 条命令回执窗口

证据：`journal_import.rs:145` 的 baseline 只用 command_status 判断是否完成，raw fragment 路径也如此；`journal_state.rs:36` 的 COMMAND_WINDOW 为 8192，超过后淘汰到半窗口；`:647` 的 LegacyImport 又禁止重复 key。state.legacy 已保存永久 key，导入器却不使用它恢复进度。

触发：导入足够多的行/片段后中断，最早 source/行回执被淘汰；或者完成导入后产生足够多的新命令，再请求重入。

后果：再次提交 source 或已导入行时撞上 duplicate legacy baseline key，无法恢复。短测试通过只证明回执尚未淘汰。

改进：以 durable legacy key + source identity + data fingerprint 判断进度和一致性；有界 RPC 重试回执只负责短期请求响应，不承担永久迁移账本。避免“为修重入把所有回执永久留内存”。

验证：窗口淘汰与重复拒绝路径确认；尚未执行超过 8192 行的导入探针。

### F18 / P2：配置更新有丢更新竞态；原子 rename 被误当成持久写入

证据：`crates/flow-rpc/src/journal_v2.rs:1003` 分别锁住 read 与最终 write，中间 save 不在同一更新锁内。`flow-config/src/lib.rs:518` 使用固定 `toml.tmp`；`flow-engine/src/secrets_store.rs:156` 也使用固定 tmp。两者没有文件 sync_all 和父目录 sync。

后果：多线程并发 config.update 可以各读旧值，不同分区更新互相覆盖；固定临时文件可互相覆盖或 rename 失败；内存配置和磁盘版本也可能不一致。rename 只解决可见性原子，不证明断电后持久。secret store 的内存锁防本对象并发，但不能补足缺失的持久化步骤。

配置还可能包含 agent token，保存时采用默认权限；新文件不能依赖调用环境的 umask 给凭证保密。

改进：串行化完整 read/merge/validate/save/publish 事务；唯一 create_new 临时文件、明确私有权限、文件 sync、rename、父目录 sync；抽取小型共用落盘函数，显式处理平台差异及多进程所有权。不能持 std RwLock 跨 async await。

验证：静态确认；没有执行断电测试，不把缺 sync 等同于每次进程崩溃都会丢文件。

### F19 / P2：读模型按字段限制大小，却没有限制一次请求的物化总量

证据：`crates/flow-backend/src/journal_views.rs:407` 深克隆全 State，再逐节点同步 materialize prepared.params/output，每项最多 8 MiB。list_versions 在 `:91` 物化所有版本定义，而 RPC workflow.versions 最终只返回元数据。`journal_v2.rs:1247` 的 9 MiB response cap 是后置限制。

后果：大量节点/版本让一次普通读请求构造很大的中间对象并阻塞 runtime worker；最后超响应限制，也已经付出 I/O、复制和内存成本。timeline 每节点还扫描所有 runs 查 child，成本为节点数 × 历史 run 数。

改进：版本列表直接读取元数据、分页；timeline 返回摘要与 ValueRef，按需取值；请求级总预算与并发物化上限；同步磁盘读取放 spawn_blocking；父子关系建索引。保持已有客户端契约时先提供兼容字段/显式过大响应，不静默截断。

验证：路径/复杂度确认，RSS 与尾延迟由 R16 实测。

## 核心数据结构的建议形状

```text
Definition DTO                 可编辑、可序列化，无运行缓存
  └─ validate + compile
CompiledDefinition             不可变、共享，node/edge 索引一次构建

ExecutionState
  ├─ active_runs               当前调度、等待、未决 operation
  ├─ workflow_versions         不可变定义引用
  └─ bounded receipts          有明确契约的请求回执窗口

Rebuildable disk indexes
  ├─ run summaries / node views
  ├─ terminal history
  └─ audit identity → digest + commit cursor

Journal                        持久事实权威，兼容旧事件并支持重建
```

这个方向不要求换 BTreeMap 为 HashMap；容器更换不能修正对象生命周期。也不要求先引入通用 actor 框架：第一步应是消除整 State 读克隆、收紧变更面和定义字段所有权。

## KISS / DRY / YAGNI 判断

| 面 | 判断 | 具体改法 |
|---|---|---|
| KISS | 不满足：历史、执行和产品视图耦合，业务变更需穿过多层回滚与全量序列化 | 按生命周期拆数据，增量变更和增量投影 |
| DRY | 不满足：flow-dto 与 journal_state 各定义 RunStatus；NodeType enum、TABLE、ALL、descriptor 下标共同维护清单 | 共享状态词汇；enum match 做穷尽检查，或一处生成清单，避免引入泛化 registry |
| YAGNI | PostgreSQL membership 对独立 journal 做租约/注册，却不提供 run ownership 或复制；它还通过 WriteAdmission 影响写权限 | 明确它到底服务哪个已存在部署需求；无需求停止扩展，有需求保留 feature 隔离并明确 PG 故障行为 |
| 缓存 | run_id 定义缓存上限 128，而活跃 run 上限 1000；同版本多个 run 重复缓存，容量超限任意淘汰 | 按不可变版本或受验证 digest 共享 CompiledDefinition；用热点规模实测决定是否需要 LRU |
| 模块边界 | flow-engine 同时承载领域模型、网络/进程协议、JS、secret 文件存储 | 先理清依赖与状态所有权；只有实际复用或依赖成本证明必要时才拆 crate |
| 标识类型 | TransferKey 看起来是 enum，编码后却把两个不同身份合并；类型包装没有保护不变量 | 先保证编码唯一和边界一致，再讨论包装形式；F09 |
| 生命周期 | 上联 budget、传输 task、listener、日志 task 的回收各自手写 | 用 permit 和被持有的 task 集表达所有权，不再靠散落 release/cancel；F03/F11–F15 |
| 迁移账本 | 原始证据、可恢复工作副本、短期请求回执混用 | 拆开不可变证据、派生快照与永久迁移进度；F16/F17 |

没有用户部署证据时，不能直接把 remote 判为“过度设计”：用户已经明确要求 master + agent + executor。该链路需要身份、背压、提交屏障和断线对账。真正多余的是重复记账、手工资源释放和为历史扫描增加的补丁。

## 成熟组件与替代方案

| 领域 | 建议 | 兼容性与实际限制 |
|---|---|---|
| 字节准入 | 使用 Tokio Semaphore + OwnedSemaphorePermit | permit 随对象所有权释放；保留公平调度策略，但去掉手工额度生命周期。参考 [Tokio Semaphore](https://docs.rs/tokio/latest/tokio/sync/struct.Semaphore.html) |
| 参数 schema | 评估 jsonschema 的可复用 validator | 发布前只校验结构与非模板字段；模板展开后校验有效参数。不能把 `${...}` 直接按静态整数校验。参考 [jsonschema](https://docs.rs/jsonschema/latest/jsonschema/) |
| 日志持久化 | 做 SQLite 事件表 + 原子事务的对照实验 | SQLite 已提供崩溃原子提交及 WAL。事件表可以仍是逻辑权威、JSONL 可成为导出格式；但改变当前 JSONL 权威契约，是独立 ADR 和迁移项目。参考 [Atomic Commit](https://www.sqlite.org/atomiccommit.html)、[WAL](https://www.sqlite.org/wal.html) |
| 服务端 durable workflow | 将 Temporal 作为独立产品路线候选 | 其 Event History/replay 可复用成熟 durable execution 方案；必须另验 Rust 接入、动态图、人工对账、部署成本。不能把桌面离线产品直接换成远程依赖，也不能把事件重放等同于外部副作用 exactly-once。参考 [Temporal Workflows](https://docs.temporal.io/workflows)、[Tasks](https://docs.temporal.io/tasks) |
| 网络与执行 | 保留现有 rustls、reqwest、rquickjs、postcard | 它们已经避免重复实现 TLS、HTTP、JS、序列化。不要为“标准化”把现有单流协议盲目换成另一个框架，重连裁决仍是业务责任 |

任务生命周期优先复用当前已有 Tokio/tokio-util 的取消、信号量和任务管理能力；日志入口使用已有有界 mpsc。原子文件落盘先做一个语义明确的小 helper，不为十几行文件操作增加新的存储框架。分位数工具在当前短期基线中足够，只有长时间采样内存成为实测问题时才换有界直方图。

结论：有成熟包的基础机制应复用；副作用授权与不确定结果裁决是本产品语义，不能指望一个包替你消除。JSONL 系统已自行实现 segment、sync、index、checkpoint、recovery，继续维护它必须有明确产品价值，例如日志独立可检验与保全；仅为了“可靠持久化”不足以证明这笔成本必要。

## 应保留的设计

- master 单一裁决者、executor 独立执行用户代码，agent 不制造持久确认。
- OperationIntent/Authorized 原子记录后才放行副作用；结果提交前有审计屏障。
- 不确定副作用不透明自动重试；已有 outcome 可以用于恢复派生输出。
- journal 与数据目录排他锁；投影可重建；公开回执区分 committed 与 visible。
- pending commit 由 coordinator 持有，请求取消不使冲突重试绕过先前提交。
- 有界帧、队列、审计窗口、大小限制以及故障测试。修缺口，不应删掉这些真实边界。

## 可执行 task list

以下是审查后的修复/研究任务，没有在本次自动实施。

### R01 / P1：统一 harness deadline 与有界进程 I/O

- 目标与原因：修 F01/F02，防管道死锁和无界输出。
- 范围：`flow-executor/src/task.rs`、`flow-engine/src/exec.rs`、harness E2E fixture。
- 方法：建立全程 deadline；并发输入/输出/退出；stdout 有界捕获、stderr ring；取消与超限统一回收，保留 uncertain。
- 测试：不读 stdin、大 prompt、先大量输出再读输入、stdout/stderr 洪泛、退出/取消竞态。
- 验收：所有 case 在指定 deadline 加回收宽限内退出；内存有上限；child 被回收；不提交截断成功结果。
- 约束：不自动重发已授权操作，不更改已有正常输出格式。

### R02 / P1：把上联预算改成所有权 permit

- 目标与原因：修 F03，消除漏 release 的控制流。
- 范围：`flow-agent/src/relay.rs`、`runtime.rs` 和重连测试。
- 方法：有界 Semaphore；消息携带 permit；由 Drop 释放，明确交给 transport 时的预算边界。
- 测试：发送失败、包装失败、队列关闭、任务取消、反复重连、不同帧大小争抢。
- 验收：任意退出路径释放正确额度；连续故障后新消息仍可发送；预算不越界。
- 约束：保留背压和有界队列，不用无界通道消除等待。

### R03 / P1：统一非终态 run 的聚合状态

- 目标与原因：修 F04，人工介入状态不能被并行事件掩盖。
- 范围：`flow-engine/src/journal_state.rs`、run 视图/统计测试。
- 方法：统一聚合函数；节点状态转移后重算；checkpoint 恢复按明确兼容策略校验/规范化旧聚合状态。
- 测试：uncertain + running、uncertain + delay、两个 uncertain 只裁决一个、最后一个裁决完成、取消终态。
- 验收：只要仍有未决 uncertain，非终态 run 就是 awaiting_resume；读模型与 replay 一致。
- 约束：不禁止独立分支继续执行；保持 wire 状态名和终态语义。

### R04 / P2：建立不可变 CompiledDefinition

- 目标与原因：修 F05，让缓存不变量由类型与接口保证。
- 范围：`flow-engine/src/model.rs`、backend 定义缓存及 executor 消费点。
- 方法：先处理现有公共字段的兼容；编辑 DTO 进入校验编译边界；索引只绑定不可变图；共享不可变版本。
- 测试：字段/图修改后重编译、节点重命名、删边、克隆、序列化 checksum、旧定义读取。
- 验收：不出现过期索引；序列化定义字节和版本契约保持；API 变更列出迁移清单。
- 约束：不能直接私有化公共字段而忽略依赖方，不引入通用插件注册系统。

### R05 / P1：拆分活跃状态与历史索引

- 目标与原因：修 F06，内存受活跃工作量约束。
- 范围：State、JournalBackend、projection schema、replay/checkpoint、history 与 remote 对账读取。
- 方法：先用 inspect 替代生产全 State 克隆；定义终态摘要与审计磁盘索引；逐步迁出历史，保留可重建关系；维护活跃 run/子 run 索引。
- 测试：历史从 1k 增到 100k、晚到审计、旧结果重提、投影删除重建、checkpoint 边界、master 重启。
- 验收：相同活跃规模下 RSS 不再线性跟随完整历史账本；历史查询与去重仍正确；checkpoint 不因累积历史引用失效。
- 约束：不得直接删 terminal runs 或 audit 回执，必须先保证按需读取和迟到消息裁决。

### R06 / P1：缩小事务 staging 与投影更新单元

- 目标与原因：修 F07，避免每条审计全量克隆和序列化。
- 范围：`journal_state.rs`、`journal.rs`、`flow-store/src/projection.rs`。
- 方法：节点/attempt 级 staged change；验证后返回可应用变更；摘要、节点、审计分表或等价增量布局；限制展开后批次大小。
- 测试：事务后续事件非法时原子回滚；check/apply/replay 等价；长审计 run；投影暂停恢复；committed-not-visible。
- 验收：单条审计成本不随完整 run 历史线性增加；回滚、持久确认和可见性屏障保持。
- 约束：不删 reducer 校验，不引入提交后可见的部分状态，不破坏 commit cursor。

### R07 / P2：收紧 Wait 与共享状态词汇

- 目标与原因：修 F08 和 DRY 重复，不再靠组合 Option 表示等待。
- 范围：engine reducer、driver、executor、DTO 和 schema 校验。
- 方法：Wait 内部 enum，兼容旧 JSON；RunStatus 共享或唯一映射；清理 NodeType 多重清单；小范围评估 jsonschema。
- 测试：每种 Wait 必要字段、非法组合拒绝、历史反序列化、状态字符串对拍、模板参数发布/展开校验。
- 验收：内部无法构造缺 deadline 的 Delay/Retry 或缺子 run 的 Child；兼容 fixture 通过。
- 约束：不一次性重写所有状态机，不把 UI 字段强行塞入执行状态。

### R08 / P2：持久化与集群路线 ADR

- 目标与原因：明确自研边界，停止凭“未来需要”增加平台复杂度。
- 范围：设计文档、backend-perf 对照原型、journal-cluster feature 的需求与故障契约。
- 方法：量化 JSONL 特有价值；SQLite 事件事务对照；明确 membership 的实际用户场景；服务端路线另做 Temporal 适配与部署评估。
- 测试：相同业务/故障/历史规模的恢复、吞吐、尾延迟和资源对比；迁移读写契约验证。
- 验收：ADR 说明保留/替换的证据、维护成本、迁移路径、回退方案与产品适用范围。
- 约束：本任务不替换生产存储，不宣称 membership 已提供 HA，不改变桌面离线能力。

### R09 / P1：统一传输键与图发布边界

- 目标与原因：修 F09，合法定义不能到执行时才触发身份冲突。
- 范围：TransferKey、Definition 校验、dispatch/task、已有定义兼容检查。
- 方法：先限定新定义的保留名与编码后长度；梳理旧定义；再以协议版本迁移到唯一编码的 tagged key。
- 测试：两个保留名、253/254 字节附近节点名、Unicode 字节长度、Inline/Ref 组合、重试的 prepared params、IPC/remote。
- 验收：每个合法逻辑 key 编码唯一且可往返；发布与接收端边界一致；旧定义迁移/拒绝行为明确。
- 约束：不偷偷重命名历史节点，不在原协议版本中静默改变 wire 格式。

### R10 / P1：所有节点输出走同一有界存储入口

- 目标与原因：修 F10，input 大值与 script 大值遵守相同契约。
- 范围：executor task/transfer、input/default/signal 的集成测试。
- 方法：input resolved 对象经现有 store_value，超过 Inline 阈值先提交分块审计再返回 Ref；检查其他手工 Inline 构造点。
- 测试：64 KiB 阈值两侧、100 KiB、接近输入总预算、多字段/default、signal、IPC/remote、恢复重提。
- 验收：合法大 input 流程成功且字节保持；结果先满足审计持久屏障；非法过大值返回明确业务错误。
- 约束：不增大控制帧上限掩盖错误，不用截断换成功。

### R11 / P1：传输任务有统一取消与回收边界

- 目标与原因：修 F11，close 的契约覆盖正在等待的操作。
- 范围：FrameTransport、executor runtime、pool/agent/remote 的调用点。
- 方法：全部等待点监听 terminate；session task 由所有者持有；关闭 deadline 后 abort + join，保留首个真实错误。
- 测试：停读、停写、flush pending、入站队列满、发送队列满、部分帧取消、Drop、重复 close、真实 TCP。
- 验收：退出时间与任务数/FD 有上界；阻塞 send 被释放；取消后的部分帧流不复用。
- 约束：正常排空保序；不能为了快速关闭提交未持久结果。

### R12 / P1：日志入口有界，批次及时发出

- 目标与原因：修 F12，用户日志不能耗尽执行器内存。
- 范围：NodeLogger 的发送边界、ObservationBridge、丢弃计数与观测测试。
- 方法：有限日志预算、有界非阻塞入口、字节准入；周期/容量/关闭触发 flush；close 超时 abort 并回收。
- 测试：info/error 洪泛、转发停滞、运行时 CPU 竞争、稀疏长任务、logger 克隆未释放、下游队列满。
- 验收：入口条数/字节上限可验证；丢弃可观测；稀疏日志在约定延迟内可见；审计和结果不因日志失败被丢弃。
- 约束：不阻塞 JS 等待日志网络发送，不把日志当 durable audit。

### R13 / P1：remote 会话不等待单个执行器

- 目标与原因：修 F13/F15，单任务慢不能让整条 agent 链路停摆。
- 范围：agent resume/serve/pool、master run_session、会话故障测试。
- 方法：对账分页 deadline + shutdown + release 校验；启动通过有限任务集完成事件回传；每 dispatch 有限入站积压和公平投递。
- 测试：Welcome 后静默、错页/错 dispatch、慢启动、一个 runner 队列满而其余任务正常、慢 journal、Cancel 与重连竞态。
- 验收：心跳/watchdog/取消持续进展；关闭无需等无限 resume；一个慢任务不会永久阻塞其他任务；正确性消息不静默丢失。
- 约束：保留 master 裁决与 uncertain 语义，不用无界队列解决背压。

### R14 / P1：listener/握手/session 的完整准入与 shutdown

- 目标与原因：修 F14，保护认证前资源并收回端口/任务所有权。
- 范围：AgentManager TLS/WSS、remote 启动与失败回滚。
- 方法：有期限的握手 permit；已认证 session 独立上限；保存监听/会话任务并统一取消、回收；多监听启动失败回滚。
- 测试：256 个不发握手字节的连接、握手截止后合法 agent 接入、反复启动关闭、第二监听 bind 失败、session 替换。
- 验收：握手名额到期释放；shutdown 后端口可重新绑定、任务/FD 回落；失败启动不留下 listener。
- 约束：不削弱 mTLS/token 身份认证，不误伤同 agent 的合法重连替换。

### R15 / P1 + P2：修永久迁移进度与文件写入边界

- 目标与原因：修 F16/F17/F18；三项分别实施，不能把迁移与产品配置塞成一个通用事务框架。
- 范围：legacy importer、state.legacy、ConfigState/Config.save、SecretFileStore。
- 方法：不可变证据与工作副本分离；永久 key/fingerprint 恢复进度；配置更新串行化；共用小型私有文件落盘 helper。
- 测试：有 WAL 的两次导入、各阶段中断重入、超过 8192 行/片段、源改变、两个不同分区并发 patch、rename/sync 注入失败、权限。
- 验收：同源重入不改原始证据、不重复提交；回执淘汰不影响导入恢复；并发配置不丢更新；成功回执与落盘契约一致。
- 约束：永久去重不靠无限 command map；不重执行 legacy 副作用；平台 sync/权限语义必须写清楚。

### R16 / P1（验证）+ P2（读模型）：补资源与长期运行的证据

- 目标与原因：验证 F06/F07/F12–F15，并修 F19；当前短流程吞吐数字证明不了这些边界。
- 范围：journal_views/RPC、backend-perf、容量/故障测试；与 `e2e-review.md` 的性能计划对应。
- 方法：先建立当前基线，按下面矩阵采 RSS/FD/task、提交与投影延迟，再随 R05/R06/R12/R13 对比；元数据读不物化 definition，timeline 总预算/分页/按需取值。
- 测试与指标：
  - 固定活跃 run 数，终态历史从 1k → 10k → 100k；记录 master/executor/agent RSS、checkpoint 字节/成功率、重启时间、普通读 P95/P99。
  - 单 run 的审计从 1k → 10k → 100k；记录每新增一批的 CPU、分配、SQLite 写入字节、commit/visible 延迟，验证整 run 重写增长。
  - 相同 workflow version 的多 run 与不同大图；记录编译次数、缓存命中、定义内存、dispatch 启动延迟。
  - JS 日志洪泛、harness 双向管道洪泛、慢 journal；记录峰值 RSS、丢弃数、Cancel 延迟、是否遗留进程。
  - remote 一慢多快、反复断线/对账、握手慢连接；记录各 dispatch 完成/饥饿、预算余额、心跳、重连和 shutdown 时间。
  - 多版本 metadata list、大节点 timeline、多订阅慢消费者；记录物化总字节、runtime 阻塞、响应上限、其他请求尾延迟。
  - 稳定混合业务至少 30 分钟基线及更长 soak；记录资源斜率、错误/超时、投影积压；区分预期保留的磁盘历史和泄漏。
- 验收：相同机器、release profile、payload/并发与历史规模可复现；结果含成功/失败/超时计数。资源上限按实现预算判定；性能门槛根据基线与产品 SLO 制定，不能先编造一个 P99 数字。修复后活跃工作资源受已声明预算控制。
- 约束：不把默认测试通过算作容量验收；PG、Windows、Tauri/Web 和真实断电需独立矩阵，不混入本次结论。

## 任务依赖与 review 顺序

| 批次 | 任务 | 交付边界 |
|---|---|---|
| A：可靠性缺口 | R01/R02/R03/R09/R10/R11/R12/R13/R14，R15 的迁移部分 | 小 PR 按发现拆开；每个有针对故障的验收测试 |
| B：数据结构 | R04/R05/R06/R07；R16 先产基线，再验证布局修改 | 先明确历史/审计兼容契约，再逐步迁移；禁止大状态机重写 |
| C：产品读写面 | R15 的配置部分、R16 的读模型部分 | 版本列表/时间线与配置更新各自保持 API 兼容 |
| D：方向决策 | R08，输入来自 R16 实测 | ADR 选择自研边界和真实部署路线；不先新增平台抽象 |

## 框架下一步方向

1. **先补可靠性缺口**：R01–R03、R09–R15 的 P1 部分，优先 input 大值、传输关闭、预算归还、日志入口与 legacy 重入；逐项加入 E2E/故障矩阵。
2. **再修数据布局**：R04–R06，让系统成本主要随活跃 run、图规模和未确认窗口变化，历史留在可重建磁盘结构。
3. **收敛概念与契约**：R07；公开并稳定发布、等待、取消、对账、幂等窗口、恢复状态的行为。
4. **用实际部署决定平台路线**：R08。桌面/单机优先本地轻依赖；remote 作为明确计算扩容；只有真实多租户/服务端 HA 需求到来时才评估成熟 durable workflow 后端。

框架的竞争力应是可解释的执行、可控的副作用和可恢复的用户流程。继续扩充节点数、协议变体或“cluster”概念之前，先证明长期运行和长历史下的正确性、资源边界与维护成本。

## 本次验证记录与结论边界

- 默认完整测试：`cargo test --offline --workspace`，**369 passed、0 failed、11 ignored**。首次受 sandbox 禁止本地 bind 影响，随后按批准的权限重跑并通过；不把 sandbox 错误当产品缺陷。日志：`/private/tmp/flow-full-audit-tests.log`。
- feature 构建：`cargo check --offline -p flow-backend --features journal-cluster` 通过。没有实际连接 PostgreSQL；cluster feature 下的外部数据库测试不在默认 369 个通过测试中。
- 前轮 2 个临时 engine 探针：过期图索引越界、并行分支覆盖 run 聚合状态；错误行为已复现。
- 本轮 `cargo test --offline -p flow-engine --test review_full_probe -- --nocapture`：2 个探针通过，证明合法节点名的传输键冲突，以及停读时 close/join 不回收。
- 本轮 `cargo test --offline -p flow-backend --test review_full_probe -- --nocapture`：最终 2 个探针通过，真实 executor 复现 100 KiB input 节点失败，SQLx WAL 快照复现二次导入失败。
- 这些探针断言的是**现有错误**，不代表修复验收已经通过。临时探针全部移除；生产实现未修改，本次交付为审查报告。
- 默认忽略的 11 项包含 isolated ENOSPC、1000-run/1 GiB 容量、混合业务性能及子进程 crash fixture。部分 ignored fixture 由父测试启动，但不能把整个 ignored 矩阵称为已经独立验收。
- 本次环境为 macOS；没有验证 Windows 编译/运行、Tauri/Web UI、真实断电、完整性能/soak 矩阵。协议嵌套深度、FD 继承/孙进程回收等还有专门加固空间，未经探针确认的风险不加入确定 findings。
