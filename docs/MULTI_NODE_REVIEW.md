# 多节点执行审查（2026-10-11）

本轮是现有代码审查和后续实现设计；没有实现 PG 后端或集群管理页面。
对应方案见 [MULTI_NODE_PG_DESIGN.md](MULTI_NODE_PG_DESIGN.md)。

## 结论

当前实现是 **单 journal coordinator + 多远程 agent + 每机 executor**。
已经支持跨机器分配节点任务，尚不支持多个 coordinator 共享权威状态、自动接管
run 或持久化管理执行集群。旧 `flow-pg` 已删除，不能照着 DESIGN.md 的历史章节
推断现在存在 PG 租约、inbox 或接管能力。

必须区分两个目标：

- 多执行机：现有 remote 模式已有基础，先修注册健康、资源等待和 drain 即可改善。
- 多管理节点与故障接管：需要 PG 权威提交、run 租约、持久工作队列和派发归属。
  PG 只存机器列表，不能消除单 journal writer 的故障点。

## 审查范围与证据

读过实际产品装配、journal 命令/驱动/状态折叠/投影、IPC/remote dispatcher、
agent/executor 生命周期与协议、RPC/触发器、配置、Vue 路由/API/设置页、桌面内嵌
服务及现有 remote/reconnect/运维测试。以下发现是静态代码路径审查；未把尚未做的
网络黑洞、drain 并发和去重窗口压力实验描述为已经复现。

严重性：P1 = 可能产生重复业务执行或资源/故障隔离失效；P2 = 运维、容量或性能问题。
“集群化缺口”表示现有单主设计的边界，不能当成其已经承诺却未实现的能力。

## 现有缺陷与行为调整

### R1 / P1：cron 触发去重随通用命令窗口淘汰，可能重复触发同一时刻

证据：`flow-rpc/src/journal_triggers.rs:16` 每个 tick 选最近一次 cron 时刻，
`flow-backend/src/journal_commands.rs:184` 使用 command.status/run.start 的通用
request_id 去重；`flow-engine/src/journal_state.rs:417` 超过 8192 条回执后淘汰最旧
一半；`flow-backend/src/journal.rs:367` 未命中回执就执行新决策。

场景：每日 cron 的某次 fire 已成功，下一次每日 fire 尚未到来，但其间其他工作流
产生超过窗口的命令。旧 fire 的回执被淘汰，下一次扫描仍选择同一 fire，重新创建
run。重启重放也按相同规则淘汰，不能靠重启恢复去重。

修复：cron 的 `(schedule_id, scheduled_at)` 用独立持久唯一约束或持久高水位，
与创建 run、推进 next_fire_at 同事务。webhook 和手动命令定义明确的幂等保留期；
不能把活跃 run 上限拿来论证历史命令不会被淘汰。

### R2 / P1：远程上联没有心跳失效检测，TCP 黑洞不会及时剔除

证据：`flow-backend/src/execution/remote.rs:513` 会话循环只等命令/收包/取消，
`:780` 收到 Heartbeat 后没有更新时间；`flow-agent/src/runtime.rs:339` serve
循环没有上联心跳期限；FrameTransport 没有连接存活 watchdog。
本地 IPC 的 `execution/pool.rs:260` 则确实维护 last_seen 和心跳超时。

场景：连接被静默丢包、对端 SIGSTOP 或空闲 agent 停止响应，但没有 EOF。
主进程仍把它当成在线候选，任务只能等 bind/dispatch 自身的超时后失败。
agent 空闲时也没有独立的周期容量/心跳发送循环；有 executor 心跳不能代替 agent 存活。

修复：独立上联心跳 + pong/watchdog；失效先停止调度，再进入对账期限。
PG heartbeat 必须由经过认证且仍属于当前 generation 的会话更新。

### R3 / P1：drain 没有先在调度端关闭准入，可能把新任务派给 draining agent

证据：`execution/remote.rs:1081` 只发送 Drain，AgentRegistration 没有行政状态；
`:943` 候选过滤只看剩余 slots；`flow-agent/src/runtime.rs:435` 在本地 draining
时拒绝 BindExecutor。主侧 bind 失败沿 `journal_driver.rs:246` 写 NodeFailed。

场景：agent-a 接到 drain，仍在线且还有空槽；agent-b 正常。新任务仍可能选中
agent-a，收到拒绝后失败，而不是交给 agent-b。
现有 `journal_remote_ops.rs` 测试主要验证下线之后继续执行，没有钉住这段并发窗口。

修复：先持久提交 `desired_state=draining`，调度事务排除该 agent，再异步送指令。
还应分开“停止接新任务并等待完成”和“期限到达后取消”；当前 grace 被硬限制为
5 秒（`runtime.rs:475`），不足以表达长任务滚动维护。

### R4 / P2 / 当前契约调整：无 agent 被记录为工作流失败，没有持久资源等待

证据：`remote.rs:939` 立即返回 no agent connected；`journal_driver.rs:225`
已经创建 DispatchStarted，`:249` 错误路径会写 NodeFailed。
`journal_remote_ops.rs:339` 明确要求 drain 后无 agent 时 run 失败：这是当前的
有意行为，需要在集群版本中调整，不能声称现有测试遗漏了这个契约。

没有槽位时，等待容量的 task 也占 coordinator 的 JoinSet 名额。
新增队列应让无资源的节点保持 queued，并返回可解释的 pending_reason。
基础设施等待不增加业务 attempt；另设 queue_timeout 时才按已定义策略收尾。

### R5 / P2：remote 二进制的测试构建和 CI 打包不完整

证据：`.github/workflows/ci.yml:118` 只收集 server/cli/perf/e2e，没有
flow-agent/flow-executor。`scripts/build-journal.sh:7` 的本地打包则包含它们。
缺少 sibling executor 会在启动 IPC 或绑定远程执行器时明确失败。

本轮剩余 Rust 测试还在 journal_remote_tls 实际遇到 `flow-agent not built`，
2 项失败；`flow-test-support/src/io.rs:210` 从 workspace 的普通二进制路径定位
agent，而该 package 没有让本轮 cargo test 自动产出产品 agent 二进制。
CI test job 也直接 cargo test、没有先 build，不能依赖本地上一次构建的产物。

修复：发布包按 server/agent/工具角色收集产物，附协议/build 信息和可执行 smoke；
测试 job 显式构建所需产品二进制，使用同一 target/profile 定位。
不要把 e2e server 当成对用户发布的执行产品。

### R6 / P2：全量测试存在启动与故障注入时序竞争

本轮 `cargo test --workspace --all-targets --locked` 在 workflow_lifecycle 中实际
出现 AddrInUse，随后 run_start_requires_published_version 连接拒绝；该 test target
改为单线程重跑 8/8 通过。这是本轮观察到的测试失败，不是工作流语义失败的证明。

`backend-e2e/src/common/server.rs:174` 先挑端口再 spawn，虽然已有重试，
`flow-test-support/src/io.rs:97` 仍只看 TCP 能连接且子进程当时存活。静态推断：
别的进程已占端口而目标子进程还未退出时，可能提前返回 Ready。
不能据这一次失败断言唯一根因已定位。

改进：让产品进程绑定 :0 并报告实际 WS/HTTP 就绪端口，或验证本次随机 token/
启动身份的 RPC readiness；必须等两个监听端点都装配完成，避免多个集群进程
故障测试放大这种竞争。可复用已有 `flow_test_support::io::spawn_reporting_ports`
（io.rs:270），不需要再写另一套端口报告解析器。

构建产品二进制后的一轮测试还出现
`journal_ipc::killing_executor_mid_script_recovers_with_new_process` 超时：kill 后
20 秒 run 仍 running；首轮该测试通过，之后单独重跑也通过（0.67 秒）。
`journal_ipc.rs:413` 仅等待带 tag 的进程出现，未等待目标 hang 节点进入执行态；
`:418` 未检查 libc::kill 返回值。静态推断存在杀到上一流程节点/空闲会话的窗口，
尚不能断言这次超时的唯一根因。验收需先等目标 dispatch 的准备/执行屏障，再
按其实际 executor boot/PID 注入并确认成功，不能只证明杀过某个带 tag 的进程。

## 集群化缺口

### C1 / 阻断：状态权威与提交串行化都在单进程

证据：`flow-journal/src/storage.rs:365` 获取目录排他锁；
`journal.rs:52` 保持完整 CommittedState；`:335` 和 `:436` 用同一 Mutex
串行决定、落盘和折叠。SQLite 是可丢弃投影，不能用于选主或跨机器续跑。

设计约束：journal 模式保持现有单机权威；cluster 模式必须明确 PG 是唯一权威，
JSONL 只作可导出的审计格式，避免 PG 与文件双写形成两套恢复事实。

### C2 / 阻断：派发归属只在内存，master 重启不能重建旧绑定

证据：`remote.rs:162` BoundDispatch 存 agent/executor boot，`:194` 注册表与绑定
都是 BTreeMap；`:816` Resume 必须先找到 binding，找不到就 ReconcileRequired。
重启后 journal 的 dispatch 仍在，但机器/会话/executor 归属不在。

设计约束：持久 dispatch assignment、agent boot/generation、executor boot、
run owner epoch 和结果身份；接管从这些事实恢复，不能信新 agent 自报归属。

### C3 / 阻断：全局 master_epoch 不能直接替代逐 run fencing

证据：`journal_driver.rs:72` 每次 remote 启动整体 bump master_epoch；
`journal_state.rs:204` 要求 dataset epoch 连续；现有 Attempt 的提交检查只涉及
当前 dispatch，依靠唯一 writer 来排除另一个 coordinator。

设计约束：多个管理节点可同时持有不同 run；同一个 run 的每次接管增加 owner_epoch，
所有提交、派发和 OperationPermit 都在 PG 事务中检查 owner/epoch/有效租约。
停止旧进程只是辅助动作，数据库条件写才是提交屏障。

### C4 / P2：容量和状态读面都按单主小规模设计

证据：`execution_protocol/contract.rs:36` A_MAX=16；`remote.rs:227` 使用全局
16 槽信号量；`journal_driver.rs:103` remote driver 同样固定 16。
当前增加 agent 不会把总派发数提升到其 slots 之和，这是内存预算保护，并非超配 bug。

`journal_views.rs:58/155` 多个产品读接口调用 backend.state，
`journal.rs:326` 克隆整个 State，包含历史 runs/attempts/audit 索引。
不能给每个 cluster API 实例复制全量历史；改成 PG 索引查询和有界 run 缓存。
提高并发上限前也必须重新验收审计窗口、传输、内存与 PG WAL 预算。

### C5 / 阻断：产品装配与管理页面都缺少集群运行时

证据：`flow-rpc/src/lib.rs:34`、`journal_v2.rs:18`、桌面
`web/src-tauri/src/service.rs:58` 都直接持有 JournalBackend；
`flow-config/src/lib.rs:86` 后端只有 Journal；`web/src/router.ts` 与 App.vue
没有节点管理路由；OPS.md §6.2 明确没有 drain CLI/RPC。

config.update 和 secrets.set 目前写本机文件，多 API 节点间不会同步。
设计约束：抽取实际使用的命令/查询/值读取端口；保留现有领域语义，加入 PG 实现。
集群策略和密钥必须有共享版本；本机配置只描述该节点的启动和连接参数。

## 值得保留的实现

- audit_seq 连续前缀、重复内容摘要检查和结果封口屏障。
- OperationIntent/Authorized 同事务持久，提交后才能发送 Permit。
- 外部操作结果未知进入 uncertain；自动接管不等于自动重新发 HTTP/email/harness。
- run 取消、人工裁决和子 run 创建作为持久事实；等待任务不持续占执行器。
- mTLS 身份、双通道绑定凭据、executor boot 校验、有界且公平的转发。
- request_id + fingerprint + 原回执查询契约，但需要修复保留期和触发器去重边界。

## 验证

- `web/` 的 `npm test`：19 文件、108 测试通过。
- `cargo test --workspace --all-targets --locked`：在 backend-e2e 的
  workflow_lifecycle 遇到上述端口竞争后终止；前面的 e2e target 通过。
- `cargo test -p backend-e2e --test workflow_lifecycle --locked -- --test-threads=1`：
  8 测试通过。
- 沙箱中部分本地 TCP 监听被拒；获准在沙箱外继续测试后，这些用例通过。
- `cargo build --workspace --locked`：通过；补齐产品 agent 等二进制后，
  `cargo test -p flow-backend --test journal_remote_tls --locked -- --test-threads=1`
  的真实 TLS 3 测试通过。
- `cargo test --workspace --exclude backend-e2e --all-targets --locked -- --test-threads=1`：
  第一轮缺 agent 二进制，构建后另一轮出现上述 IPC kill 超时。IPC kill 单独重跑
  通过，其他 backend 测试目标在前后两轮中均有通过结果；不能据此声称单次全量全绿。
- `cargo test --workspace --exclude flow-backend --exclude backend-e2e --all-targets
  --locked -- --test-threads=1`：其余 crate 全部通过。
- 未运行默认 ignored 的大容量、ENOSPC、长跑和性能验收；未运行 UI Playwright，
  本轮没有修改前端或运行逻辑。

这些基线测试不构成 PG 并发、网络分区或跨节点接管验收；新验收矩阵见方案末节。
