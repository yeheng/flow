# E2E 覆盖矩阵与套件布局（E01 盘点）

本文是 [e2e-review.md](../e2e-review.md) 的落地基线：四套 E2E 的 crate/目录布局、
每个既有测试的套件归属、场景覆盖状态（已有 / 补强 / 缺失）与独立运行入口。
迁移与新用例请同步更新本文（覆盖矩阵是最终验收依据，见 e2e-review.md §7）。

## 1. 套件布局与运行入口（E20）

| 套件 | 位置 | 运行入口 | 形态 |
|---|---|---|---|
| 单机 E2E | `crates/backend-e2e` | `cargo test -p backend-e2e` | 真起 `flow-journal-server-e2e` 进程 + executor 子进程；另含原产品 crate 迁入的后端集成 / wire 契约 / CLI 子进程用例 |
| 分布式 E2E | `crates/remote-e2e` | `cargo test -p remote-e2e` | master + agent（in-process 模拟或真 `flow-agent` 二进制）+ executor；wire 助手与单机套件共用 `backend_e2e::common::wire` |
| └ 集群（PG） | 同上，feature 门控 | `cargo test -p remote-e2e --features journal-cluster --test cluster_registry -- --ignored`（registry 面）<br>`... --test cluster_process -- --ignored`（真服务进程面，需 `FLOW_CLUSTER_TEST_DATABASE_URL`） | PG 注册表 / 租约 / 成员 |
| Web E2E | `web/e2e`（Playwright） | `cd web && pnpm test` | 浏览器 → UI → 真后端 |
| 性能 | `crates/backend-perf` | `cargo run -p backend-perf -- ...`（场景见其 README/--help） | 独立套件，扩展自现有工具（e2e-review.md §8） |
| Tauri E2E | 待建（`web/src-tauri` 已有壳） | 待 E08 可行性验证后定 | 真实 App 自动化 |

产品 crate（flow-backend / flow-rpc / flow-cli / flow-agent / flow-executor /
flow-journal / flow-engine）不再携带 `tests/` 集成测试；只有 src 内单元测试。
跨 crate 共享的测试基建（TempDir / 端口 / workspace_bin / wire 客户端 /
ServerProc）集中在 `flow-test-support` 与 `backend-e2e::common`，单点维护
（DESIGN.md §14）。

## 2. 单机套件用例清单（crates/backend-e2e/tests）

| 文件 | 来源 | 场景（e2e-review.md §2 矩阵） | 状态 |
|---|---|---|---|
| smoke.rs | 原 backend-e2e | harness 冒烟：创建→发布→运行→终态/输出/timeline | 已有 |
| workflow_lifecycle.rs | 原 backend-e2e | 创建 / 编辑 / 版本 / 发布 / 删除约束（含带运行记录拒绝删除） | 已有 |
| error_contracts.rs | 原 backend-e2e | 错误契约：非法定义、认证失败、不存在资源 | 已有 |
| run_execution.rs | 原 backend-e2e | 运行语义：分支 / 并行 / 失败重试 / 超时 / 取消 / 容量 | 已有 |
| http_calls.rs | 原 backend-e2e | HTTP 节点：成功 / 失败 / 超时 / 截断 / fixture | 已有 |
| signals_human.rs | 原 backend-e2e | 人工输入 / signal / 重复与迟到 signal | 已有 |
| sub_workflow.rs | 原 backend-e2e | 子工作流：透传 / 失败传导 / 取消传播 | 已有 |
| subscriptions.rs | 原 backend-e2e | 订阅：实时事件 / 断线重连 / 去重 | 已有 |
| triggers.rs | 原 backend-e2e | webhook/cron 触发器生命周期与归因 | 已有 |
| input_node.rs | 原 backend-e2e | 输入节点：默认值兜底 / signal 交付 | 已有 |
| crash_recovery.rs | 原 backend-e2e | server SIGKILL 重启恢复、journal 事实一致 | 已有 |
| journal_backend.rs | ← flow-backend/tests/journal.rs | JournalBackend 权威面：值闭包 / 投影重建 / 事务边界 | 已有 |
| journal_acceptance.rs | ← flow-backend | 断电验收（非掉电一期） | 已有 |
| journal_capacity.rs | ← flow-backend | 显式容量负载（非常规回归） | 已有 |
| journal_execution.rs | ← flow-backend | 执行语义（等待 / 状态机） | 已有 |
| journal_import.rs | ← flow-backend | SQLite v1 数据导入 journal | 已有 |
| journal_ipc.rs | ← flow-backend | 真实 executor 子进程：全节点类型 / 取消排空 / 重启恢复 | 已有 |
| journal_ipc_packaging.rs | ← flow-backend | 开发打包：执行器定位 / flow-journal-dev 兄弟部署 | 已有 |
| journal_prewarm.rs | ← flow-backend | 桌面预热：空闲执行器预拉起 | 已有 |
| journal_schedule.rs | ← flow-backend | cron 调度与 command 幂等键 | 已有 |
| journal_v2.rs | ← flow-rpc/tests | v2 wire 契约 + 集群配置无 feature 时拒绝启动 | 已有 |
| ws_journal.rs | ← flow-rpc/tests | 全量 RPC 面（workflow/run/触发器/模板/config）真进程 | 已有 |
| sigkill_recovery.rs | ← flow-rpc/tests | pre-auth / 提交后两窗口的 SIGKILL 恢复语义 | 已有 |
| webhook_http.rs | ← flow-rpc/tests | webhook HTTP 入口：200 触发归因 / 非法 JSON 400 | 已有 |
| contracts.rs | ← flow-rpc/tests | RPC 方法契约（进程内 AppState 面） | 已有 |
| cli.rs | ← flow-cli/tests | CLI 子进程往返：建/跑/查/输出格式 | 已有 |
| cli_journal.rs | ← flow-cli/tests | CLI journal v2 二进制交互（分页/下载不覆盖） | 已有 |

单机套件缺口（对 e2e-review.md §3 完整旅程与 §4 专项）：

- [ ] E04：§3 十步完整旅程作为单一 case 串起来（现有用例分段覆盖，未成一条旅程）
- [ ] §4 单机专项：数据目录排他锁、并发上限（journal_capacity 部分覆盖，需按契约标注）
- [ ] E10：同名工作流、并发保存、重复点击运行等未定契约——先记录当前行为再补断言

## 3. 分布式套件用例清单（crates/remote-e2e/tests）

| 文件 | 来源 | 场景（e2e-review.md §4 分布式） | 状态 |
|---|---|---|---|
| journal_remote.rs | ← flow-backend | R0-01：socketpair 与模拟中继同权威事实；错路由/伪造能力拒绝 | 已有 |
| journal_remote_reconnect.rs | ← flow-backend | R2-01：断线对账、Resume 裁决、断线期取消→CancelAndDrain | 已有 |
| journal_remote_ops.rs | ← flow-backend | R3-01：drain、多 agent 隔离、slots=1 串行不超配 | 已有 |
| journal_remote_tls.rs | ← flow-backend | R1-01：真 agent 二进制 + mTLS + 过时帧拒绝 + 取消不关上联 | 已有 |
| journal_remote_wss.rs | ← flow-backend | T13：WSS 上联（token 身份映射）、管理帧不可包装 | 已有 |
| journal_fault_matrix.rs | ← flow-backend | T09：授权/结果提交故障矩阵（持久化前后 / Permit 前后 / 提交前后） | 已有 |
| journal_raw_transfer.rs | ← flow-backend | T12：大载荷 raw 传输，本地 IPC 与远程 agent 等价 | 已有 |
| cluster_registry.rs | ← flow-backend/tests/journal_cluster.rs | PG Registry：并发注册、租约、journal 所有权 | 已有（PG 门控） |
| cluster_process.rs | ← flow-rpc/tests/journal_cluster.rs | 真服务进程集群：状态 / 节点面 / 移除重试 | 已有（PG 门控） |

分布式套件缺口：

- [ ] E05：统一「真 master + 真 agent + 真 executor」进程 harness（现有部分用例
      为 in-process 模拟 agent / 可剪断代理；契约已钉住，部署链路要补真）
- [ ] E06：与单机套件同输入输出的完整业务闭环（复用 backend-e2e fixtures）
- [ ] E14：多 agent slots 扩展、drain 后回收细粒度断言（journal_remote_ops 部分覆盖）
- [ ] E15：§4 四故障时点中「外部副作用已发生、结果未提交」与「结果已提交、
      确认未到达」的去重/对账证据（journal_fault_matrix 覆盖大部分，按矩阵核对）
- [ ] E16：身份/协议不匹配已有；mTLS 证书轮换、过期证书场景待补

## 4. Web 套件现状（web/e2e，Playwright）

已覆盖：仪表盘一致性、编辑器画布与参数保存、节点模板、右键交互（菜单/连线/
平移）、运行链路（RunPanel→列表→详情时间线）、观测日志脱敏（静态与运行中）、
设置认证与 V2 配置、触发器页（webhook 创建/触发/归因、非法 cron）、列表新建、
导入 JSON。

缺口（e2e-review.md E07/E17/E19）：

- [ ] E07：从空白 UI 完成 §3 完整旅程（v1→v2→触发器→刷新→删除约束）
- [ ] E17：保存失败重试、未保存离开确认、两页面并发编辑、RPC 断线恢复、
      实时日志去重、导出
- [ ] E19：Web → remote 后端关键闭环

## 5. Tauri 套件现状

- [ ] E08：真实 App 自动化可行性验证（未启动；`web/src-tauri` 已有工程壳）
- [ ] E09/E18：本地完整旅程、关闭重开持久化、sidecar/文件交互故障矩阵

## 6. 性能套件现状（crates/backend-perf）

已有 harness / 报告骨架与场景目录；按 e2e-review.md §8 的 PF01–PF14 映射与
基线规范由 P01 盘点（后续任务，本文不重复）。

## 7. 与 e2e-review.md 任务清单的映射

| Task | 状态 | 说明 |
|---|---|---|
| E01 盘点 | ✅ 本文 | 覆盖矩阵 + 套件归属 + 缺口标注 |
| E02 统一 harness | ✅ 部分 | TempDir/端口/workspace_bin/ServerProc/wire 已单点化；故障注入事件同步待 E15 细化 |
| E03 共用 fixtures | ✅ 部分 | `backend_e2e::common::fixtures` 已被 perf 复用；remote 复用待 E06 |
| E20 独立入口 | ✅ | 四套（单机/分布式/Web/性能）独立入口；CI 分层见 §8 |
| E04–E09、E10–E19、E21–E22 | ⬜ | 见各套件缺口小节 |

## 8. CI 分层

- PR：`cargo test --workspace --all-targets`（含两后端套件全部默认用例）+
  fmt/clippy 门禁 + Web Playwright；集群面在带 PG service 的专用作业
  （`journal-cluster` job：clippy + cluster_registry/cluster_process --ignored）。
- 夜间 / 发布矩阵：按 e2e-review.md §5 扩展（待 E20 后续批次接入）。
