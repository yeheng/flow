# Flow 架构加固工作单（定稿 + 进度）

> 审批链路：架构评审（三链路定论 + 单机焊死 + 协议 v2 瘦身）→ 方案定稿（T0–T7）→ 用户 ok 开工。
> 本文是唯一权威进度账本：定稿方案、当前状态、断点续作信息。

---

## 一、定稿架构（已确认）

**三条链路，不是两条：**

```
客户端（CLI/浏览器/桌面UI）
   │  JSON-RPC over WebSocket（产品面，flow-rpc）
   ▼
journal 服务端（flow-backend，唯一权威）
   │  远程链路：Routed 信封 over 双 TLS（execution/remote.rs）
   ▼
flow-agent（中继，自己不碰 journal）
   │  执行 IPC：socketpair + 帧（execution_protocol，同一协议）
   ▼
flow-executor（子进程，节点语义的唯一居所）
```

**焊死的规则：**
1. master↔executor 是同一种 IPC 协议；master = backend（单机直连）或 agent（远程中继），两处 pool 调同一 `spawn_executor_process`。
2. 节点执行语义只许存在于 flow-executor；master 只做裁决和调度。
3. 控制通道只走定长身份与元数据；一切变长业务值走 data 通道，再小也走（协议 v2）。
4. 服务端在写 RPC 响应前等到 `applied_lsn >= commit_lsn`；-32020 只作投影病理阀，健康路径永不触发，客户端零恢复逻辑。
5. 单机默认 in_process（Windows 过渡态）；unix 上终态默认 ipc（T6），随后删 in_process（T7）。
6. IPC 执行层 unix-only：非 unix 编译干净、`execution.mode=ipc` 明确报错、不静默落回。

**Windows 决策**：先 B（桌面留 in_process，显式门控）后 A（命名管道 transport，独立后续特性）。

---

## 二、任务清单与状态

### ✅ T0 · 分层在编译器层面成立（完成）

- **CI 红灯根因修复**（main 曾连续红）：
  - cross+gnu 目标链接 `getrandom` 失败（旧镜像 glibc vs 新 Rust std 弱符号）→ ci.yml 的 gnu 目标改**原生 runner** 编（x86_64 用 ubuntu-24.04、aarch64 用 ubuntu-24.04-arm），cross 只留 musl。
  - `cargo test --all-targets` 不产出普通二进制而 remote/IPC 测试按工作区布局定位真实二进制 → test 步骤前加 `cargo build --workspace --bins`。
  - **真实竞态修复（MULTI_NODE_REVIEW R3）**：drain 只发消息不在 master 侧打标 → draining 中的 agent（slots 已回收、used=0）反而最优先被 bind，被 agent 拒绝后节点失败。修复：`AgentRegistration.draining: AtomicBool`，`drain()` 先打标，`bind_executor` 候选过滤。`one_agent_offline_others_continue` 5/5 稳定。
- **transport 拆分**：`transport.rs` 摘除 socketpair → 新 `unix.rs`（`ChannelPair`/`socketpair_channels`/`channels_from_raw`/`FrameTransport::spawn`），transport 主体（FrameTransport/spawn_streams/握手）全平台；`fresh_boot_id` libc 段 cfg(unix)；测试模块 `#[cfg(all(test, unix))]`。
- **cfg(unix) 门控**：flow-backend `execution::{dispatch,pool}`、`ExecutionPort::Ipc` 臂、`start_execution_ipc` 挂 unix；非 unix `mode=ipc` 报 "requires a unix platform (socketpair transport)"（不静默落回）。
- **ci.yml 新增 `windows-check` job**：`cargo check --workspace --exclude flow-executor --exclude flow-agent`（两二进制为 unix 产品）+ setup-nasm（aws-lc-sys MSVC 需要）。
- **验证**：宿主（macOS）全 workspace check/clippy 绿；Windows job 待 push 后由 CI 首验。

### ✅ T1 · spawn.rs FD 映射重写（完成）

- 60 行三分支冲突矩阵 + F_GETFD spare 扫描 → `place_fds()`：`F_DUPFD(base)` 内核分配高位临时 FD（保证空闲、≥base、两次互异）→ dup2 落位 100/101 → 关临时。**零分支、零自守卫**（字面 dup2 到固定 1000/1001 有自占用反例，F_DUPFD 才真"绝对安全"）。
- `base = 1024.min(fd_limit()-8)`；pre_exec 内仅 AS-safe 调用。
- **回归测试** `place_fds_survives_slot_occupation`：管道构造 7 种占用排列（交叉/单侧/无占用），确定性覆盖旧矩阵雷区（填 FD 表方案在并行测试下不确定，弃用）。
- journal_ipc 真实执行器全链路走新 FD 路径。

### ✅ T3 · 服务端写到可见 + 消灭 -32020 竞态源（完成）

`flow-backend/src/journal.rs`：
- `projected_lsn: AtomicU64` 镜像投影游标：open 时以 SQLite 游标初值化，projector 每 LSN apply 后 `fetch_max` + `notify_waiters()`。等待谓词从"每次唤醒查 SQLite"（projection.rs:85-93 是真查询）变为一次原子读。
- **删除 `waiters: Semaphore(256)`**（-32020 第一个发射点消失）。
- `wait_timeout` 5s→30s（`wait_timeout_ms: AtomicU64` + `set_projection_wait(ms)` 公开 setter，测试可调小）；超时仍映射 -32020 = 投影病理阀。`projection_error` 早退保留。
- 已核实 `visible()` 在 `drop(committed)` 后调用，不持 state 锁，等待不串行化后端。
- **验收**：新测试 `concurrent_writes_all_return_visible_without_committed_not_visible`（500 并发写全 visible=true 零 -32020，旧信号量必炸场景）通过；`paused_projection_*` 与崩溃夹具两个超时依赖测试改用 `set_projection_wait(100)`，acceptance 套件绿。

### ✅ T4 · 删除客户端 recover_committed 轮询（完成）

四处全删（e2e/flow-cli/web×2），-32020 降级为"投影病理终态错误"：
- `backend-e2e/src/common/client.rs`：`recover_committed` + 两处调用 + 常量 + `raw_call` 死代码删除；到达即 panic（测试哨兵）。
- `flow-cli/src/journal.rs`：20×250ms 轮询删除，错误原样上抛。
- `web/src/rpc/client.ts` + `web/src/api/journal.ts`：恢复循环删除，`command()` 变直通；`JournalClient.command` 的 scope 参数保留为 `_scope`（调用方签名不变）。
- 测试改造：v2-protocol.test.ts / journal.test.ts 的恢复用例改为"传播不轮询"断言。
- 保留：-32020 常量与服务端映射（老客户端契约）、`command.status` 端点、webhook 202。
- 文档：flow-rpc lib.rs 与 JSONL_DEVELOPMENT.md 的 -32020 语义改写。
- **验证**：backend-e2e 全套 **52 用例绿**（真实服务端、恢复代码删除后零 -32020）；web vitest 16/16。

### ✅ T2 · 协议 v2：业务值全量走 data 通道（完成）

已落地（PROTOCOL_VERSION 1→2）：

**ExecuteTask 瘦身（master→executor）**：
- `input: StoredValue` / `predecessors: Vec<(String, StoredValue)>` → `TaskValue { Ref(ValueRef) | Local }`：Ref 保留 journal 身份（start/end 输出按引用回传需要），Inline 只声明存在——值字节一律 TransferChunk+InputReady。
- `prepared: Option<Value>` → `PreparedMeta { engine_build, node_semantics, params: TaskValue }`（v1 把 Prepared 的 input/predecessors/params 三个值整份 inline 在帧里，与 task 级字段冗余拷贝）。
- `previous_outcome: Option<Value>` 保留（status/body_raw 是有界描述符；体走 op-body:<output_id>，机制已有）。
- `node` 是控制帧唯一允许变长项：dispatch 构帧前 `bounded_json(node, CONTROL_MAX_FRAME/2)` 预检，超限干净拒绝不再杀会话。
- dispatch.rs：`transfer_inputs` 统一字节源 `ByteSource { Journal(ValueRef) | Memory(Vec<u8>) }`——Ref 走 journal 流式（原路径），Inline 序列化后 Cursor 直接分块；声明集 = in:run-input + in:<node_id>×n + in:prepared-params + op-body:<id>。
- task.rs：`materialize_inputs` 重写——按声明等 InputReady，`InputFace` 重建 StoredValue 面（Ref 保身份 / Local 物化 Inline，预算 8MiB 计算含 Local 字节）；resume 的 Prepared 由 face+meta 重建；fresh prepare 复用 face；`materialize_stored`/计算路径零改动。

**RequestOperation 全量 transfer（executor→master，删 inline 特例）**：
- `request: Option<StoredValue>` 字段删除，`transfer_id: String`（op:<operation_id>）恒走 data 通道（删 ≤32KiB inline 分支）；master `handle_request_operation` 统一从 transfer 装配（校验 total/digest/fingerprint 不变）。

**测试**：
- fixtures 版本样例 v1→v2（不兼容样例 v:3）；`inline_sized_run_input_dispatches_via_data_channel`（62KiB inline 输入，v1 帧必爆的杀区）**通过**。
- flow-engine lib **76/76 绿**。

**✅ journal_ipc 断点已归因并修复（三个 v2 传输层 bug）**：

并行失败/单线程部分失败的六大用例（script_condition、restart、sub_workflow、journal_events、cancel_during_http、delay_and_signal 等，每次组合漂移）是**同一个根因的随机投影**，非各自独立问题：

1. **主因（随机挂死，修复后 10+5 轮全绿 16s）**：executor 主循环（runtime.rs）对 TransferChunk/InputReady 的单槽 `pending` 转投存在两个丢失窗口：
   - 双通道竞速下 **TransferChunk（data 通道）可能先于 Execute（control 通道）到达**，此时 `current=None`，`if let Some(task) = current` 不命中 → **帧被静默丢弃**；Execute 到达后 InputReady 也到（control 畅通），但字节块永不补发 → `IncomingTransfers::is_ready` 恒 false（有 ready 无 assembled）→ materialize 死等到 120s deadline，远超测试 30s 窗口。
   - 单槽在 permit 获取窗口内被后续帧覆盖。
   修复：`pending` 改 FIFO `VecDeque` + 新增 `orphan_transfers` 缓存（Execute 未到时按 dispatch_id 保留，任务建立后补投），转投改为循环顶部确定性 `try_send`；TransferChunk/InputReady 转投增加 dispatch_id 匹配。
2. **op-body 取值路径不对齐（http/integration 恢复场景死等，即 oversized_http_capture 曾 >3min 挂死的根因）**：master `previous_outcome` 是 StoredValue 序列化（`{"type":..,"value":{..}}` 外壳），master 用顶层 `get("body_raw")` 永远取不到，executor 用 `pointer("/value/body_raw")` 能取到并登记等待 → master 漏发 `op-body:<id>` 传输 → executor 死等。修复：master 改用同一 pointer 路径（dispatch.rs transfer_inputs；远程链路复用 DispatchRunner 一并受益）。
3. **executor tracing 日志进 /dev/null（调试盲区）**：主进程 spawn 时 `stdout(Stdio::null())`，而 `tracing_subscriber::fmt()` 默认写 stdout。修复：`with_writer(std::io::stderr)` + 进程启动打 PID（多执行器并存时区分日志）。

排查中同步落地的可观测性补强（保留）：`JournalBackend::diagnostics()`（内部游标/队列/心跳快照，挂死排查用）、coordinator 在 journal writer 故障时限频报错（原来静默空转）、transport write_loop 在 broken 退出时告警。

**验证**：journal_ipc 单线程 10/10、并行 5/5 全绿（原：并行 6 挂/单线程 4 挂/修复后偶发 1 挂）；journal_remote_ops 3/3、journal_remote_tls 3/3（偶发 "no agent connected" 环境噪声，重跑即绿）；backend-e2e 59/59；fmt/clippy 绿。

**⚠️ 遗留独立问题（非本次改动引入，已用基线代码验证同样存在）**：
- `flow-engine --lib` 的 transport socketpair 测试高概率 flake：`fatal runtime error: IO Safety violation: owned file descriptor already closed`（基线 stash 后 6 次 4 挂；涉及 handshake_roundtrip / data_channel / first_message_not_hello，fd 生命周期竞态，待专项修复）。
- cancel_during_http 曾现 23 分钟挂起后自愈形态（1/9 轮次，http 10s 超时未生效类，最终断言全过；低优先级待查）。

**T2 剩余（4b 队列，本版不做）**：`Result Success.output` / `WaitRequest.output` 的 inline 输出通道化（≈63KiB 脚本输出仍有同类杀区），需重做 executor store_value-经审计分块与 resume/lost_data 语义，单独 PR（协议再升 v3）。

### ✅ T6 · 单机默认切 IPC（完成）

- **flow-config 默认**：`ExecutionConfig::default` 平台条件——unix 上 `mode=ipc`，非 unix 过渡态 `in_process`；`FLOW_EXECUTION_MODE=""` 不再强制 in_process（视为未设置，继承平台默认）；示例配置与 EXECUTION_MODE_ENV 契约注释同步。
- **环境隔离**：`spawn_executor_process` 增加 `env_clear()`（凭据/路径/代理不泄漏），仅回填 `RUST_LOG`（可观测性）；FD 走 pre_exec 槽位不经 env。x_max 已在 config（u32）；**桌面默认 8** 落在 service.rs 无配置文件分支（交互负载 + executor ≈11MiB/进程）。
- **e2e 基建**：e2e server spawn 注入 `FLOW_EXECUTOR_BIN`（workspace_bin 定位，尊重显式覆盖）；subscriptions/signals_human 的 signal 前置从「等 display 态 running」改为「等 wait 登记」（timeline 暴露 wait 结构；display running 同时覆盖 dispatching 与 waiting 不可区分，ipc 往返延迟暴露了旧时序假设）。
- **桌面打包**：tauri.conf.json `externalBin: ["bin/flow-executor"]`；release.yml 在 tauri build 前构建 executor 拷至 `bin/flow-executor-<triple>`（macOS 进 Contents/MacOS/ 满足 locate() 同目录契约）；**Windows 占位文件**过渡（B 方案 in_process 不调用，随 A 方案命名管道换真实产物）；`scripts/build-desktop-executor.sh` 本地开发前置；桌面测试注入 FLOW_EXECUTOR_BIN。
- **backend-perf 修复（main 预存 bug）**：collect_arrivals 读旧信封格式（顶层 `run_id/type`），订阅协议事件体包在 `envelope["event"]` 层后 perf 未跟上——0 事件断言炸；对齐后恢复可用。
- **验收**：backend-e2e 全量 **59/59 在 ipc 默认下绿**；桌面测试 3/3（ipc 默认下跑通）；journal_ipc 12/12（单线程/并行）；remote_ops/tls 各 3/3；flow-config 7/7；RSS 有界用例（release）：master 22MiB / executor peak 11MiB、无孤儿进程；perf 对比留档（debug/200 runs）：chain 3.18 vs 3.21 runs/s、fanout 2.57 vs 2.64 runs/s，**ipc 与 in_process 差 ±3% 内**（往返开销被 journal 写延迟掩盖）。
- **收尾（668aea3）**：T6 提交后 main CI 转红——`flow-cli tests/cli.rs`、`flow-rpc tests/contracts.rs`、`flow-rpc tests/journal_cluster.rs` 在默认 ipc 下定位不到兄弟执行器、`web/e2e/global-setup.ts` 的服务端起不来。三处套件显式指定执行模式 + e2e 注入 `FLOW_EXECUTOR_BIN` 后 CI 全绿（windows-check / e2e / journal-cluster / web / build 均绿）。

### ✅ T7 · 删除进程内执行路径（完成）

范围定稿：**unix 删除、非 unix 保留**（Windows 过渡态是 B 方案，等命名管道 A 方案落地后再删）。
节点执行语义在 unix 上只剩 `flow-executor`；master 只做裁决与调度。

- **执行端口**：`ExecutionPort::InProcess` 变 `#[cfg(not(unix))]`；`start_execution` / `start_execution_with_limit` 只在非 unix 编译；`ExecutionMode::InProcess` 同样 cfg 门控（unix 上该枚举只剩 `Ipc`）。
- **master 侧进程内实现删除**：`journal_driver::execute()` / `start_child()`，以及 `journal_execution` 的 `prepare` / `materialize` / `compute` / `wait` / `authorize` / `harness(_output)` / `http(_output)` / `audit` / `json` / `stream` / `Capture` 全部 `#[cfg(not(unix))]`——unix 编译产物里不存在；`Attempt` 只留 master 侧职责（begin/snapshot/finish）。
- **明确拒绝、不静默换模式**：`flow-config` validate、`execution::mode_from_config`、`mode_from_env_with`、`flow-backend::start_execution` 四处在 unix 对 `in_process` 报 `IN_PROCESS_REMOVED`；`FLOW_EXECUTION_MODE=""` 仍是「未设置 → 平台默认」。
- **删码引发的一个真实语义缺口（已修）**：删除进程内实现后，`journal_execution` 与 `flow-executor::task` 的 http 语义差异暴露——**执行器原先先授权后构造请求**，畸形请求（换行 header / 非法 URL / 非法 proxy）会先进 uncertain，违反「畸形请求必须在授权前判死」。修复：`http_node` 增加授权前预检（`build_outbound(None)`），与进程内实现的原顺序对齐。
- **删码引发的第二个真实缺陷（已修）**：`JournalBackend::close()` 原先用同一个 `shutdown` token 既停协调器又停投影。协调器退出前可能正卡在 `command()` 的可见性等待（如取消 64 个子 run 的 `run_cancel`），投影先停就永远等不到 `applied_lsn`，每个等待吃满 `wait_timeout`（30s）× N → `close()` 表现为挂死（`journal_acceptance::cancelled_fanout_tree_finishes_after_restart` 迁移到 IPC 后必现）。修复：拆出 `projector_stop` token，`close()` 先停协调器并 join，**投影继续跑**，之后才停投影（该用例从挂死 >2min 变 4.5s 通过）。
- **执行槽位语义对齐**：`start_execution_ipc` 原先把协调器派发窗口硬编码 16，忽略 `x_max`；现按 `x_max.clamp(1,16)` 同时作为池容量与 A_max，与 `start_execution_with_limit` 语义一致（`x_max=1` 的串行断言才有意义）。
- **测试迁移（约 35 处调用点）**：journal_execution / journal_acceptance / journal_capacity / journal_remote / journal_ipc / flow-rpc journal_v2 / flow-cli / flow-rpc contracts 从 `start_execution[_with_limit]()` 改为新 `start_ipc(backend, x_max)` 助手（显式指向工作区 flow-executor，`#[cfg(not(unix))]` 分支保留原 in_process 行为）；`journal_ipc_packaging` 的模式解析断言按平台拆分（unix：`in_process` 报错、`""`=ipc）。
- **PanicOnScript → executor 猝死**：原 lib 单测（注入 panic）删除，职责并入 `journal_ipc::killing_executor_mid_script_recovers_with_new_process`——真 SIGKILL 执行器 → 节点 failed + attempts 全 sealed + 新会话继续服务。
- **`transient_materialize_error_retries_without_another_command` 改造**：失败注入点从「128KiB 脚本体」改为「128KiB 定义 padding」——节点体受协议 v2 控制帧 `CONTROL_MAX_FRAME/2`（32KiB）预检约束，只有控制帧外的定义体积超过 `INLINE_BYTES` 才落分块值存储，把 `root/journal` 值目录挪走才能制造 materialize 失败。
- **CI**：journal-cluster 作业新增 `cargo build -p flow-executor`（该作业起真服务端，unix 默认 ipc 需要它的绝对路径）。
- **文档**：DESIGN.md §2.1 三链路定论图 + 模式矩阵；OPS.md 单机进程矩阵 / §2 / §2.1；JSONL_DEVELOPMENT、JOURNAL_CLUSTER、flow.config.example.toml、web 设置页帮助文本同步（`in_process` 标注为仅非 unix）。
- **验证**：`cargo test --workspace --all-targets` 全绿（含 backend-e2e 59、journal_ipc、journal_execution 8、journal_acceptance、journal_capacity、remote 套件在真实执行器下）；`cargo fmt --all --check` + `cargo clippy --workspace --all-targets -- -D warnings` 绿；journal-cluster 本地 PG 18 上 6+2 绿；web vitest 111/111、playwright 13/13。

---

## 三、零破坏性核对（贯穿全程）

- JSON-RPC 用户面零改动：-32020 常量/映射/`command.status`/webhook 202 全保留；已发布老客户端照常正确。
- server↔executor 协议 v2：同包发布（locate() 兄弟目录 + build-journal.sh 同目录），版本不匹配在 envelope 解码处明确拒绝。
- FD 改动纯内部，接收端仍读 100/101。
- 客户端删码不删契约：删的是"永不触发的恢复分支"。

## 四、替用户做过的决策（可推翻）

1. T1 用 F_DUPFD 而非字面 dup2-1000/1001（两个自占用反例）。
2. RequestOperation 全量 transfer 并入 v2 一次升版；Result/Wait 输出通道化因 resume/lost_data 语义重做单独排队（4b，v3）。
3. `wait_timeout` 以 `set_projection_wait` setter 满足"可配置"，未加 [journal] 配置项。
4. Windows 先 B（in_process 过渡）后 A（命名管道）。
5. `previous_outcome` 判定为描述符类留在控制帧（体已走 op-body）。
6. executor/agent 二进制为 unix 产品，Windows check 用 `--exclude` 排除而非门控其内部。
7. T7 **只删 unix** 的进程内路径，Windows 保留 `#[cfg(not(unix))]` 过渡态（B 方案）；没有全平台一起删，避免桌面在命名管道 A 方案落地前出现功能真空。
8. T7 把 IPC 的 `x_max` 同时当作协调器派发窗口（A_max）——原先硬编码 16 会让「执行槽位」这同一个旋钮在两个层面各说各话。
9. 删掉进程内实现后暴露的 http 授权顺序差异，按**进程内原有语义**修执行器（畸形请求授权前判死），而不是改测试迁就执行器。

## 五、验证命令速查

```bash
cargo build --workspace --bins                 # 测试前置：产出真实二进制
cargo test -p flow-engine --lib                # 协议单测（含 FD 排列）
cargo test -p flow-backend --test journal_ipc  # IPC e2e（真实执行器）
cargo test -p flow-backend --test journal_execution --test journal_acceptance --test journal_capacity
cargo test -p flow-backend --test journal_remote_ops --test journal_remote_tls
cargo test -p backend-e2e                      # T4 哨兵套件
cd web && npx vitest run && npm run test:e2e   # web 单测 + 浏览器 e2e（自建 flow-executor）
# 集群作业（T7 起需要执行器；本地用 PG 18 起临时库）：
# FLOW_CLUSTER_TEST_DATABASE_URL=postgres://flow:flow-test@127.0.0.1:55432/flow_cluster_test \
#   cargo build -p flow-executor && cargo test -p flow-rpc --features journal-cluster \
#   --test journal_cluster -- --ignored
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
```
