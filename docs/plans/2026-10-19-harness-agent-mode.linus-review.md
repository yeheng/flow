# Linus Review：Harness 节点内置 Agent 模式（复用 conga）实施计划

> 审阅对象：`docs/plans/2026-10-19-harness-agent-mode.md`（待审稿，2026-10-19）
> 审阅者：linus（只 review，不写实现）
> 审阅方式：读计划 + 抽查 flow/conga 源码 + 3 个离线可行性实验（git 依赖子目录 workspace、`workspace = true` 继承、optional path 依赖是否进 lock）
> 结论状态：**不按现状批。目标成立，但实现路径选错；至少 4 处阻断级问题，其中 1 处会破坏已文档化的执行契约。**

---

## 0. 结论先行

**【核心判断】**
**值得做，但不值得按这份计划做。** 问题是真的（用户不该为了 harness 装 kimi/claude/codex），方案也做了认真分析；但计划把「一个外部 agent 运行时」塞进受管执行器进程内部，为了省掉一个侧车二进制，把 flow 已经验证过的**进程边界、取消语义、后代回收、环境隔离**全部换成自己新写的失败面。计划 §7.1 用来排除 `conga exec` 子进程方案的三条理由里，有两条是错的，一条被夸大。最简路径应是「agent 模式 = 用现有的受管理子进程机制跑 conga 无头入口」，而不是在 `flow-executor` 里再养一个 agent 运行时。

**【品味评分】**
**凑合（偏下）。** 加分项：证据索引、失败矩阵、兼容/回滚、对 conga 公开面的核对、对 UX 取舍的坦白，都是真做过功课的样子；事实抽查大多数成立。扣分项：核心边界选型（进程内 vs 进程边界）被错误理由带偏；配置被放在「每次派发现取的全局状态」而不是「run 快照」这一层数据结构上；「command 模式逐字节不变」这类承诺没有对应测试支撑；失败矩阵自称「与现有 harness 一致」但两处语义实为不同。

**【最严重的四件事】**
1. **取消/后代回收契约被破坏**：agent 的 bash 工具执行中不观察取消信号，工具子进程不在受管进程组里；计划却写「与现有 harness 超时/取消一致」，并引 `EXECUTION.md:39` 作边界依据——那一行恰好承诺的是「超时、取消及正常结束时回收后代进程」。见 §3.B1。
2. **env 契约缺失到「功能基本不可用」**：执行器被 `env_clear` 后只剩 `RUST_LOG`（loopback 另有两个 `FLOW_*`，又被计划自己的过滤规则排掉），计划却写「进程环境白名单过滤」。按字面实现，agent 的 `bash` 得到没有 `PATH`/`HOME` 的 shell，模型生成的 `cargo`/`node`/`git` 命令大面积不可用。见 §3.B2。
3. **`_flow_execution.agent` 注入挂在 `run.execution` 分支内**：semantics=1 的 run 根本没有这个对象，agent 节点恒 Business，且错误信息说成「master 未下发配置」，误导运维。见 §3.B3。
4. **授权后失败的 outcome 形状没定义**：凭据缺失/授权后失败要「记 outcome」，但 `harness_output` 强依赖 `body_raw`；不定义 marker 字段，恢复路径会在派生阶段炸，而不是给出「密钥未配置」。见 §3.B4。

---

## 1. 事实核查（不是只读计划）

### 1.1 计划成立的事实

| 计划断言 | 结论 | 证据 |
| --- | --- | --- |
| command 模式请求恰好 5 字段 `{command,args,workdir,timeout_ms,prompt}` | ✅ | `crates/flow-executor/src/task.rs:1252-1263` |
| 回归 fixture 对请求 JSON 严格相等 | ✅ | `crates/flow-executor/src/task.rs:1357-1375` |
| harness 现有恢复路径：有 outcome 直接派生、不重跑 | ✅ | `task.rs:1230-1245` |
| master→executor 现有唯一配置注入点 `ExecuteTask.node._flow_execution` | ✅ | `crates/flow-backend/src/execution/dispatch.rs:327-345`（注入）；IPC 与 remote 共用 `drive_link`（`dispatch.rs:292-311` 两个入口） |
| 执行器由 master `env_clear` spawn，只透传极少变量 | ✅（行号错） | `crates/flow-engine/src/execution_protocol/spawn.rs:52-55`；`loopback.rs:108-113`。计划引的 `spawn.rs:14-23,66-70` 是文档注释与 pre_exec，不是 env_clear |
| 密钥只以 `secret_ref` 进请求、真值经 `OperationPermit`、不落 journal | ✅ | executor：`task.rs:947-955`；master 授权：`dispatch.rs:1303-1316`；resume 重发：`dispatch.rs:398-410` |
| `resolve_node_secrets` 无调用点 | ✅ | 只有 `flow-engine/src/secrets.rs:90` 定义与 `model.rs:346` 注释引用 |
| conga 公开面（`run_agent_loop`/`built_in_tools`/`with_client`/`cleanup_session_resources`/`CancelSignal`）确实可用 | ✅ | `conga/src/agent_loop.rs:29-37`、`conga/src/lib.rs:26-40`；`conga-host/src/tools/mod.rs:23-35`；`conga/src/providers/openai_compat.rs:41-51`、`anthropic.rs:40-50`；`conga-host/src/session_cleanup.rs:41-44`；`conga/src/cancel.rs:24-107` |
| reqwest 可共用：executor 构建 client 传给 conga | ✅ | flow 与 conga workspace 都是 reqwest `0.13`（flow `Cargo.lock:2617-2619`；`/tmp/conga/conga/Cargo.toml` workspace deps） |
| rquickjs 不冲突（optional + feature 关，不进构建） | ✅（实测） | `conga-host/Cargo.toml` 中 `rquickjs` 是 optional `ext-js`；我用同构 mini repo 实测：被关闭的 optional path 依赖**不进 lock、不参与编译** |
| conga git 依赖：仓库根无 `Cargo.toml`、workspace 在子目录 | ✅（实测） | `/tmp/conga` 根无 manifest，workspace 在 `/tmp/conga/conga/Cargo.toml`；我构造 file:// 同构 repo（根无 manifest、workspace 在 `sub/`、成员用 `workspace = true` 继承），`cargo metadata` + `cargo check` 均成功。**这条计划是对的，不用再怀疑** |
| `toml 0.9` / flow `0.8` 双版本 | ✅ | flow `Cargo.lock:3574-3575`；conga workspace `toml = "0.9"` |

### 1.2 计划写错或写偏的事实

| 计划断言 | 实际 | 证据 |
| --- | --- | --- |
| §3.7「`MaxTokens` 截断 → 失败（与 conga 语义一致）」 | ❌ conga 遇 `MaxTokens` 是**把本回合工具调用全部判失败、`emit TurnEnd`、然后 `continue` 下一轮**，不是失败退出；最终 `run_agent_loop` 可能以 `EndTurn` 正常返回 | `/tmp/conga/conga/conga/src/agent_loop.rs:92-117`（`MaxTokens` 分支在 99-116，`continue` 在 116 行附近）；`agent_loop.rs:51` 的 `for turn in 0..max_turns` 结束后直接 `Ok(new_messages)`，耗尽不回错 |
| §3.2 注释暗示 `tool_timeout_secs` 默认 600（「默认对齐 conga 默认值」） | ❌ conga 的 `AgentTunables.tool_timeout_secs` 默认是 `None`（无引擎级工具超时）；600 只是用例/示例里出现的值 | `/tmp/conga/conga/conga/src/types/context.rs:160,177-179`；`conga-host/src/config.rs:73` |
| §3.9「工具 state/spill 落在 `<config_dir>/conga/sessions/...`」 | 实际是 `<home>/.conga/sessions/<session_id>/tool_state/<tool_name>/`，home 由 `dirs::home_dir()` 决定（HOME 为空会回落 passwd） | `/tmp/conga/conga/conga/src/storage/mod.rs:41-50`；`conga/src/agent_loop.rs:491-511` |
| §5「新增依赖改变 `Cargo.lock` → `RUNTIME_ID` 变化」 | 结论是「会变」，但归因错：`build.rs` 哈希 flow-engine/flow-executor **全部 `.rs`** + lock + manifests，任何本计划的实现改动都会改 `RUNTIME_ID`，与是否加依赖无关 | `crates/flow-engine/build.rs:8-25` |
| 附录 A / §2.2 / §2.3 的大量行号 | ⚠️ 系统性漂移，不可照抄：`dispatch.rs:290-314`→实际注入 327-345；`dispatch.rs:1253-1274`→实际凭据解析 1303-1316；`spawn.rs:14-23,66-70`→env_clear 在 52-55；`flow-rpc/src/lib.rs:208`→实际 210 | 见上各项 |
| §3.5「`conga-host` 默认 feature 全关」 | `conga-host` 没有定义 `default`，`default-features = false` 是空操作；真正的作用是不启用 `session-index`/`rag`/`sandbox-landlock`/`ext-js` | `conga-host/Cargo.toml` `[features]` |
| 附录 B「+49 crate」 | ⚠️ 未独立复核（离线环境无法解析 conga 全量依赖）。方向合理（`dom_query`/`html5ever`/`ignore`/`glob`/`regex`/`toml 0.9` 是主要新增），但必须按计划自己说的在 Step 4 记录实测 | — |

---

## 2. 五层分析（摘要）

**数据结构**：核心数据是「一次外部调用的输入字节」（operation request）与「run 的执行身份」（`ExecutionSnapshot`）。计划把 agent 配置放进前者是对的（回归指纹需要），但把它放在「每次派发从 master 全局配置现取」，而不是像 `runtime`/`clock_ms`/`seed`/`definitions` 那样冻结进 run 快照——**同一个 run 的不同节点、恢复后的重派发、重试、回归候选，可能用不同模型配置**。这不是实现细节，是数据结构放错了层。

**边界情况**：计划新增的失败分支（凭据缺失、provider 构造失败、取消、超时、max_turns、MaxTokens）里，至少 4 个没有定义可观察的终态形状；其中「授权后失败」的 outcome 形状直接决定恢复能不能正确派生。边界不是靠"多写几个 if"解决的，是靠先定 outcome 的封闭形状。

**复杂度**：方案的本质是一句话——「agent 模式 = 另一种外部操作后端」。如果它真的只是"另一种后端"，那它应该复用现有外部操作后端的一切：进程管理、取消、回收、回归、指纹、日志。计划实际增加的是：一个库依赖（+49 crate）、一个新运行时配置类型、一个新 `_flow_execution` 键、一套新请求/输出/outcome 形状、一个 10 行的新失败矩阵、一个 3 平台的新构建面。这比"换一个命令"多了一个数量级的面积。

**兼容性**：command 模式的说法（默认值 + 5 字段 + 严格指纹）基本可信，但缺少测试守护；`semantics=1` 流程加 agent 节点会静默落到误导性 Business；回滚要同时删配置（`deny_unknown_fields`，`flow-config/src/lib.rs:55`），否则旧二进制起不来——这一点计划说对了一半。

**实际性**：目标真实。但「在 `env_clear` 的执行器里跑一个需要常规 shell 环境的 coding agent」这个组合，没被当作真问题处理。

---

## 3. 关键问题

### 3.1 阻断（不修不能批）

#### B1. 取消 / 超时 / 后代回收契约被破坏，但计划声称"一致"

- **位置**：计划 §3.7 矩阵第 6 行（「与现有 harness 超时/取消一致」）、§3.5「结束清理…回收持久 shell」、§3.9（引 `EXECUTION.md:39` 作为安全边界依据）。
- **证据链**：
  - conga 的 `bash` 工具**只在开始前**查一次 abort（`conga-host/src/tools/bash.rs:32-33`），执行中不观察 `CancelSignal`；持久 shell 的执行路径（`conga-host/src/tools/shell.rs:179-198`）只有命令级 timeout，没有取消。
  - 持久 shell 的 spawn 是普通 `Command::new("sh")`，没有 `process_group`/`setsid`（`shell.rs:56-70`），只挂 `kill_on_drop`。tokio 的 `kill_on_drop` 只 SIGKILL 直接子进程 `sh`，不会回收进程树；源码注释说的 "reap the tree"（`shell.rs:259-263`）与实现不符。`cleanup_session_resources` 只做 `shell::evict_session` + hooks（`conga-host/src/session_cleanup.rs:41-44`）。
  - flow 现有 command 模式：`capture_harness` 用 `tokio::select!` 监听取消并 `start_kill`（`crates/flow-executor/src/task.rs:1661-1671`），进程由 `ManagedChild::spawn_harness` 放进独立进程组（`crates/flow-engine/src/process.rs:20-21`）；退化为 executor 会话级回收只在 executor 进程被 drop 时发生（`process.rs:109-160`），而正常提交后 executor 会复用槽位（`crates/flow-executor/src/runtime.rs:196`）。
  - 现有 e2e 明确守护这个契约：`crates/backend-e2e/tests/crate_repairs.rs:245-303`（超时/取消后后代进程必须不存活）；`docs/EXECUTION.md:39` 原文承诺「受管理 harness 在超时、取消及正常结束时回收后代进程」。
- **触发条件**：用户取消或节点 `timeout_ms` 到期时，agent 正在跑 `bash`（`cargo build`、`sleep`、`run_in_background` 的后台命令）。
- **影响**：① 若 runner 只是 await `run_agent_loop`，取消不会立即生效，要等当前工具跑完（bash 默认 120s，模型可传更大值）；② 工具派生的子进程会留在 executor 会话里，**槽位复用的后续任务期间仍在跑**；③ 节点被判 Uncertain 后仍可能有进程在改 workdir——这正是 `EXECUTION.md:39` 要排除的情况。
- **修复方向**：优先换进程边界（见 §4）；若坚持嵌入：runner 必须用 `select!` 在取消时立即 drop loop（不能只依赖 `signal` 桥接），并明确承认「工具后代进程仅能等到 executor 被回收时才清掉」，同时修改 `EXECUTION.md` 的承诺与 e2e 预期——但那等于承认能力回退。给 conga 提 patch（`process_group` + 组杀）不在计划「不 fork、不 vendor」范围内。

#### B2. env 契约缺失：按计划字面实现，agent 的 bash 基本不可用

- **位置**：计划 §3.5「`env` = 进程环境白名单过滤（排除 `FLOW_*` / `CONGA_*`，executor 本身已 `env_clear`）。
- **证据**：
  - executor spawn 时 `env_clear`，只回填 `RUST_LOG`（`spawn.rs:52-55`）；loopback 另加 `CONNECT_ADDR_ENV` 与 `SESSION_TOKEN_ENV`（`loopback.rs:108-113`），两者都以 `FLOW_` 开头，会被计划自己的过滤规则排除。
  - 也就是说，executor 里的「进程环境」几乎为空——**没有 `PATH`、没有 `HOME`、没有 `TMPDIR`**。计划说要"过滤进程环境"，但被过滤的源头已经空了；计划没说 `PATH`/`HOME` 从哪来。
  - conga 的 bash 工具把 `ctx.env` 原样交给子进程（`conga-host/src/tools/bash.rs:92-99`，只额外过滤 `CONGA_*`），持久 shell 也是 `env_clear` 后只注入 `ctx.env`（`shell.rs:56-70`）。
  - 结果：`sh` 能用 execvp 默认路径找到，但 shell 里 `homebrew`/`rustup`/`nvm` 安装的工具不在其中；`HOME` 为空时 `cargo`/`git` 行为也会退化。`env_snapshot` 的 `git` 调用同样受影响（`conga-host/src/prompt.rs:144-178`，git 不可用时静默退化成只带日期）。
  - command 模式没暴露这个问题，是因为外部 CLI 自己管理工具链；一旦嵌入，工具链环境就变成 flow 的责任。
- **触发条件**：agent 第一次要跑非 `/bin`/`/usr/bin` 里的命令（macOS 上几乎必然）。
- **影响**：Step 4/5 的"可单测"能过，真实冒烟大概率失败；失败形态还是"模型反复试工具、最后 max_turns"，很难排查。
- **修复方向**：在计划里显式定义 env 契约：谁提供（建议由 master 随 `_flow_execution` 下发一个固定允许名单，或 `[harness.agent] env` 配置）、包含哪些（至少 `PATH`/`HOME`/`TMPDIR`/`LANG`，可加 `RUSTUP_HOME`/`CARGO_HOME`）、禁止哪些（`FLOW_*`/`CONGA_*`/密钥类）。并加三平台 smoke：`command -v cargo`、`echo $HOME`、在 workdir 里 `git status`。**这一条不解决，Step 4 的验收标准无法定义。**

#### B3. `_flow_execution.agent` 注入条件覆盖不到 semantics=1，agent 节点恒 Business 且报错误导

- **位置**：计划 §3.3-2「`DispatchRunner::drive_link` 在写 `_flow_execution` 时…追加 `agent`」；§3.3-3「缺失即 Business（「master 未下发 harness.agent 配置」）」；§5 兼容性。
- **证据**：注入发生在 `if let Some(execution) = &run.execution { ... }` 内（`dispatch.rs:327-345`）；而 semantics=1 的定义在冻结执行上下文时直接返回 `None`（`crates/flow-backend/src/execution_snapshot.rs:52-54`）。因此 v1 run 的节点根本没有 `_flow_execution` 对象。编辑器保存既有定义会保留语义（`docs/EXECUTION.md:11`），所以「在存量 v1 流程里加一个 agent 节点」是正常操作，它会稳定地得到「master 未下发配置」——而真实原因是没有执行快照。
- **触发条件**：v1 工作流 + agent 节点。
- **影响**：误导性运行期错误；用户会去查 `[harness.agent]` 配置（其实配了）；计划 §5 的兼容性承诺没覆盖这个组合。
- **修复方向**：二选一——(a) 在发布期拒绝 `semantics=1` 定义中出现 `mode=agent`（`Definition::validate`，`crates/flow-engine/src/model.rs:627` 已集中处理定义级规则，成本最低）；(b) 让注入不依赖 `run.execution`（但那样就得给 v1 run 定义 agent 配置的来源，等于把问题扩大）。推荐 (a)，并让运行期错误文案区分「v1 不支持」与「配置缺失」。

#### B4. 「授权后失败」的 outcome 形状没定义，恢复路径会炸

- **位置**：计划 §3.4-4「credential 缺失 → 记 outcome(error) 后 Business（视为未发生外部调用，但保持 operation 有终态）」、§3.7 第 8 行「`OperationPermit` 未给 credential → 记 `outcome(stderr=密钥未配置)` 后 Business（operation 不悬空）」。
- **证据**：
  - master 确实可能发出 `credential = None` 的 permit：`get_secret(...).filter(|v| !v.is_empty())`（`dispatch.rs:1311-1313`），所以执行器必须处理。
  - 但 `harness_output` 派生强制读 `outcome["body_raw"]`（`task.rs:1394-1397`），且 `spawn_error` 走的是单独的 marker 分支（`task.rs:1386-1392`，写入在 `1286-1299`）。计划只说记 `stderr=...`，没有 `body_raw`/`exit_code`/marker → 一旦发生恢复（`previous_outcome` 存在 → 派生），派生阶段报的是 serde/`Invalid` 而不是「密钥未配置」；人工裁决逻辑也不认这个半形状 outcome。
  - 同类还有「授权后、provider 构造/reqwest client 构建失败」——计划矩阵没列。
- **触发条件**：发布后删掉/改名密钥；代理或 TLS 客户端构建失败；授权后 executor 侧准备失败。
- **影响**：journal 里出现半形状 outcome；恢复/裁决路径行为未定义。
- **修复方向**：像 `spawn_error` 一样定义一个封闭的 outcome marker（如 `{"agent_error": "...", "stderr": "<tail>"}` 或 `{"stop_reason":"credential_missing"}`），派生函数先处理 marker；把「授权后、开始循环前的所有失败」统一映射到这个终态，并补「落 outcome → 重启 → 派生 Business」的测试。

### 3.2 应尽快（不修会埋坑）

#### R1. agent 配置没进 run 快照——影响外部调用的配置应该冻结，而不是每次派发现取

- **位置**：计划 §3.3-1（`JournalBackend` 持有 `OnceLock<AgentRunConfig>`）、§3.3-2（派发时注入）、§7.4（「配置进 operation 请求」）。
- **事实**：`ExecutionSnapshot` 已经冻结 `runtime`/`clock_ms`/`seed`/`definitions`/`bindings`（`crates/flow-engine/src/journal_state.rs:137-149`），`EXECUTION.md:14` 明确「时间、随机种子、子流程定义和执行器源码/锁定依赖身份记入运行快照」。**模型/端点/限额是同级的外部依赖身份，却没有进快照。**
- **影响**：同一个 run 在配置重启变更后，节点重派发会用新配置；`repeatable` 重试会以新配置重放；`run.regress` 候选继承 baseline 快照却用当前配置构造请求——两种行为都不合"快照固定执行身份"的既有语义。计划 §7.4 用"配置必须进指纹"论证了后者重要，却不给它一个稳定的来源。
- **修复方向**：把 `AgentRunConfig` 加进 `ExecutionSnapshot`（`Option` + `serde(default)`，旧 journal 可读），派发时从 `run.execution` 取，而不是从后端全局 `OnceLock` 取。代价是 RunStarted/证据包多一个字段（计划 §5「不改 journal 格式」要改成「只加可选字段」）。收益：v1 问题自然收敛（无快照 → 发布期拒绝）、回归/恢复/重试语义一致、多 agent 一致。若坚持现取，至少要在计划里写清「配置不属 run 身份」并接受上述漂移 + 测试。

#### R2. §3.8「绝不记 prompt」不成立：conga 自己在 debug 级别打完整请求体

- **证据**：`conga/src/providers/openai_compat.rs:79-82` 与 `anthropic.rs:83-87` 在 `tracing::debug!` 里打 `request_body`（含 system prompt、全部消息、工具定义）；executor 启动就装了 tracing subscriber（`crates/flow-executor/src/main.rs:18-24`），且 `RUST_LOG` 由 master 原样透传给 executor（`spawn.rs:52-55`）。运维一开 `RUST_LOG=debug` 排障，prompt 就进 executor stderr（进而进 master/桌面日志）。此外 conga 的 info 级日志（`agent loop start`、`tool execute/done`，`agent_loop.rs:59,398,478`）会绕过 `NodeLogger` 直接进 stderr。
- **修复方向**：agent 运行路径的日志结论要改：默认 filter 追加 `conga=info,conga_host=warn`（或在 agent 任务期间动态降级），并明确 `RUST_LOG=debug` 与 agent prompt 的冲突；补一条测试：fake provider 下 `RUST_LOG=debug` 跑一轮，断言 executor stderr 不含 prompt 片段。

#### R3. 工具 state/spill 落在 `~/.conga/sessions/<dispatch_id>`，没人清、且后续 run 可读

- **证据**：`tool_state_dir` 用全局 `config_dir()`（`conga/src/agent_loop.rs:491-511`）；spill 写到 `<state_dir>/spill`（`conga-host/src/tools/mod.rs:86-101`）；`cleanup_session_resources` 只 evict shell + hooks，不删目录（`session_cleanup.rs:41-44`）。read 工具允许绝对路径读 config dir 下任意文件（`tools/mod.rs:143-168`），bash 更不受限。
- **影响**：每次 agent run 在用户 home 留下会累积的目录；工具输出/待办状态（可能含 workdir 内容）被后续任意 agent run 读取——这是 command 模式不会创建的持久数据通道。
- **修复方向**：run 结束（成功/失败/超时/取消）删除 `config_dir/sessions/<dispatch_id>`（放在同一个 finally 路径）；或在 Step 4 引入 per-run 隔离根（但 conga 的 config_dir 是全局函数，只能靠事后清理）。文档里明示残留面。

#### R4. `proxy` 没禁 userinfo，代理凭据会进 operation 请求 → journal → 证据包

- **位置**：计划 §3.2 校验规则：「`base_url`/`model` 成对…URL 必须 http(s):// 且禁止 userinfo…`proxy` 若设置必须是合法 http(s) URL」。禁 userinfo 只说了 base_url。
- **影响**：`proxy = "http://user:pass@host:port"` 是常见写法；计划明确要把 agent 配置整包放进 operation request（§3.6），而 operation request 会落 journal，导出证据包默认"不自动脱敏"（`EXECUTION.md` 证据资产段）。等于把代理凭据写进 run 证据。
- **修复方向**：proxy 同样禁 userinfo/query（或至少从 operation 请求/fingerprint 中剔除代理凭据，只留主机端口）。加单测。

#### R5. MaxTokens 语义写错 + 成功判定规则没定

- **证据**：conga 对 `MaxTokens` 是"本回合工具调用全判失败 → `continue`"（`agent_loop.rs:99-116`）；`max_turns` 耗尽后直接 `Ok` 返回（`agent_loop.rs:51` 循环结束）。所以「最终 stop_reason」不一定是失败信号：中间 MaxTokens、最后一轮 EndTurn 的 run 在 conga 看来是**成功**（它把截断当作可恢复回合）。
- **影响**：计划矩阵第 3/4 行按字面实现会出现两种错误之一：把成功的 run 判失败，或把"最后一轮还在调工具"的 run 判成功。
- **修复方向**：在计划里钉死判定：成功 ⇔ 最后一条 assistant 的 `stop_reason == EndTurn`；`Error` → 失败；`Aborted` → Uncertain；末条 `ToolUse` → max_turns 失败；中间 `MaxTokens` 不单独判失败。按这个规则重写矩阵第 3 行并加单测（fake `StreamFn` 造出三种序列）。

#### R6. 测试适配点漏了 `crates/backend-e2e/tests/cli.rs:45`

- **位置**：计划 Step 3 列了 `flow-rpc/src/lib.rs:208`、`web/src-tauri/src/service.rs:120`、`flow-backend/src/bin/flow-journal-dev.rs:201`、`backend-e2e/tests/contracts.rs:29`。
- **实际**：`crates/backend-e2e/tests/cli.rs:45` 也直接调 `flow_backend::start_execution(&execution, &backend)`。改签名会断编译。
- **修复方向**：`grep -rn "start_execution("` 全量列点，写进 Step 3。

#### R7. 条件必填的双源问题，以及跨模式参数被静默忽略

- **事实**：前端表单的必填星号/`（不设置）`只读 descriptor `required`（`web/src/components/ParamsModal.vue:146`、`SchemaField.vue:236,278`）；后端 `validate_params` 已有「从 `required_params` 读，不在这里重写一份」的明确设计声明（`model.rs:820-829`）。计划把条件必填放进 `validate_params`，等于接受一次前端/后端必填来源分叉；计划只提了"两组字段同时展示"的 UX 取舍，没提前端不会有星号、错误只能等保存 400。
- **另一个洞**：agent 模式忽略 `command`/`args`，command 模式忽略 `workdir`/`api_key`。计划说"忽略"，意味着用户填了也不生效、不报错——这是经典的静默失败。
- **修复方向**：`validate_params` 里对"跨模式参数同时出现"直接拒绝（如 agent 模式带 `command`/`args` 且非空、command 模式带 `api_key`），比"忽略"便宜且不会误导；前端不引入新机制可以接受，但 Step 2 要补测试并在 `x-help` 里写清。另注意 `mode` 的枚举校验要明确 `${}` 模板是拒绝还是放行到执行期（现有 required 循环有模板放行规则，`model.rs:840-846`），否则 `mode: "${input.x}"` 行为未定义。

#### R8. `tool_timeout` 默认值与取消延迟上界没写

- **证据**：conga 默认 `tool_timeout = None`（`types/context.rs:160,177`）；bash 工具自己的默认命令超时 120s（`conga-host/src/tools/bash.rs:10,38`）。
- **影响**：若 flow 不显式给 `tool_timeout`，引擎级安全网没有；取消时当前工具最长可跑 120s（模型可传更大）。计划矩阵没写这两个上界。
- **修复方向**：明确 `tool_timeout` 默认（建议 600s 或明确 None + 说明依赖整体 timeout），并写进 §3.7「取消延迟上界 = 当前工具剩余 timeout」。

#### R9. 回归/恢复对配置的依赖没写清

- **事实**：`previous_outcome` 恢复路径不需要配置（先派生输出）；但回归路径要"构造请求 → 比对 fixture 指纹"，agent 请求含 agent 配置，所以**回归模式下缺 `[harness.agent]` 就无法比对指纹，会 Business**——即使 0 网络。计划 §3.7 只写「回归模式 0 网络调用」，没写「仍需配置」。
- **修复方向**：矩阵补一行「回归模式 + 配置缺失 → Business（无法构造指纹）」，并补测试。

### 3.3 可选（不阻塞，但建议顺手处理）

- **O1（行号）**：附录 A 的行号系统性漂移（见 §1.2）。计划是实施依据，行号错会让实现者照抄到错的函数。建议 Step 0 用 `grep -n` 重刷全部引用。
- **O2（归因）**：`RUNTIME_ID` 变化不是"新增依赖"造成的，任何 executor 改动都会变（`build.rs:8-25`）。发布说明改成「本次改动需要 executor 与 master 锁步升级」更准确。
- **O3（工具装配）**：用工具名过滤 `built_in_tools()`（去掉 `spawn_subagents`/`evolve`）能工作，但按字符串过滤脆弱；`conga_host::tools` 的模块是 `pub` 的，直接 `read::tool()` 等构造更明确。另外 `subagent::tool(None)` 走的是 `NoopSubagentSpawner`（`conga-host/src/tools/subagent.rs:15-16`），错误可预期；`CODING_AGENT_PROMPT` 没宣传 subagent/evolve（只在未启用的 Plan 模式提示里提到，`prompt.rs:93`），所以提示词与工具集一致，这点没问题。
- **O4（trace_granularity）**：agent 模式仍标 `process`（`model.rs:306-311`）名不副实——它没有子进程可追溯，证据在 operation + 有损 observations。语义上说得通（沿用 `EXECUTION.md:41` 的进程级证据声明），但前端会显示"进程级"。可留，文档写一句。
- **O5（文案）**：`api_key` 复用的 secret widget 帮助文案还是「服务端未配置 `FLOW_SECRET_*` 环境变量」（`web/src/components/SchemaField.vue:292-294`），与 stored secret 现状不符；agent 节点用户会看到过时文案。
- **O6（`env_snapshot`）**：它是 `pub async fn`（`prompt.rs:144`），且输出含 workdir 的 git status/diffstat。它只进 system prompt、不进 operation 请求，所以不影响回归指纹；但会让同一个定义的两次真实运行 prompt 不同（可接受）。建议按计划 §8 先注入一次并实测首 token 延迟；注意无 PATH 时 git 会静默退化。
- **O7（`max_turns` 默认）**：保持 50 可以（真约束是 `timeout_ms`，上限 1h）；但要在文档写明「max_turns 耗尽 = 失败，且末轮工具副作用已发生」，避免用户以为它等价于回滚。
- **O8（publish 期配置校验）**：不建议在 `workflow.update` 硬拦「未配置 `[harness.agent]`」——定义可迁移到另一台 master，配置是环境态；运行期 Business + 可操作文案即可。但可以像 `missing_secrets` 一样给前端一个 warning 通道（不阻断）。
- **O9（outcome 里放 `agent` 元数据）**：计划选择 outcome 与 output 都放是对的——只放 output 会让恢复派生丢失元数据。这条自问自答成立。

---

## 4. 更简单 / 更正确的做法

### 4.1 首选：把 agent 当"另一种外部操作后端"，用现有受管理子进程跑 `conga exec`

计划 §7.1 已经考虑过这条路，但拒绝理由经不起核对：

| 计划拒绝理由 | 核对结果 |
| --- | --- |
| 「侧车需要额外分发二进制」 | 真实成本，但 flow 已有 `flow-executor` 的定位/打包机制（`ExecutorInvocation::locate`），多一个二进制是已知问题，不是新问题。 |
| 「配置/密钥只能进子进程 env」 | 不构成缺陷：进程内方案里 key 也在 executor 内存；子进程 env 的 key 会被 conga 自己从工具 shell 里过滤掉（`conga-host/src/tools/shell.rs:96` 过滤 `CONGA_*`），而 operation 请求/journal 里仍然只有 secret_ref。安全性至少不差。 |
| 「conga Host 会用 `~/.conga/settings.json` 覆盖 provider」 | 真问题，但可解：给子进程一个隔离的 `HOME`（计划自己也写了「子进程方案需隔离 HOME」）。注意**嵌入方案同样落 `~/.conga`**（工具 state 走全局 `config_dir()`，`agent_loop.rs:491-511`），所以这条不是子进程独有。 |
| 「无法复用 flow 的 OperationPermit 与取消语义」 | **不成立。** permit 流程完全不变（executor 仍 `authorize_operation` 拿 credential，只是把它写进子进程 env）；取消/超时反而**更好**——`ManagedChild::spawn_harness` 的进程组 + `capture_harness` 的取消/超时/后代回收是现成的（`process.rs:20-21`、`task.rs:1661-1671`），比进程内嵌入更能满足 `EXECUTION.md:39`。 |

**具体形态**：`mode=agent` 分支把 `conga exec --json "<prompt>"` 当命令，workdir 用节点参数，config/key 从 `_flow_execution` + permit 注入子进程 env，stdout 解析 NDJSON 取最终文本 + usage/stop_reason，其余（授权、outcome、journal、回归指纹、超时、取消、后代回收、恢复）**全部复用 command 模式现有代码**。flow 侧改动从"新增运行时 + 10 条失败路径 + 49 个 crate"缩到"一条分支 + 一个 NDJSON 解析 + 配置注入"。代价是侧车分发与 conga CLI 输出契约（rev 已 pin，可控），换取失败矩阵几乎不变、进程边界免费。

注意这条路也有两个必须验证的点：`conga exec` 的 `SessionAssembly` 会加载 `~/.conga/settings.json`（隔离 HOME 解决）；conga 的 `config_file::apply()` 会加载 cwd 的 `.env`（cwd = workdir，dotenvy 不覆盖已有 env，但会补充未设置的 `CONGA_*`，需实测确认无副作用）。这两点比"重写工具/进程内失败矩阵"便宜得多。

### 4.2 若坚持嵌入：先补齐"等价外部 CLI"的合同，再谈实现

嵌入方案不是不能做，但不能一边说"等价现有外部 CLI harness"，一边丢掉它已有的合同。最低要求：
1. 取消用 `select!` 立即 drop loop（不依赖 `signal` 桥接），并给出工具耗时上界；
2. 后代进程：要么接受"只能等 executor 回收"并改 `EXECUTION.md:39` 与 e2e 预期，要么给 conga 提 patch（与"不 fork"冲突，等于重新选型）；
3. env 契约（B2）；
4. session 目录清理（R3）；
5. tracing filter（R2）；
6. outcome 封闭形状（B4）+ MaxTokens/max_turns 判定（R5）。

如果这 6 条都要做，嵌入方案的净复杂度已经超过子进程方案——这正是选型结论。

### 4.3 `mode` 参数 vs 新节点类型

用户已拍板 `mode`，不推翻。但计划对代价只写了一半：descriptor 的 `required` 无法表达条件必填，导致前端/后端必填来源分叉（R7）；建议用"发布期拒绝跨模式参数"把静默忽略变成显式错误，把分叉控制在可接受范围。**更好的数据模型确实是一个新节点类型**（静态 required、独立 trace/证据语义），但那与用户决定冲突，作为记录即可，不阻塞。

### 4.4 配置注入点

`_flow_execution` 是现有唯一注入点，选它没错；错的是**来源**。应冻结进 `ExecutionSnapshot`（R1），派发时从快照取。这样 `_flow_execution.agent` 与 operation 请求里的 `agent` 是同一份冻结值的两次序列化，测试里断言两份 JSON 相等即可防漂移。

---

## 5. 验证缺口（测试矩阵要补的）

计划 §6 的 8 条覆盖了 happy path 和部分失败，但下列路径不补，等于没验证：

1. **取消语义**：agent 在 `bash` 中（`sleep`/`cargo build`）时取消 → 多久生效？executor 槽位何时释放？——对照 `crate_repairs.rs:245-303` 写一个 agent 版。
2. **后代回收**：bash 起 `(sleep 100; touch marker) &` 后取消/超时 → marker 不允许出现（现有契约测试的 agent 版）。
3. **env 契约**：三平台 smoke，断言 agent 的 shell 能看到 `PATH`/`HOME`，`command -v cargo`（或平台等价物）成功；断言 `FLOW_*`/`CONGA_*` 不可见。
4. **凭据缺失**：permit credential=None → outcome 形状合法 → 重启/重放派生 Business 且文案正确；journal 里无 key。
5. **授权后、provider 前失败**（client 构建/配置反序列化）→ 同一个封闭终态。
6. **v1 + agent 节点**：发布期拒绝（若选 B3-a）或明确的运行期文案；不能是"master 未下发配置"。
7. **回归**：baseline agent → candidate fixture 匹配且 mock server 命中数 = 0；配置改一项 → 明确失配；缺 `[harness.agent]` → 明确 Business。
8. **MaxTokens 三种序列**：中间截断后恢复成功（应成功）、最后一轮截断（按定死的规则判）、末轮 ToolUse（max_turns 失败）。
9. **取消早于第一次 provider 请求**；**取消在 permit 等待期间**（现有 `authorize_operation` 已处理，回归一下）。
10. **secret 泄漏扫描**：journal + observations + **executor stderr（含 `RUST_LOG=debug`）** + tool state/spill 文件 + 错误字符串，全部不得出现 key 真值、不得出现 prompt 正文（plan §3.8 的承诺）。
11. **proxy userinfo**：拒绝；且 journal/证据包里不出现代理凭据。
12. **并发**：同一个 executor 进程里两个 agent 派发并发（不同 session）→ shell/state 不串；一个取消不影响另一个。
13. **command 模式逐字节不变**：目前只有代码事实，没有测试。补一条直接断言 operation request 序列化恰好是 5 个字段/固定顺序（或至少断言与基线字面量相等）的测试；`harness_and_email_require_their_params`（`model.rs:1615-1640`）和 `secret_params_are_the_expected_keys`（`model.rs:1505-1519`）也要按新语义更新（后者硬编码"只有 Email 有 x-secret"，加 Harness 后必须改）。
14. **remote wire**：Execute 帧里 agent 节点带 `_flow_execution.agent`、command 节点不带；旧执行器被 `RUNTIME_ID` 拒绝（v2 run）。
15. **配置面**：旧 flow.toml（无 section）能解析；merge_patch 整节替换；`config_view` 往返；`deny_unknown_fields` 下"旧二进制读新配置会拒绝"的回滚说明。
16. **前端**：两模式字段同屏、保存时后端条件必填错误能显示；`api_key` 候选来自 `secrets.list`（含 stored 来源）；Settings 保存/重启生效。

---

## 6. 回答计划 §8 的待决问题

1. **`max_turns` 默认 50 还是 25**：保持 50 可以。真实墙钟约束是 `timeout_ms`（默认 5min，上限 1h），`max_turns` 只是逻辑上界；但要写清"耗尽即失败、末轮工具副作用已发生"。真正该补的是 `tool_timeout` 默认值（R8）。
2. **`env_snapshot` 是否每次注入**：建议注入一次并实测首 token 延迟；注意无 PATH 时它会静默退化成只有日期（`prompt.rs:153-156`），所以它不能替代 B2 的 env 契约。
3. **`workflow.update` 是否对 agent + 未配置报错**：不建议硬拦（定义可迁移到别的 master，配置是环境态）；保持运行期 Business，但文案要区分"配置关闭/缺失"与"v1 不支持"。（B3）
4. **`agent` 元数据是否只放 output**：保持 outcome + output 两处。只放 output 会让恢复派生时丢元数据（恢复只读 outcome），这是数据流向决定的，不是体积问题。

---

## 7. 必须修改 / 建议修改 / 可忽略

### 必须修改（阻断，改完再谈批准）
1. **B1**：承认进程内嵌入不满足 `EXECUTION.md:39` 的取消/后代回收契约；改用受管理子进程跑 agent（首选），或在计划里明确降级合同 + 用 `select!` 立即中止 + 更新契约文档与 e2e 预期（不推荐）。
2. **B2**：定义 agent 运行时的 env 契约（来源、允许名单、禁止名单），并写进 Step 4 的可验证标准；补三平台 smoke。
3. **B3**：处理 semantics=1：发布期拒绝 agent 节点（推荐）或改注入点；运行期错误文案不得误导。
4. **B4**：定义"授权后失败"的封闭 outcome 形状（marker）与派生分支，覆盖凭据缺失和 provider 前置失败；补恢复测试。
5. **R5**：按 conga 真实语义重写 MaxTokens/max_turns 判定规则（成功 ⇔ 末条 assistant `stop_reason == EndTurn`），并改写 §3.7 矩阵。

### 建议修改（不阻塞，但应在 Step 1-4 内完成）
6. **R1**：配置进 `ExecutionSnapshot`（或明确写出"配置不属 run 身份"及漂移/测试）。
7. **R2**：修 tracing 泄漏面（默认 filter + `RUST_LOG=debug` 说明 + 测试）。
8. **R3**：清理 `~/.conga/sessions/<dispatch_id>`；文档写明残留面。
9. **R4**：proxy 禁 userinfo/query，或从证据/fingerprint 剔除代理凭据。
10. **R6**：补 `backend-e2e/tests/cli.rs:45` 适配点。
11. **R7**：跨模式参数发布期拒绝；`mode` 模板策略明确。
12. **R8**：明确 `tool_timeout` 默认与取消延迟上界。
13. **R9**：矩阵补"回归 + 配置缺失"一行。
14. 更新 `secret_params_are_the_expected_keys` / `harness_and_email_require_their_params` 与 nodetypes 快照；补 command 请求字节不变的直接测试。

### 可忽略（记录即可）
15. 附录行号漂移（O1）：实施前重刷一遍即可，不影响方案。
16. `RUNTIME_ID` 归因措辞（O2）。
17. 工具装配按名字过滤（O3）——能工作，且 prompt 与工具集一致。
18. `trace_granularity=process`（O4）与 secret widget 文案（O5）。
19. `max_turns` 默认 50、`env_snapshot` 一次注入、outcome 双处放 `agent` 元数据：原计划判断成立。
20. `+49 crate`/构建体积：方向合理，Step 4 实测记录即可（不作为门禁）。

---

## 8. 一句话总结

**这不是"能不能复用 conga"的问题，是"把 agent 放在哪个进程里"的问题。** 放错边界的代价不是多写几百行，而是把 flow 已经用测试和文档钉死的取消、进程回收、环境隔离合同重新变成"计划里的承诺"。把 agent 放回进程边界，这份计划能砍掉一半以上；砍完剩下的那部分，才是真正值得 review 的实现细节。

---

# 复审（v2）

> 审阅对象：重写后的 `docs/plans/2026-10-19-harness-agent-mode.md`（v2，402 行，2026-10-19）
> 范围：只复核 v2 delta 与 v1 review 的 B1-B5、R1/R5 是否真正闭合；不重复上一轮完整分析。

## 结论

**仍需改。方向已经对了（进程边界 + 快照冻结 + 封闭 outcome + env 契约），v1 的主要设计债基本还清；但有 4 条必须先闭环，其中 2 条是 v2 新引入的泄漏/机制问题，2 条是"声称已修但机制不成立"。修完 V1-V4 即可批。**

下面所有判断都重新抽查了 flow 与 `/tmp/conga` 源码；没有采信 §9 的自我处理表。

## v2 已独立核实成立的部分（不需要再动）

- **B1 进程边界结构性闭合**：`ManagedChild::spawn_harness` 建独立进程组（`flow-engine/src/process.rs:20-21`），`capture_harness` 在超时/取消分支 `start_kill` 走组杀（`flow-executor/src/task.rs:1661-1671`），`ManagedChild::Drop` 对 harness 组发 `SIGKILL`（`process.rs:109-160`）；runner 内 conga 派生的 `sh`/后台命令/MCP 子进程都继承这个组。补 agent 版 `crate_repairs` 即可，逻辑不需要新写。
- **runner 装配面与计划一致**：`SessionAssembly::build_cli(mode, approver, resume, hooks, tools)` 签名对得上（`conga-host/src/assembly.rs:283-301`）；`Host::run_turn` 返回 `TurnSummary{reason,new_messages}`（`conga-host/src/lib.rs:378-385,599-602`）；`TurnEndReason` 三态与计划一致（`conga/src/types/session_event.rs:56-60`）；`ConfigLoader::load` 读 `CONGA_LLM_*` 并 `dotenvy::dotenv()`（`assembly.rs:21-33`），runner "先设 env 再装配"的顺序成立；`CONGA_PROJECT_DIR` 确实存在并覆盖项目根（`conga-host/src/lib.rs:76-85`），`assemble_host` 用它做 cwd / AGENTS.md 根（`assembly.rs:349`）。
- **R5 判定可实现**：`Host::run_turn` 的 `Completed` 只区分 signal 与"最后一条 assistant 是 `Error`"（`lib.rs:516-560`）；末条 `ToolUse`（max_turns 耗尽）和中间 `MaxTokens` 都落在 `Completed`。所以计划"按 last_stop_reason 再细分"是必要且可行的，`TurnSummary.new_messages` 足够，不必依赖事件流。
- **R1 快照兼容性成立**：`ExecutionSnapshot` 无 `deny_unknown_fields`，加 `Option`+`serde(default)` 旧 journal 可读；子 run 克隆父 `execution`（`flow-backend/src/execution/dispatch.rs:1134`）也会继承。
- **调用点/打包清单完整**：`start_execution` 5 个调用点已列全（含 `backend-e2e/tests/cli.rs:45`）；tauri `externalBin`（`web/src-tauri/tauri.conf.json:31`）、`scripts/build-desktop-executor.sh`、`scripts/build-journal.sh` 的 `BINS`、CI 的 `cargo build --workspace --bins` 都覆盖到了新 runner。
- **journal FORMAT=4 不 bump 可接受**：旧程序能打开新 journal，只是忽略 `agent` 字段；agent 节点会退化成 command 语义后 Business，不会静默跑错。但 §6 要写明"降级后 agent 节点不可运行"（见 A8）。
- **runner vs conga CLI 的取舍成立**：类型化契约、密钥走 stdin、fake `StreamFn` 测试缝、不跨仓分发 conga 二进制、`session_id` 可控，这些是 CLI 路径拿不到的；多一个 flow 二进制与多分发 conga 二进制的打包成本相当，真正的区别是契约所有权。**成立，不推翻**。但"体积/构建代价被隔离"不成立（A2）。

## 阻断项（必须修，带最小修法）

### V1（R1 未真正闭合）：回归候选不会继承 baseline 的 agent 配置

- **证据**：`run_regression_with_fixtures` 克隆 baseline 的 `execution`（`flow-backend/src/regression.rs:35-39`）并把它作为 `regression` 参数传给 `start_with_regression`（`regression.rs:141`）；但 `run_start` 先用**当前 master 配置**调 `freeze_execution`（`journal_commands.rs:614`），regression 分支只覆盖 `clock_ms/seed/regression`（`:630-636`），**没有拷贝 `agent`**。所以 §4.2 "回归候选 run 克隆 baseline 快照（regression.rs:34-40）自然继承 baseline 配置" 不成立。
- **触发/影响**：baseline 之后改过 `[harness.agent]`（改模型/端点/限额）再跑 `run.regress`，候选请求用新配置构造 → fixture 指纹失配 → 回归报"request differs from recorded fixture"，正是 R1 要消除的漂移。
- **最小修法**：regression 分支补 `snapshot.agent = recorded.agent.clone();`（或让 `freeze_execution` 在 regression 时优先用 recorded），并在 plan Step 5 显式列出；补测试「baseline 后改配置 → 候选仍匹配且 0 runner 调用」。

### V2（新泄漏）：workdir 的 `.conga/hooks.json` 可以外带 API key

- **证据**：`Host` 装配会加载 `<workdir>/.conga/hooks.json`（装配点在 `conga-host/src/assembly.rs:367`；加载函数 `conga-host/src/process_hooks.rs:91-95,187-198` 明说 project hooks），钩子用 `sh -c` 执行且**没有 `env_clear`**（`process_hooks.rs:275-285`），继承 runner 进程环境。v2 §4.4 把 `CONGA_LLM_KEY` 放在 runner env（§4.8 也写"runner 内只写入 `CONGA_LLM_KEY` 环境"），而 §4.8 的"子进程 bash 会过滤 `CONGA_*`"只覆盖 conga 的 bash 工具，**不覆盖 process hooks**。
- **触发/影响**：workdir 指向一个含 `.conga/hooks.json` 的仓库（例如 agent 去审查/修改第三方项目）→ 第一次工具调用前钩子自动执行，可读 `CONGA_LLM_KEY` 并外带，不需要模型配合。这直接推翻"密钥只经 stdin/内存、模型不可见"的声明。
- **最小修法**：(a) executor 只把 key 放进 runner 的 stdin JSON，不放 runner env；(b) runner main 若必须用 env 让 conga 读，就在 `build_cli` 成功后、`run_turn` 之前 `std::env::remove_var("CONGA_LLM_KEY")` —— 此时 provider 已在 `HostConfig` 内存里，temp HOME 下 settings 为空，`resolve_turn_provider` 走内存 cfg（`lib.rs:201-218`），逐轮不需要 env；(c) 测试：workdir 放一个把 `env` 落盘的 hook，断言无 key。顺带在 §4.8 明示 hooks/MCP/skills/subagents 是 Host 装配带来的有效执行面。

### V3（HOME 语义自相矛盾，决定 R3 生死）

- **证据**：§4.4 写 "`HOME`、`XDG_*`、`TMPDIR`、`PATH`、`LANG` 等来自 executor 白名单（§3.7）；`cwd = 临时 HOME`"；§4.8 写 "临时 HOME…作为 runner 的 `HOME/XDG/TMPDIR` 与 `cwd`"。两处对 `HOME` 的来源相反。
- **影响**：若按 §4.4（HOME=真实用户目录），`conga::storage::config_dir()`（`conga/src/storage/mod.rs:41-50`）会回到 `~/.conga`：settings.json 重新覆盖 provider（v1 里反对 conga CLI 的原始理由回来了）、state/spill 重新落用户 home → **R3 未修**；若按 §4.8（HOME=临时），则 `~/.gitconfig`、`SSH_AUTH_SOCK` 等用户工具链配置不可用，需要显式写清取舍（CARGO_HOME/RUSTUP_HOME 可在白名单里，git/ssh 不行）。
- **最小修法**：统一为"临时 HOME + 显式透传 CARGO_HOME/RUSTUP_HOME/PATH 等"，删掉 §4.4 中 HOME/XDG/TMPDIR "来自白名单" 的表述，并在 §8 记录 git/ssh 配置不可用的取舍与测试。

### V4（数据模型）：`runner_bin` 的归属未定，按现文字会进快照/指纹

- **证据**：§4.2 把 `runner_bin` 写进 `[harness.agent]`，又说 `HarnessAgentConfig` 与 `AgentRunConfig`"字段一一对应"→ 照此实现，`runner_bin` 会进 `ExecutionSnapshot`、`_flow_execution.agent`、operation 请求与回归指纹。§4.5 的请求示例又没列它。
- **影响**：部署本地路径进了 run 身份（远端/多 agent 部署时该路径可能不存在）；换机器回归/重跑会因路径不同而失配；operation 证据里多一个无外部语义的本机路径。
- **最小修法**：明确 `runner_bin` 只是 executor 侧部署参数，**不进** `AgentRunConfig`/快照/请求/fingerprint（用两个类型强制，而不是靠注释）。

## 建议项（不阻塞，实现期约束）

- **A1（subagent stderr 泄漏内容）**：`build_cli` 硬编码 `eprintln!("[subagent] {ev:?}")`（`assembly.rs:307-309`），`SubagentEvent` 含 task/thinking/content/tool arguments/output（`subagent_types.rs:42-72`）→ 进 runner stderr，`capture_harness` 取 512B 尾写进 outcome/journal，与 §4.8「不记 prompt」冲突。建议改用 `SessionAssembly::build`：它接受 no-op `approval_emit`/`subagent_emit`、显式 `store_root`/`session_id`、`CONGA_GATEWAY_MODE=full-auto`（`assembly.rs:199-206,212-215`），顺带让 stdin 里的 `session_id` 真正生效；若坚持 `build_cli`，就在文档/测试里承认 subagent stderr 尾部可能含内容。
- **A2（"体积隔离"打折）**：runner 依赖 `flow-engine` 会把 rquickjs/interprocess/tokio-tungstenite/aes-gcm 一起拉进 runner（`flow-engine/Cargo.toml`）。契约类型建议放 `flow-dto`（现依赖只有 serde/serde_json/chrono）或新小 crate；至少不要用"runner 体积被隔离"当卖点。
- **A3（command 模式环境有变）**：env 白名单加在 executor spawn 上，`command` 模式的 harness 子进程也会继承 PATH/HOME（今天它们只看到 env_clear 后极小环境）。§6 的"command 模式全不变"应限定为"请求/输出/错误语义不变，子进程可见环境有变"，并更新 `spawn.rs:50-52` 的 T6 注释；另 runner 的 spawn 必须显式 `env_clear` 再注入白名单+`CONGA_*`（否则 executor 的 `FLOW_CONNECT_ADDR/FLOW_SESSION_TOKEN` 会进 runner），Step 4 要写出这个实现点。
- **A4（workdir/runner_bin 校验）**：runner 的 `project_dir` 是 env 原样（`lib.rs:79-85`），相对 workdir 会按 runner 的 cwd（临时 HOME）解析；执行前校验 workdir 为绝对路径且存在、runner 可执行，放在授权前（不产生 OperationIntent）。
- **A5（runner 装配失败形状）**：`ConfigLoader`/`build_cli` 返回 Err（provider env 不全、HOME 不可写等）是"runner 正常退出但失败"，应固定为 exit 2 + 信封 `error`，executor 记为 outcome A（exit≠0）+ stderr；矩阵补这一行。
- **A6（信封 error 映射）**：信封 `error` 要落到 outcome 的 `stderr`，否则 Business 文案只有"退出码 1"。`agent_error`/`spawn_error` 的派生顺序保持 v2 不变。
- **A7（测试基建）**：agent 版 e2e 需要能定位/构建 runner；现有 `flow-test-support` 只有 `executor_bin()`（`crates/flow-test-support/src/io.rs:237-247`），本地 `cargo test -p backend-e2e` 不先 `cargo build --workspace --bins` 会得到"runner not found"Business。在 plan 的验证命令里写清，或加测试辅助。
- **A8（降级说明）**：journal FORMAT=4 不 bump 可以，但 §6 要写"旧二进制能打开新 journal、会忽略 agent 配置；agent 节点在降级窗口内不可运行（Business），回滚前先改回 command"，否则运维会误判回滚成功。

## 可忽略 / 记录即可

- `CONGA_PROJECT_DIR`、`build_cli`、`TurnEndReason`、`ExecutionSnapshot` serde 兼容、`start_execution` 调用点、tauri/脚本/CI 打包清单，均已核实与 v2 一致。
- workdir 的 `.conga/skills`、AGENTS.md、subagents 都是"复用完整 conga harness"的组成部分：在文档里列一次有效工具/提示词面即可（MCP 因临时 HOME 自动为空；external tools 需要 `CONGA_EXTERNAL_TOOLS`，未设置即无）。
- stdin 里的 `session_id` 在 `build_cli` 下没被使用；采用 A1 的 `build` 入口后自然生效，否则删掉该字段以免误解。
- 临时目录在 executor 被强杀时可能残留（正常/超时/取消路径已覆盖），OS tmp 兜底，不列为问题。

## 一句话

**v2 把"放错进程边界"这个根本问题修掉了，剩下的都是小修；但 V1 是"声称已修、机制为假"，V2 是"新引入的密钥外带路径"，V3 是"两节自相矛盾"，V4 是"部署路径混进 run 身份"——这四条不补，批了就是把上一轮的教训原样再交一次学费。**

---

# 终审（v3）

> 审阅对象：`docs/plans/2026-10-19-harness-agent-mode.md` v3（442 行）
> 范围：只核对 v2 复审的 V1-V4 与 A1/A3/A5/A6/A8 是否真正闭合、以及是否引入新阻断；不重复完整 review。

## 结论

**结论：可批**（附 1 条硬性前置与 2 条实现约束）。v2 的 V1-V4 都已从"声称"落到可执行的机制/步骤，A1/A3/A5/A6/A8 的修法成立且我抽查过对应 API 可用性。唯一需要作为 Step 3 硬性前置的是 **Windows 下的 conga `config_dir()` 隔离**：v3 的"文档记录、后续可选 patch"不够，因为 Windows 上它不是"可能"，而是确定不认 `HOME`，会静默让用户 conga settings 覆盖 flow 配置。把这条改成 fail-loud 自检或明确 Windows V1 不支持（或把 `CONGA_CONFIG_DIR` patch 提到必做），即可批准。

## V1-V4 / A1/A3/A5/A6/A8 独立核对

| 项 | 结论 | 证据 |
| --- | --- | --- |
| **V1** 回归继承 baseline agent 配置 | ✅ 真闭合 | 计划 §3.2:63、§4.2:119、Step1:309 都写明 `snapshot.agent = recorded.agent.clone()`，且矩阵/测试项（:350,:372,:377）覆盖"配置重启后变更仍匹配"。修法位置与代码事实一致：`regression.rs:35-39,141` 把 baseline execution 传入，`journal_commands.rs:614,630-636` 正是需要补拷贝的分支。 |
| **V2** workdir hooks 读不到 key | ✅ 真闭合（前提见 N2） | 计划 §4.4:164 明确 key 只走 stdin、`SessionAssembly::build` 返回后立刻 `remove_var("CONGA_LLM_KEY")` 再 `run_turn`，Step3:322,324 有实现点与 hooks 测试。可行性已核：`SessionAssembly::build` 会把 provider（含 key）放进 `HostConfig` 内存（`assembly.rs:199-282`），临时 HOME 下 settings 为空 → `resolve_turn_provider` 走内存 cfg（`conga-host/src/lib.rs:201-218`）；`context.env` 在 `run_turn` 起始才从 `std::env::vars()` 采样，移除后不含 key。hooks 的 `sh -c` 无 `env_clear`（`process_hooks.rs:275-285`）但运行发生在移除之后。 |
| **V3** HOME 语义统一 | ✅ 真闭合 | §4.4:161-162（executor 白名单含真实 HOME；runner 覆盖 HOME/XDG/TMPDIR/CARGO_HOME 为临时目录；PATH/RUSTUP_HOME 透传）、§4.7:270,275,278-279（git/ssh、私有 registry 取舍）、§8.5 一致，不再自相矛盾。 |
| **V4** `runner_bin` 归属 | ✅ 真闭合 | §4.3:148-152 + §4.2:117（"配置里没有 runner_bin"）+ §4.5:196 + Step1:307 一致：只由 executor 本地 `FLOW_HARNESS_AGENT_BIN`/同目录兄弟解析，不进 `AgentRunConfig`/快照/请求/指纹；`FLOW_HARNESS_AGENT_BIN` 的透传在 §4.7:273 与 Step2:315 有落点。 |
| **A1** 不用 `build_cli`，改 `SessionAssembly::build` | ✅ 可用 | `SessionAssembly.host` 是 `pub host: Host`（`assembly.rs:138-139`）；`build` 的 `store_root/session_id` 可用 dispatch_id（`SessionManager::resume` 对空会话返回空历史，`session.rs:149-157`），`CONGA_GATEWAY_MODE=full-auto` 被 `Mode::parse` 接受（`permission.rs:22-30`）；no-op approval/subagent emit 类型匹配。计划 §4.4:166-168、Step3:322 写清。 |
| **A3** command env 超集 + runner env_clear | ✅ 写清 | §4.6:244、§6:358 明确"command 请求/输出不变，子进程可见环境变超集"；Step4:335 明确 runner spawn `env_clear` 后显式 env。 |
| **A5/A6** 装配失败/非法信封/error 映射 | ✅ 写清 | §4.5:221-229（outcome B 含"输出不可解析/版本不匹配"；装配失败 → 信封 `error`+退出 1 → outcome A）、矩阵:251-253、Step4:337。 |
| **A8** 旧 master 降级失败 | ✅ 写清 | §6:362 明确"agent 节点缺 `_flow_execution.agent` → agent_error；旧 master 路径因缺 command 明确失败"，回滚说明:363-365 完整。 |

## 新的阻断/前置

### N1（硬性前置，Windows 隔离）：`HOME=临时目录` 在 Windows 上不生效，会静默用用户 conga settings 覆盖 flow 配置

- **证据**：conga 的 `config_dir()`（`conga/src/storage/mod.rs:41-50`）用 `dirs::home_dir()`；`dirs 6` 在 Windows 上 `home_dir()` 取 `FOLDERID_Profile`（`dirs-6.0.0/src/lib.rs:60-76` 的文档与实现），**不读 `HOME`/`USERPROFILE` 环境变量**。因此 Windows 上：runner 的临时 HOME 不能隔离 conga；`Host::run_turn` 每轮从 `config_dir()/settings.json` 重解析 provider（`conga-host/src/lib.rs:197-218,423-430`），用户机器上只要存在 conga settings，就会**静默覆盖** `[harness.agent]` 的 base_url/model/key；tool state/spill 也会落到真实用户目录（R3 在 Windows 未修）。
- **计划现状**：§4.8:287 写"可能走系统 Known Folder……文档记录并用后续可选 conga patch `CONGA_CONFIG_DIR` 彻底解决"——把确定性行为写成了"可能"，且用文档兜一个"静默用错 provider"的路径；Windows 是桌面产品目标平台，冒烟也覆盖，不能只靠文档。
- **最小修法（三选一，写进计划/Step 3 验收）**：
  1. runner 启动自检：设置 HOME 后比较 `conga::storage::config_dir()` 是否落在临时根内；不落在 → 输出 `agent_error`（"当前平台 conga 状态无法隔离，拒绝运行"），不进入 `run_turn`；
  2. 明确 agent 模式 V1 不支持 Windows（executor 检测平台 → Business，文档/前端提示）；
  3. 把本仓库（conga 是自家 repo）的 `CONGA_CONFIG_DIR` patch 从"后续可选"提到 Step 3 必做（固定 rev 更新），runner 显式指向临时目录。
  推荐 1 或 3；无论选哪个，不能保留"文档记录"作为终点。

### N2（实现约束）：`remove_var("CONGA_LLM_KEY")` 必须发生在没有其他线程读 env 的时刻

- **问题**：计划让 runner "建 runtime → 调 lib"，lib 内部 `SessionAssembly::build`（async）返回后再 `remove_var`。若 runner 用多线程 tokio runtime（默认 `#[tokio::main]`），此时 worker/blocking 线程已存在，`std::env::remove_var`（以及任何 set_var）按 std 约定不是线程安全的，属于应避免的未定义行为路径。
- **最小修法**：Step 3 明确 runner 使用 `new_current_thread` runtime（单轮任务，无并发需求；conga 的 spawn 在单线程 runtime 同样可跑），或先在 current_thread runtime 里 `block_on(build)`、drop runtime、在主线程 `remove_var`，再起 run_turn；并把"移除发生在前"写进测试断言。
- 这是 A1/V2 修法的实现细节，不影响 V2 的设计结论，但不写清实现者很可能默认多线程 runtime。

### N3（文字统一）：workdir 的"保存期拒绝不存在"与 remote 可移植性冲突

- **问题**：§4.1:86 表写 workdir "必须是绝对、存在、可 canonicalize"；§4.1:93 的 `validate_params` 只要求"workdir 为绝对路径"；但 §4.6:260 的矩阵写"workdir 非绝对/不存在 → **保存期拒绝**"。定义是跨机器可移植的（remote 模式下 workdir 在远端 agent 机器上，master 侧根本不存在该路径），保存期做存在性/canonicalize 校验会误拒合法定义。
- **最小修法**：把三处统一为——**保存期只校验"非空 + 绝对路径 + 非模板"；存在性、目录、canonicalize 在 executor 授权前校验（失败 Business，不产生 OperationIntent）**。§4.1:86 的"存在/可 canonicalize"标注为运行期要求，§4.6:260 改成"非绝对 → 保存期拒绝；不存在/非目录 → 执行器运行期 Business"。

## 建议项（不阻塞）

- **S1 `CARGO_HOME=临时目录` 的代价被低估**：每次 agent run 都用全新 CARGO_HOME，`cargo build` 会重新下载 registry/依赖（大项目每次几十秒到几分钟 + 网络）；而 FullAuto 下 bash 本来就能读任意绝对路径，隔离 `~/.cargo/credentials` 的安全收益有限。建议改为"共享缓存（例如 flow 数据目录下的 `cargo-home`）+ 不继承 credentials"，或保留隔离但在 §8 写明"首次 run 会重新拉依赖，必要时预热缓存"。
- **S2 `AgentRequest` 含 api_key**：契约类型放 `flow-dto` 后，请确保 `Debug` 输出对 `api_key` 打码（runner 任何 `tracing::debug!("{request:?}")` 都会泄漏），并加一条"装配日志不含 key"的断言。
- **S3 runner 信封大小**：`capture_harness` 的 stdout 上限是 `flow_journal::MAX_VALUE_BYTES`（8MiB），信封把最终文本整段 JSON 化，极端 `max_tokens=1_000_000` 时可能触顶并落成 `Uncertain`。建议 runner 对 `text` 做上限（如 8MiB 前截断并标记）或明确该边界行为。
- **S4 runner/executor 版本漂移**：runner 不依赖 flow-engine（A2）后拿不到 `RUNTIME_ID`；信封 `v`+`FLOW_HARNESS_PROTOCOL` 只防协议不防行为漂移。部署上"同 workspace/Cargo.lock 构建"已够，建议部署文档明确"runner 与 executor 必须同批发布"。

## 可忽略 / 记录即可

- `SessionAssembly::build` 的 `CONGA_APPROVAL_TIMEOUT_S`、approval registry 在 FullAuto 下不会触发；no-op emit 兜底足够。
- subagent 在 Host 装配下仍启用（`spawn_subagents` 工具存在），事件被 no-op 吞掉、其会话落在临时 store root；这是"完整 harness 行为"的一部分，文档列一次即可。
- MCP 因临时 HOME 自动为空；external tools 需要 `CONGA_EXTERNAL_TOOLS`（runner 不设）；workdir 的 `.conga/skills`、AGENTS.md 属于预期提示词面。
- executor 被强杀时临时目录可能残留（正常/超时/取消路径已覆盖），OS tmp 兜底；Windows 下 `remove_dir_all` 可能因文件锁失败，重试一次即可，不列为问题。

## 一句话终审

**v3 把 v2 的四条阻断真正修成了机制而不是声明，可以进入实现；唯一不能再留给"文档记录"的是 Windows 上的 conga 状态隔离——那是"用错 provider"的静默路径，不是卫生问题。**
