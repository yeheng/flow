# Harness 节点内置 Agent 模式（复用 conga）实施计划 · v5.1

> 状态：**v5.1，按 Linus review（v5）修订：settings 不读挂到 conga 现成的 `stream_fn_overridden` 缝上（文件级不可达）；env 解析与 `CONGA_CONFIG_DIR` 设置全部收进 conga-host（`build_headless(api_key, roots, project_dir, mode)`），runner 零 env 知识；stdin 载荷补 `workdir`（v5 漏写）；patch 3 补「headless ≡ 空 settings」等价测试与 headless approver deny**
> 日期：2026-10-19
> 用户已拍板（不变）：①扩展现有 `harness` 节点，加 `mode = command | agent`，默认 `command`；②配置放 flow 统一配置 `[harness.agent]`；③工具权限 FullAuto，等价现有外部 CLI harness。
> **v5 拍板（新增）：runner = conga-host headless 装配入口的直接调用者（patch 3 新增）；settings.json 在 flow 路径上永不读取；环境变量只承载非敏感配置且由 executor 从冻结快照生成；API key 走 flow 密钥体系（stored secret / `FLOW_SECRET_*` → `OperationPermit`）→ runner stdin → `ProviderConfig` 字段，杜绝 key 进 env/argv/journal。进程边界不变（runner 仍是受管子进程，B1/终审结论）。**
> 评审记录：`2026-10-19-harness-agent-mode.linus-review.md`（v1 完整 review、v2 复审、v3 终审）。
> v5.1 相对 v5（Linus review P1/P2/P3 落实）：①settings 短路机制修正——只短路 provider 不够，`run_turn` 每轮 `load_settings_async()`（`conga-host/src/lib.rs:423-430`），且 `settings.system_prompt` 会**替换内置 coding prompt**（`settings.rs:44`）、`settings.max_tokens` 覆盖 tunables（`lib.rs:437`）；改挂现成的缝：注入 stream_fn 后该分支直接取 `EnvSettings::default()`，文件读取不可达，`resolve_turn_provider` 零改动（`effective_provider(default)` 为 None → 返回装配时的显式 cfg）。②收权：`build_headless(api_key, roots, project_dir, mode)`——provider/tunables 的 env 解析、`CONGA_CONFIG_DIR` 的设置与自检（同步 `headless_prepare(roots)`，主线程、先于任何 runtime）、`CONGA_LLM_KEY` fail-loud 全在 conga-host；executor 下发的 env 不再含 `CONGA_CONFIG_DIR`，runner 不再拼 `ProviderConfig`。③stdin 载荷补 `{api_key, prompt, workdir}`（v5 的 workdir「显式参数」没有到达 runner 的通道，属漏写）。④patch 3 单测补「毒化 settings.json 不生效」（headless ≡ 空 settings 等价）与 headless approver deny。
> v5 相对 v4：装配从 `build_cli` 换为 conga-host 新增的 headless 入口（settings=None 短路 + provider/workdir/store_root 显式参数 + env-only tunables，复用 ConfigLoader 解析代码防漂移）；`CONGA_LLM_KEY` env 通道与 set_var/remove_var 机制删除（N2 的机制前提、V2 的 env 面、`/proc/<pid>/environ` 初始块暴露一并消失）；headless 入口不调 `config_file::apply`/dotenvy（workdir `.env`/`.conga/config.toml` 注入通道消失）；`CONGA_CONFIG_DIR` patch 退守为状态目录隔离（tool state/spill/memory/skills/mcp 落盘根），不再承担配置正确性；`CONGA_GATEWAY_MODE`/`CONGA_PROJECT_DIR` 改为显式参数。
> v4 相对 v3.1 的简化：删 stdin 大契约与版本握手；删 `AgentRequest`/`AgentEnvelope`/`AgentMeta` 契约类型与 S2 打码；runner 不依赖任何 flow crate；usage/turns/tool_calls 元数据砍掉。
> v4 顺带修复（v5 保留）：S3 截断阈值 7.5 MiB（低于派生期 `task.rs:1398` 的 `> 8 MiB` 检查；capture 期上限实为 256 MiB，`flow-journal/src/lib.rs:19`）；§4.4 env 矩阵含 Windows 条目；N2 理由在 v5 中随机制删除而失效。
> v3.1 相对 v3：落实终审 N1（Windows 上 conga `config_dir()` 不认临时 HOME → conga 仓库加 5 行 `CONGA_CONFIG_DIR` patch + runner 启动自检）、N2（runner 单线程 runtime / `remove_var` 时机）、N3（workdir 保存期只查字面量，存在性留给 executor）；补 S1-S4 记录。
> v3 相对 v2 的修正：修复「回归候选不继承 baseline agent 配置」；密钥只经 stdin、装配后从 runner 环境移除；统一 runner HOME=临时目录；`runner_bin` 移出 run 快照/请求，改为 executor 本地解析；契约类型移到 `flow-dto`；补 subagent 事件泄漏、workdir 校验、装配失败形状、降级说明。

---

## 0. 一句话

`harness` 节点加 `mode=agent`：不再要求用户安装外部 CLI，而是由 `flow-executor` 用**现有受管子进程机制**启动 flow 自带的 `flow-harness-agent` 子进程；该子进程是对 conga-host **headless 装配入口**的直接调用——**不读 settings.json、非敏感配置只读 `CONGA_*` 环境变量（executor 从冻结快照生成）、API key 由 flow 密钥体系经 `OperationPermit` 下发、stdin 进入后直达结构体内存，全程不经过任何环境变量**，stdout 返回单行 JSON 结果。授权、OperationIntent/Authorized、Outcome、journal、回归指纹、超时/取消、后代回收、恢复全部复用 command 模式现有代码。**command 模式逐字节不变。**

## 1. 目标与非目标

### 目标

1. harness 节点可选择内置 agent，不依赖用户安装 kimi/claude/codex。
2. 模型配置（base_url / model / api / 限额 / proxy）来自 `[harness.agent]` 统一配置，并在 run 启动时**冻结进执行快照**；回归候选继承 baseline 快照。
3. API key 只以密钥名出现在节点参数（`api_key`，`x-secret`）；真值由 flow 密钥体系（`secrets.*`：stored 优先、`FLOW_SECRET_*` 兜底）解析，经既有 `OperationPermit.credential` 下发给 executor，再经 **stdin 管道**交给 runner、直达 `ProviderConfig` 结构体内存；**不进 definition、journal、operation 请求、日志、命令行参数，也不进 runner 及其子进程的任何环境变量**。
4. agent 运行在独立、受管子进程里：超时/取消/正常结束都按现有 `ManagedChild::spawn_harness` 进程组语义回收后代；不降低 `EXECUTION.md:39` 的承诺。
5. 输出兼容现有约定：`stdout` 仍是最终文本，`exit_code/stderr/result` 保持原义；新增 `agent` 元数据为附加字段。

### 非目标

- 不做逐轮/逐工具 journal；runner 内部事件只用于汇总，不写 flow journal/observations。
- 不做审批 UI、不做 Plan 模式、不做工具 allowlist（本阶段固定 FullAuto）。
- 不做配置热更新（沿用 Settings 页「重启生效」）；配置在 run 启动时冻结。
- 不做 per-node model 覆盖；不替换、不删除 command 模式。
- 不分发 conga CLI 二进制；runner 是 flow workspace 的二进制，输入输出契约由 flow 定义。

## 2. 设计决策（含三轮 review 后的修正与 v4 简化）

| 决策 | v1 | v4（本计划） | 理由 |
| --- | --- | --- | --- |
| agent 运行时进程 | 嵌入 `flow-executor` | **独立 `flow-harness-agent` 子进程** | 保留进程边界/进程组回收/取消语义；executor 不链接 conga |
| conga 复用面 | `run_agent_loop` + 自组 tools | **conga-host headless 装配入口（patch 3 新增）**：`build_headless(api_key, roots, project_dir, mode)`——settings 文件读取不可达（stream_fn 注入缝）、provider/tunables 由 conga-host 从 env 同源解析、key 走参数 | flow-harness 直接实现在 conga-host 上；CLI 路径（`build_cli`/`ConfigLoader`）逐字不变；A1 由 patch 2 闭合 |
| 配置传递 | 全局 OnceLock，每次派发现取 | **executor 把冻结的 `AgentRunConfig` 构造为非敏感 `CONGA_*` 环境变量**；结构参数（workdir/store_root/mode）与密钥走显式通道（参数/stdin） | env 只放可公开的标量配置；密钥与结构参数类型化传递，env 变量名即契约，由固定 rev 锁步 |
| 配置来源 | 全局 OnceLock，每次派发现取 | **`ExecutionSnapshot.agent`**（run 启动冻结；回归候选继承 baseline） | 重试/恢复/回归必须用同一执行身份 |
| 密钥传输 | Permit → executor 内存 | Permit → executor → **runner stdin**（`{api_key, prompt}`）；装配后从 runner env 移除 | key 不进 exec 环境初始块（`/proc/<pid>/environ` 同 uid 可读），也不进 argv/journal；workdir 的 hooks 也读不到 |
| runner 定位 | 配置字段 runner_bin | **executor 本地解析**：`FLOW_HARNESS_AGENT_BIN` 或同目录兄弟；**不进配置/快照/请求** | 部署本地路径不能成为 run 身份 |
| runner HOME | 未定 | **临时 HOME**；workdir 走 headless 入口显式参数 | 隔离 conga 状态目录与 cargo 凭据；workdir `.env`/`.conga/config.toml` 注入通道由「headless 不调 `config_file::apply`/dotenvy」（patch 3）根除，不依赖 cwd |
| conga config 根 | 依赖 `dirs::home_dir()` | **patch 1：`CONGA_CONFIG_DIR` 覆盖 `storage::config_dir()`（状态目录隔离，由 conga-host 从 `roots` 自行设置）+ patch 3：settings 文件读取不可达**；自检 config_dir 在临时根内 | Windows `dirs` 不读 HOME；conga `run_turn` 每轮读 settings 且覆盖面不止 provider（`lib.rs:423-430`）——v5.1 用 stream_fn 注入缝让读取分支不可达（结构性闭合），`CONGA_CONFIG_DIR` 只管 tool state/spill/memory/skills/mcp 落盘 |
| 成功判定 | 按 stop_reason 粗略映射 | 末条 assistant `EndTurn` 才算成功；末条 `ToolUse` = max_turns 失败；中间 `MaxTokens` 不单独判死 | 对齐 conga 真实语义 |
| 失败终态 | 未定义 | 封闭 marker：`spawn_error` / `agent_error` / 正常 outcome 三选一 | 恢复派生必须有确定形状 |
| 配置校验 | 运行期 | `[harness.agent]` 启动期校验 + semantics=1 保存/发布校验拒绝 agent 节点 | v1 没有执行快照，不能运行 agent |

## 3. 现状与硬约束（事实已按 review 校正）

### 3.1 harness 现状（command 模式）

- 节点能力：`required = ["command", "prompt"]`，参数 `command/args/prompt/workdir/timeout_ms`，`trace_granularity = "process"`（`flow-engine/src/model.rs:226-244,306-311`）。
- 请求构造：`flow_engine::exec::harness_request`（`flow-engine/src/exec.rs:73-113`），请求 JSON 恰好 5 字段 `{command,args,workdir,timeout_ms,prompt}`。
- 执行：`harness_node`（`flow-executor/src/task.rs:1222-1350`）→ `authorize_operation`（`task.rs:1136-1217`）→ `ManagedChild::spawn_harness`（`task.rs:1283-1285`；进程组 `flow-engine/src/process.rs:20-21`）→ `capture_harness`（`task.rs:1591-1674`）→ Outcome 落 journal（`task.rs:1317-1344`）→ 派生输出（`task.rs:1379-1410`）。
- 回归指纹：`fixture_outcome` 对请求 JSON **严格相等**（`task.rs:1357-1375`）；command 请求字节必须不变。
- 恢复：有 `previous_outcome` 时直接派生输出（`task.rs:1230-1245`）。
- 后代回收契约：`ManagedChild` 为 harness 建独立进程组并在 Drop 时组杀（`process.rs:20-21,109-160`），e2e 钉死（`backend-e2e/tests/crate_repairs.rs:245-303`）；`EXECUTION.md:39` 承诺超时/取消/正常结束回收后代。

### 3.2 配置 / 派发边界

- executor 由 master `env_clear` spawn，只回填 `RUST_LOG`（`flow-engine/src/execution_protocol/spawn.rs:52-55`）；loopback 另加 `FLOW_CONNECT_ADDR` / `FLOW_SESSION_TOKEN`（`loopback.rs:108-113`）。**executor 环境里没有 PATH/HOME**——v3 显式修掉（§4.7）。
- master→executor 唯一配置注入点：`ExecuteTask.node._flow_execution`（`flow-backend/src/execution/dispatch.rs:327-345`）；IPC/remote 共用 `drive_link`。`_flow_execution` 只在 `run.execution` 存在时注入。
- `ExecutionSnapshot` 已冻结 `runtime/clock_ms/seed/definitions/bindings`（`flow-engine/src/journal_state.rs:137-149`）；`semantics=1` 的 run 在 `freeze_execution` 直接返回 `None`（`flow-backend/src/execution_snapshot.rs:52-54`）。
- 回归候选：`run_regression_with_fixtures` 克隆 baseline execution 作为 `recorded` 传入（`flow-backend/src/regression.rs:35-39,141`），`run_start` 先按**当前**配置 `freeze_execution`（`journal_commands.rs:614`），regression 分支只拷贝 `clock_ms/seed/regression`（`:630-636`）——**agent 配置必须在此显式从 recorded 拷贝**（v2 review V1）。
- `config.update` 落盘 + 更新内存，但执行层只在启动时取配置（`flow-rpc/src/lib.rs:210`）；Settings 页明示重启生效。
- `[agent]` 段已被 `flow-agent` 部署参数占用（`flow-config/src/lib.rs:213-255`），新段用 `[harness.agent]`。

### 3.3 密钥边界

- `secrets.*`：stored 优先，`FLOW_SECRET_*` 兜底（`flow-engine/src/secrets.rs:17-85`）。
- email/llm 既有路径：executor 请求只带 `credential.secret_ref`（`flow-executor/src/task.rs:947-955`），master 授权后 `get_secret` 解析放 `OperationPermit`（`dispatch.rs:1303-1316`；恢复重发 `dispatch.rs:398-410`），真值不落 journal。
- `workflow.update` 用 `missing_secrets` 提前校验 `x-secret`（`flow-backend/src/journal_commands.rs:427-439`）。
- `resolve_node_secrets` 无调用点（`flow-engine/src/secrets.rs:90`），不要复活第二条注入路径。

## 4. 设计

### 4.1 节点契约（`harness` 扩展，不新增节点类型）

参数：

| 参数 | 类型 | command 模式 | agent 模式 | 说明 |
| --- | --- | --- | --- | --- |
| `mode` | 字面量 `command` \| `agent` | 缺省/`command` | `agent` | 默认 command；**不接受 `${}` 模板** |
| `prompt` | string (`x-widget: code`) | 必填 | 必填 | 任务输入 |
| `command` | string | **必填** | **禁止** | 从 descriptor `required` 移除，改为按 mode 校验 |
| `args` | array | 可选 | **禁止** | 跨模式参数不再静默忽略 |
| `workdir` | string | 可选 | **必填；保存期只要求非空、绝对、非模板**；存在/目录/canonicalize 在 executor 授权前校验 | agent 工具沙箱根（remote 模式下 master 看不到远端路径，不能在保存期判存在） |
| `timeout_ms` | number | 可选 | 可选 | 整体墙钟，沿用 `HARNESS_TIMEOUT_MAX_MS`（1h） |
| `api_key` | string，`x-secret: true` | **禁止** | **必填** | 只存密钥名；`secrets.list` 提供候选 |

- descriptor：`required = ["prompt"]`；新增 `mode`、`api_key`；`command` 的 help 改为「mode=command 时必填」。
- `validate_params`（`flow-engine/src/model.rs:832`）新增：
  - `mode` 必须是 `command|agent` 字面量（出现 `${}` 直接拒绝）；
  - command 模式要求 `command` 非空；agent 模式要求 `workdir`、`api_key` 非空且 workdir 为绝对路径；保存期**不判 workdir 是否存在**（remote 模式下 master 看不到远端路径，终审 N3）；存在/目录/canonicalize 在 executor 授权前 Business；
  - **跨模式参数拒绝**：agent 模式带非空 `command`/`args`、command 模式带 `api_key`，保存期直接报错；
  - `api_key` 自动进入 `secret_params` → `missing_secrets` 保存前校验。
- `Definition::validate`（`model.rs:627`，`workflow.update` 与运行前都会走）新增：`semantics == 1` 且存在 `mode=agent` 节点 → 拒绝（错误文案「semantics=1 不支持内置 agent，请升级工作流语义」）。v1 没有执行快照，不能冻结配置。
- `effect_policy` / `has_side_effect` / `supports_retry` / `trace_granularity=process` 不变（process 语义改为「单次受管 operation，内部不可逐个受控」，文档写清）。
- 前端：`SchemaField` 现有 `enum`/`secret` widget 直接渲染，不引入条件显示机制；两组字段同屏，用 `x-help` 说明互斥。前端必填星号只认 `prompt`，条件/跨模式必填以后端 `validate_params` 为准（保存 400 能显示错误）；作为已知 UX 取舍记录。

### 4.2 配置模型（run 启动时冻结）

```toml
[harness.agent]
# base_url 与 model 成对配置才启用；都留空 = 关闭（agent 模式运行期报清晰错误）
base_url = "https://api.deepseek.com/v1"
model = "deepseek-chat"
# openai（默认，OpenAI 兼容）| anthropic
api = "openai"
# proxy = "http://127.0.0.1:7890"   # 禁止 userinfo/query
# max_turns = 50                    # 1..=200；conga 默认值
# max_tool_calls_per_turn = 20      # 1..=100
# max_tokens = 4096                 # 1..=1_000_000
# tool_timeout_secs =               # 省略 = conga 默认（无引擎级工具超时）
```

- 类型：`flow_config::{HarnessConfig, HarnessAgentConfig}`，加入 `Config`、`merge_patch` 白名单、`validate`、`config_view`；Settings 页新增 section。
- 校验：`base_url`/`model` 成对；URL 必须 `http(s)` 且 **禁止 userinfo**；`api` 枚举；数值边界；`proxy` 同样禁止 userinfo/query（R4）。**配置里没有 `runner_bin`**（见 §4.3）。
- 运行期类型只有 **`AgentRunConfig`**（放 `flow-dto`，executor 经 `flow-engine` re-export 使用；快照/`_flow_execution`/operation 请求指纹用）。v3.1 的 `AgentRequest`/`AgentEnvelope`/`AgentMeta` 随协议简化删除：runner 输入 = env + stdin 两字段、输出 = 单行 JSON，形状由 runner 私有类型与 executor 侧解析函数定义，不需要共享契约类型；runner **不依赖任何 flow crate**（v2 review A2 收敛为零）。`flow-config::HarnessAgentConfig`（用户输入 schema）在 backend 启动时转换成 `AgentRunConfig`，转换点唯一。
- **冻结位置**：`ExecutionSnapshot` 增加 `#[serde(default, skip_serializing_if = "Option::is_none")] agent: Option<AgentRunConfig>`（`journal_state.rs:137-149`）。`freeze_execution` 在 semantics≥2 时把 backend 当前安装的 `AgentRunConfig` 写入快照；回归候选在 `run_start` 的 regression 分支显式 `snapshot.agent = recorded.agent.clone()`（与 clock/seed 同处，V1）；配置在进程重启后修改只影响新 run。旧 journal 因 `serde(default)` 可读。
- `JournalBackend` 持有启动时安装的当前配置（`OnceLock<Option<AgentRunConfig>>`），只作为「新 run 的快照来源」；派发时**从 run.execution 读**，不再现取全局。

### 4.3 执行边界：受管理 runner 子进程

```
master:
  authorize_operation(request, credential.secret_ref)  → OperationIntent/Authorized 同事务
  OperationPermit.credential = 密钥真值（仅内存）
dispatch:
  ExecuteTask.node._flow_execution.agent = 冻结的 AgentRunConfig
executor（现有 node 进程，不链接 conga）:
  1. previous_outcome → agent_output 派生（不启动 runner）
  2. 回归 → fixture 指纹比较（不启动 runner、不联网）
  3. 组装 agent 请求（含冻结配置 + credential.secret_ref）→ authorize_operation → 取 credential
  4. credential 缺失 → 写 agent_error outcome → Business
  5. 建临时 HOME → 按 §4.4 环境矩阵构造非敏感 CONGA_* env（无 CONGA_LLM_KEY）→ ManagedChild::spawn_harness(flow-harness-agent)
  6. stdin 写 {api_key, prompt} → capture（timeout/cancel/进程组回收）
  7. 解析 stdout 单行 JSON → 组装 outcome → operation_outcome 落 journal → agent_output 派生输出
  8. 删除临时目录；无论成功/失败/超时/取消
runner（flow-harness-agent，独立进程，conga-host headless 直连）:
  stdin {api_key, prompt, workdir} → headless_prepare(roots=cwd)（主线程：set CONGA_CONFIG_DIR + 自检 + 拒绝 CONGA_LLM_KEY）
  → build_headless(api_key, roots, project_dir=workdir, FullAuto)
  （conga-host 内部：ConfigLoader 同源解析 env → provider/tunables；注入 stream_fn → settings 文件读取不可达；不调 dotenvy）
  → run_turn(prompt) → 单行 JSON {text, stop_reason, error} → 退出码 0/1/130/2
```

要点：

- **进程边界**：runner 用 `ManagedChild::spawn_harness` 启动（独立进程组），`capture_harness` 的超时/取消分支 `start_kill` 组杀；runner 内 conga bash 派生的 `sh`/后台命令继承该进程组，随组回收。满足 `EXECUTION.md:39` 与 `crate_repairs.rs:245-303`。
- **executor 不链接 conga**：新增 workspace member `crates/flow-harness-agent`；executor 只新增「构造 env + 解析单行输出」的小段代码。runner 体积/构建代价被隔离到独立二进制（不是"不存在"，因为桌面/服务端都要分发它）。
- **runner 定位（executor 本地部署事实，不进 run 身份）**：
  - `FLOW_HARNESS_AGENT_BIN` 环境变量（绝对路径）优先；该变量由 master/agent 在 spawn executor 时像 `RUST_LOG` 一样显式透传（本地 master 与远端 flow-agent 各自解析自己机器上的路径）；
  - 否则取 executor 自身可执行文件同目录的 `flow-harness-agent`；
  - 找不到 → 运行期 `agent_error`，错误含期望路径。
  - **不写入 `[harness.agent]`、`AgentRunConfig`、`ExecutionSnapshot`、operation 请求或回归指纹**（v2 review V4）。
- **版本门（v4 删除）**：不再有信封 `v` 与 `FLOW_HARNESS_PROTOCOL` 握手。协议 = 「conga env 变量名 + stdin 两字段 + 单行输出格式 + 退出码 0/1/130/2」，由 conga 固定 git rev 与 workspace 锁步构建挡住漂移；部署文档要求 runner 与 executor 同批发布（S4）。runner 的 `config_dir()` 自检（§4.4.1）保留为 fail-loud 防线。
- **并发**：每个 agent 节点一个独立 runner 进程、独立临时 HOME；executor 当前串行处理任务（现有语义），不共享 conga 进程级 shell 状态。
- **超时/取消/恢复/回归**：全部复用 `capture_harness` 与 `harness_node` 的前置分支，不新写生命周期逻辑。

### 4.4 runner 内部装配（conga-host headless 入口：不读 settings.json、密钥显式注入、runner 零 env 知识）

- 入口：flow 自带 `flow-harness-agent` 二进制，约 30 行：同步读 stdin `{api_key, prompt, workdir}` → 同步调 conga-host 的 `headless_prepare(roots)`（roots = 自身 cwd，executor 已设为临时根）→ runtime `block_on(build_headless(api_key, roots, project_dir=workdir, Mode::FullAuto))` → `run_turn(prompt)` → 汇总单行结果 → 退出码。**runner 自身不读写任何 `CONGA_*` 变量**——env 解析与 `CONGA_CONFIG_DIR` 设置全部在 conga-host 内部（v5 让 runner 自己拼 `ProviderConfig` 的写法废弃：flow 侧不得出现第二份 env 解析，env 名字只有一个读者）。
- **settings.json 永不被读（终审 N1 配置面的结构性闭合，挂现成的缝）**：conga 的 `run_turn` 每轮 `load_settings_async()` 读 settings（`conga-host/src/lib.rs:423-430`），且 settings 的覆盖面**不止 provider**——`system_prompt` 会替换内置 coding prompt（`settings.rs:44`）、`max_tokens` 会覆盖 tunables（`lib.rs:437`）、provider 三元组覆盖（`lib.rs:197-218`）。headless 入口利用既有分支：注入 provider 自带 stream_fn（`with_stream_fn`）后，`lib.rs:427-428` 直接取 `EnvSettings::default()`，**文件读取分支不可达**；default 下 system_prompt=None（内置 prompt 生效）、max_tokens=None（env tunables 生效）、`effective_provider` 为 None（`resolve_turn_provider` 返回装配时的显式 cfg——**该函数零改动**）。即使用户机器任何位置存在 settings.json，flow 路径也读不到——不是被隔离，是**不存在**。patch 1 的 `CONGA_CONFIG_DIR` 因此退守为纯状态目录隔离（tool state/spill/memory/skills/mcp 的落盘根）。
- **headless 入口不调 `config_file::apply`、不调 dotenvy**：workdir 的 `.env`/`.conga/config.toml` 自动装配通道（`discover` 从 cwd 找 config.toml、dotenvy 读 cwd `.env`——例如 `.env` 里一条 `CONGA_LLM_PROXY` 就能把 provider 流量引到攻击者代理）从机制上消失。cwd 仍设为临时 HOME（卫生，非安全依赖）。
- **密钥通道（杜绝 env）**：flow 密钥体系（`secrets.*`：stored 优先、`FLOW_SECRET_*` 兜底）→ `OperationPermit.credential` → executor 内存 → runner stdin → `build_headless` 的 `api_key` 参数 → `ProviderConfig` 字段（内存）。**任何环境变量都不携带 key**：hooks（`process_hooks` 用 `sh -c` 且不 `env_clear`）继承的环境里从来没有 key；`/proc/<pid>/environ` 初始块也没有。env 中出现 `CONGA_LLM_KEY` → conga-host fail loud 返回 Err（变量名归 conga-host 所有，检查也归它；防上游误用 env 传密钥）。
- 环境契约（executor `env_clear` 后显式给出，即 runner 的初始可见环境）：
  - **隔离覆盖**（executor 设为临时根 `<tmp>/flow-harness-<uuid>/...`）：`HOME`、`USERPROFILE`（Windows git 回退路径）、`TMPDIR`/`TEMP`/`TMP`、`XDG_CONFIG_HOME`/`XDG_DATA_HOME`/`XDG_CACHE_HOME`、`CARGO_HOME`；
  - **白名单透传**（§4.7 executor 白名单中其余平台条目原样透传）：`PATH`、`LANG`/`LC_ALL`/`LC_CTYPE`、`USER`/`LOGNAME`/`SHELL`、`RUSTUP_HOME`（Unix）；`SYSTEMROOT`、`COMSPEC`、`PATHEXT`、`APPDATA`、`LOCALAPPDATA`（Windows——缺 `SYSTEMROOT` 时 WinSock/cmd/git 大面积失灵）；
  - **非敏感配置**（executor 从冻结的 `AgentRunConfig` 构造；**唯一读者是 conga-host**，解析复用 ConfigLoader 现有代码）：`CONGA_LLM_BASE_URL` / `CONGA_LLM_MODEL` / `CONGA_LLM_API`、可选 `CONGA_LLM_PROXY`、可选 `CONGA_MAX_TURNS` / `CONGA_MAX_TOOL_CALLS` / `CONGA_MAX_TOKENS` / `CONGA_TOOL_TIMEOUT_S`（未配不设）；
  - **不设**：`CONGA_LLM_KEY`（conga-host 检测即拒）、`CONGA_CONFIG_DIR`（由 conga-host 在 runner 进程内从 `roots` 自行设置并自检，executor 不再下发——消除「env + 参数」双通道）、`CONGA_GATEWAY_MODE` 与 `CONGA_PROJECT_DIR`（显式参数：mode 固定 FullAuto，workdir 经 stdin 到达）、`FLOW_*`、`RUST_LOG`（runner 安装固定 filter `conga=warn,conga_host=warn`，R2）。
- 事件汇总（只用于结果行）：最终文本 = `TurnSummary.new_messages` 末条 assistant 的 Text blocks 拼接；**超过 7.5 MiB 截断并写 `[truncated]` 标记**（S3：flow 派生期在 `task.rs:1398` 检查 `body_raw > 8 MiB` → Business，阈值必须低于 8 MiB 减标记长度；capture 期上限实为 256 MiB，`flow-journal/src/lib.rs:19`，不构成约束）；`stop_reason` 取同一条 assistant 的 stop_reason（Host 的 `Completed` 不能区分 max_turns 耗尽，review R5）。usage/turns/tool_calls 元数据已砍掉（如需后续在输出行加回，字段向后兼容）。
- 成功判定（R5，不变）：`TurnEndReason::Completed` **且** 末条 stop_reason `EndTurn` → 退出码 0；`Error` → 1；`Aborted` → 130；`Completed` 但末条 `ToolUse`（max_turns 耗尽）→ 1，error「max turns exceeded」；中间 `MaxTokens` 不单独判失败。退出码契约与 `conga exec` 对齐：0 完成 / 1 回合失败 / 130 中止 / 2 装配或用法错误。
- 输出：stdout 单行 JSON `{"text", "stop_reason", "error"}`；装配/自检失败（provider env 不全、config_dir 不在临时根内）→ `{"error": "..."}` + 退出码 2。
- 结束：`cleanup_session_resources(host.session().current_id())`；临时 HOME 由 executor 递归删除（含 runner 被 kill 的情况）。
- 测试缝：runner crate 暴露 `run_once(env, stdin_payload, stream_fn)`；单测注入 fake `StreamFn`，生产 main 用真实 provider。
- 调试打码（S2）：不适用——key 只经 stdin 局部变量进入结构体，不进任何 flow 类型、不进任何环境变量；runner 内不打印请求与 stdin 内容。

#### 4.4.1 conga 侧 patch（三项；conga 是自家仓库，flow 固定到含三 patch 的 rev）

- **patch 1（状态目录隔离）**：`conga::storage::config_dir()` 支持 `CONGA_CONFIG_DIR` 覆盖（~5 行 + 单测），使 tool state/spill/memory/skills/mcp/session 的落盘在任何平台都进临时根（`config_dir()` 是这些路径的唯一收口）。**规格：设置即返回，必须跳过 `~/.gasket` legacy 回退分支**（`storage/mod.rs:41-50`）；未设置时与现行为逐字一致；单测覆盖 legacy 分支。runner 启动自检 `config_dir()` 落在临时根内，否则 fail loud（Windows 防线）。
- **patch 2（A1，一行修）**：`conga-host/src/assembly.rs` 中 subagent `eprintln!("[subagent] {ev:?}")` 改为 `tracing::debug!`。不改则 subagent 事件内容会进 runner stderr → `capture_harness` 的 512B 尾 → outcome → journal，破坏「不记 prompt/内容」承诺。CLI 本体同样受益；runner 的 warn filter 使其静默。
- **patch 3（N1-config / N2 / V2 的结构性闭合，本版核心）**：conga-host 新增 headless 装配入口（约 100-150 行 + 单测）：
  - 签名形状（以实际代码为准）：
    - `headless_prepare(roots: &Path) -> Result<(), AssemblyError>`（**同步**；契约：主线程、创建任何 runtime 之前调用）：`set_var("CONGA_CONFIG_DIR", roots.join("conga"))` + 自检 `config_dir()` 落在 roots 内 + env 已存在 `CONGA_LLM_KEY` 则 Err；
    - `build_headless(api_key: &str, roots: &Path, project_dir: &Path, mode: Mode) -> Result<SessionAssembly, AssemblyError>`；
  - **provider**：base_url/model/api/proxy/tunables 全部从 env 解析（**复用 ConfigLoader 现有解析代码，禁止第二份解析器**），与 `api_key` 参数组成 `ProviderConfig`；
  - **settings 分支不可达**：装配时注入 provider 自带 stream_fn（`with_stream_fn(cfg.provider_stream_fn())`），`run_turn` 的 settings 加载走 `EnvSettings::default()`（`lib.rs:427-428` 现成分支）；`resolve_turn_provider` 零改动；
  - **approver**：headless deny（对齐 `conga exec`：无人应答即拒绝）；
  - `store_root = roots/sessions`；`project_dir`/`mode` 显式参数；不调 `config_file::apply`/dotenvy；
  - 单测：①**毒化 settings.json**（含 system_prompt/max_tokens/provider 覆盖）放在 config 根下，断言 headless 用内置 prompt、env tunables、env+参数 provider——即「headless ≡ 空 settings 的 CLI 语义」等价性；②env 带 `CONGA_LLM_KEY` → Err；③headless approver deny 生效；④CLI 路径（`build_cli`/`ConfigLoader::load`）行为逐字不变。
- 若暂时不做 patch 3：agent 模式 V1 不支持（无降级路径——v4 的 `build_cli` 方案作为 fallback 保留在 git 历史中，但不随 v5 交付）；计划默认走三 patch 路线。


### 4.5 请求 / 输出 / outcome 契约

Operation 请求（授权 + journal 证据 + 回归指纹；command 模式保持 5 字段不变）：

```json
{
  "mode": "agent",
  "prompt": "...",
  "workdir": "...",
  "timeout_ms": 300000,
  "agent": { "base_url": "...", "model": "...", "api": "openai", "proxy": null,
             "max_turns": 50, "max_tool_calls_per_turn": 20, "max_tokens": 4096,
             "tool_timeout_secs": null },
  "credential": { "scheme": "bearer", "secret_ref": "<api_key 密钥名>" }
}
```

- 只有密钥名；prompt 与 command 模式一样进请求（回归指纹需要）。
- `agent` 配置来自 run 快照，与 `_flow_execution.agent` 是同一份数据的两次序列化；测试断言两者相等。
- **runner 的本地路径、env、HOME 不在此请求中**。

runner stdin JSON（不落盘、不进 journal；三字段，配置一律走 env，workdir 走参数通道）：

```json
{ "api_key": "<真值>", "prompt": "...", "workdir": "<canonical>" }
```

runner stdout 单行结果（成功/失败都输出，尽力而为；v4 无版本字段，格式由固定 rev 锁步）：

```json
{ "text": "最终文本", "stop_reason": "end_turn", "error": null }
```

Outcome（journal，封闭三选一）：

```json
// A. 正常完成/失败（runner 正常退出，输出行合法）
{ "exit_code": 0, "body_raw": "<text 的 ValueRef>", "stderr": "<≤512B 尾部>",
  "agent": { "model": "...", "stop_reason": "end_turn", "elapsed_ms": 1234 } }

// B. 授权后、启动 runner 前/runner 输出不可解析等确定性失败
{ "agent_error": "runner output invalid: ...", "stderr": "<tail>" }

// C. runner 启动失败（沿用现有 marker）
{ "spawn_error": "..." }
```

- runner 内部装配失败（配置 env 不全、config_dir 自检失败等）：runner 输出带 `error` 的结果行并退出 2 → outcome A（`exit_code=2`），error 文案同时进 `outcome.stderr`（A5/A6）。
- runner 被 timeout/cancel 杀死：沿用 command 模式 → `TaskError::Uncertain`，不写 outcome。

节点输出（对下游/前端）：

```json
{ "exit_code": 0, "stdout": "<最终文本>", "stderr": "...", "result": "<若 text 是 JSON 对象/数组>",
  "agent": { "...": "同上" } }
```

`agent_output` 派生顺序：`spawn_error` → `agent_error` → 正常 outcome（同 `harness_output` + `agent` 附加字段）。恢复路径只读 outcome，因此 `agent` 元数据必须同时存在于 outcome 与 output。

### 4.6 失败 / 超时 / 取消 / 恢复 / 回归矩阵

| 情形 | 行为 |
| --- | --- |
| command 模式 | JSON 请求/输出/错误语义不变；子进程 env 变为白名单超集（见 §6） |
| agent 正常结束（末条 `EndTurn`） | outcome A，`exit_code=0`，派生输出成功 |
| provider/LLM 错误 / `TurnEndReason::Error` | outcome A，`exit_code=1` + error，节点 Business 失败 |
| 中间 `MaxTokens` 后继续并最终 `EndTurn` | 成功（conga 语义） |
| max_turns 耗尽（末条 `ToolUse`） | outcome A，`exit_code=1`，error「max turns exceeded」；末轮工具副作用已发生 |
| `Aborted`（runner 自行返回） | `exit_code=130` → executor 按 Uncertain 处理（防御性保留；flow 的取消/超时走进程组 SIGKILL → capture 分支 Uncertain，不依赖此路径） |
| 整体 timeout / 用户取消（executor 杀进程组） | `TaskError::Uncertain`；后代进程随组回收；无 outcome（同 command） |
| 授权后、runner 前失败（凭据缺失/配置缺失/runner 缺失） | outcome B（`agent_error`）→ Business；恢复派生稳定 |
| runner 输出非法/缺失结果行 | outcome B（`agent_error`）→ Business（不把半截 stdout 当成功） |
| runner 内部装配失败 | runner 输出行 `error` + 退出 2 → outcome A → Business |
| runner spawn 失败 | outcome C（`spawn_error`）→ Business（沿用 command 语义） |
| 已有 `previous_outcome` | 只派生输出，不启动 runner、不联网 |
| 回归模式 fixture 匹配 | 派生输出且 **0 网络/0 runner**；使用 baseline 冻结的 agent 配置 |
| 回归模式 fixture 不匹配/缺 fixture | Business（同 command） |
| 回归模式 + 缺 `[harness.agent]` | 无法构造请求指纹 → Business（明确「配置缺失」） |
| semantics=1 + agent 节点 | 保存/发布校验拒绝；运行期兜底文案区分「v1 不支持」 |
| agent 节点 workdir 非绝对/含模板 | 保存期拒绝；存在/目录/canonicalize 在 executor 授权前 Business（remote 兼容，N3） |
| 工具内部失败 | conga 作为 tool_result 交回模型继续/收口；不直接判节点失败 |
| workdir 自带 `.env` / `.conga/config.toml` | 不被自动装配（headless 入口不调 `config_file::apply`/dotenvy，patch 3）；full-auto 下工具读写 workdir 内容不受限（FullAuto 既有面） |
| 机器任何位置存在 conga `settings.json` | 不被读取：stream_fn 注入缝使 settings 加载分支不可达（patch 3），system_prompt/max_tokens/provider 三条覆盖向量都不存在（N1-config） |
| 密钥被删除/改名 | permit credential=None → outcome B → Business，文案指向设置页密钥管理 |

### 4.7 env 契约（B2，必须修）

**问题**：executor 当前 `env_clear` 后只剩 `RUST_LOG` + 两个 `FLOW_*`，没有 `PATH/HOME`。command 模式靠外部 CLI 自己管理工具链；内置 agent 需要明确的 shell 环境。

**修法（独立、通用的小改动）**：master/agent 在 spawn executor 时，按**白名单**从父进程环境透传：

- Unix：`PATH`、`HOME`、`TMPDIR`、`TMP`、`LANG`、`LC_ALL`、`LC_CTYPE`、`USER`、`LOGNAME`、`SHELL`、`RUSTUP_HOME`、`CARGO_HOME`、`XDG_DATA_HOME`、`XDG_CONFIG_HOME`、`XDG_CACHE_HOME`；
- Windows：`PATH`、`USERPROFILE`、`APPDATA`、`LOCALAPPDATA`、`TEMP`、`TMP`、`SYSTEMROOT`、`COMSPEC`、`PATHEXT`、`LANG`。

**禁止**：`FLOW_*`、`CONGA_*`、`RUST_LOG`（executor 自己的日志仍按现状单独注入）、任何密钥/token 变量。`FLOW_HARNESS_AGENT_BIN` 像 `RUST_LOG` 一样作为**显式部署变量**透传（不进入通用白名单）。remote 模式下由 flow-agent 在**所在机器**上 spawn executor，因此拿到的是远端机器的正确环境。

executor 再把这批变量显式传给 runner（runner `env_clear` 后只设 §4.4 的矩阵）；`HOME` 被临时目录覆盖。因此：
- runner 里的 `bash` 有 `PATH`/`LANG`/`RUSTUP_HOME`，能跑 `cargo`/`git` 等；
- conga settings/state/session 全在临时 HOME，不碰 `~/.conga`；
- 用户真实 `HOME` 下的 git/ssh 配置不可用（明确取舍：`git commit` 身份与 SSH push 可能失败；需要时在 workdir 的 `.git/config` 里配置）；
- `CARGO_HOME` 指向临时目录，私有 registry 凭据不继承（安全取舍；公开依赖可正常下载）。

验收（单测/冒烟）：agent 的 bash 能看到 `PATH/HOME`；`command -v cargo`（或平台等价物）成功；`FLOW_*`/`CONGA_*` 不可见；三平台 CI 至少覆盖 Unix，Windows 手工冒烟记录。

### 4.8 日志 / 泄漏面 / 安全边界

- executor 只记：agent 启动（model/mode）、runner 退出码/耗时/stdout 字节数、错误尾；**不记 prompt、key、runner stdin**。runner stderr 只保留 512B 尾部进 outcome。
- runner 日志：固定 `warn` filter，**不继承 `RUST_LOG`**；conga `debug!` 请求体不会进 stderr；subagent tap 由 §4.4.1 patch 2（`eprintln!`→`tracing::debug!`）消除。补测试：fake provider 下断言 stderr 无 prompt/key 片段（R2）。
- 临时 HOME + conga config 根：executor 创建 `<tmp>/flow-harness-<uuid>/`；runner 退出后（含超时/取消）executor `remove_dir_all`。conga session/tool state/spill/memory/skills/mcp 全落在 `CONGA_CONFIG_DIR`/临时 HOME 下，后续 run 读不到（R3）。**Windows 不依赖 HOME 的环境变量语义**：patch 1 把状态根钉在临时目录，`headless_prepare` 自检 config_dir 必须位于临时根内，否则 fail loud。**settings.json 不被读**：stream_fn 注入缝（patch 3，§4.4），配置正确性不依赖目录隔离。**workdir 的 `.env`/`.conga/config.toml` 不构成注入通道**：headless 入口不调 `config_file::apply`/dotenvy（§4.4）。
- 密钥：来源与管理始终在 flow 密钥体系（`secrets.*`：stored 优先、`FLOW_SECRET_*` 兜底），经 `OperationPermit.credential` → executor 内存 → runner stdin → `build_headless` 的 `api_key` 参数 → `ProviderConfig` 字段（内存）。**任何环境变量都不携带 key**（conga-host 的 `headless_prepare` 对 env 中出现的 `CONGA_LLM_KEY` fail loud）：不进 argv、operation 请求、journal、flow 日志、证据包，也不进 runner 及其任何子进程（hooks/bash）的环境或 `/proc/<pid>/environ`。
- proxy：配置校验禁止 userinfo/query，确保代理凭据不进 operation 请求/journal（R4）；`CONGA_LLM_PROXY` 经 env 下发，同样只允许 host:port 形态。
- FullAuto 信任面：agent 可读写 workdir、联网、起 shell——与现有外部 CLI harness 等价（用户已拍板）；安全边界仍是「一个 `OperationIntent` 覆盖整次运行」，回归模式 0 外部调用。文档明示，不新增沙箱。
- workdir 校验：绝对、存在、目录、canonicalize 后作为 headless 入口的显式参数（`project_dir`）传给 runner；拒绝相对路径/不存在路径，避免按临时 HOME 解析。

### 4.9 打包 / 部署

- **conga 侧 patch（前置）**：先落 §4.4.1 的三个 patch（`CONGA_CONFIG_DIR` 状态隔离、subagent tap 改 `tracing::debug!`、headless 装配入口，conga 自家仓库，含单测），再把 flow 的 git rev 固定到含 patch 的 commit；文档记录 patch 1/2 的默认行为不变（未设置 env / CLI 路径逐字一致）。
- 新二进制 `flow-harness-agent` 与 `flow-executor` 同级分发：
  - 桌面：`web/src-tauri/tauri.conf.json` 的 `externalBin` 增加 `bin/flow-harness-agent`；`scripts/build-desktop-executor.sh` 扩展为同时构建/拷贝两个二进制；`beforeDev/BuildCommand` 保持顺序。
  - 服务端：`scripts/build-journal.sh` 的 BINS 增加 `flow-harness-agent`；部署文档说明与 executor 同目录（或设 `FLOW_HARNESS_AGENT_BIN`）。
- executor 缺省按自身可执行文件目录定位 runner；`FLOW_HARNESS_AGENT_BIN` 可覆盖（远端由 flow-agent 环境提供）。
- 版本锁步：runner 与 executor 由同一 workspace/Cargo.lock 构建，conga 固定 git rev；env 变量名与输出行格式即协议，漂移由锁步发布挡住（S4：部署要求同批发布）。
- backend-e2e 测试基建新增 `flow_test_support::io::harness_agent_bin()`（与 `executor_bin()` 同型），e2e 注入 `FLOW_HARNESS_AGENT_BIN`（A7）。

## 5. 实施步骤（每步独立可验证）

### Step 1：契约类型 + 配置 + run 快照 + 节点模型 + 前端 schema

- `flow-dto`：仅 `AgentRunConfig`（serde；无重依赖）；`flow-engine` re-export。**无 `AgentRequest`/`AgentEnvelope`/`AgentMeta`，无 Debug 打码测试**（S2 随协议简化消失，key 不进任何 flow 类型）。
- `flow-engine::exec`：新增 agent 模式解析、`agent_env(config) -> Vec<(String,String)>`（§4.4 非敏感配置矩阵的唯一构造点）、`agent_stdin(prompt, key, workdir) -> Vec<u8>`、`parse_agent_result(stdout) -> Result<AgentResult, ...>`（text/stop_reason/error 的 executor 侧解析）；command 的 `harness_request/harness_output` 原样保留。
- `flow-config`：`HarnessConfig/HarnessAgentConfig` + `Config.harness` + validate（URL/userinfo/边界/proxy）+ merge_patch + 单测；`flow.config.example.toml`。**不含 runner_bin**。
- `flow-engine`：`model.rs` harness descriptor + `validate_params` 条件必填/跨模式拒绝/`mode` 字面量/workdir 绝对 + `Definition::validate` 拒绝 semantics=1 agent + 更新 `secret_params_are_the_expected_keys`/`harness_and_email_require_their_params` 测试。workdir 保存期只查「非空+绝对+非模板」，存在性留给 executor（N3）。
- `flow-engine::journal_state::ExecutionSnapshot` 加 `agent: Option<AgentRunConfig>`；`flow-backend::execution_snapshot::freeze_execution` 写入当前配置；**`run_start` regression 分支 `snapshot.agent = recorded.agent.clone()`**（V1）；旧 journal 反序列化测试。
- `web`：types + SettingsView section + SettingsView.test；节点编辑器依赖 descriptor 自动渲染。
- 验证：`cargo test -p flow-dto -p flow-config -p flow-engine -p flow-backend`；`cd web && pnpm vitest run src/views/SettingsView.test.ts`；节点能力快照更新（`flow-rpc/src/lib.rs` 的 SNAPSHOT）。

### Step 2：executor spawn env 白名单（B2）

- `flow-engine/src/execution_protocol/spawn.rs` / `loopback.rs`：按 §4.7 白名单透传；`FLOW_HARNESS_AGENT_BIN` 显式透传；不改协议。
- 测试：白名单变量存在/禁止变量不存在；`RUST_LOG` 仍单独注入；现有握手/回连测试全绿。
- 验证：`cargo test -p flow-engine execution_protocol`；相关 `backend-e2e` 用例。

### Step 3：conga patch ×3 + runner crate（conga-host headless 直连）

- **前置**：在 conga 仓库落 §4.4.1 三个 patch——patch 1 `CONGA_CONFIG_DIR`（状态隔离，含 `.gasket` legacy 分支单测）、patch 2 subagent tap→`tracing::debug!`、patch 3 headless 装配入口（`headless_prepare` + `build_headless`；settings 分支经 stream_fn 注入不可达；env 解析与 ConfigLoader 同源；不调 `config_file::apply`/dotenvy；单测含毒化 settings.json 等价性、`CONGA_LLM_KEY` 拒绝、approver deny、CLI 逐字不变）；flow 固定到含三 patch 的 rev。
- 新 workspace member `crates/flow-harness-agent`：依赖 `conga`/`conga-host`（固定 git rev、feature 关闭）、tokio/serde/tracing-subscriber；**不依赖任何 flow crate**。
- `src/main.rs`（约 30 行）：同步读 stdin `{api_key, prompt, workdir}` → 主线程（无 runtime）调 `headless_prepare(current_dir())` → runtime `block_on(build_headless(api_key, roots=current_dir(), project_dir=workdir, Mode::FullAuto))` → `run_turn(prompt)` → 汇总/截断 → stdout 单行 JSON → cleanup → 退出码 0/1/130/2。**runner 零 env 读写**。
- 测试：fake `StreamFn` 覆盖 EndTurn/Error/中间 MaxTokens/max_turns 耗尽/工具调用；key 不出现在任何 env（含 workdir `.conga/hooks.json` 打印 `env` 的断言）；workdir 放 `.env`（`CONGA_LLM_PROXY=...`）与 `.conga/config.toml` 断言不生效（headless 不调 apply/dotenvy）；毒化 settings.json 放 config 根内断言不生效（flow 侧再钉一次等价性）；prompt 不进 stderr；text 7.5 MiB 截断带标记；三 patch 单测。
- 验证：`cargo test -p flow-harness-agent`；`cargo build -p flow-harness-agent`；`cargo tree -d -p flow-harness-agent`（记录 toml 双版本与新增 crate）；实测编译时间/体积写 PR；conga patch 的 `cargo test -p conga -p conga-host`。

### Step 4：executor agent 分支（生命周期复用 command 模式）

- `flow-engine::exec`：新增 agent 模式解析、`agent_request(params, frozen_config)`（授权/指纹请求，不变）、`agent_env(...)`/`agent_stdin(...)`/`parse_agent_result(...)`（Step 1 已列）、`agent_output(outcome)`；command 的 `harness_request/harness_output` 原样保留。
- `flow-executor/src/task.rs`：`NodeType::Harness` 按 mode 分流；agent 分支：
  - 恢复/回归前置与 `harness_node` 同构；
  - 从 `_flow_execution.agent` 取冻结配置（缺失 → `agent_error`；v1 已被保存期拒绝）；
  - 校验 workdir（授权前：存在、目录、canonicalize；保存期只查字面量）、请求含配置 + credential、`authorize_operation`；
  - credential 缺失 → outcome B；
  - 临时 HOME → 解析 runner（§4.3）→ `ManagedChild::spawn_harness` + `env_clear` 显式 env（§4.4 矩阵：隔离覆盖 + 白名单 + `CONGA_*` 非敏感配置，无 `CONGA_CONFIG_DIR`）→ stdin `{api_key, prompt, workdir}` → `capture_harness`（复用）→ 解析单行结果 → outcome A/B/C → 落 journal → 派生；
  - 所有退出路径删除临时目录；日志只记摘要。
- 测试（fake runner 脚本 + fake provider 两套）：请求只含密钥名；runner env 断言（executor 传入的 `CONGA_*` 恰为 §4.4 非敏感矩阵，无 `CONGA_LLM_KEY`/`CONGA_CONFIG_DIR`/`CONGA_GATEWAY_MODE`/`CONGA_PROJECT_DIR`、无 `FLOW_*`、`HOME` 为临时目录）；恢复不重跑；回归 0 runner；timeout/cancel → Uncertain 且临时目录删除；credential 缺失 outcome 形状 + 重启派生；runner 输出非法 → agent_error；`RUST_LOG=debug` 不泄漏；并发两个 agent 节点互不串。
- 验证：`cargo test -p flow-executor`；`cargo test -p flow-engine`。

### Step 5：派发注入 + 产品入口 + 保存期校验

- `flow_backend::start_execution` 增加 `Option<&flow_config::HarnessAgentConfig>` 参数：转 `AgentRunConfig`、安装到 backend、作为新 run 快照来源；更新全部调用点（`grep -rn "start_execution("`：`flow-rpc/src/lib.rs`、`web/src-tauri/src/service.rs`、`flow-backend/src/bin/flow-journal-dev.rs`、`backend-e2e/tests/contracts.rs`、`backend-e2e/tests/cli.rs`）。
- `flow-backend/src/execution/dispatch.rs`：`drive_link` 从 `run.execution.agent` 注入 `_flow_execution.agent`（唯一注入点，IPC/remote 共用）；不读全局。
- `journal_commands.rs`：`workflow.update` 的 `missing_secrets` 覆盖 `api_key`；`Definition::validate` 的 semantics=1 拒绝落点。
- 验证：backend 单测断言 `_flow_execution.agent` 与 operation 请求中的 agent 配置相等；remote-e2e wire 级断言可见；command 节点不带该键；`cargo test -p flow-backend -p flow-rpc`。

### Step 6：回归 / e2e / 文档 / 打包收口

- `backend-e2e`：mock OpenAI server + runner 真进程跑通 agent 节点（多轮工具调用、最终文本、`agent` 元数据、journal/observation 无 key、operation 请求含配置不含 key）；command harness 旧用例全绿。
- 契约测试：agent 版 `crate_repairs`（取消/超时后后代不存活）；env 白名单 smoke；`RUST_LOG=debug` 泄漏扫描；regression（baseline agent run → 候选 fixture 匹配、mock provider 命中 0；**配置重启后变更仍匹配**（V1）；改请求一项 → 明确失配；缺配置 → 明确 Business）。
- 文档：`docs/DESIGN.md` 节点表、`docs/EXECUTION.md:39` 补充「agent 模式同样是受管 operation，runner 内部调用不逐个受控」、`docs/OPS.md` 配置/部署/HOME 取舍、`flow.config.example.toml`。
- 打包：tauri externalBin + 构建脚本 + `scripts/build-journal.sh` + 部署文档；`flow_test_support::io::harness_agent_bin()`。
- 验证：`cargo fmt --all --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace`、`cd web && pnpm test && pnpm build`；手工冒烟（真实 provider + 设置页密钥）作为发布前检查。

## 6. 兼容性与回滚

- **旧 workflow**：无 `mode` → command 模式；请求 JSON/输出/错误/effect_policy/回归 fixture 全不变。
- **command 模式子进程环境**：executor 现在获得 PATH/HOME 等白名单变量（原先为空），外部 CLI 行为可能因此更正常（也属于可观察变化）——文档记录，不改变 flow 的请求/输出契约。
- **semantics=1 + agent**：保存期 `Definition::validate` 拒绝（明确报错）。
- **旧 journal**：`ExecutionSnapshot.agent` 为 `Option` + `serde(default)`，旧 run 可读；新 run 才带字段。
- **旧执行器 / 新 master**：`_flow_execution.runtime` 与 `RUNTIME_ID` 检查拒绝，不会静默跑错实现。
- **新执行器 / 旧 master**：agent 节点缺 `_flow_execution.agent` → `agent_error` 明确「配置/runner 未下发」；旧 master 的 `harness_request` 会因缺 `command` 报错（降级后 agent 节点失败而非静默）。
- **配置回滚**：删除 `[harness.agent]` 段；`deny_unknown_fields` 下旧二进制读新配置会拒绝，回滚需同时回退配置（文档写明）。
- **发布注意**：任何 executor/engine 改动都会改 `RUNTIME_ID`（`flow-engine/build.rs` 哈希源码 + lock），升级需 master/executor/runner 锁步；in-flight run 按现有规则阻塞。runner 自身不带 `RUNTIME_ID` 也没有版本握手字段，部署文档要求与 executor **同批发布**（S4）；conga env 变量名与输出行格式由固定 git rev 挡住漂移。
- **回滚路径**：删 `[harness.agent]` + 将 agent 节点改回 command（或整体 revert）；不迁移数据、不改 journal 格式版本、不 bump 协议。

## 7. 测试矩阵

1. **command 逐字节不变**：直接断言 operation request 序列化恰好 5 字段（固定字面量），现有 harness e2e 全绿。
2. **节点校验**：mode 字面量/跨模式参数拒绝/条件必填/workdir 绝对/`api_key` 进 secret_params/semantics=1 拒绝。
3. **配置**：成对校验、URL 与 proxy userinfo 拒绝、边界、merge_patch、config_view 往返、旧 toml 无 section 可解析。
4. **快照**：新 run 冻结配置；配置重启后变更不影响 in-flight run；**回归候选继承 baseline（即使当前配置已改）**；旧 journal 反序列化。
5. **env 契约**：executor 白名单；runner 内 `PATH/HOME` 可见且 HOME 为临时目录；executor 传入的 `CONGA_*` 恰为 §4.4 非敏感矩阵（无 `CONGA_LLM_KEY`/`CONGA_CONFIG_DIR`/`CONGA_GATEWAY_MODE`/`CONGA_PROJECT_DIR`，无多余项）；无 `FLOW_*`；workdir 的 `.env`/`.conga/config.toml` 不生效（headless 不调 apply/dotenvy）；三平台 smoke（Windows 含 `SYSTEMROOT`/`COMSPEC`/`PATHEXT`，`command -v cargo` 或平台等价物成功）。
6. **runner**：EndTurn 成功；Error；中间 MaxTokens 后成功；末轮 ToolUse=max_turns 失败；Aborted=130（防御性）；装配失败退出 2 + error 行；工具失败不致命；key 不经 env（stdin → 参数 → 结构体）；毒化 settings.json 不生效（内置 prompt/env tunables/env provider）；cleanup。
7. **生命周期**：agent 在 bash 中取消/超时 → 进程组后代不存活；临时 HOME 删除；并发 agent 节点互不串。
8. **凭据/泄漏**：credential=None → 合法 outcome B → 重启派生 Business；journal/observations/executor stderr(`RUST_LOG=debug`)/runner stderr/workdir `.conga/hooks.json` 均无 key/prompt；runner 及其全部子进程环境无 key（结构体注入，含 `/proc/<pid>/environ`）；proxy 凭据不进 journal。
9. **恢复/回归**：previous_outcome 不启动 runner；fixture 匹配 0 runner 调用；配置变更仍匹配（V1）；改请求失配；缺配置 Business。
10. **dispatch/remote**：`_flow_execution.agent` 与请求配置相等；command 节点无该键；remote wire 可见；旧 executor 被 runtime 拒；`FLOW_HARNESS_AGENT_BIN` 远端定位。
11. **前端**：Settings 表单保存/重启提示；节点两模式字段渲染；后端条件错误能显示；`api_key` 候选来自 `secrets.list`（含 stored）。
12. **打包**：Tauri externalBin 构建含 runner；服务端 BINS 含 runner；runner 缺失/版本不匹配错误可诊断；runner 与 executor 同批发布（runner 不带 `RUNTIME_ID`，S4）。
13. **config 根隔离（N1）**：conga-host 从 `roots` 设置 `CONGA_CONFIG_DIR` 后 session/tool state/spill/memory/skills/mcp 全在临时根；settings 不被读由 stream_fn 注入缝 + 毒化 settings.json 单测覆盖（与目录隔离解耦）；`headless_prepare` 自检失败即 fail loud；conga 未设置该 env 时与旧行为一致；Windows 冒烟（`dirs` 不读 HOME 的回归被 patch 1 覆盖）。
14. **上限**：7.5 MiB 文本截断带标记，且低于派生期 `> 8 MiB` 检查（S3；capture 期 256 MiB 上限不构成约束）。

## 8. 已知取舍

1. **runner 与 conga（v5 收紧、v5.1 收权）**：runner = conga-host headless 装配入口的直接调用者——settings 不读（stream_fn 缝）、key 走参数、env 只承载非敏感配置且唯一读者是 conga-host；runner 自有代码约 30 行（stdin 载荷、单行输出、成功判定）。不自检、不解析 env、不拼 ProviderConfig。不分发 conga 二进制，输出契约由 flow 定义。
2. **配置传递（v4 拍板、v5 细化）**：协议 = conga 的非敏感 env 变量名 + stdin 两字段 + 单行输出格式 + headless 入口签名，由 conga 固定 rev 与 workspace 锁步构建挡漂移（S4）；conga 侧改名/改形状属破坏性变更，升级时锁步。代价：无运行时版本握手；收益：删掉自定义契约类型与 key 的 env 通道。
3. **conga 双装配路径漂移（v5 新增，v5.1 收权）**：headless 入口与 CLI（`build_cli`）并存；tunables/proxy 的 env 解析**必须复用 ConfigLoader 现有代码**（patch 3 规格），conga 仓库 review 把关，防两套解析行为分叉。env 的唯一写者是 executor（构造）、唯一读者是 conga-host（解析）；runner 二进制零 env 知识，`CONGA_CONFIG_DIR` 由 conga-host 从 `roots` 参数自行设置（env 与参数不再双通道）。
4. **usage/turns/tool_calls 元数据砍掉（v4）**：`agent` 元数据收缩为 `{model, stop_reason, elapsed_ms}`；如需用量统计，后续在 runner 输出行加字段即可（向后兼容）。
5. **`mode` vs 新节点类型**：用户已拍板 mode；条件必填/跨模式拒绝的校验分叉是已知代价，用保存期错误兜住。
6. **FullAuto**：与现有外部 CLI 等价；不新增沙箱，文档明示。
7. **不逐轮 journal**：延续 `EXECUTION.md:39` 的受管 operation 边界。
8. **HOME=临时目录 + `CONGA_CONFIG_DIR`（patch 1）**：隔离 conga 状态目录与 cargo 凭据；settings 隔离由 patch 3 结构性保证（不读），目录隔离只管落盘。代价是真实 HOME 的 git/ssh 配置不可用、私有 registry 凭据不继承；需要时在 workdir 的 `.git/config` 配置身份。conga 仓库三个 patch，flow 固定到含 patch 的 rev。
9. **CARGO_HOME 每次 run 全新**：隔离 cargo 凭据，但公开依赖会重复下载（S1）。Step 3 实测成本；若不可接受，后续加 `[harness.agent].cargo_home` 可选共享缓存（需接受该缓存不隔离），本计划不做。
10. **`tool_timeout_secs` 默认**：省略（conga 默认 None，bash 自身默认 120s）；取消/超时由 executor 杀进程组，不依赖工具超时。
11. **`trace_granularity=process`** 对 agent 模式沿用（受管 operation）；文档解释。

## 9. Review 处理表

### v1 review（`2026-10-19-harness-agent-mode.linus-review.md` §3）

| Review 项 | 处理 |
| --- | --- |
| B1 取消/后代回收 | v2 起改为受管 runner 子进程；v3 复用 `ManagedChild::spawn_harness` 进程组 + `capture_harness` 组杀；补 agent 版 `crate_repairs` |
| B2 env 契约 | §4.7 独立修 executor spawn 白名单；runner 显式 env；三平台验收 |
| B3 semantics=1 | `Definition::validate` 保存期拒绝 agent 节点；运行期兜底文案 |
| B4 授权后失败 outcome | §4.5 封闭三选一 marker；补恢复派生测试 |
| R1 配置进快照 | `ExecutionSnapshot.agent`；dispatch 从快照取；**v3 回归候选显式继承 baseline** |
| R2 tracing 泄漏 | runner 固定 warn filter、不继承 `RUST_LOG`；补 debug 泄漏测试 |
| R3 state 目录 | 临时 HOME + 退出清理；Windows 残留兜底记录 |
| R4 proxy userinfo | 配置校验拒绝 |
| R5 MaxTokens/max_turns | runner 用 `TurnSummary.new_messages` 末条 stop_reason 判定 |
| R6 调用点遗漏 | §5 Step 5 用 `grep -rn "start_execution("` 收口（含 `backend-e2e/tests/cli.rs`） |
| R7 跨模式参数/模板 | 保存期拒绝；`mode` 禁止模板；workdir 绝对 |
| R8 tool_timeout | 默认 None + 文档说明取消由进程组杀保证 |
| R9 回归需配置 | 矩阵补「缺配置 → Business」 |

### v2 复审（同文件「## 复审（v2）」）

| Review 项 | 处理 |
| --- | --- |
| V1 回归不继承 agent 配置 | `run_start` regression 分支 `snapshot.agent = recorded.agent.clone()`；补「改配置后 regress 仍匹配」测试 |
| V2 workdir hooks 可读 key | 密钥只走 stdin；`SessionAssembly::build` 后立即 `remove_var("CONGA_LLM_KEY")`；补 `.conga/hooks.json` 打印 env 的测试 |
| V3 HOME 语义矛盾 | 统一：executor 白名单含真实 HOME（command 模式用）；**runner 显式覆盖 HOME=临时目录**；§4.7 写明 git/ssh 取舍 |
| V4 runner_bin 归属 | 移出配置/快照/请求；executor 本地解析 `FLOW_HARNESS_AGENT_BIN` 或同目录兄弟 |
| A1 subagent stderr 泄漏 | 改用 `SessionAssembly::build` + no-op subagent emit（不用 `build_cli`） |
| A2 runner 拉重依赖 | 契约类型放 `flow-dto`；runner 不依赖 flow-engine |
| A3 command 模式 env 变化 | §6 明确记录；runner spawn `env_clear` + 显式 env，防 `FLOW_*` 进 runner |
| A4 workdir 未校验 | 绝对/存在/目录/canonicalize，保存期 + 运行期双校验 |
| A5 装配失败形状 | runner 信封 `error` + 退出 1 → outcome A；非法信封 → outcome B |
| A6 error→stderr 映射 | 明确 error 同时进 `outcome.stderr` 与节点输出 stderr |
| A7 e2e runner 定位 | `flow_test_support::io::harness_agent_bin()` + `FLOW_HARNESS_AGENT_BIN` |
| A8 降级说明 | §6 补「旧 master 下 agent 节点缺 command 明确失败」 |

### v3 终审（同文件「## 终审（v3）」）

| Review 项 | 处理 |
| --- | --- |
| N1 Windows 临时 HOME 不隔离 conga | §4.4.1 增 conga `CONGA_CONFIG_DIR` 最小 patch（Step 3 前置），runner 启动自检 config_dir 在临时根内否则 `agent_error`；flow 固定到含 patch 的 rev |
| N2 `remove_var` 线程安全 | runner 使用 `new_current_thread` runtime；`remove_var` 在装配后、`run_turn` 前于主线程执行 |
| N3 workdir 保存期误拒 | 保存期只查非空/绝对/非模板；存在/目录/canonicalize 在 executor 授权前 |
| S1 CARGO_HOME 重复下载 | Step 3 实测；记录为已知取舍，后续可选共享缓存 |
| S2 AgentRequest Debug 泄漏 | Debug 对 `api_key` 打码 + 断言测试 |
| S3 信封 8 MiB 上限 | runner 在信封前截断并加标记；executor 仍保留 8 MiB 解码上限 |
| S4 runner/executor 版本 | 部署文档写明同批发布；v3.1 以信封 `v` + `FLOW_HARNESS_PROTOCOL` 双保险，v4 改为固定 rev + 锁步发布（见下） |

### v4 简化（用户拍板，2026-10-19）：runner 走 conga CLI 同款装配与 env 配置

| 变更 | 内容 |
| --- | --- |
| runner 协议 | 删 stdin 大契约（`{v, config, workdir, session_id, store_root}`）与版本握手（信封 `v` + `FLOW_HARNESS_PROTOCOL`）；配置全部走 conga CLI env 面（`ConfigLoader`），stdin 只带 `{api_key, prompt}` |
| 装配 | `SessionAssembly::build` + no-op emit → `build_cli`（`conga exec` 同款）；A1（subagent tap 泄漏 stderr）改由 patch 2（`eprintln!`→`tracing::debug!`）闭合 |
| 事件汇总 | usage/turns/tool_calls 砍掉；`agent` 元数据 = `{model, stop_reason, elapsed_ms}` |
| 契约类型/依赖 | `AgentRequest`/`AgentEnvelope`/`AgentMeta` 删除，仅留 `AgentRunConfig`；runner 不再依赖任何 flow crate（A2 收敛为零）；S2 Debug 打码随之消失 |
| key 通道不变的理由 | env 直传会让 key 进 runner exec 环境初始块，`/proc/<pid>/environ` 同 uid 可读，workdir hooks 无需模型配合即可外带；stdin 通道（v2 review V2 的修法）保留 |
| 新增安全边界 | runner cwd=临时 HOME：`config_file::apply` 从 cwd 发现 `.conga/config.toml`、dotenvy 读 cwd `.env`，cwd=workdir 会让仓库自带配置自动注入（如 `CONGA_LLM_PROXY` 劫持 provider 流量） |
| 终审条件复核 | N1 仍硬性前置（`resolve_turn_provider` 每轮用 settings.json 覆盖 env，`conga-host/src/lib.rs:197-218` 已复核）；N2 机制保留并加强（装配后 **drop runtime** 再移除，消除 blocking-pool 线程并发读 env 的残余风险）；N3 不受影响 |
| 顺带修复（v3.1 复查） | S3 截断阈值 8 MiB→7.5 MiB（标记会越过派生期 `task.rs:1398` 检查）；§4.4 env 矩阵补 Windows 条目（消除与 §4.7 矛盾）；删 `FLOW_HARNESS_*` 冗余 env（消除与 §7.5 断言矛盾）；N2 理由按 drop-runtime 重写 |

### v5 收紧（用户拍板，2026-10-19）：conga-host headless 直连——不读 settings.json、密钥显式注入

| 变更 | 内容 |
| --- | --- |
| conga 复用面 | `build_cli` → conga-host 新增 headless 装配入口（patch 3，约 100-150 行）：settings=None 短路、provider/project_dir/store_root/mode 显式参数、env 只解析非敏感面（复用 ConfigLoader 代码）；runner 瘦到约 50 行 |
| settings.json | flow 路径**永不读取**（N1-config 结构性闭合，不再依赖目录隔离保证配置正确）；patch 1 `CONGA_CONFIG_DIR` 退守为状态目录隔离（tool state/spill/memory/skills/mcp/session 落盘） |
| 密钥 | env 通道彻底删除：flow 密钥体系（stored / `FLOW_SECRET_*`）→ `OperationPermit` → runner stdin → `ProviderConfig` 字段；`set_var`/`remove_var`（N2 机制）与 V2 的 env 面消失；runner 对 env 中 `CONGA_LLM_KEY` fail loud |
| 注入通道 | headless 入口不调 `config_file::apply`/dotenvy——workdir `.env`/`.conga/config.toml` 自动装配通道机制性消失（v4 靠 cwd=临时根规避，v5 靠不调用根除）；`CONGA_GATEWAY_MODE`/`CONGA_PROJECT_DIR` 改显式参数 |
| 终审条件终态 | N1 = patch 1（状态落盘）+ patch 3（配置读取）双重闭合；N2/V2 **消失**（零 env 变更、env 无 key）；A1 = patch 2 不变；B1 进程边界不变（runner 子进程，`EXECUTION.md:39`） |
| 新增风险 | conga 双装配路径（CLI/headless）漂移——patch 3 规格强制 tunables 解析与 ConfigLoader 同源；S1/S3/S4 取舍不变 |

### v5.1 修订（Linus review，2026-10-20）

| Review 项 | 处理 |
| --- | --- |
| P1 settings 短路机制不完整（只挡 provider，`system_prompt`/`max_tokens` 仍会被 settings.json 覆盖） | 改挂现成的 `stream_fn_overridden` 缝（`lib.rs:427-428`）：注入 stream_fn 后 settings 取 `EnvSettings::default()`，`load_settings_async` 不可达；`resolve_turn_provider` 零改动（§4.4 第二条 + patch 3 规格）；等价性由「毒化 settings.json」单测钉死（conga 侧 + flow 侧各一次） |
| P2 flow 侧第二份 env 解析（runner 拼 ProviderConfig） | `build_headless(api_key, roots, project_dir, mode)`：provider/tunables 的 env 解析全在 conga-host（ConfigLoader 同源）；runner 瘦到约 30 行、零 env 读写 |
| P2 存储根双通道（store_root 参数 + `CONGA_CONFIG_DIR` env） | 合并为单一 `roots` 参数；conga-host 的同步 `headless_prepare(roots)` 在主线程（无 runtime）自行 set_var + 自检；executor 下发的 env 不再含 `CONGA_CONFIG_DIR` |
| P3 `CONGA_LLM_KEY` fail-loud 归属 | 移进 conga-host（`headless_prepare`），变量名与检查同一所有权 |
| P3 patch 3 测试缺口 | 补「headless ≡ 空 settings CLI 语义」等价断言（system_prompt/max_tokens/provider 三条）与 headless approver deny |
| 漏写（review 中发现） | stdin 载荷补 `workdir`——v5 称 workdir 为显式参数但未定义到达 runner 的通道 |
| 可忽略项 | `Aborted`→130 标注为防御性路径（flow 取消走进程组 SIGKILL，不依赖它）；usage 砍掉的取舍维持 |

## 附录 A：关键证据索引

- 节点/校验：`flow-engine/src/model.rs:39-52,226-244,306-311,330-362,627-669,832-873,1505-1519,1615-1645`
- command 执行：`flow-engine/src/exec.rs:73-158`；`flow-executor/src/task.rs:1136-1217,1222-1350,1379-1410,1591-1674`；`flow-engine/src/process.rs:20-21,109-160`
- 派发/授权/密钥：`flow-backend/src/execution/dispatch.rs:327-345,398-410,1303-1316`；`flow-engine/src/secrets.rs:17-85,90-144`
- 配置/快照/回归：`flow-config/src/lib.rs:57-64,435-503,528-557`；`flow-engine/src/journal_state.rs:137-149`；`flow-backend/src/execution_snapshot.rs:52-54,85-94`；`flow-backend/src/regression.rs:35-39,141`；`flow-backend/src/journal_commands.rs:614,630-636`；`flow-rpc/src/lib.rs:208-210`
- executor 隔离：`flow-engine/src/execution_protocol/spawn.rs:52-55`；`loopback.rs:108-113`
- conga：`conga-cli/src/exec.rs`（headless 入口参照：`--json`/stdin `-`/退出码 0/1/130/2/headless approver deny）、`conga/src/config_file.rs:296-336`（`apply`/`discover`/`overlay`——headless 入口**不调用**；env 名单来源）、`conga-host/src/assembly.rs:199-282`（`build`）、`283-320`（`build_cli`）、`conga-host/src/lib.rs:142-235,197-218,378-560`（Host/run_turn）、**`lib.rs:423-438,495-502`（每轮 settings 加载、`stream_fn_overridden` 缝（427-428）、`settings.max_tokens` 覆盖（437）、`settings.system_prompt` 替换内置 prompt（495）——patch 3 的机制依据）**、`conga-host/src/settings.rs:39-44,179`（`EnvSettings` 字段与 `effective_provider`）、`conga-host/src/process_hooks.rs:91-95,275-285`（hooks 无 env_clear）、`conga-host/src/tools/mod.rs:23-35`、`conga-host/src/prompt.rs:33,97,144`、`conga-host/src/session_cleanup.rs:41-44`、`conga/src/agent_loop.rs:51,92-117`、`conga/src/providers/openai_compat.rs:79-82`、`conga/src/storage/mod.rs:41-50`（patch 1 目标：`config_dir()` 支持 `CONGA_CONFIG_DIR`，须跳过 `.gasket` legacy 回退）、`conga-host/src/assembly.rs` subagent `eprintln!`（patch 2 目标）

## 附录 B：依赖决策

- 固定 git rev（MIT），只被新 runner crate 依赖；executor 不链接；默认 feature 全关，不引入 rquickjs/sqlx/qdrant/landlock。
- 已知副作用：`toml 0.9` 与 flow 的 `0.8` 双版本（仅 runner 内）；v4 起 runner 不依赖任何 flow crate（配置经 env、输出为私有单行类型，v3.1 的「runner 依赖 flow-dto」随之删除）。新增 crate 数/体积/构建时间在 Step 3 实测记录，不作为门禁。
- runner 与 flow 共用 workspace/Cargo.lock，避免跨仓二进制版本漂移。
