# 权限模型实现计划（P0+P1+P2）

> 状态：**已实现**（P0+P1+P2；G4 已于 2026-11 审计修复后接线，Event.actor 的缺口见 09 §13 实现注记）。依据 [09-permission-model.md](../design/09-permission-model.md)。

## Context
09 设计要把「谁允许谁做什么」从散落、隐式、事后补问的状态，收敛为「能力上限 + 审批」分离的模型。现状问题：
- G3 `handle_request_operation`（`crates/flow-backend/src/execution/dispatch.rs:1360`）对副作用自动授权。
- harness 插件把 `Mode::FullAuto` 写死（`plugins/harness/plugin/src/main.rs:200`）。
- `plugin_node`（`crates/flow-plugin-runtime/src/task.rs:1694`）不经任何授权。
- 鉴权只有一个 token，通过即全通（`crates/flow-rpc/src/journal_v2.rs:394`）。
- 审计没有 actor 和裁决记录，也没有哈希链。

按文档 P0→P1→P2 顺序全部落地。每一期结束时系统可用、测试全绿，再进入下一期。

## 已定决策
用户已确认：
1. 范围：P0+P1+P2。
2. legacy 定义（没有 `permissions` 块）：含副作用节点（http_call/email/llm/harness/动态插件）的，run.start 拒绝（cron、webhook、子工作流触发同样拒绝），返回新错误码 `-32020` 并列出节点；只含纯计算节点的照常运行。已在飞的 legacy run 一旦走到 G3，一律 Deny（文档：缺少策略快照的未完成 run 不自动恢复执行）。（2026-11 审计修复：动态插件节点正式纳入 legacy 门与 G4 授权链，本条全部兑现。）
3. Ask 等待期间执行器原地等待。待审批的派发释放调度槽和本地池许可，挂起总数上限为 A_MAX=16。远程 agent 的槽位照常占用，记为已知限制。
4. web：审批卡片、收件箱、workflow/节点权限编辑器、legacy 迁移横幅；P2 另加 API key 设置。

文档未定义或自相矛盾的地方，取舍如下（实现后写回 09）：
- Action 闭集补 `shell` 和 `file_delete`（§3.4 的 matcher 表区分了它们，§3.2 的枚举漏列）。`SendEmail` spec 加 `recipients`；Exec/Shell 加 `stdin_digest`，因为 harness 的 prompt 走 stdin。
- 比文档更严的几条：email 额外要求 endpoint 的 Net(POST) 能力；http/llm 如果设了 `proxy`，额外要求 proxy URL 的 Net 能力。目的是防止模板化的端点把密钥送出去。
- LlmCall 的 provider 取规范化端点 URL（scheme://host[:port]/path），model 取 body.model（llm 节点）或冻结的 agent 配置。
- harness command：`command` 展开后必须是规范化的绝对路径，否则 Deny；不做 PATH 解析，避免检查与执行之间被替换（TOCTOU）。形如 `<sh|bash|zsh|dash|ksh> -c <script>` 且没有多余参数的，归为 Shell{executable, sha256(script), cwd}；Shell matcher 额外精确匹配 executable。
- 审批等待不复用 WaitRegistered：它是结果事件，会封口 attempt（I17 屏障）。改为由 PermissionAsked 折叠出非封口的 `Wait::Approval`（run 保持 Running），PermissionDecided 解除。
- request_hash 按文档包含 dispatch。重启换 dispatch 时，执行器沿用原 operation_id（ExecuteTask 带上 `pending_operation`），生成新 hash、继承原 deadline，需要重新批准。
- Run 授权摘要 = sha256(canonical{actor, specs, request_fingerprint, policy_digest})。请求字节本身不含 operation_id 和 dispatch，天然排除了调用实例。
- 权限 Deny 视为不可重试的业务失败。
- manifest 能力声明允许族级写法 `any = true`（必须显式写出，不能靠省略字段得到），只作上限用；workflow/node 层的 matcher 永远精确、不支持通配。manifest 的 exec 用 `program`（裸名，匹配已解析路径的 basename）+ `argv_exact`，绝对路径由 workflow 层把关。
- 不支持反向通道的第三方插件（协议 v1）：typed 授予要求 macOS sandbox-exec 执法，其他平台直接拒绝。legacy-unrestricted 插件需管理员显式确认后才能用，Plan 模式下拒绝；节点权限约束不了它，UI 上明确标注。
- bundled 的 harness/llm 插件同样按内容哈希授予，app 升级后需要重新授予。
- agent-host 只改 headless 受管路径；交互式 gateway 模式不在本次范围。
- journal 格式 4→5：writer 打开时用单个事务原地迁移，新增 `prev_digest`/`chain_digest` 两列并顺链回填。`digest` 仍是 sha256(body)，链值 = sha256(prev_chain ‖ digest)。效果与文档等价（改动任一帧，其后的链全部断开），且不破坏读时复核。

## 共享底座
- `crates/flow-dto/src/permission.rs`（新）：Principal、Action、OperationSpec、ResourcePattern、Capability、Approval、ExecutionMode、WorkflowPermissions{version=1, mode, allow}、NodePermissions{Inherit|Restrict}、PermissionRequest/RequestContext、DecisionRecord/Modifiers、PermissionProjection。
  - ResourcePattern 是闭集，手写反序列化：拒绝未知字段/action/版本，拒绝 `${`，要求 ID 唯一；URL 拒绝 userinfo 和 query；Exec 要求规范化绝对路径；摘要必须是 64 位 hex。
  - canonical 摘要在 flow-dto 内自实现（排序键 + sha256），因为 P2 会让 flow-journal 依赖 flow-dto，反过来依赖会成环。
  - workspace 新增依赖 `url = "2"`。
- `crates/flow-backend/src/policy.rs`（新）：纯函数 `check(req, &FrozenPolicy, &RunApprovals, evaluated_at) -> Evaluation`，严格按 §4 步骤 0–4 执行：
  - Plan 模式下变更性操作直接 Deny，然后检查硬上限层（P1 的插件授予）。
  - 各层取 AND，层内取 OR；任一命中项是 Ask 即需询问；精确的 Run 授权可免除询问。
  - matcher：Net 按 scheme/host/port/路径段前缀匹配；Exec 要求 argv 全等；File 相对 workdir 做词法规范化，拒绝越根；Email 要求全部收件人都在允许集合内；Secret/LLM 精确集合匹配。
- `policy/derive.rs`：从冻结的节点定义 + 请求 JSON 派生需求集合，不信任插件自报的身份、风险或摘要。

## P0 主闸门
1. **定义字段**（`crates/flow-engine/src/model.rs`）：新增 `DefinitionData.permissions` 和 `Node.permissions`，用 default + skip_none，旧定义的序列化字节不变；新字段计入 checksum。`validate_inner` 增加：有权限块必须 semantics=2；节点权限要求工作流也有权限块；节点能力明显越出 workflow 上限时给诊断。
2. **事件**（`crates/flow-journal/src/codec.rs`）：PolicyFrozen、PermissionAsked、PermissionDecided、RunApprovalUpserted/Removed、WorkflowPermissionsChanged。
3. **折叠**（`crates/flow-engine/src/journal_state.rs`）：
   - 新增 `WaitKind::Approval` 和 `Wait::Approval{operation_id, request_hash, deadline}`；WaitWire 的新字段用 skip_none，并用 golden 测试钉住旧 JSON 形状。
   - Run 增加 `policy`（digest/mode/legacy/子工作流上限层）和 `approvals`（Run 授权投影 + 事件水位）。
   - Node 增加 `approval`，跨 DispatchStarted 保留，用来继承 deadline。
   - **I26 折叠闸门**：有策略的 run 中，OperationAuthorized 之前必须已有同一 operation_id 和 fingerprint 的 PermissionDecided{allow}。
4. **G2 冻结**（`journal_commands.rs::run_start`、`execution_snapshot.rs`、`dispatch.rs:1292` 子 run 注册）：
   - RunStarted 与 PolicyFrozen 同一事务写入；legacy 静态门在这里执行。
   - 子 run 的策略 = 子定义的权限层 + 父节点的有效层作为上限，不继承父节点凭据。
   - Plan 模式 + harness agent 拒绝启动（P1 完成后解除）。
5. **G3**（`dispatch.rs::handle_request_operation`）：派生需求 → 构造 PermissionRequest（actor=Node，context 来自已认证的 attempt）→ check：
   - **Allow**：同一事务写 PermissionDecided{System} + OperationIntent + OperationAuthorized，再发 Permit。凭据在全部需求通过后才解析（I37）。
   - **Deny**：写 PermissionDecided，向执行器发 `OperationDenied`，节点业务失败。
   - **Ask**：写 PermissionAsked（含请求值、fingerprint、deadline），发 `OperationPending`。runner 挂起：释放槽位，暂停 agent_deadline 和远程空闲判定。之后在 commits 通知里观察裁决：已授权 → 取回槽位后发 Permit；拒绝或过期 → 发 `OperationDenied`。100ms tick 内执行 TTL：用守卫事务写 System Deny（I34）。
6. **执行协议 v8**（`crates/flow-engine/src/execution_protocol/{message,contract}.rs`）：
   - 新增 OperationPending、OperationDenied；ExecuteTask 增加 `pending_operation`；Permit 支持多个 `credentials`。
   - PROTOCOL_VERSION 7→8，旧执行器在握手阶段即被拒。
   - 执行器侧 `task.rs::authorize_operation` 处理两条新消息；`remote.rs` 的中继覆盖新变体。
7. **挂起不计槽**：
   - `execution/pool.rs` 的 SessionLease 许可改为可 park/unpark。
   - `journal_driver.rs` 的 `has_capacity` 扣除挂起数。
   - 重启后，没有活动任务的 Approval 等待节点按「无 operation 的在飞节点」重派发。
8. **RPC 与命令**（`crates/flow-dto/src/rpc.rs`、`crates/flow-rpc/src/journal_v2.rs`、`journal_commands.rs`）：
   - `run.approve` / `run.deny`：仿照 `run_adjudicate` 的三段式（deduped / preflight / command）。提交时重新 check，能力已过期则 System Deny。幂等：同一 hash 返回既有裁决；换 hash 视为冲突；只能裁决当前 pending 的请求（I32）。Allow 时同一事务写 PermissionDecided{by}、（scope=run 时）RunApprovalUpserted、Intent + Authorized。
   - 新增 `run.approvals.revoke`、`approvals.list`（收件箱，返回规范化需求和有界请求正文，只含 secret 引用）、`permissions.effective`。
   - workflow 写入时，若权限摘要变化，写 WorkflowPermissionsChanged。
   - 业务触发面 `journal_triggers.rs` 不提供任何审批入口（I33）。
9. **配置**：`[permissions] approval_ttl_ms`，默认 24h，夹在 60s 到 7d 之间。
10. **视图**：timeline 和 run 视图带上节点的审批状态与裁决历史。
11. **web**：
    - 类型、API 与 `rpc/methods.ts` 镜像新方法。
    - `ApprovalCard.vue`：宿主用闭集组件渲染 argv/cwd、URL、收件人、摘要、actor、命中的能力项、服务端 deadline；默认 Once；只用纯文本渲染，不用 v-html。接入 RunMonitor，仪表盘加收件箱。
    - `CapabilityEditor.vue`：按 action 提供结构化字段；Shell 用 WebCrypto 计算脚本摘要。
    - 编辑器顶栏增加「权限」页；ParamsModal 增加「权限」页签（Inherit/Restrict，同屏显示派生状态）。
    - 复制节点、改节点类型时保留 permissions；legacy 定义显示迁移横幅。
12. **CLI**：新增 `run approve|deny|approvals`。

## P1 插件与工具级
1. **manifest**（`crates/flow-config/src/plugin_manifest.rs`）：新增 typed `[permissions]`（schema、required_features、capabilities 及 required）；`secret_fields` 必须指向本插件的 `Widget::Secret` 字段；任何错误都拒绝加载；缺少该段的标记为 legacy-unrestricted。新增 PermissionProjection；给 bundled 插件的 plugin.toml 补上声明。
2. **内容哈希**：两侧 registry 都计算 sha256(manifest ‖ entry)；执行器 spawn 前复核（换二进制即拒）；插件请求携带 (id, content_hash)，master 核验与冻结授予一致（远程执行器脱钩即拒）。（取舍：不做执行协议 Hello 清单上报——逐请求核验比启动期清单快照更强；I35 授予查找只认精确哈希。）
3. **授予**：
    - 新增 `permissions.grant` RPC，写 PluginGrantUpserted；只写 journal，不回写 plugin.toml。
    - `plugins.list` 显示授予状态和升级差异；`plugins.recheck` 把「待授予」检出。
    - run 启动时冻结授予快照。
4. **SDK 协议**：Capability 新增 `master_permit_v1`，协议 minor 1→2；新增 `permission/request` 方法；补 vectors，同步 `wire.md` 和 Python 示例。
5. **执行器反向通道**（`plugin_session.rs`、`agent_plugin.rs`）：invoke 并发处理插件发来的请求，经执行协议转给 master 的 ToolPermissionRequest，再把 ToolPermissionDecision 回给插件。等待期间暂停 watchdog。
6. **master G5**：
    - 工具映射闭集：read/list/grep→FileRead；write/edit→FileWrite；bash→Shell；fetch→Http GET；todo/spawn_subagents 视为无外部动作；evolve、未知工具、后台 bash 一律 Deny。
    - operation_id 由 master 分配，按 (dispatch, tool_call_id, hash) 只去重传输重试。
    - 有效层 = workflow ∩ node ∩ 插件授予 ∩ 上限。
    - Ask 时会话内等待；会话丢失走现有 Uncertain 路径，并关闭 pending。
    - ToolEnd 作为审计 `tool_outcome` 记为执行证据。
7. **harness**：
    - RunTurnParams 增加 TurnPermission；删除 FullAuto 硬编码。
    - 不再本地 Allow，执行端只做提前 Deny；ApprovalLedger 按 (operation_id, hash) 去重。
    - main.rs 增加请求/响应关联；受管模式下 bash 改用 one-shot `sh -c`，fetch 不跟随重定向。
    - 握手缺少 `master_permit_v1` 时拒绝执行。
8. **通用插件节点**：
    - 走 authorize_operation，并校验授予和 required 能力。
    - 支持反向通道的插件，逐条走 `permission/request`。
    - 不支持的插件：要求被 workflow∩node 覆盖，并在 macOS 上套 sandbox-exec profile（扩展 `agent-host/src/tools/sandbox.rs`）；其他平台拒绝。
    - 凭据只解析已授予的 secret_fields，经 Permit 交付。
9. **web**：插件授予与升级差异页面；历史 run 渲染冻结的投影。

## P2 认证与审计强化
1. **API key**：
    - 新增 `[[api_keys]]{id, token_hash, scopes, expires_at}`。
    - `admit_call` 映射 Principal：FLOW_JOURNAL_TOKEN 为 LocalAdmin；其余 key 常时比较。
    - 分档：approve scope 只允许 Once；授予、Run 授权、权限变更、key 管理需要 admin。
    - Principal 贯穿到裁决记录的 by；~~提供 key 管理 RPC，明文只在创建时显示一次~~（v1 取舍：key 管理走 `[[api_keys]]` 配置文件 + 重启生效，管理 RPC 与 web 设置卡未实现；明文 token 由管理员自行生成并哈希入库。见 09 §13.4）。
2. **权限变更鉴权**：纯函数分类器，非 admin 只能做可证明的收紧；update/import/publish 全部校验。
3. **Event.actor**：新增 `Event.actor: Option<Principal>`（flow-journal 依赖 flow-dto，旧事件字节不变）；run.start 不再写死 manual。
4. **哈希链**：格式升到 5 并原地迁移；`append` 写链；读时和 `maintenance::verify()` 顺链校验。
5. **审计查询**：`run.audit.page` 增加 `Filter::Permission`；新增 `permissions.audit.page`，支持 JSONL 导出（CLI 同步）。
6. **web**：~~API key 设置卡；没有 approve scope 时隐藏操作入口，服务端仍独立拒绝~~（v1 取舍：key 管理走配置文件，web 不感知调用方 scope；服务端 scope 裁决独立生效。见 09 §13.4）。

## 文档
- 09 标为已实现，写入上述取舍。
- 08 增加 I26–I38。
- 06-rpc 补新方法与错误码。
- DESIGN.md 地图挂上 09。
- 写明执行协议 v8 与插件协议 0.2 的契约。

## 验证
每期跑：`cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace`、`cd web && npm run typecheck && npm test && npx playwright test`。

**单元测试**
- DTO 闭集解析。
- policy 真值表：层交集、OR+Ask、与排序无关、过期、Plan、Net 路径段/端口/userinfo、Exec/Shell、收件人、Run 授权在 cwd/body 变化时失配、I31 确定性。
- 派生、折叠（含 WaitWire 旧形状 golden 与 I26 闸门）、manifest、工具映射、权限变更分类器、哈希链。

**e2e**（`crates/flow-test/e2e/tests/permissions_*.rs`）
- P0：
  - auto/ask/deny 三条路径。
  - Secret 未授权时零凭据、零请求。
  - Plan 不会被 Allow 提权。
  - approve 幂等、换 hash 冲突、取消后无法批准、TTL 到期。
  - pending 中重启：生成新 hash、继承原 deadline。
  - Run 授权：adjudicate retry 后免问，其他节点仍需询问。
  - x_max=1 时，挂起的审批不阻塞并行分支。
  - legacy 拒绝与纯计算放行；子工作流上限。
- P1：
  - 插件未授予或哈希变化后执行被拒。
  - harness 的 `git status` 免人工审批但逐次落盘，`git push` 弹出审批（mock provider 扩展为可脚本化的 tool_call）。
  - 旧 host 握手被拒；会话丢失进入 Uncertain；重复 Permit 不重复执行。
- P2：approve-only key 不能写 workflow；verify 能发现中段篡改。

**现有测试**
- test-support 新增夹具帮助函数，按测试节点显式生成精确能力。
- 约 23 个 e2e 文件、web e2e 和 examples 补 permissions 块；更新协议版本断言。

**手动走查**
- 启动 server + web dev，跑一遍 http 节点的询问→批准→完成。
- 授予 bundled 插件后跑 agent 节点。

## 风险
- 改动面极大，按期推进，每期保持测试全绿。
- 两套协议同时升版本，远程 agent 必须同步升级。
- bundled 插件哈希随构建变化，需要重新授予；测试夹具统一授予。
- journal 格式升级后不可降级。
