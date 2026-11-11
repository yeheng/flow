# 09 · 权限模型：从单条命令到 auto-approve（认证 · 授权 · 审批 · 审计）

> 本文是新增分篇，不在 DESIGN.md 原章节编号内。设计状态：待实现；本文中的新字段与协议不代表现有实现。采用能力上限与审批分离的模型。提出新不变量 I26–I38（续
> [08-invariants-and-limits.md](08-invariants-and-limits.md)）。所有落点均标注现有代码位置；
> 未标注的实现细节以本文为准。

---

## 1. 问题、目标与威胁模型

### 1.1 要解决的问题

今天系统里「谁允许谁做什么」是散的、隐式的、事后才问的：

- harness 插件硬编码 `Mode::FullAuto`（`plugins/harness/plugin/src/main.rs:200`），
  工具级权限引擎（`plugins/harness/agent-host/src/permission.rs`）整条链路存在但被短路；
- 通用插件节点 `plugin_node`（`flow-plugin-runtime/src/task.rs:1694`）spawn 任意可执行文件，
  拿到的是用户全部权限，没有任何授予语义；
- `handle_request_operation`（`flow-backend/src/execution/dispatch.rs:1360`）对副作用
  **自动授权**——它是耐久性闸门（C3/C4），不是权限闸门；
- API 单 token 全通（`FLOW_JOURNAL_TOKEN`），认证有、授权无；
- 审计流水有（`audit_seq`、Intent→Authorized→Outcome），但没有 actor、没有决策记录。

### 1.2 目标

一条从**最细粒度（单条命令的精确 argv）到最粗（全局 auto-approve）**的连续光谱，
覆盖四个层面：

| 层面 | 主体 | 例 |
| --- | --- | --- |
| 工作流节点 | http_call / email / llm / harness / 动态节点 | 「此节点只能访问 `api.github.com`」 |
| 插件 | 已部署插件（harness / llm / 三方） | 「授予此插件 exec:git,npm；拒绝联网」 |
| 工具调用 | harness turn 内的 bash/write/fetch | 「`git status` 免批，`git push` 要问」 |
| API 与触发器 | 人 / cron / webhook / 远程 agent | 「cron 触发的 run 高危操作必须停下等人」 |

以及三张配套网：**认证**（主体是谁）、**审计**（谁在何时批准/拒绝了什么）、
**确定性**（回放不重新问人）。

### 1.3 非目标与诚实的威胁模型

- **不做多租户。** C7（单用户凭据）仍是边界；本文引入 Principal 抽象是为了让
  「未来加用户」不推翻数据结构，不是为了现在做租户隔离。
- **同 uid 插件进程不是安全边界。** 任何与执行器同 uid 的进程本质上已拥有用户全部
  权限（现有 `ipc/challenge.rs` 的文档自己也承认这点）。本模型防的是**意外与越权声明**
  （agent 乱跑命令、插件用了没授予的能力、cron 半夜发邮件），不是防已沦陷进程的恶意
  逃逸。`sandbox-exec`（`plugins/harness/agent-host/src/tools/sandbox.rs`）是降险修饰符，
  不是边界承诺；进程级强隔离属于 OS 沙箱课题，见 §12。

---

## 2. 现有资产：复用，不另起炉灶

好权限系统的烂法千篇一律：再发明一套平行机制。本设计的每块底座都已存在：

| 现有机制 | 位置 | 在新模型中的角色 |
| --- | --- | --- |
| 副作用授权协议（OperationIntent→Authorized→Permit，C3/C4） | `task.rs:1384` / `dispatch.rs:1360` | **授权漏斗**。插入策略评估的唯一位置 |
| Wait/Signal 暂停恢复（`WaitKind`、`run.signal`、`AwaitingResume`） | `journal_state.rs:108`、`journal_commands.rs:1109` | 审批等待复用这套机制（新增 `WaitKind::Approval`） |
| 工具级权限引擎（Mode/RiskLevel/Approver/ApprovalLedger） | `agent-host/src/permission.rs` | 复用工具 hook 与取消机制；移除本地 Allow，接入 master Permit |
| manifest 三源合一 + 升级重检 | `flow-config/src/plugin_manifest.rs`、`plugin_registry.rs` | 插件**能力声明**与**授予**的挂点 |
| journal 逐事务 SHA-256 + 离线 verify | `flow-journal/src/sqlite.rs:319`、`maintenance.rs` | 审计底座，补 `prev_digest` 链即得防篡改 |
| mTLS / WSS token / `FLOW_JOURNAL_TOKEN` / `run_token` | `execution/remote.rs`、`journal_triggers.rs:212` | 认证底座，Principal 的现有来源 |
| `ExecutionSnapshot` 插件冻结（U1） | `execution_snapshot.rs:7` | 权限策略快照随 run 冻结，同一套语义 |
| `Widget::Secret` + secrets store | `plugin_manifest.rs`、`secrets_store.rs` | secret 访问作为一类 Action 接入裁决 |

---

## 3. 核心模型（数据结构优先）

先定数据，再定流程。权限 DTO 落在 `flow-dto`，定义字段落在 `flow-engine::model`；评估器在 master（§5）。

### 3.1 Principal —— 谁在行动 / 谁在裁决

```rust
pub enum Principal {
    /// 本机唯一管理员（今天的唯一人类主体；桌面/CLI/持管理 token 者）
    LocalAdmin,
    /// API key 主体（P2；见 §9）。桌面注入的 UUID token 也归此类
    ApiKey { id: String },
    /// 触发器：schedule / webhook，id = schedule_id / webhook_id
    Trigger { source: RunSource, id: String },
    /// 远程执行 agent（mTLS CN 或 WSS token 反查）
    Agent { id: String },
    /// 插件进程：身份 = id + 版本 + 内容哈希（§7）
    Plugin { id: String, version: String, content_hash: String },
    /// 发起操作的执行主体：run 内的某个节点
    Node  { run_id: String, node_id: String },
    /// 系统自动决策（TTL 超时拒绝、内置 deny 命中）
    System,
}
```

关键决定：**一次裁决同时记录「发起者」（actor）与「批准者」（approver）**。今天
`run.start` 硬编码 `source="manual"`（`journal_v2.rs:508`），事件没有 principal 字段——
这是审计的第一个洞，先补字段再谈别的。

### 3.2 Action / OperationSpec / Risk —— 想做什么

```rust
pub enum Action {
    Exec,        // 执行命令
    FileWrite,   // 写/删文件
    FileRead,    // 受管文件读取；工作区内也按节点能力检查
    Net,         // HTTP 出网
    Secret,      // 读取某个 secret
    LlmCall,     // 调用某模型
    SendEmail,   // 发邮件
}

/// 每个变体的 action() 唯一确定 Action；禁止另外传入不一致的 action。
/// Digest 指向不可变的完整载荷；执行前核验，不能只保存 UI 摘要。
pub enum OperationSpec {
    Exec { executable: String, argv: Vec<String>, cwd: String, env_digest: String },
    Shell { executable: String, script_digest: String, cwd: String, env_digest: String },
    FileWrite { path: String, content_digest: String },
    FileDelete { path: String },
    FileRead { path: String },
    Http { method: String, url: String, headers_digest: String, body_digest: String },
    Secret { secret_name: String },
    LlmCall { provider: String, model: String, payload_digest: String },
    SendEmail { payload_digest: String },
}

pub enum RiskLevel { Low, Medium, High }   // 复用 agent-core 现有定义
```

沿用 harness 工具的 risk 标注作 UI 提示，不新增风险表，不以 Low/Medium 自动授予能力。

### 3.3 PermissionRequest / Decision —— 一次裁决

```rust
pub struct PermissionRequest {
    pub actor: Principal,           // 谁发起（Node/Plugin/Trigger）
    pub operation_id: String,       // 调用实例；沿用 operation 身份，工具调用含 turn/tool_call_id
    pub spec: OperationSpec,        // 完整执行对象，action 由变体推导
    pub context: RequestContext,    // run/node/workflow/attempt/dispatch 与执行主体身份
    pub policy_digest: String,      // 冻结基础策略身份
    pub request_hash: String,       // 完整请求 canonical 编码的摘要（不含此字段与展示 preview）
}

pub enum Decision { Allow, Deny }

pub struct DecisionRecord {
    pub operation_id: String,
    pub request_hash: String,
    pub evaluated_at: DateTime<Utc>, // master 提供并持久化的求值时间
    pub policy_revision: String,    // 基础策略 + run 授权事件水位
    pub decision: Decision,
    pub by: Principal,              // LocalAdmin / ApiKey / System
    pub matched_capabilities: Vec<String>, // 分层记录命中项，拒绝时记录未满足的层
    pub modifiers: Modifiers,        // 见下
}

pub struct Modifiers {
    pub sandbox: bool,        // 执行时套 sandbox-exec profile
    pub timeout_ms: Option<u64>,
    pub egress: Option<Vec<String>>,  // Net 放行时收紧的出网白名单
}
```

`request_hash` 沿用 I11 的思路：**授权对象是请求字节摘要**，不是「善意」。Permit 与
Decision 都锚定同一个 hash，Permit 同时绑定 operation_id、dispatch_id 与 request_hash，不能挪给下一次调用。
同内容的第二次调用具有新的 operation_id，仍产生独立裁决与执行证据。master 从认证
连接推导 actor/context、计算摘要和风险，不信任插件自报的身份、hash 或风险级别。

执行载荷与规则匹配分离：OperationSpec 使用具体路径、完整 argv 与载荷摘要；
ResourcePattern 是按 Action 区分的 matcher 枚举，只支持 §3.4 明确列出的匹配方式。
UI preview 从同一请求派生，不参与授权身份。执行器只能消费摘要绑定的不可变载荷，
不能批准后重新读取可变输入。secret 仅以引用参与请求，真值走现有 Permit 凭据通道，
不得将密钥真值或含密钥的原始载荷写入 canonical 请求或审计。

### 3.4 能力上限与审批策略分离

**能力决定能不能做，审批决定已获能力的操作是否需要人确认。** 未授予的操作
直接 Deny，不能通过点批准获得能力。首版只保留 workflow 与 node 两层，取消
Global/Plugin/Run 与节点规则之间的特异性排序。部署限制和插件授予是额外硬上限，
不是可覆盖 workflow 的 Allow 规则；Run 授权只消除重复询问，不增加能力。

```rust
pub enum Approval { Auto, Ask }
pub enum ExecutionMode { Execute, Plan }

pub struct Capability {
    pub id: String,                    // 本策略内唯一，供诊断与 UI 引用
    pub pattern: ResourcePattern,      // tagged matcher；由变体唯一确定 Action
    pub approval: Approval,
    pub expires_at: Option<DateTime<Utc>>,
}

pub struct WorkflowPermissions {
    pub version: u32,                  // 首版 = 1；未知版本拒绝发布和执行
    pub mode: ExecutionMode,
    pub allow: Vec<Capability>,        // 默认为空，即无任何外部能力
}

pub enum NodePermissions {
    Inherit,                          // 继承 workflow 上限与审批要求
    Restrict { allow: Vec<Capability> }, // 再取交集；空列表 = 禁止所有外部能力
}
```

权限块缺省与空列表不同：新权限工作流中节点缺省为 Inherit；显式 Restrict 空列表
为 DenyAll。所有未匹配操作都拒绝，**不存在“默认 Auto 所以允许所有动作”**。
`approval=auto` 仅让匹配且未被上限拒绝的操作免人工审批。

同层多项匹配以 OR 合并能力，但匹配项中任一 Ask 都要求询问；各层以 AND 相交，
任一层没有匹配即 Deny。避免规则顺序、pattern 具体度与 id 排序改变授权。
过期项在显式 evaluated_at 下视为未匹配。首版不提供显式 Deny 规则，删去允许项
或使用 Restrict 空列表即可禁止动作；Plan 与部署限制始终优先。

ResourcePattern 首版闭集如下；它与 OperationSpec 分开，不能作为待执行载荷：

| 动作 | matcher | 边界 |
| --- | --- | --- |
| Secret | `names` 精确密钥名列表 | 只授权凭据读取/交付；不自动允许 LLM、HTTP 或邮件 |
| Exec | `executable` + `argv_exact` | executable 为执行机器上的已解析绝对路径；不做 basename、argv 前缀或 shell 拆词匹配 |
| Shell | `script_digest` + 工作目录 | 默认没有此能力；仅授权完全相同脚本，UI 标明进程内动作无法细分控制 |
| FileRead/FileWrite/FileDelete | `root=workdir` + `relative_path` 精确路径，或 `subtree` 目录 | 不支持任意 glob；解析真实根、路径组件及符号链接，越根拒绝；删除单独授权 |
| Net | HTTP `methods` + `url_prefix` | 解析 scheme/host/port/路径段；重定向每跳再校验；不允许 URL userinfo |
| LlmCall | provider/model 精确集合 | 请求内容仍绑定完整摘要，API key 另走 Secret |
| SendEmail | 收件人精确集合 | To/Cc/Bcc 全部校验，正文/附件绑定完整摘要 |

只读约束 `Plan` 在能力检查前生效；低风险不代表只读。RiskLevel 只用于 UI 提醒，
不参与放行；首版不做灾难命令黑名单与可覆盖风险表。内置 harness Mode 不再映射成
权限授予：Plan 增加只读上限，其余模式仅作为迁移输入；所有执行都走 master。

### 3.5 节点权限在工作流定义中的设计

权限属于宿主执行定义，**不能塞进插件 params 或由 manifest 默认值生成**。
在 `DefinitionData` 增加可选顶层 `permissions: WorkflowPermissions`；在 `Node`
增加可选 `permissions: NodePermissions`。以下为待实现的 JSON 契约示例：

```json
{
  "semantics": 2,
  "permissions": {
    "version": 1,
    "mode": "execute",
    "allow": [
      {"id":"summary-key", "action":"secret", "names":["summary-api-key"], "approval":"auto"},
      {"id":"summary-api", "action":"net", "methods":["POST"], "url_prefix":"https://api.example.com/summarize", "approval":"auto"},
      {"id":"publish-main", "action":"exec", "executable":"/usr/bin/git", "argv_exact":["push","origin","main"], "approval":"ask"}
    ]
  },
  "nodes": [
    {
      "id":"summarize", "type":"http_call", "params":{},
      "permissions": {
        "mode":"restrict",
        "allow":[
          {"id":"key", "action":"secret", "names":["summary-api-key"], "approval":"auto"},
          {"id":"post", "action":"net", "methods":["POST"], "url_prefix":"https://api.example.com/summarize", "approval":"auto"}
        ]
      }
    },
    {
      "id":"publish", "type":"harness", "params":{},
      "permissions": {
        "mode":"restrict",
        "allow":[
          {"id":"push", "action":"exec", "executable":"/usr/bin/git", "argv_exact":["push","origin","main"], "approval":"ask"}
        ]
      }
    }
  ],
  "edges":[]
}
```

示例仅展示权限字段；各节点 params 仍须补齐原有类型要求。Exec 的 argv_exact 不含
可执行文件自身。summarize 无法读取别的 secret、GET 别的 URL 或执行命令；publish
无法使用 summary-api-key。把 publish 的 approval 改成 auto 也不能覆盖 workflow Ask。

- **保存/发布**：Rust 权威校验版本、闭集字段、matcher 类型、ID 唯一性及参数类型。
  权限字段禁止模板/表达式；动作中的动态目标在运行时求值后重新匹配。节点新增能力
  必须同时受 workflow 上限约束，永远不通过“更具体规则优先”扩大权限。首版无需实现
  任意 matcher 的集合包含算法，保存时对明显越界给诊断，执行时逐请求取交集为准。
  节点 permission 不参与插件 params schema，不得被 raw params 编辑覆盖。
- **写权限**：普通 workflow 编辑者可以修改 params 和收紧 node 权限；创建或扩大
  workflow 上限、改变权限块首版仅 admin 可提交。非 admin 不得删除 node Restrict
  以恢复 Inherit，也不得将 Ask 改 Auto；无法证明收紧的修改统一要求 admin。
  删除/新增节点、替换节点类型、导入/克隆定义与 publish 都校验，不能只拦专用 UI。
- **冻结与身份**：发布版本包括权限内容，参与 definition checksum；run 冻结完整
  workflow/node 权限及实际执行插件授予身份，派发从快照读取。权限升级只影响新 run。
  master 依据 run/node/dispatch 的冻结关联确定请求权限，不能使用插件自报 node_id。
- **兼容**：新增字段用 Option + default + skip_none 保持旧定义的序列化；顶层无
  permissions 是 legacy，不等于 Inherit 或 Auto。旧定义可读取/编辑/离线回放，首次
  新执行前由 admin 显式迁移；不自动生成全能力。权限工作流需新的执行能力标识，旧
  executor/remote agent 不支持则拒绝 dispatch，防忽略字段后无限制执行。
- **纯计算节点**：无需外部能力即可做内存运算。JS/script 只有在运行时确认不暴露
  文件、网络、进程和凭据 API 时才能按纯计算处理；否则纳入对应受管请求或拒绝。
  human/wait/signal 控制流不授予后续节点权限；子工作流首版禁止继承父节点凭据或
  自动提升能力，拥有独立冻结策略；调用方有额外上限时继续向下取交集。

### 3.6 节点动作与凭据执行契约

| 节点/执行面 | 必查动作 | 执行要求 |
| --- | --- | --- |
| http_call | Net；配置了认证再加 Secret | master 先裁决具体 URL/method 与 secret 引用；全部通过后才解析凭据并发请求 |
| llm | LlmCall + Secret（如需） | 模型、provider 与密钥分别受限，批准模型不批准其他密钥 |
| email | SendEmail + Secret（如需） | 校验全部收件人，拒绝时不交付 SMTP/API 凭据 |
| harness command | Exec 或 Shell | 整个子进程受管；无 OS 沙箱时不能承诺约束进程内部文件与网络动作 |
| harness agent | 初始 LlmCall/Secret；随后每次工具动作 | 禁用本地 Allow，工具分别申请 File/Exec/Net 等能力；拿不到能力直接拒绝 |
| 通用插件 | 插件声明与授予上限 + 当前节点权限 | 能通过 reverse request 执法才支持细粒度；未接入协议的进程不能伪装已受管 |

多个必需动作构成同一次外部操作的授权需求集合。master 先检查全部能力，任一 Deny
则整次拒绝；Ask 按同一操作实例展示全部需求，批准后持久化各项裁决与组合 Permit。
只有全部通过才交付凭据和执行；不能先拿到 secret 再询问 Net。工具运行中新增需求
创建新的请求，不复用外层 harness 的 Permit。

**Secret 的边界**：secret 名称写在权限表里不等于解析；真值仅在所有相关裁决允许后
经既有凭据通道交付给目标执行端，UI/manifest/日志不拿真值。一旦交给插件进程，
进程就能使用它；限制读取不等于限制后续用途，需要限制用途的凭据应保留在宿主，
由宿主代发请求。其他节点不能从上游输出继承凭据授权，secret 真值禁止作为节点输出。

**进程能力的边界**：Exec、Shell、插件进程都可能内部联网或读文件，精确 argv 也
不保证命令没有这些行为。若用户要求“能执行命令但禁止联网/只能写指定目录”，必须
在目标平台使用经过验证的 OS 执法；不可用时拒绝该组合，不能 no-op 后标记受限。
支持受管工具协议的插件仍处于 §1.3 的合作进程信任模型内，不宣称防恶意同 uid 逃逸。

## 4. 决策算法

master 内唯一评估器为纯函数，输入为完整请求、冻结权限、Run 精确授权与显式时间：

```text
check(request, frozen_permissions, run_approvals, evaluated_at) -> Evaluation:
  0. 核验请求身份、执行能力与完整载荷；Plan 下变更性操作 -> Deny
  1. 检查部署限制、插件声明/授予硬上限（适用时）；任一不满足 -> Deny
  2. 匹配 workflow allow 中未过期能力；没有匹配 -> Deny
  3. node=Inherit 则继承；node=Restrict 则也必须匹配，否则 Deny
  4. 任一匹配层要求 Ask -> 检查同 run/node 的精确 Run 授权
     未匹配 -> Ask；匹配 -> 免再次询问
  5. 持久化独立 Decision 与 OperationAuthorized，提交后发 Permit
```

步骤 0–4 的纯函数返回 Allow/Deny/Ask，步骤 5 由 master 事务执行；Ask 的最终结果
按 §6 持久化。risk 仅供展示。Run 授权匹配的是完整操作内容摘要（排除调用实例与
transport dispatch，但保留 actor 节点/插件身份、cwd、载荷、策略 digest）；它不是
Permit。每次调用仍绑定新的 operation_id/request_hash，并核验能力、期限和只读限制。

期限相对于 evaluated_at 求值，不读隐式时钟；审批提交时重新检查能力是否已过期，
过期直接拒绝，不向外层默认策略回退。重放使用既有裁决，既有 Allow/Authorized 无
Outcome 时仍按 Uncertain 处理。首版不开放随意修改 modifiers：超时使用现有预算，
OS 强制约束必须实际生效；不能在同层 OR 规则中合并出更宽松的执行环境。

---

## 5. 执行点（PEP）：六个闸门，各自只认 master 的 Decision

「检查」分布在六处，「裁决」只在一处（master 的 `PolicyEngine`）。执行器与插件是
执法者不是立法者——C3 的既有哲学，原样推广：

| # | 闸门 | 位置 | 检查什么 |
| --- | --- | --- | --- |
| G1 | API 准入 | `journal_v2.rs:394 admit_call` | Principal 认证 + 方法 scope（read/write，`Method::is_read/is_write` 已有）；P2 加 API key 分档（§9） |
| G2 | 策略冻结 | `freeze_execution`（`execution_snapshot.rs`） | run 启动时冻结策略快照（执行模式 + workflow/node 能力及期限 + 插件授予），记 `PolicyFrozen` 事件（I30） |
| G3 | 副作用授权 | `handle_request_operation`（`dispatch.rs:1360`） | **主闸门**。原「自动授权」改为：`PolicyEngine::check` → Allow 则照旧落 `OperationAuthorized` 发 Permit；Ask 则进入审批等待（§6）；Deny 则节点业务失败 |
| G4 | 插件会话 | `plugin_session.rs:58 open/invoke` | spawn 前校验插件能力授予（§7）；invoke 校验 method 在授予集内；SpawnConfig 按 modifiers 套沙箱 |
| G5 | 工具调用 | harness turn 内 | 工具调用逐条上报 master 裁决（reverse request，§8）；仅按 `(operation_id, request_hash)` 去重传输重试 |
| G6 | 业务面 | `journal_triggers.rs` | 已隔离（run_token、payload 红线、只能 signal human 节点）；**明确：业务 token 永远不能审批**（I33） |

G3 是唯一新增的 master 状态机；G2/G4/G5 是既有代码的扩展；G1/G6 是收口。

---

## 6. 审批流（Ask 路径）

复用 Wait 机制，新增 `WaitKind::Approval`（`journal_state.rs:108` 的枚举加一个变体）：

```text
节点执行 → G3 评估得 Ask
  → 同一事务: PermissionAsked{request, request_hash} + WaitRegistered(kind=Approval, deadline)
  → run.status 保持 Running（I14 同款：业务等待不是平台故障，不碰 AwaitingResume）
  → UI/CLI 审批卡片（命令全文/URL/diff 摘要、发起 principal、命中的能力项、风险提示）
  → run.approve / run.deny { run_id, node_id, operation_id, request_hash, scope, reason }
  → 同一事务: PermissionDecided{...} + WaitResolved +（Allow 时 OperationIntent/Authorized）
  → 提交后 Allow 才发 Permit 继续，Deny 则 NodeFailed
```

规则：

- **幂等**：`request_hash` 重复审批 = 返回既有裁决；只能裁决该节点**当前 pending**
  的请求（I32）；同一实例重复提交返回既有结果，换了 hash 则拒绝；
- **TTL**：`approval_ttl_ms`（默认 24h）到期 → `PermissionDecided{Deny, by=System}` →
  节点业务失败。宁可失败，不可静默放行（I34）；
- **审批不授予能力**：仅支持 Once 与 Run 内相同操作。Once 批当前实例；Run 由
  admin 显式选择，同事务写 `RunApprovalUpserted`，仅免除同 run/node 下精确操作的
  再次询问，不支持 prefix/glob/全局持久规则。每次执行仍独立检查全部能力和生成
  Decision/Permit；修改能力必须走工作流权限编辑、发布与新 run，不能靠审批提权。
- **无人值守语义**：cron/webhook 触发的 run 跑到 Ask 就停下等 TTL。这是特性不是缺陷：
  「半夜的批处理停在 `git push` 前面，明早有人点一下」正是这套模型该给的东西；
- 审批权限：仅 admin 主体（LocalAdmin / 有 approve scope 的 key）。webhook 业务 key、
  run_token 一律不可（I33）。

### 6.1 等待、交付与恢复

- 状态为 `PendingApproval → DecidedAllow | DecidedDeny`；Allow 对应的
  `OperationAuthorized` 与裁决同事务提交，Permit 交付可重试，但同一 operation
  不得触发第二次执行。裁决、TTL、取消通过 master 事务串行竞争，只有当前 pending
  实例能被决定。Deny 不创建 OperationAuthorized。
- G3 的节点级审批在副作用授权之前挂起；PendingApproval 不占用运行中的工具进程。
  重启从 Wait 与请求恢复待办和原 deadline；批准后在原 operation 身份下恢复尚未执行的授权流程；dispatch 变更时先持久化绑定转移，
  重新计算含 dispatch 的请求摘要并让旧 pending/Permit 失效，旧审批不能自动套用。不得因等待重启而刷新 TTL。
- G5 的工具级审批发生在活跃 turn 内：保存 turn/tool_call_id、operation_id 与
  dispatch 绑定。首版不提供工具回合的持久续执行；插件、executor 或会话丢失时，
  按现有 Uncertain 路径处理，关闭 pending 审批，要求显式恢复；不得自动重跑整轮。
  旧 dispatch 的批准或 Permit 不得交给新会话。master 重启且不能验证原会话仍有效时
  同样进入 Uncertain；已有 Allow/Authorized 但无 Outcome 也不得推断未执行。
- 活跃会话收到重复 Permit 时按 operation_id 去重；已有 Outcome 则直接派生。
  断线后不能确认是否执行时保留不确定状态，禁止靠重发操作保证“最终成功”。
- 人工等待暂停节点的执行预算，恢复后只使用剩余预算；审批使用独立的绝对 deadline。
  修改 executor 的整体 timeout 与插件调用计时，使其识别等待状态。必须先完成这项
  变更，才能宣称支持 24h 等待。取消始终立即生效，并回收现有进程及其后代。
- 修改 workflow/node 能力、撤销插件授予只影响新 run；当前 run 继续使用冻结权限。管理员要
  立即停止当前权限须取消受影响 run；这不撤销已经发生的副作用。审批凭据的有效性
  在每次 approve/deny 时按当前认证状态检查，不能随 run 冻结。

---

## 7. 插件认证与能力授予

现状：插件身份 = TOML 文本 + 目录位置，无哈希、无签名（plan 文档 R-1 把签名分发
推后了）。在签名落地前，用**内容哈希 + 安装时授予**先把身份立起来：

1. **manifest 声明能力**（`plugin_manifest.rs` 扩展；迁移语义见 §7.1）：

   ```toml
   [permissions]
   schema = 1
   required_features = ["master-permit-v1"]

   [[permissions.capabilities]]
   id = "git-status"
   action = "exec"
   argv_exact = ["git", "status"]
   required = true

   [[permissions.capabilities]]
   id = "provider-http"
   action = "net"
   url_prefix = "https://api.openai.com/v1/"
   required = true

   [[permissions.capabilities]]
   id = "provider-key"
   action = "secret"
   secret_fields = ["config.api_key"]
   required = true
   ```

   manifest 声明的是**需求上限**，不是授予；校验沿用「解析失败 = 禁用，不猜不跳过」。

2. **内容哈希**：registry load 时对 `entry` 二进制 + `plugin.toml` 算 SHA-256，
   `RegisteredPlugin` 增加 `content_hash`（`plugin_registry.rs` 两侧）。身份键从
   `(id, version)` 变为 `(id, content_hash)`（I35）。

3. **安装/升级授予**：首次部署或哈希变化时，插件进入「待授予」态——节点类型照常
   可见，**执行被拒**，直到管理员批准能力集（`permissions.grant` RPC，写
   `PluginGrantUpserted` 事件）。已发布工作流引用的插件升级后哈希变了 → 既有
   「插件升级重检」（`plugins.recheck`）顺理成章地把「待授予」检出来。

4. **执行约束**（G4）：`plugin_session::open` 查授予；harness/llm 这类自带反向通道的
   走 §8 逐条裁决；**不支持 reverse request 的通用插件**（protocol v1）只有进程级
   约束——按授予集生成 sandbox profile（exec 白名单/出网白名单/路径白名单）+
   凭据仅解析已授予 secret_fields 的引用，按既有凭据通道交付；不新增全量 env 注入。§1.3 的诚实条款在此适用：这是降险，不是隔离。

### 7.1 manifest 文件契约与兼容

现有 `PluginManifest` 的 `[plugin]`、`[config]`、`[ui]`、`[methods]` 继续承担
身份、配置、展示和路由；新增独立 `[permissions]`，不把安全含义藏进 widget、
summary_keys 或任意 UI 字符串。示例中的字段是待实现契约，不是当前解析器已有能力。

- `schema` 是权限声明版本；`required_features` 必须同时由注册端、executor 与插件
  握手协商。未知版本、未知 action/matcher、拼错字段、重复 capability id、action
  与 matcher 不匹配、必需能力不受支持，均拒绝加载/执行，不能忽略后继续。
  旧宿主可能忽略新增 TOML 段，因此新权限插件还必须提高最低协议要求，并在缺少
  `master-permit-v1` 握手能力时自行拒绝 invoke；仅写 schema 不能阻止旧宿主运行。
- matcher 使用闭集 tagged 类型；Exec 只支持精确 argv，Shell 必须单独
  声明；Net 解析 URL 后按 scheme/host/port/路径段匹配，不能字符串 starts_with；
  文件路径声明相对于确定的工作区根，解析规则与执行请求一致。未知工具不得默认
  映射为已允许的能力。声明只是需求上限，动态操作仍须逐次 master 裁决。
- `required` 能力未获安装授予则插件不能启用；optional 能力可拒绝，插件必须处理缺失。
  required 只约束安装授予是否完整，不能强迫每个节点授予插件全部能力；节点的更小
  权限会使对应工具/方法返回能力拒绝，插件应按该次 invoke 的需求处理。
  实际允许范围是声明、安装授予、run 策略和调用裁决的交集；任何 Allow 都不能越过
  声明或授予的上限。缩小能力重新形成内容身份；升级新增能力不得沿用旧授予。
- `secret_fields` 引用本插件已声明为 `Widget::Secret` 的配置/节点字段；解析时校验
  字段归属及类型。manifest 不硬编码用户实际密钥名，UI 只选择引用，master 在
  dispatch 时将引用解析为具体 Secret 请求并裁决，拒绝后不交付凭据。
- 缺少 `[permissions]` 的旧 manifest 标记 `legacy-unrestricted`，不伪装为空能力。
  首次启用权限模型时旧安装进入待迁移，管理员显式确认无限制执行风险后才能用于
  新 run；不自动继承授予。现存 run/旧 journal 仍可读取与离线回放；缺少策略快照的
  未完成 run 不自动恢复执行，须取消或显式迁移后启动新 run。
- manifest 解析、能力规范化与校验只有 Rust 一份实现；RPC 返回结构化需求与错误。
  UI 不解析 TOML 再造授权器。部署文件仍是需求来源，授予记录只写 journal，
  `permissions.grant` 不回写 plugin.toml。远端按执行 agent 的实际插件哈希授予，
  不能拿本机同名插件的授权代替；执行前复核实际入口与 manifest 的内容身份。

### 7.2 UI、插件 UI 与审批的边界

沿用 [插件 UI 设计](../plans/2026-11-06-plugin-ui.md)：插件 UI 是 manifest 的
声明式表单和 journal/nodelog 投影，不提供插件 JavaScript、iframe 或直连插件进程。
新增权限功能也遵守这条边界：所有批准、拒绝和授予 RPC 均由宿主调用并在服务端鉴权。
插件输出、manifest 文案及日志不构成审批指令，不能触发宿主操作。

| UI 面 | 展示与交互 | 权威与约束 |
| --- | --- | --- |
| 插件安装/升级页 | 展示 id、实际 agent、版本、内容哈希；逐项列需求、required/optional、范围及升级差异；明确标注 legacy 无限制风险 | 管理员显式授予，服务端校验交集；拒绝 required 保持不可执行，未授予节点仍可见并显示原因 |
| 工作流/节点参数页 | 使用既有 params schema 与 Secret 引用选择器；展示当前有效权限来源及预检结果，编辑后标记预检失效 | 保存参数不产生授予；预检不是 Permit；运行时以冻结策略与实际请求为准 |
| run 审批卡片/收件箱 | 宿主按 OperationSpec 渲染命令、完整 argv/cwd、URL/method、文件目标与内容摘要；展示 actor、插件身份、能力依据、期限与执行预算 | 由 master 的持久请求生成，插件不能覆盖关键字段；批准绑定 operation_id/hash，请求实例或内容改变后旧批准拒绝；单纯刷新不改变审批身份 |
| 权限与 run 授权页 | Once/Run 显式选择，默认 Once；Run 展开完整操作、期限；workflow/node 能力在工作流权限页编辑 | Run 只免重复询问，不改变任何能力；不提供持久 Rule 或前缀授权按钮 |
| 历史运行/插件运行视图 | 沿用冻结 UiProjection 与 Decision；显示当时身份、策略和裁决；待办状态另取当前 master | 卸载、升级后仍能查看；历史 Allow 不显示为可再次执行的授权 |

审批页必须显示 Pending、Allowed、Denied、Expired、Cancelled、Uncertain 等明确状态；
重复点击返回原裁决，冲突返回已被决定；到期时间以服务端为准。无 approve scope 的
用户可以按读取权限查看脱敏状态，但操作入口不可用；服务端仍须独立拒绝越权 RPC。
批准权限不自动包含规则管理或插件授予权限，创建 Run 授权、修改能力与插件授予首版仅限 admin；approve scope 只允许 Once，服务端拒绝其他 scope。

插件可以继续声明 palette、params、run_view.summary_keys；**首版不新增审批 widget、
插件自定义按钮、审批模板、链接回调或权限 RPC 别名**。关键权限展示由宿主闭集组件
控制，manifest 只能携带被转义的描述文案。超长请求提供宿主只读展开，摘要不得冒充
完整内容；载荷不可用时不能批准。URL、命令和日志作为不可信文本显示，不执行 HTML。

`UiProjection` 仍冻结 palette/params/run_view；新增独立 `PermissionProjection`
冻结规范化能力需求、实际 content_hash、授予快照标识与策略 digest。两者通过同一
插件身份关联，展示文案变化不影响正在运行的快照。当前可审批性来自待办状态，不从
快照或插件 `summary_keys` 推断。UI 表单保留未知参数键的旧约定继续有效，但未知
权限声明必须拒绝，二者不能混用。

### 7.3 节点权限 UI 与宿主 schema

节点检查器增加宿主固定的“权限”页签，与 manifest 参数表单并列；不让插件决定
是否显示，也不由 `[[ui.params.field]]` 声明。首版交互：

1. 顶部选择 Inherit / Restrict；Restrict 列出 Secret、HTTP、文件、Exec、Shell、
   LLM、邮件能力。未添加项明确显示“禁止”；空列表显示“全部外部能力禁止”。
2. Secret 使用宿主密钥引用选择器，只显示当前身份有权查看的名称，不取真值。
   动作使用按类型固定的字段编辑器，每项选择 Auto/Ask 与可选期限；Exec 分别编辑
   executable 和 argv 数组，不能把 shell 输入框伪装成结构化命令。
3. 同屏展示 workflow 上限与 node 限制，给出“未被 workflow 授予”“需审批”
   “可自动执行”等派生状态。workflow Ask 时 node Auto 明确显示最终仍需审批。
   Inherit 列出将继承的能力，不能只用一个默认开关隐藏实际权限。
4. workflow 权限页管理共同上限；普通编辑者看到只读限制及所需管理员权限。
   节点参数里选了 API key 并不自动勾选 Secret 能力；宿主可提示缺少能力，不能
   保存表单时偷偷授权。未授权动作可存在于定义中，但运行时稳定拒绝。
5. 更换节点类型保留权限字段供重新核对，复制节点显式复制 Restrict，禁止默默变成
   Inherit。宿主 raw definition JSON 同样经 Rust 校验与权限变更鉴权。

新增宿主权限 schema/RPC 投影，与现有插件 params schema 分离；CLI、web 和导入
都使用同一 Rust 校验。`permissions.effective { workflow_id, version, node_id }`
返回各层能力、匹配诊断与执行限制，不返回 secret 值；run 查询使用冻结快照，设计期
查询使用指定定义版本，不能混用“当前配置”与“该次运行”。动作级预检为可选请求，
无实际操作参数时只展示能力列表，不能宣称某个动态目标已获准。

---

## 8. harness 集成：解掉 FullAuto 短路

1. `RunTurnParams`（`plugins/harness/protocol/src/lib.rs`）增加权限字段：

   ```rust
   pub struct TurnPermission {
       pub execution_mode: ExecutionMode, // Plan 是不可提升的只读上限
       pub policy_digest: String,        // 指向 master 冻结的 workflow/node 权限
       pub permission_channel: PermissionChannel, // ReverseRequest | FailClosed；所有调用都走
   }
   ```

   `plugins/harness/plugin/src/main.rs:200` 的 `Mode::FullAuto` 硬编码删除，改为所有工具都请求 master 的授权 hook；
   不再把 Ask/Auto 映射成本地 Mode 放行。headless deny-always approver 由受管通道替换。

2. **所有工具调用走 master 授权**：握手协商 ReverseRequests 与
   `permission/request`，每次工具调用（包括 Low、Auto 与结构化命令）在执行前发送
   完整请求。master 评估、持久化 Decision 后才返回绑定调用实例的 Permit；Ask 先
   走 §6。能力未协商、连接中断或 master 不可达时 fail closed，不回退 FullAuto。
   增加协议 minor 并提供能力协商 vectors；旧 host 不具备该能力时拒绝受管工具执行。

3. **实例去重**：ApprovalLedger 按 `(operation_id, request_hash)` 只去重传输重试；
   第五次 `git status` 是新工具调用，必须有独立 Decision 与执行证据。减少人工询问
   使用 Run 授权 matcher，不能复用第一次调用的 Permit。会话缓存不能替代 journal。

4. **执行端仅提前 Deny**：PermissionPolicy 不再按 FullAuto/AutoEdit/Suggest
   返回本地 Allow；模式与风险只作为 master 策略输入。插件可按冻结限制提前拒绝，
   如需审计则上报拒绝事件；任何将执行的操作都必须经 master。golden vectors 验证
   请求编码、拒绝约束和 Permit 绑定，不能用测试代替唯一授权路径。

---

## 9. 认证：Principal 的来源与 API key 分档

现状盘点（全部保留，只加不减）：

| 面 | 现有 | Principal 归类 |
| --- | --- | --- |
| WS-RPC | `FLOW_JOURNAL_TOKEN`（`admit_call` 恒时比较） | `LocalAdmin`（隐式 admin key） |
| 值下载 / hooks | 同一管理 token，Bearer | `LocalAdmin` |
| 业务 invoke | per-webhook 哈希 key + `run_token` HMAC | `Trigger{webhook}`（能力天然受限，G6） |
| 远程执行 | mTLS CN / WSS token 反查 `AgentCredential` | `Agent{id}` |
| cron | 无凭据（进程内触发） | `Trigger{schedule}` |
| 桌面 | `web/src-tauri/src/service.rs` 注入随机 UUID token | `LocalAdmin` |

P2 增加 `[[api_keys]]`（`flow-config`）：`{id, token_hash, scopes ∈ read|write|approve|admin,
expires_at}`，`admit_call` 按 `Method::is_read/is_write` + 方法白名单裁决。
`FLOW_JOURNAL_TOKEN` 降级为「隐式 admin key」永久兼容。分档的价值不是多租户，是
**审批权与触发权的分离**——能批准 `git push` 的 key 不该同时能改 workflow。

---

## 10. 审计：把「检测」升级成「防篡改 + 可追问」

journal 已是 append-only、逐事务 SHA-256、读时校验、离线 `maintenance::verify()`。
缺三样，补三样：

1. **actor 与决策事件**。`Event` 增加 `actor: Option<Principal>`；新 EventKind：

   ```text
   PolicyFrozen        { run_id, policy_digest, permission_schema_version }
   PermissionAsked     { run_id, node_id, request, request_hash }
   PermissionDecided   { operation_id, request_hash, evaluated_at, policy_revision, decision, by, matched_capabilities, modifiers, scope }
   RunApprovalUpserted / Removed { run_id, node_id, operation_digest, expires_at, by }
   WorkflowPermissionsChanged { workflow_id, definition_version, by }
   PluginGrantUpserted { plugin_id, content_hash, capabilities, by }
   ```

   载荷沿用 secret 红线原则（secret 值注入在事件落盘之后），已知凭据必须使用 secret 引用；argv 展示与审计遵循同一脱敏规则，不允许将 secret 真值嵌入 argv。

2. **哈希链**。`transactions` 表加 `prev_digest` 列，写入时 `digest = SHA-256(prev_digest
   || body)`；`maintenance::verify()` 顺链校验。把「改了数据会被读时发现」升级为
   「改了任何一帧，其后全部链断」（独立审计强化，不作为 OS 安全边界）。周期性把链头哈希导出锚定
   （OPS 层操作，不入代码）。

3. **查询面**。`run.audit.page` 增加 `Filter::Permission`；新增
   `permissions.audit.page { actor?, decision?, run_id?, before }` 与
   `permissions.effective`，支持导出 JSONL。回答的问题从「这个 run 干了什么」升级为
   「**谁**在**何时**批准了**哪些**命令、依据哪些能力项」。

auto-approve 不是审计豁免：`approval=auto` 的每次放行同样落 `PermissionDecided{by=System,
matched_capabilities=[...]}`。全自动 = 决策全自动，记录一条不少。

---

## 11. 落地计划与 crate 变更

| 阶段 | 内容 | 验收 |
| --- | --- | --- |
| **P0** 堵主闸门 | `flow-dto` 类型与事件；master `PolicyEngine`；G3 接入（http/llm/email/harness 节点级）；完整载荷摘要、Plan 上限、run 授权投影与等待预算/恢复协议；`WaitKind::Approval` + `run.approve/deny` + TTL；`PolicyFrozen`；web 审批卡片；workflow/node 能力交集与 Secret 授权 | 节点级免批/询问/拒绝 e2e；不同 cwd/body/content 不得串用审批；Plan 不被 Allow 提权；过期/撤权/Run 授权；审批前后重启、TTL/取消竞争与预算暂停；I26–I34 用例 |
| **P1** 插件与工具级 | typed manifest permissions + 最低协议/能力协商 + legacy 迁移 + 内容哈希 + 授予流 + 宿主授予 UI/权限投影 + `plugins.recheck` 联动；G4 校验与沙箱 profile；harness `TurnPermission` + `permission/request` 反向通道 + ApprovalLedger 改造；插件能力上限 | 插件升级后未重授予 → 执行被拒；harness `git status` 免人工审批但逐次落盘、`git push` 弹批 e2e；所有模式无本地 Allow；断线/旧 host 拒绝；会话丢失进入 Uncertain；重复 Permit 不重复执行 |
| **P2** 认证与审计强化 | `[[api_keys]]` 分档 + admit_call 接 scope；`prev_digest` 链 + verify；`permissions.audit.page` / 导出；API key 设置 | 审批 key 无法写 workflow；verify 能发现中段篡改 |

crate 变更清单：

| crate | 变更 |
| --- | --- |
| `flow-dto` | Principal/Action/OperationSpec/ResourcePattern/WorkflowPermissions/NodePermissions/DecisionRecord DTO；新 EventKind 与 RPC 方法枚举（`run.approve/deny`、`permissions.*`） |
| `flow-journal` | EventKind 扩展、`actor` 字段、`prev_digest` 列与链校验、工作流权限版本 / Run 精确授权 / 插件授予投影 |
| `flow-backend` | `policy.rs`（评估器，纯函数）、freeze 策略快照、G3 状态机、审批 wait 折叠、`permissions.*` RPC |
| `flow-engine` | DefinitionData/Node 独立 permissions 字段与 checksum/兼容校验；`WaitKind::Approval`、`journal_state` 审批与 run 授权折叠 |
| `flow-plugin-sdk` | ReverseRequests 服务端：`permission/request` 方法；协议 minor +1 + vectors |
| `flow-plugin-runtime` | `plugin_session` 能力校验与反向请求服务；registry `content_hash`；SpawnConfig 沙箱参数 |
| `flow-config` | manifest `[permissions]` / `[[permissions.capabilities]]`；部署 `[permissions] approval_ttl_ms`；workflow/node 权限在定义内；matcher 闭集校验 |
| `plugins/harness` | `TurnPermission` 传导、删 FullAuto 硬编码、ApprovalLedger 按调用实例去重 |
| `web` | 宿主审批卡片/收件箱、workflow/node 权限与 Run 精确授权页、插件需求/升级差异与授予页、节点权限预检、历史权限投影、API key 设置 |

---

## 12. 新不变量（I26–I38）与已知限制

### 12.1 不变量

| # | 不变量 |
| --- | --- |
| I26 | 任何副作用发生前，`PermissionDecided{Allow}` 与对应 `request_hash` 必须已落盘，且 Permit 绑定同一 operation_id、dispatch_id 与 hash，执行前核验载荷（I6/I11 的推广） |
| I27 | 任一能力上限不满足即 Deny；审批与 Run 精确授权不能越过能力上限或 Plan |
| I28 | workflow 为能力上限，node 只能取交集收紧；未知动作及未匹配目标默认拒绝 |
| I29 | 权限发布、Run 精确授权与每次裁决均记录 by；扩大能力仅限 admin，审批者不能借 Once 提升权限 |
| I30 | 策略在 run 启动时冻结并记 `PolicyFrozen`；Run 精确授权仅免重复询问；权限修改只影响新 run，不改基础快照 |
| I31 | 同一请求、冻结策略、run 授权事件水位与 evaluated_at 必得同一评估结果；regression 使用已记录 Decision，不重新询问 |
| I32 | `run.approve/deny` 幂等（按 `request_hash`），且只能裁决该节点当前 pending 的请求 |
| I33 | 业务面凭据（webhook key、run_token）永远不能执行审批或规则管理 |
| I34 | 审批等待必有 deadline；到期 = Deny + 节点业务失败，不存在无限等待或静默放行 |
| I35 | 插件授予以 `(plugin_id, content_hash)` 为键；哈希变化未重授予 → 一切执行被拒 |
| I36 | 按 `(operation_id, request_hash)` 去重传输重试；新调用必有独立裁决与执行证据，不复用 Permit |
| I37 | Secret 读取与动作授权分别校验；同一操作全部需求获准后才解析交付凭据，审批不扩大密钥可见范围 |
| I38 | 节点权限取 workflow 上限交集；Exec/Shell 内部细粒度限制要求实际 OS 执法，不支持则拒绝该限制组合 |

### 12.2 已知限制（诚实清单）

- **同 uid 进程逃逸不在防御范围**（§1.3）。要强隔离，下一步是 OS 沙箱/容器化执行器，
  那是独立课题，本文的 modifiers 给它留了接口；
- 插件二进制**签名与分发**（plan 文档 R-1）仍缺席；内容哈希挡「悄悄换二进制」，
  挡不住首次安装恶意插件——授予页是唯一人工闸门；
- Exec 匹配使用已解析绝对 executable 与精确 argv；**符号链接替换、wrapper 脚本、
  可执行内容变化**仍要求执行前身份复核，且不构成同 uid 安全边界；
- Shell 请求默认没有能力；只有显式授予完全相同脚本后才进入审批，不能将任意
  shell 自动拆词后冒充精确 Exec；
- `sandbox-exec` 是 macOS 专属；要求 OS 强制限制但目标平台无实现时直接拒绝，
  不使用 no-op 冒充限制已生效。

### 12.3 UI 与 manifest 验收补充

- P0：审批卡片与 master 请求逐字段一致；编辑参数/切换 dispatch 后旧卡片不能批准；
  重复点击、多人并发、TTL 与取消竞态最终只产生一个裁决。无 approve scope 直接调用
  RPC 也被拒绝；approve-only key 不能创建规则、授予插件或修改 workflow。
- P1：覆盖权限 schema/字段/action 错误、缺失 Secret 字段、required/optional 部分授予、
  同名不同 agent/哈希、新能力升级、legacy 显式迁移、旧 host 握手拒绝。所有情况在
  spawn/invoke 前给出可诊断错误，不得静默降为 FullAuto。
- P1：恶意 manifest 文案、插件输出及 URL 不能注入 HTML、伪造宿主审批按钮或自动
  调用权限 RPC；插件卸载后历史 UI 与权限详情仍可渲染，历史批准不可再次执行。
- P1：manifest 只读需求与 journal 授予独立；授予不改部署文件；表单未知参数保留，
  未知权限字段拒绝。预检后改变节点、策略或插件身份，执行期仍重新核验。
- 修改范围：flow-config 的 typed manifest/校验、flow-dto 的权限投影/RPC、backend 的
  授予与鉴权、runtime 的握手/身份校验、web 的宿主组件；不引入插件脚本运行时，
  不把 Secret 真值下发 UI，不用隐藏按钮代替服务端授权。

### 12.4 节点权限验收

- 同一 workflow 的两个节点分别只能读取自己的 secret；没有 Secret 能力、仅有
  Net/LlmCall 能力、审批通过但 Secret 未授权，均不能取得凭据。
- workflow 未授予、node 未匹配、node Restrict 空列表、能力过期时全部 Deny；
  node Auto 不覆盖 workflow Ask；重叠 Ask/Auto 与列表排序无关。
- 节点权限缺省为 Inherit，legacy workflow 缺省不变成全能力；旧 executor 拒绝新
  权限工作流。复制、导入、删除 Restrict、替换节点类型不能绕过管理员权限校验。
- 动态 URL/路径/secret 引用在展开后匹配；重定向、路径穿越、符号链接越根失败；
  HTTP 与 Secret 组合请求部分通过时零凭据交付、零网络副作用。
- Secret Widget 仅存引用；插件自报 node_id/权限块、上游输出携带权限声明都不能
  改变 master 的冻结权限。UI raw JSON 与专用表单保存均保持宿主 permissions 字段。
- 运行中改权限不影响旧快照；新 run 使用新权限；Plan/Run 精确授权不提升能力。
  不支持沙箱的平台遇到需要限制子进程出网/文件范围的配置时拒绝，禁止静默 no-op。
