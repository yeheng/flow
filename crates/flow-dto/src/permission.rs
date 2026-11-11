//! 权限模型词汇表（09-permission-model.md）：主体、动作、请求、模式与匹配器。
//!
//! 本模块是权限链路的**单一契约层**：定义编辑器（校验）、master 评估器
//! （policy.rs）、journal 折叠（事件载荷）与 web 投影消费同一份类型，
//! 避免出现「三处对同一权限块给出三种解释」的漂移。原则：
//!
//! - **闭集**：`Action` / matcher 字段全部显式枚举，未知值在解析期拒绝，
//!   绝不「忽略后继续」——权限字段没有向后兼容的宽容（§7.1）。
//! - **载荷与规则分离**：[`OperationSpec`] 描述一次待执行的具体载荷（完整
//!   argv、具体 URL、载荷摘要）；[`ResourcePattern`] 是按 Action 区分的
//!   matcher，永远不能当作待执行载荷使用（§3.3）。
//! - **摘要自足**：canonical 编码（对象键排序）+ SHA-256 在本 crate 内
//!   自实现，供 request_hash / Run 授权摘要复用；不依赖 journal codec，
//!   因为 journal（P2 起依赖本 crate）不能反向依赖。
//!
//! 解析风格说明：`Capability` / `NodePermissions` / `OperationSpec` 的线形状
//! 是「扁平 + 按 action 区分字段」，serde 派生无法表达「未知字段拒绝 +
//! action 与 matcher 一致性」的闭集要求，因此手写 `Deserialize`（经
//! `serde_json::Value` 中转，错误信息面向编辑器用户）。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// 权限声明版本。首版 = 1；未知版本在保存与执行两侧都拒绝。
pub const PERMISSION_SCHEMA_VERSION: u32 = 1;

/// 角色上限：能力 id、路径、URL 等自由文本字段的字节上限。
const MAX_FIELD_BYTES: usize = 512;
/// 能力列表长度上限（每层）。
const MAX_CAPABILITIES: usize = 256;

// ---------------------------------------------------------------------------
// Principal
// ---------------------------------------------------------------------------

/// 谁在行动 / 谁在裁决（§3.1）。
///
/// 一次裁决同时记录发起者（`PermissionRequest.actor`）与批准者
/// （`DecisionRecord.by`）；业务面凭据（webhook key、run_token）永远不能
/// 出现在 `by` 里（I33）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Principal {
    /// 本机唯一管理员（桌面/CLI/持管理 token 者）。
    LocalAdmin,
    /// API key 主体（P2）；桌面注入的 UUID token 也归此类。
    ApiKey { id: String },
    /// 触发器：schedule / webhook；id = schedule_id / webhook_id。
    Trigger { source: String, id: String },
    /// 远程执行 agent（mTLS CN 或 WSS token 反查）。
    Agent { id: String },
    /// 插件进程：身份 = id + 版本 + 内容哈希（§7）。
    Plugin { id: String, version: String, content_hash: String },
    /// 发起操作的执行主体：run 内的某个节点。
    Node { run_id: String, node_id: String },
    /// 系统自动决策（TTL 超时拒绝、auto 放行、内置 deny 命中）。
    System,
}

impl Principal {
    /// 展示名（审批卡片与审计查询用）。
    pub fn label(&self) -> String {
        match self {
            Self::LocalAdmin => "local-admin".into(),
            Self::ApiKey { id } => format!("api-key:{id}"),
            Self::Trigger { source, id } => format!("trigger:{source}:{id}"),
            Self::Agent { id } => format!("agent:{id}"),
            Self::Plugin { id, version, .. } => format!("plugin:{id}@{version}"),
            Self::Node { run_id, node_id } => format!("node:{run_id}/{node_id}"),
            Self::System => "system".into(),
        }
    }
}

// ---------------------------------------------------------------------------
// Action / Approval / ExecutionMode
// ---------------------------------------------------------------------------

/// 动作闭集（§3.2，含 §3.4 表区分而 §3.2 漏列的 `shell` 与 `file_delete`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Exec,
    Shell,
    FileWrite,
    FileDelete,
    FileRead,
    Net,
    Secret,
    LlmCall,
    SendEmail,
}

impl Action {
    /// 变体唯一清单；`as_str` / `parse` 与之一致。
    pub const ALL: [Action; 9] = [
        Action::Exec,
        Action::Shell,
        Action::FileWrite,
        Action::FileDelete,
        Action::FileRead,
        Action::Net,
        Action::Secret,
        Action::LlmCall,
        Action::SendEmail,
    ];

    /// 线上名称（定义 JSON 与事件共用）。
    pub fn as_str(self) -> &'static str {
        match self {
            Action::Exec => "exec",
            Action::Shell => "shell",
            Action::FileWrite => "file_write",
            Action::FileDelete => "file_delete",
            Action::FileRead => "file_read",
            Action::Net => "net",
            Action::Secret => "secret",
            Action::LlmCall => "llm_call",
            Action::SendEmail => "send_email",
        }
    }

    /// 反查；未知名称返回 None（调用方按闭集拒绝）。
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|a| a.as_str() == name)
    }

    /// 是否变更性动作：Plan 模式下变更性动作一律 Deny（只读上限在能力
    /// 检查前生效，低风险不代表只读）。
    pub fn is_mutating(self) -> bool {
        !matches!(self, Action::FileRead)
    }
}

impl std::fmt::Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 审批要求：能力命中后是否需要人确认（§3.4）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Approval {
    Auto,
    Ask,
}

impl Approval {
    /// 线上名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Approval::Auto => "auto",
            Approval::Ask => "ask",
        }
    }
}

/// 执行模式：Plan 是不可提升的只读上限（§3.4）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    Execute,
    Plan,
}

impl ExecutionMode {
    /// 线上名称。
    pub fn as_str(self) -> &'static str {
        match self {
            ExecutionMode::Execute => "execute",
            ExecutionMode::Plan => "plan",
        }
    }

    /// 反查；未知名称返回 None。
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "execute" => Some(Self::Execute),
            "plan" => Some(Self::Plan),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// OperationSpec
// ---------------------------------------------------------------------------

/// 一次待执行的具体载荷（§3.2）。每个变体唯一确定 `action()`；摘要字段
/// 指向不可变的完整载荷，执行前核验，不能只保存 UI 摘要。
///
/// 路径约定：`File*` 的 `path` 是**会话工作目录内的规范化相对路径**
/// （组件不含 `..`）；绝对路径表示「越出会话根」的操作，永远不会匹配
/// 相对 matcher（等价 Deny）。会话根本身是 run 期临时目录，不进权限块。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum OperationSpec {
    Exec {
        executable: String,
        argv: Vec<String>,
        cwd: String,
        env_digest: String,
        stdin_digest: Option<String>,
    },
    Shell {
        executable: String,
        script_digest: String,
        cwd: String,
        env_digest: String,
        stdin_digest: Option<String>,
    },
    FileWrite {
        path: String,
        content_digest: String,
    },
    FileDelete {
        path: String,
    },
    FileRead {
        path: String,
    },
    /// 线上 tag 用 "net"（与 [`Action::Net`] 一致，Http 是载荷形态）。
    #[serde(rename = "net")]
    Http {
        method: String,
        url: String,
        headers_digest: String,
        body_digest: String,
    },    Secret {
        secret_name: String,
    },
    LlmCall {
        provider: String,
        model: String,
        payload_digest: String,
    },
    SendEmail {
        recipients: Vec<String>,
        payload_digest: String,
    },
}

impl OperationSpec {
    /// 由变体唯一推导的 Action；禁止另外传入不一致的 action。
    pub fn action(&self) -> Action {
        match self {
            Self::Exec { .. } => Action::Exec,
            Self::Shell { .. } => Action::Shell,
            Self::FileWrite { .. } => Action::FileWrite,
            Self::FileDelete { .. } => Action::FileDelete,
            Self::FileRead { .. } => Action::FileRead,
            Self::Http { .. } => Action::Net,
            Self::Secret { .. } => Action::Secret,
            Self::LlmCall { .. } => Action::LlmCall,
            Self::SendEmail { .. } => Action::SendEmail,
        }
    }
}

// ---------------------------------------------------------------------------
// ResourcePattern / Capability
// ---------------------------------------------------------------------------

/// 按 Action 区分的 matcher（§3.4 闭集表）。tagged 形状由 `Serialize` 派生；
/// `Deserialize` 在 [`parse_capability`] 中手写（闭集校验集中在那里）。
///
/// 仅 manifest 层（插件需求上限）允许 [`ResourcePattern::Any`] 与
/// [`ResourcePattern::ExecProgram`]；workflow/node 层的校验会拒绝它们。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ResourcePattern {
    /// Secret：精确密钥名列表。只授权凭据读取/交付。
    Secret { names: Vec<String> },
    /// Exec：执行机器上的已解析绝对路径 + 精确 argv（不含可执行文件自身）。
    Exec { executable: String, argv_exact: Vec<String> },
    /// Exec（manifest 层专用）：裸程序名（匹配已解析路径的 basename）+
    /// 精确 argv。绝对路径把关由 workflow 层完成。
    ExecProgram { program: String, argv_exact: Vec<String> },
    /// Shell：完全相同脚本的 SHA-256 + 可选解释器路径。默认没有此能力。
    Shell { executable: Option<String>, script_digest: String },
    /// FileRead：会话根内精确相对路径或子树。
    FileRead { path: Option<String>, subtree: Option<String> },
    /// FileWrite：同上；删除必须单独授权（FileDelete）。
    FileWrite { path: Option<String>, subtree: Option<String> },
    /// FileDelete：删除单独授权。
    FileDelete { path: Option<String>, subtree: Option<String> },
    /// Net：HTTP 方法集合 + URL 前缀（scheme/host/port 精确，路径按段前缀）。
    Net { methods: Vec<String>, url_prefix: String },
    /// LlmCall：provider/model 精确集合。请求内容仍绑定完整摘要。
    LlmCall { providers: Vec<String>, models: Vec<String> },
    /// SendEmail：收件人精确集合（To/Cc/Bcc 全部校验）。
    SendEmail { recipients: Vec<String> },
    /// 族级上限（manifest 层专用）：显式声明才存在，匹配该 Action 的任意
    /// 目标。只作上限用；workflow/node 层禁止。
    Any { of: Action },
}

impl ResourcePattern {
    /// 由变体唯一推导的 Action。
    pub fn action(&self) -> Action {
        match self {
            Self::Secret { .. } => Action::Secret,
            Self::Exec { .. } => Action::Exec,
            Self::ExecProgram { .. } => Action::Exec,
            Self::Shell { .. } => Action::Shell,
            Self::FileRead { .. } => Action::FileRead,
            Self::FileWrite { .. } => Action::FileWrite,
            Self::FileDelete { .. } => Action::FileDelete,
            Self::Net { .. } => Action::Net,
            Self::LlmCall { .. } => Action::LlmCall,
            Self::SendEmail { .. } => Action::SendEmail,
            Self::Any { of } => *of,
        }
    }

    /// 是否为 manifest 层专用变体（workflow/node 定义拒绝）。
    pub fn is_manifest_only(&self) -> bool {
        matches!(self, Self::Any { .. } | Self::ExecProgram { .. })
    }

    /// 匹配一个具体载荷。matcher 与 spec 的 Action 不同即不匹配。
    pub fn matches(&self, spec: &OperationSpec) -> bool {
        if self.action() != spec.action() {
            return false;
        }
        match (self, spec) {
            (Self::Secret { names }, OperationSpec::Secret { secret_name }) => {
                names.contains(secret_name)
            }
            (
                Self::Exec {
                    executable,
                    argv_exact,
                },
                OperationSpec::Exec { executable: e, argv, .. },
            ) => executable == e && argv_exact == argv,
            (
                Self::ExecProgram {
                    program,
                    argv_exact,
                },
                OperationSpec::Exec { executable: e, argv, .. },
            ) => {
                let base = e.rsplit('/').next().unwrap_or(e);
                base == program && argv_exact == argv
            }
            (
                Self::Shell {
                    executable,
                    script_digest,
                },
                OperationSpec::Shell {
                    executable: e,
                    script_digest: d,
                    ..
                },
            ) => executable.as_deref().is_none_or(|x| x == e) && script_digest == d,
            (Self::FileRead { .. } | Self::FileWrite { .. } | Self::FileDelete { .. }, spec) => {
                let (path, subtree) = match self {
                    Self::FileRead { path, subtree }
                    | Self::FileWrite { path, subtree }
                    | Self::FileDelete { path, subtree } => (path, subtree),
                    _ => unreachable!(),
                };
                let spec_path = match spec {
                    OperationSpec::FileWrite { path, .. }
                    | OperationSpec::FileDelete { path }
                    | OperationSpec::FileRead { path } => path,
                    _ => return false,
                };
                if let Some(exact) = path {
                    // 空路径（工作区根，来自 list/grep 的 "."）只可能被 subtree
                    // 授权——精确路径授权不覆盖「枚举整个工作区」。
                    !spec_path.is_empty() && spec_path == exact
                } else if let Some(dir) = subtree {
                    spec_path.is_empty()
                        || spec_path == dir
                        || spec_path.starts_with(&format!("{dir}/"))
                } else {
                    false
                }
            }
            (Self::Net { methods, url_prefix }, OperationSpec::Http { method, url, .. }) => {
                methods.iter().any(|m| m.eq_ignore_ascii_case(method))
                    && url_prefix_matches(url_prefix, url)
            }
            (
                Self::LlmCall { providers, models },
                OperationSpec::LlmCall {
                    provider, model, ..
                },
            ) => providers.contains(provider) && models.contains(model),
            (
                Self::SendEmail { recipients },
                OperationSpec::SendEmail {
                    recipients: to, ..
                },
            ) => to.iter().all(|r| recipients.contains(r)),
            (Self::Any { .. }, _) => true,
            _ => false,
        }
    }
}

/// 一条能力：matcher + 审批要求 + 可选期限（§3.4）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Capability {
    /// 本策略内唯一，供诊断与 UI 引用。
    pub id: String,
    pub pattern: ResourcePattern,
    pub approval: Approval,
    pub expires_at: Option<DateTime<Utc>>,
}

impl Serialize for Capability {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.to_json().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Capability {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        parse_capability(&value).map_err(serde::de::Error::custom)
    }
}

impl Capability {
    /// 展开成线上的扁平对象（编辑器 / 事件共用同一形状）。
    pub fn to_json(&self) -> Value {
        let mut object = serde_json::Map::new();
        object.insert("id".into(), Value::String(self.id.clone()));
        match &self.pattern {
            ResourcePattern::Secret { names } => {
                object.insert("action".into(), json_str(Action::Secret.as_str()));
                object.insert("names".into(), json_strings(names));
            }
            ResourcePattern::Exec {
                executable,
                argv_exact,
            } => {
                object.insert("action".into(), json_str(Action::Exec.as_str()));
                object.insert("executable".into(), json_str(executable));
                object.insert("argv_exact".into(), json_strings(argv_exact));
            }
            ResourcePattern::ExecProgram {
                program,
                argv_exact,
            } => {
                object.insert("action".into(), json_str(Action::Exec.as_str()));
                object.insert("program".into(), json_str(program));
                object.insert("argv_exact".into(), json_strings(argv_exact));
            }
            ResourcePattern::Shell {
                executable,
                script_digest,
            } => {
                object.insert("action".into(), json_str(Action::Shell.as_str()));
                if let Some(executable) = executable {
                    object.insert("executable".into(), json_str(executable));
                }
                object.insert("script_digest".into(), json_str(script_digest));
            }
            ResourcePattern::FileRead { path, subtree }
            | ResourcePattern::FileWrite { path, subtree }
            | ResourcePattern::FileDelete { path, subtree } => {
                let action = self.pattern.action().as_str();
                object.insert("action".into(), json_str(action));
                if let Some(path) = path {
                    object.insert("path".into(), json_str(path));
                }
                if let Some(subtree) = subtree {
                    object.insert("subtree".into(), json_str(subtree));
                }
            }
            ResourcePattern::Net { methods, url_prefix } => {
                object.insert("action".into(), json_str(Action::Net.as_str()));
                object.insert("methods".into(), json_strings(methods));
                object.insert("url_prefix".into(), json_str(url_prefix));
            }
            ResourcePattern::LlmCall { providers, models } => {
                object.insert("action".into(), json_str(Action::LlmCall.as_str()));
                object.insert("providers".into(), json_strings(providers));
                object.insert("models".into(), json_strings(models));
            }
            ResourcePattern::SendEmail { recipients } => {
                object.insert("action".into(), json_str(Action::SendEmail.as_str()));
                object.insert("recipients".into(), json_strings(recipients));
            }
            ResourcePattern::Any { of } => {
                object.insert("action".into(), json_str(of.as_str()));
                object.insert("any".into(), Value::Bool(true));
            }
        }
        object.insert(
            "approval".into(),
            json_str(self.approval.as_str()),
        );
        if let Some(expires_at) = &self.expires_at {
            object.insert(
                "expires_at".into(),
                Value::String(expires_at.to_rfc3339()),
            );
        }
        Value::Object(object)
    }
}

/// workflow / node 层的能力列表校验（闭集 + 唯一 id + manifest 专用变体拒绝）。
///
/// # Errors
/// 任一能力非法时返回面向编辑器的错误文本。
pub fn validate_capabilities(capabilities: &[Capability], layer: &str) -> Result<(), String> {
    validate_capabilities_in(capabilities, layer, false)
}

/// manifest 层的能力列表校验（允许族级 any 与裸程序名）。
///
/// # Errors
/// 同 [`validate_capabilities`]。
pub fn validate_manifest_capabilities(capabilities: &[Capability]) -> Result<(), String> {
    validate_capabilities_in(capabilities, "manifest ", true)
}

fn validate_capabilities_in(
    capabilities: &[Capability],
    layer: &str,
    manifest_layer: bool,
) -> Result<(), String> {
    if capabilities.len() > MAX_CAPABILITIES {
        return Err(format!("{layer}能力数量超过上限 {MAX_CAPABILITIES}"));
    }
    let mut seen = std::collections::BTreeSet::new();
    for capability in capabilities {
        if capability.id.is_empty() || capability.id.len() > 128 {
            return Err(format!("{layer}能力 id 必须为 1..=128 字节"));
        }
        if !seen.insert(capability.id.as_str()) {
            return Err(format!("{layer}能力 id 重复：{}", capability.id));
        }
        if !manifest_layer && capability.pattern.is_manifest_only() {
            return Err(format!(
                "{layer}能力 {} 使用了仅插件 manifest 层可用的 matcher",
                capability.id
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// WorkflowPermissions / NodePermissions
// ---------------------------------------------------------------------------

/// 工作流层能力上限（§3.4）。`allow` 默认为空 = 无任何外部能力。
///
/// `Deserialize` 经 [`parse_workflow_permissions`] 闭集解析：定义 JSON、
/// 冻结快照与事件读回走同一条严格路径。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WorkflowPermissions {
    pub version: u32,
    pub mode: ExecutionMode,
    pub allow: Vec<Capability>,
}

impl<'de> Deserialize<'de> for WorkflowPermissions {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        parse_workflow_permissions(&value).map_err(serde::de::Error::custom)
    }
}

impl WorkflowPermissions {
    /// 权限内容摘要（策略身份的一部分；变化即 WorkflowPermissionsChanged）。
    pub fn digest(&self) -> String {
        canonical_digest(&serde_json::to_value(self).unwrap_or(Value::Null))
    }
}

/// 节点层权限：Inherit 继承 workflow 上限；Restrict 再取交集（空 = DenyAll）。
///
/// `Deserialize` 经 [`parse_node_permissions`] 闭集解析。
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum NodePermissions {
    Inherit,
    Restrict { allow: Vec<Capability> },
}

impl<'de> Deserialize<'de> for NodePermissions {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        parse_node_permissions(&value).map_err(serde::de::Error::custom)
    }
}

// ---------------------------------------------------------------------------
// 请求 / 裁决
// ---------------------------------------------------------------------------

/// 派发上下文：run/node/workflow/attempt/dispatch 与执行主体身份（§3.3）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestContext {
    pub run_id: String,
    pub workflow_id: String,
    pub workflow_version: u64,
    pub node_id: String,
    pub node_type: String,
    pub dispatch_id: String,
    pub attempt: u32,
    /// 操作请求字节摘要（I11 指纹）：request_hash 因此绑定完整载荷。
    pub request_fingerprint: String,
    /// 工具级请求（G5）：harness turn 内的 tool_call_id；节点级为 None。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

/// 一次裁决请求：需求集合（同一次外部操作的全部必需动作）+ 完整上下文。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PermissionRequest {
    pub actor: Principal,
    pub operation_id: String,
    /// 非空需求集合；任一 Deny 则整次拒绝（§3.6）。
    pub specs: Vec<OperationSpec>,
    pub context: RequestContext,
    pub policy_digest: String,
    /// 完整请求 canonical 编码的摘要（不含此字段与展示 preview）。
    pub request_hash: String,
}

impl PermissionRequest {
    /// 计算 request_hash：去掉 `request_hash` 字段后的 canonical 摘要。
    ///
    /// 折叠层用它复核 PermissionAsked/Decided 里自报的 hash（I11 同款：
    /// 授权对象是请求字节摘要，不是「善意」）。
    pub fn compute_hash(&self) -> String {
        let mut value = serde_json::to_value(self).unwrap_or(Value::Null);
        if let Value::Object(object) = &mut value {
            object.remove("request_hash");
        }
        canonical_digest(&value)
    }

    /// 以计算出的 hash 封口（构造请求的唯一出口）。
    pub fn sealed(mut self) -> Self {
        self.request_hash = self.compute_hash();
        self
    }
}

/// 裁决：Allow / Deny。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    Allow,
    Deny,
}

impl Decision {
    /// 线上名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Decision::Allow => "allow",
            Decision::Deny => "deny",
        }
    }
}

/// 审批范围：Once 批当前实例；Run 免除同 run/node 下精确操作的再次询问。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalScope {
    Once,
    Run,
}

impl Default for ApprovalScope {
    /// 缺省 Once：不提供持久规则，Run 必须显式选择（§6）。
    fn default() -> Self {
        ApprovalScope::Once
    }
}

/// 裁决的执行修饰（§3.3）。首版不开放随意修改：超时使用现有预算，OS
/// 强制约束必须实际生效。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Modifiers {
    #[serde(default)]
    pub sandbox: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub egress: Option<Vec<String>>,
}

/// 一条已持久化的裁决（PermissionDecided 事件载荷）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionRecord {
    pub operation_id: String,
    pub request_hash: String,
    pub evaluated_at: DateTime<Utc>,
    /// 基础策略 + run 授权事件水位。
    pub policy_revision: String,
    pub decision: Decision,
    pub by: Principal,
    /// 分层记录命中项；拒绝时记录未满足的层。
    pub matched_capabilities: Vec<String>,
    #[serde(default)]
    pub modifiers: Modifiers,
    #[serde(default)]
    pub scope: ApprovalScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Run 精确授权投影（RunApprovalUpserted 折叠产物；仅免重复询问）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunApproval {
    pub node_id: String,
    pub actor: Principal,
    /// 操作内容摘要（排除调用实例与 transport dispatch）。
    pub digest: String,
    pub expires_at: Option<DateTime<Utc>>,
    pub by: Principal,
}

/// 插件能力上限（P1 G4）：按 `(plugin_id, content_hash)` 冻结的授予集合。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PluginCeiling {
    pub plugin_id: String,
    pub content_hash: String,
    /// 已授予的能力（manifest 声明与安装授予的交集；Any/program 变体只
    /// 在此层出现，随快照冻结与事件回读）。
    pub allow: Vec<Capability>,
}

impl<'de> Deserialize<'de> for PluginCeiling {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        /// 授予层能力按 manifest 闭集解析（允许族级 any 与裸程序名）。
        #[derive(Deserialize)]
        struct Wire {
            plugin_id: String,
            content_hash: String,
            allow: Vec<Value>,
        }
        let wire = Wire::deserialize(deserializer)?;
        let allow = wire
            .allow
            .iter()
            .map(|value| {
                parse_manifest_capability(value)
                    .map_err(|error| serde::de::Error::custom(format!("plugin ceiling: {error}")))
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(Self {
            plugin_id: wire.plugin_id,
            content_hash: wire.content_hash,
            allow,
        })
    }
}

/// 子工作流的上限层：调用方（父 run 节点）的一个有效能力层。多层之间取
/// AND（祖先链逐层收窄），不继承父节点凭据、不自动提升能力（§3.5）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CeilingLayer {
    /// 诊断标签（如 `parent:<run>/<node>:workflow`）。
    pub source: String,
    pub allow: Vec<Capability>,
}

/// run 启动时冻结的策略快照（G2 / I30）：执行模式 + workflow/node 能力及
/// 期限 + 插件授予（P1）+ 子工作流的父上限层。
///
/// 评估器（policy.rs）只读这份快照；workflow/node 能力修改只影响新 run。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FrozenPolicy {
    /// 策略身份：workflow/node/parent 层内容的 canonical 摘要。
    pub digest: String,
    pub mode: ExecutionMode,
    /// legacy 定义（无 permissions 块）：所有外部操作 Deny，静态门在
    /// run.start 拒绝副作用节点后，这里是对已飞 run 的兜底。
    pub legacy: bool,
    pub workflow: WorkflowPermissions,
    /// 节点层（缺省 Inherit）。
    pub nodes: BTreeMap<String, NodePermissions>,
    /// 子工作流：祖先节点有效层的上限（逐层 AND 相交），不继承父节点凭据。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ceilings: Vec<CeilingLayer>,
    /// 插件授予上限（P1）。
    pub plugin_ceilings: Vec<PluginCeiling>,
}

impl FrozenPolicy {
    /// 计算 policy digest：对 workflow/node/parent/plugin 层内容做 canonical
    /// 编码后取摘要（与 freeze 构造处一致）。
    pub fn compute_digest(
        mode: ExecutionMode,
        legacy: bool,
        workflow: &WorkflowPermissions,
        nodes: &BTreeMap<String, NodePermissions>,
        ceilings: &[CeilingLayer],
        plugin_ceilings: &[PluginCeiling],
    ) -> String {
        canonical_digest(&serde_json::json!({
            "mode": mode,
            "legacy": legacy,
            "workflow": workflow,
            "nodes": nodes,
            "ceilings": ceilings,
            "plugin_ceilings": plugin_ceilings,
        }))
    }

    /// 由各层内容构造并封口 digest（构造冻结策略的唯一出口）。
    pub fn sealed(
        mode: ExecutionMode,
        legacy: bool,
        workflow: WorkflowPermissions,
        nodes: BTreeMap<String, NodePermissions>,
        ceilings: Vec<CeilingLayer>,
        plugin_ceilings: Vec<PluginCeiling>,
    ) -> Self {
        let digest =
            Self::compute_digest(mode, legacy, &workflow, &nodes, &ceilings, &plugin_ceilings);
        Self {
            digest,
            mode,
            legacy,
            workflow,
            nodes,
            ceilings,
            plugin_ceilings,
        }
    }

    /// 复核自报 digest 与层内容一致（折叠层与派发前复核共用）。
    pub fn digest_is_consistent(&self) -> bool {
        self.digest
            == Self::compute_digest(
                self.mode,
                self.legacy,
                &self.workflow,
                &self.nodes,
                &self.ceilings,
                &self.plugin_ceilings,
            )
    }
}

// ---------------------------------------------------------------------------
// 摘要工具
// ---------------------------------------------------------------------------

/// SHA-256 hex。
pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// canonical JSON 编码：对象键按字典序排序，无空白；超预算返回错误。
///
/// 与 journal codec 的 `fingerprint` 语义一致但独立实现（见模块注释）。
///
/// # Errors
/// 值超过 `limit` 字节时返回错误。
pub fn canonical_json(value: &Value, limit: usize) -> Result<String, String> {
    let mut out = String::new();
    encode(value, limit, &mut out)?;
    Ok(out)
}

fn encode(value: &Value, limit: usize, out: &mut String) -> Result<(), String> {
    if out.len() > limit {
        return Err(format!("canonical encoding exceeds {limit} bytes"));
    }
    match value {
        Value::Object(object) => {
            let mut keys: Vec<&String> = object.keys().collect();
            keys.sort();
            out.push('{');
            for (index, key) in keys.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                let encoded_key =
                    serde_json::to_string(key.as_str()).map_err(|e| e.to_string())?;
                out.push_str(&encoded_key);
                out.push(':');
                encode(&object[*key], limit, out)?;
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                encode(item, limit, out)?;
            }
            out.push(']');
        }
        other => {
            let encoded = serde_json::to_string(other).map_err(|e| e.to_string())?;
            out.push_str(&encoded);
        }
    }
    Ok(())
}

/// canonical 编码的 SHA-256 摘要。
pub fn canonical_digest(value: &Value) -> String {
    // 权限对象有界（闭集 + 字段上限），1 MiB 足够；超限视为损坏。
    sha256_hex(canonical_json(value, 1024 * 1024).unwrap_or_default().as_bytes())
}

/// Run 授权匹配摘要：完整操作内容（排除调用实例与 transport dispatch，
/// 保留 actor 节点/插件身份、载荷与策略 digest）（§4）。
pub fn run_approval_digest(
    actor: &Principal,
    specs: &[OperationSpec],
    request_fingerprint: &str,
    policy_digest: &str,
) -> String {
    canonical_digest(&serde_json::json!({
        "actor": actor,
        "specs": specs,
        "request_fingerprint": request_fingerprint,
        "policy_digest": policy_digest,
    }))
}

// ---------------------------------------------------------------------------
// 闭集解析（Capability / NodePermissions / WorkflowPermissions）
// ---------------------------------------------------------------------------

/// 解析一条能力（workflow/node 定义层：拒绝 manifest 专用变体）。
///
/// # Errors
/// 未知字段 / 未知 action / action 与 matcher 不匹配 / 模板表达式 / 摘要
/// 非法等，返回面向编辑器的错误文本。
pub fn parse_capability(value: &Value) -> Result<Capability, String> {
    parse_capability_in(value, false)
}

/// 解析一条 manifest 层能力（允许族级 `any` 与裸程序名 `program`）。
///
/// # Errors
/// 同 [`parse_capability`]；`any=true` 时不得再携带 matcher 字段。
pub fn parse_manifest_capability(value: &Value) -> Result<Capability, String> {
    parse_capability_in(value, true)
}

fn parse_capability_in(value: &Value, manifest_layer: bool) -> Result<Capability, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "能力必须是对象".to_string())?;
    let action_name = object
        .get("action")
        .and_then(Value::as_str)
        .ok_or_else(|| "能力缺少 action 字段".to_string())?;
    let action =
        Action::parse(action_name).ok_or_else(|| format!("未知动作：{action_name}"))?;
    let id = string_field(object, "id")?;
    // manifest 层能力只声明需求（required），不声明审批要求：缺省 ask
    //（授予层的 approval 只影响该层是否询问，harmless）。
    let approval = match object.get("approval").and_then(Value::as_str) {
        None if manifest_layer => Approval::Ask,
        None => {
            return Err(format!("能力 {id} 缺少 approval 字段（auto|ask）"));
        }
        Some(approval) => match approval {
            "auto" => Approval::Auto,
            "ask" => Approval::Ask,
            other => {
                return Err(format!(
                    "能力 {id} 的 approval 必须是 auto|ask，当前为 {other}"
                ))
            }
        },
    };
    let expires_at = match object.get("expires_at") {
        None | Some(Value::Null) => None,
        Some(Value::String(text)) => Some(
            DateTime::parse_from_rfc3339(text)
                .map_err(|e| format!("能力 {id} 的 expires_at 不是 RFC3339 时间：{e}"))?
                .with_timezone(&Utc),
        ),
        Some(_) => return Err(format!("能力 {id} 的 expires_at 必须是字符串")),
    };
    // 族级 any：只允许 manifest 层；显式 true 才存在，不能靠省略字段得到。
    let wants_any = matches!(object.get("any"), Some(Value::Bool(true)));
    if wants_any && !manifest_layer {
        return Err(format!(
            "能力 {id}：any 仅插件 manifest 层可用，workflow/node 层必须写精确 matcher"
        ));
    }
    let allow_key = |key: &str| -> Option<&Value> { object.get(key).filter(|v| !v.is_null()) };
    let pattern = if wants_any {
        ResourcePattern::Any { of: action }
    } else {
        match action {
            Action::Secret => ResourcePattern::Secret {
                names: string_list(object, &id, "names")?,
            },
            Action::Exec => {
                let argv_exact = string_list(object, &id, "argv_exact")?;
                match (allow_key("executable"), allow_key("program")) {
                    (Some(_), Some(_)) => {
                        return Err(format!("能力 {id}：executable 与 program 互斥"));
                    }
                    (Some(executable), None) => {
                        let executable = executable.as_str().unwrap_or_default();
                        validate_absolute_path(executable)
                            .map_err(|e| format!("能力 {id}：{e}"))?;
                        ResourcePattern::Exec {
                            executable: executable.to_string(),
                            argv_exact,
                        }
                    }
                    (None, Some(program)) => {
                        let program = program.as_str().unwrap_or_default();
                        if program.contains('/') || program.is_empty() {
                            return Err(format!("能力 {id}：program 必须是不含 / 的裸程序名"));
                        }
                        ResourcePattern::ExecProgram {
                            program: program.to_string(),
                            argv_exact,
                        }
                    }
                    (None, None) => {
                        return Err(format!(
                            "能力 {id}：exec 需要 executable（绝对路径）或 program"
                        ));
                    }
                }
            }
            Action::Shell => {
                let script_digest = string_field(object, "script_digest")?;
                validate_digest(&script_digest).map_err(|e| format!("能力 {id}：{e}"))?;
                let executable = match allow_key("executable") {
                    None => None,
                    Some(executable) => {
                        let executable = executable.as_str().unwrap_or_default();
                        validate_absolute_path(executable)
                            .map_err(|e| format!("能力 {id}：{e}"))?;
                        Some(executable.to_string())
                    }
                };
                ResourcePattern::Shell {
                    executable,
                    script_digest,
                }
            }
            Action::FileRead | Action::FileWrite | Action::FileDelete => {
                let path = optional_relative_path(object, &id, "path")?;
                let subtree = optional_relative_path(object, &id, "subtree")?;
                let (path, subtree) = match (path, subtree) {
                    (Some(_), Some(_)) => {
                        return Err(format!("能力 {id}：path 与 subtree 互斥"));
                    }
                    (None, None) => {
                        return Err(format!(
                            "能力 {id}：需要 path（精确相对路径）或 subtree（目录）"
                        ));
                    }
                    pair => pair,
                };
                match action {
                    Action::FileRead => ResourcePattern::FileRead { path, subtree },
                    Action::FileWrite => ResourcePattern::FileWrite { path, subtree },
                    Action::FileDelete => ResourcePattern::FileDelete { path, subtree },
                    _ => unreachable!(),
                }
            }
            Action::Net => {
                let methods = string_list(object, &id, "methods")?;
                for method in &methods {
                    if !HTTP_METHODS.contains(&method.to_uppercase().as_str()) {
                        return Err(format!("能力 {id}：非法 HTTP 方法 {method}"));
                    }
                }
                let url_prefix = string_field(object, "url_prefix")?;
                validate_url_prefix(&url_prefix).map_err(|e| format!("能力 {id}：{e}"))?;
                ResourcePattern::Net {
                    methods: methods.into_iter().map(|m| m.to_uppercase()).collect(),
                    url_prefix,
                }
            }
            Action::LlmCall => ResourcePattern::LlmCall {
                providers: string_list(object, &id, "providers")?,
                models: string_list(object, &id, "models")?,
            },
            Action::SendEmail => ResourcePattern::SendEmail {
                recipients: string_list(object, &id, "recipients")?,
            },
        }
    };
    // 闭集：多余字段一律拒绝（权限字段没有「未知键保留」的旧约定）。
    let base = [
        "id",
        "action",
        "approval",
        "expires_at",
        // manifest 声明层的额外键（required：是否必须授予才可启用）。
        "required",
    ];
    let matcher_keys: &[&str] = match &pattern {
        ResourcePattern::Secret { .. } => &["names"],
        ResourcePattern::Exec { .. } => &["executable", "argv_exact"],
        ResourcePattern::ExecProgram { .. } => &["program", "argv_exact"],
        ResourcePattern::Shell { .. } => &["executable", "script_digest"],
        ResourcePattern::FileRead { .. } | ResourcePattern::FileWrite { .. }
        | ResourcePattern::FileDelete { .. } => &["path", "subtree"],
        ResourcePattern::Net { .. } => &["methods", "url_prefix"],
        ResourcePattern::LlmCall { .. } => &["providers", "models"],
        ResourcePattern::SendEmail { .. } => &["recipients"],
        ResourcePattern::Any { .. } => &["any"],
    };
    for key in object.keys() {
        let known = base.contains(&key.as_str()) || matcher_keys.contains(&key.as_str());
        if !known {
            return Err(format!("能力 {id}：未知字段 {key}"));
        }
    }
    Ok(Capability {
        id,
        pattern,
        approval,
        expires_at,
    })
}

/// HTTP 方法闭集（http_node 的现有白名单）。
pub const HTTP_METHODS: [&str; 7] =
    ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"];

/// 解析 workflow 层权限块。
///
/// # Errors
/// 版本未知 / mode 非法 / 能力非法 / 模板表达式。
pub fn parse_workflow_permissions(value: &Value) -> Result<WorkflowPermissions, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "permissions 必须是对象".to_string())?;
    for key in object.keys() {
        if !matches!(key.as_str(), "version" | "mode" | "allow") {
            return Err(format!("permissions：未知字段 {key}"));
        }
    }
    let version = match object.get("version") {
        Some(Value::Number(number)) => number
            .as_u64()
            .and_then(|v| u32::try_from(v).ok())
            .ok_or_else(|| "permissions.version 必须是整数".to_string())?,
        _ => return Err("permissions.version 缺失".to_string()),
    };
    if version != PERMISSION_SCHEMA_VERSION {
        return Err(format!(
            "不支持的权限声明版本 {version}（当前支持 {PERMISSION_SCHEMA_VERSION}）"
        ));
    }
    let mode = match object.get("mode").and_then(Value::as_str) {
        Some(mode) => ExecutionMode::parse(mode)
            .ok_or_else(|| format!("permissions.mode 必须是 execute|plan，当前为 {mode}"))?,
        None => return Err("permissions.mode 缺失".to_string()),
    };
    let allow = match object.get("allow") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(items)) => items
            .iter()
            .map(parse_capability)
            .collect::<Result<Vec<_>, _>>()?,
        Some(_) => return Err("permissions.allow 必须是数组".to_string()),
    };
    validate_capabilities(&allow, "workflow ")?;
    Ok(WorkflowPermissions {
        version,
        mode,
        allow,
    })
}

/// 解析节点层权限块。
///
/// # Errors
/// mode 非法 / 能力非法。
pub fn parse_node_permissions(value: &Value) -> Result<NodePermissions, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "节点 permissions 必须是对象".to_string())?;
    for key in object.keys() {
        if !matches!(key.as_str(), "mode" | "allow") {
            return Err(format!("节点 permissions：未知字段 {key}"));
        }
    }
    match object.get("mode").and_then(Value::as_str) {
        Some("inherit") => Ok(NodePermissions::Inherit),
        Some("restrict") => {
            let allow = match object.get("allow") {
                None | Some(Value::Null) => Vec::new(),
                Some(Value::Array(items)) => items
                    .iter()
                    .map(parse_capability)
                    .collect::<Result<Vec<_>, _>>()?,
                Some(_) => return Err("节点 permissions.allow 必须是数组".to_string()),
            };
            validate_capabilities(&allow, "节点 ")?;
            Ok(NodePermissions::Restrict { allow })
        }
        Some(other) => Err(format!(
            "节点 permissions.mode 必须是 inherit|restrict，当前为 {other}"
        )),
        None => Err("节点 permissions.mode 缺失".to_string()),
    }
}

// ---------------------------------------------------------------------------
// 字段校验助手
// ---------------------------------------------------------------------------

/// 字符串字段：拒绝模板表达式（权限字段禁止模板/表达式，§3.5）。
fn string_field(object: &serde_json::Map<String, Value>, key: &str) -> Result<String, String> {
    let text = object
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("缺少 {key} 字段"))?;
    if text.is_empty() {
        return Err(format!("{key} 不能为空"));
    }
    if text.len() > MAX_FIELD_BYTES {
        return Err(format!("{key} 超过 {MAX_FIELD_BYTES} 字节"));
    }
    if text.contains("${") {
        return Err(format!("{key} 不支持模板表达式（运行时目标需重新匹配）"));
    }
    Ok(text.to_string())
}

/// 字符串列表字段：非空、每项非空且不含模板表达式。
fn string_list(
    object: &serde_json::Map<String, Value>,
    id: &str,
    key: &str,
) -> Result<Vec<String>, String> {
    let items = object
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("能力 {id} 缺少 {key} 列表"))?;
    if items.is_empty() {
        return Err(format!("能力 {id} 的 {key} 不能为空列表"));
    }
    items
        .iter()
        .map(|item| {
            let text = item
                .as_str()
                .ok_or_else(|| format!("能力 {id} 的 {key} 必须是字符串列表"))?;
            if text.is_empty() {
                return Err(format!("能力 {id} 的 {key} 含空字符串"));
            }
            if text.len() > MAX_FIELD_BYTES {
                return Err(format!("能力 {id} 的 {key} 项超过 {MAX_FIELD_BYTES} 字节"));
            }
            if text.contains("${") {
                return Err(format!(
                    "能力 {id} 的 {key} 不支持模板表达式（运行时目标需重新匹配）"
                ));
            }
            Ok(text.to_string())
        })
        .collect()
}

/// 可选相对路径字段（None = 字段缺省）。
fn optional_relative_path(
    object: &serde_json::Map<String, Value>,
    id: &str,
    key: &str,
) -> Result<Option<String>, String> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => {
            let text = value
                .as_str()
                .ok_or_else(|| format!("能力 {id} 的 {key} 必须是字符串"))?;
            validate_relative_path(text).map_err(|e| format!("能力 {id}：{e}"))?;
            Ok(Some(text.to_string()))
        }
    }
}

/// 相对路径：非空、不以 / 开头、组件不含 `..`（词法规范化后比较）。
fn validate_relative_path(path: &str) -> Result<(), String> {
    if path.is_empty() || path.len() > MAX_FIELD_BYTES {
        return Err("相对路径必须为 1..=512 字节".into());
    }
    if path.starts_with('/') || path.contains('\\') {
        return Err(format!("相对路径不能以 / 开头或包含反斜杠：{path}"));
    }
    for component in path.split('/') {
        if matches!(component, "" | "." | "..") {
            return Err(format!("相对路径包含空/./.. 组件：{path}"));
        }
    }
    Ok(())
}

/// 绝对路径：以 / 开头、无 `.`/`..` 组件、无重复分隔符（规范化形态）。
fn validate_absolute_path(path: &str) -> Result<(), String> {
    if !path.starts_with('/') {
        return Err(format!("executable 必须是绝对路径：{path}"));
    }
    if path.len() > MAX_FIELD_BYTES {
        return Err("executable 超过 512 字节".into());
    }
    for component in path[1..].split('/') {
        if matches!(component, "" | "." | "..") {
            return Err(format!("executable 必须规范化（无 ./..// 组件）：{path}"));
        }
    }
    Ok(())
}

/// 摘要：64 位小写 hex。
fn validate_digest(digest: &str) -> Result<(), String> {
    let valid =
        digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_digit() || matches!(b, b'a'..=b'f'));
    if valid {
        Ok(())
    } else {
        Err(format!("摘要必须是 64 位小写 hex：{digest}"))
    }
}

/// URL 前缀：http/https、无 userinfo、无 query/fragment。
fn validate_url_prefix(prefix: &str) -> Result<(), String> {
    let parsed = url::Url::parse(prefix).map_err(|e| format!("url_prefix 非法：{e}"))?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return Err(format!("url_prefix 只支持 http/https：{prefix}"));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err("url_prefix 不允许 userinfo".into());
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err("url_prefix 不允许 query/fragment".into());
    }
    Ok(())
}

/// URL 前缀匹配：scheme/host/port 精确，路径按段前缀。
///
/// 解析后比较（不做字符串 starts_with，§7.1）：默认端口归一
/// （http:80 / https:443），路径段边界匹配（`/v1` 不匹配 `/v10`）。
pub fn url_prefix_matches(prefix: &str, url: &str) -> bool {
    let Ok(pattern) = url::Url::parse(prefix) else {
        return false;
    };
    let Ok(target) = url::Url::parse(url) else {
        return false;
    };
    if pattern.scheme() != target.scheme()
        || pattern.host_str().is_none()
        || pattern.host_str() != target.host_str()
    {
        return false;
    }
    if pattern.port_or_known_default() != target.port_or_known_default() {
        return false;
    }
    // 查询串是载荷的一部分：模式不允许带 query（校验期拒绝），目标带
    // query 不影响路径段匹配。
    let pattern_path = pattern.path();
    let target_path = target.path();
    if pattern_path == "/" || pattern_path.is_empty() {
        return true;
    }
    let pattern_path = pattern_path.trim_end_matches('/');
    let target_path = target_path.trim_end_matches('/');
    target_path == pattern_path || target_path.starts_with(&format!("{pattern_path}/"))
}

fn json_str(value: &str) -> Value {
    Value::String(value.to_string())
}

fn json_strings(values: &[String]) -> Value {
    Value::Array(values.iter().map(|v| json_str(v)).collect())
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn capability(value: Value) -> Capability {
        parse_capability(&value).expect("capability parses")
    }

    #[test]
    fn parses_the_design_example() {
        // 09 §3.5 的示例三件套。
        let secret = capability(json!({
            "id": "summary-key", "action": "secret", "names": ["summary-api-key"],
            "approval": "auto"
        }));
        assert_eq!(secret.pattern.action(), Action::Secret);
        assert_eq!(secret.approval, Approval::Auto);
        let net = capability(json!({
            "id": "summary-api", "action": "net", "methods": ["POST"],
            "url_prefix": "https://api.example.com/summarize", "approval": "auto"
        }));
        assert_eq!(net.pattern.action(), Action::Net);
        let exec = capability(json!({
            "id": "publish-main", "action": "exec", "executable": "/usr/bin/git",
            "argv_exact": ["push", "origin", "main"], "approval": "ask"
        }));
        assert_eq!(exec.pattern.action(), Action::Exec);
        // 序列化往返：to_json 的输出能再次解析。
        for cap in [secret, net, exec] {
            let round = capability(cap.to_json());
            assert_eq!(round, cap);
        }
    }

    #[test]
    fn rejects_unknown_fields_and_actions() {
        assert!(parse_capability(&json!({"id":"x","action":"net","methods":["GET"],
            "url_prefix":"https://a.b/","approval":"auto","surprise":1})).is_err());
        assert!(parse_capability(&json!({"id":"x","action":"teleport","approval":"auto"})).is_err());
        assert!(parse_capability(&json!({"id":"x","action":"exec",
            "executable":"/bin/ls","argv_exact":[],"approval":"yes"})).is_err());
        // 模板表达式拒绝。
        assert!(parse_capability(&json!({"id":"x","action":"secret",
            "names":["${input.key}"],"approval":"auto"})).is_err());
        // exec 缺 executable/program。
        assert!(parse_capability(&json!({"id":"x","action":"exec","argv_exact":[],
            "approval":"auto"})).is_err());
        // 摘要非法。
        assert!(parse_capability(&json!({"id":"x","action":"shell",
            "script_digest":"deadbeef","approval":"auto"})).is_err());
        // path 与 subtree 互斥。
        assert!(parse_capability(&json!({"id":"x","action":"file_write",
            "path":"a.txt","subtree":"src","approval":"auto"})).is_err());
        // URL 带 userinfo / query。
        assert!(parse_capability(&json!({"id":"x","action":"net","methods":["GET"],
            "url_prefix":"https://u:p@a.b/","approval":"auto"})).is_err());
        assert!(parse_capability(&json!({"id":"x","action":"net","methods":["GET"],
            "url_prefix":"https://a.b/?q=1","approval":"auto"})).is_err());
    }

    #[test]
    fn manifest_only_variants_rejected_in_workflow_layer() {
        let any = parse_manifest_capability(&json!({"id":"x","action":"net","any":true,"approval":"auto"}))
            .expect("manifest layer accepts any");
        assert!(validate_capabilities(&[any], "workflow ").is_err());
        let program = parse_manifest_capability(&json!({"id":"x","action":"exec","program":"git",
            "argv_exact":["status"],"approval":"auto"}))
            .expect("manifest layer accepts program");
        assert!(validate_capabilities(std::slice::from_ref(&program), "workflow ").is_err());
        // manifest 层自身的校验放行。
        assert!(validate_manifest_capabilities(&[program]).is_ok());
    }

    #[test]
    fn net_matcher_is_segment_and_port_aware() {
        let net = capability(json!({
            "id":"api","action":"net","methods":["post","get"],
            "url_prefix":"https://api.example.com/v1/","approval":"auto"
        }));
        let spec = |url: &str| OperationSpec::Http {
            method: "POST".into(),
            url: url.into(),
            headers_digest: "0".repeat(64),
            body_digest: "0".repeat(64),
        };
        assert!(net.pattern.matches(&spec("https://api.example.com/v1/x?k=1")));
        assert!(net.pattern.matches(&spec("https://api.example.com/v1")));
        assert!(!net.pattern.matches(&spec("https://api.example.com/v10")));
        assert!(!net.pattern.matches(&spec("https://api.example.com:8443/v1/x")));
        assert!(!net.pattern.matches(&spec("http://api.example.com/v1/x")));
        assert!(!net.pattern.matches(&spec("https://api.example.com/")));
        // 方法大小写不敏感。
        let get = OperationSpec::Http {
            method: "GET".into(),
            url: "https://api.example.com/v1/y".into(),
            headers_digest: "0".repeat(64),
            body_digest: "0".repeat(64),
        };
        assert!(net.pattern.matches(&get));
        let put = OperationSpec::Http {
            method: "PUT".into(),
            url: "https://api.example.com/v1/y".into(),
            headers_digest: "0".repeat(64),
            body_digest: "0".repeat(64),
        };
        assert!(!net.pattern.matches(&put));
    }

    #[test]
    fn exec_matcher_requires_exact_argv() {
        let exec = capability(json!({
            "id":"push","action":"exec","executable":"/usr/bin/git",
            "argv_exact":["push","origin","main"],"approval":"ask"
        }));
        let spec = |argv: &[&str]| OperationSpec::Exec {
            executable: "/usr/bin/git".into(),
            argv: argv.iter().map(|s| s.to_string()).collect(),
            cwd: "/tmp".into(),
            env_digest: "0".repeat(64),
            stdin_digest: None,
        };
        assert!(exec.pattern.matches(&spec(&["push", "origin", "main"])));
        assert!(!exec.pattern.matches(&spec(&["push", "origin", "dev"])));
        assert!(!exec.pattern.matches(&spec(&["push"])));
        // 同名不同路径不匹配（basename 检查只存在于 manifest 层）。
        let other = OperationSpec::Exec {
            executable: "/usr/local/bin/git".into(),
            argv: vec!["push".into(), "origin".into(), "main".into()],
            cwd: "/tmp".into(),
            env_digest: "0".repeat(64),
            stdin_digest: None,
        };
        assert!(!exec.pattern.matches(&other));
    }

    #[test]
    fn file_matcher_subtree_boundary() {
        let write = capability(json!({
            "id":"src","action":"file_write","subtree":"src","approval":"auto"
        }));
        let spec = |path: &str| OperationSpec::FileWrite {
            path: path.into(),
            content_digest: "0".repeat(64),
        };
        assert!(write.pattern.matches(&spec("src/main.rs")));
        assert!(!write.pattern.matches(&spec("srcx/main.rs")));
        assert!(!write.pattern.matches(&spec("build/main.rs")));
        // 读能力不匹配写载荷。
        let read = capability(json!({
            "id":"src-r","action":"file_read","subtree":"src","approval":"auto"
        }));
        assert!(!read.pattern.matches(&spec("src/main.rs")));
    }

    #[test]
    fn email_matcher_requires_all_recipients() {
        let email = capability(json!({
            "id":"to","action":"send_email","recipients":["a@x.y","b@x.y"],"approval":"auto"
        }));
        let spec = |to: &[&str]| OperationSpec::SendEmail {
            recipients: to.iter().map(|s| s.to_string()).collect(),
            payload_digest: "0".repeat(64),
        };
        assert!(email.pattern.matches(&spec(&["a@x.y"])));
        assert!(email.pattern.matches(&spec(&["a@x.y", "b@x.y"])));
        assert!(!email.pattern.matches(&spec(&["a@x.y", "c@x.y"])));
    }

    #[test]
    fn canonical_digest_is_key_order_insensitive() {
        let a = json!({"b": 1, "a": {"y": 2, "x": [3, {"z": 4, "w": 5}]}});
        let b = json!({"a": {"x": [3, {"w": 5, "z": 4}], "y": 2}, "b": 1});
        assert_eq!(canonical_digest(&a), canonical_digest(&b));
        assert_ne!(canonical_digest(&a), canonical_digest(&json!({"a":1,"b":2})));
    }

    #[test]
    fn run_approval_digest_excludes_instance_fields() {
        let specs = [OperationSpec::Secret {
            secret_name: "k".into(),
        }];
        let a = run_approval_digest(&Principal::System, &specs, "fingerprint", "digest");
        let b = run_approval_digest(&Principal::System, &specs, "fingerprint", "digest");
        assert_eq!(a, b);
        let c = run_approval_digest(&Principal::System, &specs, "fingerprint2", "digest");
        assert_ne!(a, c);
    }

    #[test]
    fn parse_workflow_and_node_blocks() {
        let workflow = parse_workflow_permissions(&json!({
            "version": 1, "mode": "execute",
            "allow": [{"id":"k","action":"secret","names":["k"],"approval":"auto"}]
        }))
        .expect("workflow permissions parse");
        assert_eq!(workflow.mode, ExecutionMode::Execute);
        assert_eq!(workflow.allow.len(), 1);
        assert!(parse_workflow_permissions(&json!({"version": 2, "mode": "execute"})).is_err());
        assert!(parse_workflow_permissions(&json!({"version": 1, "mode": "fly"})).is_err());
        assert!(
            parse_workflow_permissions(&json!({"version": 1, "mode": "execute", "x": 1})).is_err()
        );
        assert_eq!(
            parse_node_permissions(&json!({"mode": "inherit"})).unwrap(),
            NodePermissions::Inherit
        );
        let restrict =
            parse_node_permissions(&json!({"mode": "restrict", "allow": []})).unwrap();
        assert_eq!(
            restrict,
            NodePermissions::Restrict { allow: vec![] }
        );
        assert!(parse_node_permissions(&json!({"mode": "wild"})).is_err());
    }

    #[test]
    fn digest_validator_accepts_only_lowercase_hex() {
        assert!(validate_digest(&"a".repeat(64)).is_ok());
        assert!(validate_digest(&"A".repeat(64)).is_err());
        assert!(validate_digest(&"a".repeat(63)).is_err());
    }

    #[test]
    fn capability_json_shape_matches_design() {
        // 线形状：{"id","action",...,"approval"}——与 09 §3.5 示例逐字段一致。
        let capability = capability(json!({
            "id": "publish-main", "action": "exec", "executable": "/usr/bin/git",
            "argv_exact": ["push", "origin", "main"], "approval": "ask"
        }));
        let json = capability.to_json();
        assert_eq!(json["action"], "exec");
        assert_eq!(json["argv_exact"], json!(["push", "origin", "main"]));
        assert_eq!(json["approval"], "ask");
        assert!(json.get("expires_at").is_none());
    }

    #[test]
    fn any_variant_matches_whole_action_family() {
        let any = parse_manifest_capability(&json!({
            "id":"all","action":"net","any":true,"approval":"ask"
        }))
        .expect("manifest layer accepts any");
        // any=true 时不允许再携带 matcher 字段。
        assert!(parse_manifest_capability(&json!({
            "id":"all","action":"net","any":true,"methods":["GET"],"approval":"ask"
        }))
        .is_err());
        let spec = OperationSpec::Http {
            method: "GET".into(),
            url: "https://anywhere.example/".into(),
            headers_digest: "0".repeat(64),
            body_digest: "0".repeat(64),
        };
        assert!(any.pattern.matches(&spec));
        let secret = OperationSpec::Secret {
            secret_name: "k".into(),
        };
        assert!(!any.pattern.matches(&secret));
    }
}
