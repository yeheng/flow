//! Explicit single-user JSONL RPC endpoint — the one product surface. Every call
//! authenticates, including pages. Product form (`module_product`) additionally
//! carries the config/secrets assembly and the materialized product reads
//! (`workflow.*.view` / `run.*.view` / triggers / templates), so the browser
//! frontend, the embedded desktop service and flow-cli all speak this module.
//!
//! 分发结构：`module_product` 只做注册；每条请求先过统一的前置层（参数
//! 归一 → token 常时比较 → request_id 幂等键检查），再由 `dispatch_call`
//! 把方法路由到独立的命名 Handler。对外 JSON-RPC 2.0 契约（错误码、错误
//! 文本、回执形状）逐字保持，改动仅限内部组织。
use flow_backend::journal::{JournalBackend, JournalError};
use flow_journal::page::{Cursor, Filter};
use jsonrpsee::{types::ErrorObjectOwned, RpcModule};
use serde_json::{json, Value};
use std::{net::SocketAddr, sync::Arc};

/// 每条 RPC 请求的上下文：权威后端、单一令牌与可选产品装配。
///
/// 只有这三样东西进入上下文，是为了让「请求间不共享可变业务状态」这条
/// 规则在类型上成立——任何跨请求残留的状态都必须显式地放进 [`Context`]。
pub struct Context {
    backend: Arc<JournalBackend>,
    token: String,
    /// 分档 API key（09 §9；FLOW_JOURNAL_TOKEN 恒为隐式 admin key）。
    api_keys: Vec<flow_config::ApiKey>,
    /// 产品装配（统一配置面 + 持久化密钥）。None = 未装配（开发模式），
    /// config/secrets 方法报「未启用」。
    product: Option<Arc<crate::AppState>>,
}

/// admit_call 解析出的调用主体与所需 scope。
pub struct Caller {
    pub principal: flow_dto::permission::Principal,
    /// 该调用要求的最低 scope（读=read，写=write，审批=approve）。
    pub required_scope: &'static str,
    /// 是否允许改变工作流权限块（LocalAdmin 或 admin-scope API key；09 §9）。
    pub permission_admin: bool,
}

/// 解析主体并按方法 scope 裁决（admin key 恒通过）。
fn resolve_caller(ctx: &Context, method: &str, supplied: &str) -> Result<Caller, ErrorObjectOwned> {
    if constant_time_eq(supplied, &ctx.token) {
        return Ok(Caller {
            principal: flow_dto::permission::Principal::LocalAdmin,
            required_scope: "read",
            permission_admin: true,
        });
    }
    // 明文 → sha256 与配置比对（常时比较逐 key）。
    let digest = {
        use sha2::Digest as _;
        sha2::Sha256::digest(supplied.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    };
    let now = chrono::Utc::now();
    for key in &ctx.api_keys {
        if !constant_time_eq(&digest, &key.token_hash) {
            continue;
        }
        // scope 裁决：读面 read；审批 write 级（Once）+ approve；其余写 write；
        // 权限管理（grant/keys）admin。
        let required: &'static str = match method {
            "run.approve" | "run.deny" => "approve",
            "permissions.grant" => "admin",
            m if flow_dto::rpc::is_read_method(m) => "read",
            _ => "write",
        };
        if !key.has_scope(required, now) {
            return Err(ErrorObjectOwned::owned(
                PERMISSION_ERROR,
                format!("api key {} lacks '{required}' scope", key.id),
                None::<()>,
            ));
        }
        return Ok(Caller {
            principal: flow_dto::permission::Principal::ApiKey { id: key.id.clone() },
            required_scope: required,
            permission_admin: key.has_scope("admin", now),
        });
    }
    Err(ErrorObjectOwned::owned(-32001, "unauthorized", None::<()>))
}
use crate::invalid;
/// 把 journal 错误映射为 v2 错误码。
///
/// 映射按**错误类型**而非错误文案：文案会随迭代变化，按文案分支会让
/// 客户端在一次措辞调整后静默走进错误分支（这条规则由单测守着）。
///
/// # Errors
/// 返回 `-32012` conflict / `-32021` limit / `-32011` not found /
/// `-32020` permission（09 权限模型拒绝）/ `-32602` invalid params，其余
/// 一律降级为 `-32603` internal——不向客户端泄漏内部细节。
fn failure(error: JournalError) -> ErrorObjectOwned {
    match error {
        JournalError::Journal(flow_journal::Error::Conflict(message)) => {
            ErrorObjectOwned::owned(-32012, message, None::<()>)
        }
        JournalError::Journal(flow_journal::Error::Limit(message)) => {
            ErrorObjectOwned::owned(-32021, message, None::<()>)
        }
        JournalError::Journal(flow_journal::Error::NotFound(message)) => {
            ErrorObjectOwned::owned(-32011, message, None::<()>)
        }
        JournalError::Journal(flow_journal::Error::Forbidden(message)) => {
            ErrorObjectOwned::owned(PERMISSION_ERROR, message, None::<()>)
        }
        JournalError::Journal(flow_journal::Error::Invalid(message)) => invalid(message),
        _ => ErrorObjectOwned::owned(-32603, "journal service unavailable", None::<()>),
    }
}

/// 权限模型拒绝的错误码（09）：legacy 定义需迁移、能力不足、无审批权、
/// 审批实例冲突/过期。
pub const PERMISSION_ERROR: i32 = -32020;
/// 视图层错误到 v2 错误码的映射（复用 journal 的同一套码表）。
///
/// 产品面读法（journal_views）产生的错误必须与写面同码，否则客户端要
/// 为读和写维护两套分支。
fn view_failure(error: flow_backend::ViewError) -> ErrorObjectOwned {
    failure(error.into())
}
/// 存储层错误到 v2 错误码的映射。
///
/// 只有「响应过大」是客户端可归因的（`-32021`）；其余存储故障一律
/// `-32603`，因为它们对客户端而言不可区分也不可自愈。
fn storage_failure(error: flow_backend::StorageError) -> ErrorObjectOwned {
    if matches!(error, flow_backend::StorageError::ResponseTooLarge) {
        ErrorObjectOwned::owned(-32021, "RESPONSE_TOO_LARGE", None::<()>)
    } else {
        ErrorObjectOwned::owned(-32603, "projection unavailable", None::<()>)
    }
}
/// 解析十进制字符串形态的序号参数（如 from_seq / from_lsn）。
///
/// 必须是**规范**十进制字符串：无前导零、无正负号。JSON 数字会在 u64
/// 边界上丢精度，且不同客户端对大整数的表示不一（字符串/数字），规范化
/// 才能让「同一个序号」在链上唯一。缺失按 0 处理——0 是「从头开始」的
/// 无害默认值。
fn decimal_param(p: &Value, key: &str) -> Result<u64, ErrorObjectOwned> {
    match p.get(key) {
        None => Ok(0),
        Some(Value::String(s))
            if !s.is_empty()
                && (s == "0" || !s.starts_with('0'))
                && s.bytes().all(|b| b.is_ascii_digit()) =>
        {
            s.parse().map_err(invalid)
        }
        _ => Err(invalid(format!("{key} must be a decimal string"))),
    }
}
/// 取必填的字符串字段。
///
/// 错误文案 "missing string {key}" 是 wire 契约的一部分，不能改写。
fn text<'a>(params: &'a Value, key: &str) -> Result<&'a str, ErrorObjectOwned> {
    params[key]
        .as_str()
        .ok_or_else(|| invalid(format!("missing string {key}")))
}
/// 取可选的版本号（缺省 = None = 由服务端按规则选版本）。
///
/// 字符串与数字都接受：CLI 与前端传法不同，但都要落在同一条版本语义上。
fn optional_version(params: &Value) -> Result<Option<u64>, ErrorObjectOwned> {
    match params.get("version") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(v)) => v.parse().map(Some).map_err(invalid),
        Some(v) => v
            .as_u64()
            .map(Some)
            .ok_or_else(|| invalid("invalid version")),
    }
}

// ---- 写命令的边缘解析：参数一次 struct 化，键名只在 parse 里出现一次 ----
// 错误文本与旧字段级获取器逐字相同（"missing string {key}" 是 wire 契约）。

/// workflow.create 的参数视图。
struct WorkflowCreateReq<'a> {
    name: &'a str,
}
impl<'a> WorkflowCreateReq<'a> {
    /// 从原始参数解析出本命令需要的字段。
    ///
    /// # Errors
    /// 缺字段或类型不符时返回 `-32602`。
    fn parse(p: &'a Value) -> Result<Self, ErrorObjectOwned> {
        Ok(Self {
            name: text(p, "name")?,
        })
    }
}

/// workflow.update 的参数视图。
struct WorkflowUpdateReq<'a> {
    workflow_id: &'a str,
    definition: Value,
}
impl<'a> WorkflowUpdateReq<'a> {
    /// 从原始参数解析出本命令需要的字段。
    ///
    /// # Errors
    /// 缺 workflow_id 时返回 `-32602`；definition 缺失时取 JSON null，由
    /// 后端判定其是否合法。
    fn parse(p: &'a Value) -> Result<Self, ErrorObjectOwned> {
        Ok(Self {
            workflow_id: text(p, "workflow_id")?,
            definition: p["definition"].clone(),
        })
    }
}

/// workflow.publish 的参数视图。
struct WorkflowPublishReq<'a> {
    workflow_id: &'a str,
    version: u64,
}
impl<'a> WorkflowPublishReq<'a> {
    /// 从原始参数解析出本命令需要的字段。
    ///
    /// # Errors
    /// 缺 workflow_id 或缺 version 时返回 `-32602`——发布必须点名版本，
    /// 不能沿用「当前版本」，否则发布对象会随并发改动而漂移。
    fn parse(p: &'a Value) -> Result<Self, ErrorObjectOwned> {
        Ok(Self {
            workflow_id: text(p, "workflow_id")?,
            version: optional_version(p)?.ok_or_else(|| invalid("version required"))?,
        })
    }
}

/// workflow.delete 的参数视图。
struct WorkflowDeleteReq<'a> {
    workflow_id: &'a str,
}
impl<'a> WorkflowDeleteReq<'a> {
    /// 从原始参数解析出本命令需要的字段。
    ///
    /// # Errors
    /// 缺 workflow_id 时返回 `-32602`。
    fn parse(p: &'a Value) -> Result<Self, ErrorObjectOwned> {
        Ok(Self {
            workflow_id: text(p, "workflow_id")?,
        })
    }
}

/// run.start 的参数视图。
struct RunStartReq<'a> {
    workflow_id: &'a str,
    version: Option<u64>,
    input: Value,
}
impl<'a> RunStartReq<'a> {
    /// 从原始参数解析出本命令需要的字段。
    ///
    /// # Errors
    /// 缺 workflow_id 时返回 `-32602`；version 缺省表示「按发布规则选」。
    fn parse(p: &'a Value) -> Result<Self, ErrorObjectOwned> {
        Ok(Self {
            workflow_id: text(p, "workflow_id")?,
            version: optional_version(p)?,
            input: p["input"].clone(),
        })
    }
}

/// run.cancel 的参数视图。
struct RunCancelReq<'a> {
    run_id: &'a str,
}
impl<'a> RunCancelReq<'a> {
    /// 从原始参数解析出本命令需要的字段。
    ///
    /// # Errors
    /// 缺 run_id 时返回 `-32602`。
    fn parse(p: &'a Value) -> Result<Self, ErrorObjectOwned> {
        Ok(Self {
            run_id: text(p, "run_id")?,
        })
    }
}

/// run.signal 的参数视图。
struct RunSignalReq<'a> {
    run_id: &'a str,
    node_id: &'a str,
    payload: Value,
}
impl<'a> RunSignalReq<'a> {
    /// 从原始参数解析出本命令需要的字段。
    ///
    /// # Errors
    /// 缺 run_id 或 node_id 时返回 `-32602`。
    fn parse(p: &'a Value) -> Result<Self, ErrorObjectOwned> {
        Ok(Self {
            run_id: text(p, "run_id")?,
            node_id: text(p, "node_id")?,
            payload: p["payload"].clone(),
        })
    }
}

/// run.adjudicate 的参数视图。
struct RunAdjudicateReq<'a> {
    run_id: &'a str,
    node_id: &'a str,
    operation_id: &'a str,
    reason: &'a str,
    output: Value,
    decision: &'a str,
}
impl<'a> RunAdjudicateReq<'a> {
    /// 从原始参数解析出本命令需要的字段。
    ///
    /// decision 缺省为 `accept_output`：裁决者不表态时按「接受已有输出」
    /// 处理，比留空让 run 永久悬挂更安全。
    ///
    /// # Errors
    /// 缺 run_id / node_id / operation_id / reason 时返回 `-32602`。
    fn parse(p: &'a Value) -> Result<Self, ErrorObjectOwned> {
        Ok(Self {
            run_id: text(p, "run_id")?,
            node_id: text(p, "node_id")?,
            operation_id: text(p, "operation_id")?,
            reason: text(p, "reason")?,
            output: p["output"].clone(),
            decision: p["decision"].as_str().unwrap_or("accept_output"),
        })
    }
}

/// schedule.change / webhook.change 的参数视图。
struct ConfigChangeReq<'a> {
    key: &'a str,
    patch: Value,
}
impl<'a> ConfigChangeReq<'a> {
    /// 从原始参数解析出本命令需要的字段。
    ///
    /// # Errors
    /// 缺 key 时返回 `-32602`。
    fn parse(p: &'a Value) -> Result<Self, ErrorObjectOwned> {
        Ok(Self {
            key: text(p, "key")?,
            patch: p["patch"].clone(),
        })
    }
}

/// command.status 的参数视图。
struct CommandStatusReq<'a> {
    scope: &'a str,
    request_id: &'a str,
}
impl<'a> CommandStatusReq<'a> {
    /// 从原始参数解析出本命令需要的字段。
    ///
    /// # Errors
    /// 缺 scope 或 request_id 时返回 `-32602`——回执查询必须同时给出
    /// 幂等作用域与幂等键，否则无法定位到那条命令。
    fn parse(p: &'a Value) -> Result<Self, ErrorObjectOwned> {
        Ok(Self {
            scope: text(p, "scope")?,
            request_id: text(p, "request_id")?,
        })
    }
}

use flow_dto::rpc::Method;
pub use flow_dto::rpc::{is_read_method, is_write_method, READ_METHODS, WRITE_METHODS};

/// 写命令的服务端命令 scope（`command.status` 查询键）。run.start 带来源
/// 归因、run 族按 run_id 分域；其余就是方法名。
///
/// 服务端自己不用它做去重（去重键是 request_id），它只是把「方法 + 参数」
/// 映射成一个**可重放的幂等作用域**：客户端回复丢失后用同一个 scope 重发，
/// 服务端才能认出「这是同一次写」。scope 规则必须与服务端侧一致，否则
/// 客户端查回执会查错作用域。
pub fn command_scope(method: &str, params: &Value) -> String {
    flow_dto::command_scope(method, params)
}

/// 开发形态模块（不含产品装配）。
///
/// 存在的意义是「能跑起来的最小面」：非产品嵌入（如自测工具）只需要 journal
/// 能力，不需要配置/密钥。
///
/// # Errors
/// token 短于 32 字节时返回 Err。
pub fn module(
    backend: Arc<JournalBackend>,
    token: String,
) -> Result<RpcModule<Context>, Box<dyn std::error::Error>> {
    module_product(backend, token, None)
}

/// 产品形态：装配统一配置面和持久化密钥（journal-server / 桌面内嵌用）。
///
/// 方法注册**只做一次**：遍历 `Method::ALL` 逐个注册到同一个分发器，
/// 于是「注册表」「准入校验」「分发 match」三处共用同一份方法目录——任何
/// 新增方法漏改一处，症状都会是「注册了却调不通」或「调得通却没注册」。
///
/// # Errors
/// token 短于 32 字节时返回 Err（服务起不来，好过用弱 token 开放）。
pub fn module_product(
    backend: Arc<JournalBackend>,
    token: String,
    product: Option<Arc<crate::AppState>>,
) -> Result<RpcModule<Context>, Box<dyn std::error::Error>> {
    module_product_with_keys(backend, token, product, Vec::new())
}

/// 同 [`module_product`]，但带 `[[api_keys]]`（09 §9 分档鉴权）。
pub fn module_product_with_keys(
    backend: Arc<JournalBackend>,
    token: String,
    product: Option<Arc<crate::AppState>>,
    api_keys: Vec<flow_config::ApiKey>,
) -> Result<RpcModule<Context>, Box<dyn std::error::Error>> {
    if token.len() < 32 {
        return Err("FLOW_JOURNAL_TOKEN must contain at least 32 bytes".into());
    }
    for key in &api_keys {
        key.validate()
            .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
    }
    let mut module = RpcModule::new(Context {
        backend,
        token,
        api_keys,
        product,
    });
    for &method in Method::ALL {
        module.register_async_method(method.as_str(), move |params, ctx, _| async move {
            dispatch_call(&ctx, method, params).await
        })?;
    }
    // 订阅是唯一的有状态长连接路径：单独注册（jsonrpsee 的订阅 API 与
    // 普通方法不同），但鉴权口径与普通方法完全一致。
    module.register_subscription(
        "run.subscribe",
        "run.event",
        "run.unsubscribe",
        |params, pending, ctx, _| async move { subscribe_run_events(params, pending, &ctx).await },
    )?;
    module.register_subscription(
        "services.subscribe", "services.event", "services.unsubscribe",
        |params,pending,ctx,_|async move {subscribe_service_events(params,pending,&ctx).await},
    )?;
    Ok(module)
}

/// 全量 token 常时比较（不在第一个差异字节提前退出）。
///
/// 长度不等也要走完比较：直接按长度拒绝会泄漏「长度对不对」，让攻击者
/// 能把搜索空间从「猜对 32 字节」缩到「猜对长度」。
fn constant_time_eq(supplied: &str, expected: &str) -> bool {
    supplied.len() == expected.len()
        && supplied
            .bytes()
            .zip(expected.bytes())
            .fold(0u8, |diff, (a, b)| diff | (a ^ b))
            == 0
}

/// 统一前置层：token 校验 + `_token` 剥离 + request_id 幂等键检查。
/// 通过后返回 `(归一参数, request_id)`。
///
/// 顺序是有意为之：**先鉴权再碰业务参数**。否则未鉴权的调用可以用错误
/// 响应区分「参数不合法」与「不存在」，等价于把对象存在性泄露给未认证方。
///
/// # Errors
/// token 不符返回 `-32001`；参数不是对象或 request_id 类型错返回 `-32602`；
/// 写方法缺 request_id 返回 `-32602`。
async fn admit_call(
    ctx: &Context,
    method: &str,
    params: jsonrpsee::types::Params<'_>,
) -> Result<(Value, Option<String>, Caller), ErrorObjectOwned> {
    // JSON-RPC 允许整体省略 params（到达的是 null）——归一为空对象。
    let mut p: Value = match params.parse::<Value>() {
        Ok(Value::Null) => json!({}),
        Ok(v) => v,
        Err(e) => return Err(e),
    };
    let supplied = p.get("_token").and_then(Value::as_str).unwrap_or("");
    let caller = resolve_caller(ctx, method, supplied)?;
    // 令牌在鉴权后从参数里摘掉：它不该进入命令指纹（幂等去重），更不该
    // 被回显给客户端。
    p.as_object_mut()
        .ok_or_else(|| invalid("object params required"))?
        .remove("_token");
    let request_id = p
        .get("request_id")
        .map(|v| {
            v.as_str()
                .map(str::to_owned)
                .ok_or_else(|| invalid("request_id must be a string"))
        })
        .transpose()?;
    let request = request_id.as_deref();
    // 写命令必须带稳定 request_id：无幂等键的客户端重试=双 run/双
    // workflow（服务端无 UUID 透传键可去重）。读面免检。
    if is_write_method(method) && request.is_none_or(|id| id.trim().is_empty()) {
        return Err(invalid("request_id required for write commands"));
    }
    Ok((p, request_id, caller))
}

/// 单条方法分发：cluster 族与读面直接返回；写命令产出回执。
///
/// 分发是**穷尽**的：`Method` 枚举的每个变体都必须在这里落到一个分支，
/// 编译器替我们守着「新增方法忘了实现」。读面直接返回自己的 JSON，写面
/// 统一返回 `CommandReceipt`——客户端因此可以只写一套回执处理逻辑。
///
/// # Errors
/// 前置层与各分支的错误按 v2 错误码词汇返回（`-32001` / `-32011` /
/// `-32012` / `-32021` / `-32602` / `-32603`）。
async fn dispatch_call(
    ctx: &Context,
    method: Method,
    params: jsonrpsee::types::Params<'_>,
) -> Result<Value, ErrorObjectOwned> {
    let (p, request_id, caller) = admit_call(ctx, method.as_str(), params).await?;
    let request = request_id.as_deref();
    let b = &ctx.backend;
    let credentials = if matches!(
        method,
        Method::WorkflowImport | Method::WorkflowUpdate | Method::WorkflowValidate
    ) {
        match p
            .get("environment_id")
            .and_then(Value::as_str)
            .filter(|id| *id != "default")
        {
            Some(id) => {
                let store = b
                    .environments
                    .get()
                    .ok_or_else(|| invalid("environment store unavailable"))?;
                Some(
                    store
                        .credentials(&store.bind(id).map_err(invalid)?)
                        .map_err(invalid)?,
                )
            }
            None => None,
        }
    } else {
        None
    };
    let dispatch = dispatch_admitted(ctx, method, p, request, caller);
    match credentials {
        Some(values) => {
            flow_backend::secrets::ENVIRONMENT_CREDENTIALS
                .scope(values, dispatch)
                .await
        }
        None => dispatch.await,
    }
}

async fn dispatch_admitted(
    ctx: &Context,
    method: Method,
    p: Value,
    request: Option<&str>,
    caller: Caller,
) -> Result<Value, ErrorObjectOwned> {
    let b = &ctx.backend;
    let receipt = match method {
        // ---- 观测 / 读面（直接返回，不走回执解包） ----
        Method::RunObservationsPage => return observations_page(b, &p).await,
        Method::CommandStatus => {
            let req = CommandStatusReq::parse(&p)?;
            return serde_json::to_value(
                b.command_status(req.scope, req.request_id)
                    .await
                    .map_err(failure)?,
            )
            .map_err(invalid);
        }
        Method::WorkflowList | Method::RunList | Method::LegacyList => {
            return snapshot_list(b, method.as_str(), &p).await
        }
        Method::WorkflowGet | Method::RunGet | Method::LegacyGet => {
            return snapshot_get(b, method.as_str(), &p).await
        }
        Method::RunEventsPage | Method::RunAuditPage => {
            return events_page(b, method.as_str(), &p).await
        }
        Method::PermissionsAuditPage => {
            // 数据集级权限审计（09 §10.3）：run_id 缺省 = 全部 run 的权限事实，
            // 按 decision/actor 过滤，服务端逐页有界。
            return permissions_audit_page(b, &p).await;
        }

        // ---- 写命令（CommandReceipt 回执面）：边缘一次 struct 化 ----
        Method::WorkflowValidate => {
            return b
                .workflow_validate(p["definition"].clone())
                .map_err(failure)
        }
        Method::WorkflowImport => {
            let publish = p
                .get("publish")
                .map(|v| {
                    v.as_bool()
                        .ok_or_else(|| invalid("publish must be boolean"))
                })
                .transpose()?
                // 缺省即发布：导入一份 workflow 的常见意图就是「导入并上线」，
                // 要求调用方额外传 publish=true 会让默认路径多一次往返。
                .unwrap_or(true);
            b.workflow_import_as(
                text(&p, "name")?,
                p["definition"].clone(),
                publish,
                request,
                Some(&flow_backend::WriteActor {
                    principal: &caller.principal,
                    permission_admin: caller.permission_admin,
                }),
            )
            .await
        }
        Method::WorkflowCreate => {
            let req = WorkflowCreateReq::parse(&p)?;
            b.workflow_create(req.name, request).await
        }
        Method::WorkflowUpdate => {
            let req = WorkflowUpdateReq::parse(&p)?;
            b.workflow_update_as(
                req.workflow_id,
                req.definition,
                request,
                Some(&flow_backend::WriteActor {
                    principal: &caller.principal,
                    permission_admin: caller.permission_admin,
                }),
            )
            .await
        }
        Method::WorkflowPublish => {
            let req = WorkflowPublishReq::parse(&p)?;
            b.workflow_publish(req.workflow_id, req.version, request)
                .await
        }
        Method::WorkflowDelete => {
            let req = WorkflowDeleteReq::parse(&p)?;
            b.workflow_delete(req.workflow_id, request).await
        }
        Method::RunStart => {
            let req = RunStartReq::parse(&p)?;
            // 来源写死为 "manual"：RPC 触发的 run 都是人工发起，回归跑
            // 另有 run.regress 通道并在后端标注来源。
            b.run_start_in_environment(
                req.workflow_id,
                req.version,
                req.input,
                request,
                p.get("environment_id")
                    .map(|v| {
                        v.as_str()
                            .ok_or_else(|| invalid("environment_id must be a string"))
                    })
                    .transpose()?,
            )
            .await
        }
        Method::RunRegress => {
            b.run_regression_with_fixtures(
                text(&p, "baseline")?,
                p["workflow_id"].as_str(),
                optional_version(&p)?,
                p.get("fixtures")
                    .cloned()
                    .map(serde_json::from_value)
                    .transpose()
                    .map_err(invalid)?
                    .unwrap_or_default(),
                request.ok_or_else(|| invalid("request_id required"))?,
            )
            .await
        }
        Method::RunCompare => {
            return b
                .compare_runs(text(&p, "baseline")?, text(&p, "candidate")?)
                .await
                .map_err(failure)
        }
        Method::RunCancel => {
            let req = RunCancelReq::parse(&p)?;
            b.run_cancel(req.run_id, request).await
        }
        Method::RunSignal => {
            let req = RunSignalReq::parse(&p)?;
            b.run_signal(req.run_id, req.node_id, req.payload, request)
                .await
        }
        Method::RunAdjudicate => {
            let req = RunAdjudicateReq::parse(&p)?;
            b.run_adjudicate(
                req.run_id,
                req.node_id,
                req.operation_id,
                req.reason,
                req.output,
                req.decision,
                request.ok_or_else(|| invalid("request_id required"))?,
            )
            .await
        }
        Method::ScheduleChange | Method::WebhookChange => {
            // 事件种类与方法一一对应：journal 里「什么被改了」是事实，
            // 不能由调用方自由声明。
            let kind = if method == Method::ScheduleChange {
                flow_journal::EventKind::ScheduleChanged
            } else {
                flow_journal::EventKind::WebhookChanged
            };
            let req = ConfigChangeReq::parse(&p)?;
            b.config_change(method.as_str(), kind, req.key, req.patch, request)
                .await
        }
        // 这批方法共用一条产品命令通道：它们的参数形状差异由后端
        // ProductCommand::parse 消解，服务端不再逐个手写分支。
        Method::ScheduleCreate
        | Method::ScheduleUpdate
        | Method::ScheduleDelete
        | Method::WebhookCreate
        | Method::WebhookSetEnabled
        | Method::WebhookDelete
        | Method::TemplateCreate
        | Method::TemplateUpdate
        | Method::TemplateDelete => return product_command_route(ctx, method.as_str(), &p).await,

        // ---- 产品面：V2 product views（journal_views 物化，直接返回） ----
        Method::WorkflowListView => return workflow_list_view(b).await,
        Method::WorkflowGetView => return workflow_get_view(b, &p).await,
        Method::WorkflowVersions => return workflow_versions(b, &p).await,
        Method::RunGetView => return run_get_view(b, &p).await,
        Method::RunListView => return run_list_view(b, &p).await,
        Method::RunStats => {
            let stats = flow_backend::journal_views::run_stats(b, p["workflow_id"].as_str())
                .await
                .map_err(view_failure)?;
            return serde_json::to_value(stats).map_err(invalid);
        }
        Method::RunTimeline => {
            return flow_backend::journal_views::timeline(b, text(&p, "run_id")?)
                .await
                .map_err(view_failure);
        }
        Method::RunEventsView => return events_view(b, &p).await,
        Method::ScheduleList => return schedule_list(b, &p).await,
        Method::WebhookList => {
            let webhooks = flow_backend::journal_views::list_webhooks(b, p["workflow_id"].as_str())
                .await
                .map_err(view_failure)?;
            return serde_json::to_value(json!({"webhooks": webhooks})).map_err(invalid);
        }
        Method::TemplateList => {
            let templates = flow_backend::journal_views::template_list(b)
                .await
                .map_err(view_failure)?;
            return Ok(json!({"templates": templates}));
        }
        Method::TemplateGet => {
            let template = flow_backend::journal_views::template_get(b, text(&p, "id")?)
                .await
                .map_err(view_failure)?;
            return serde_json::to_value(template).map_err(invalid);
        }
        Method::EnvironmentsList => {
            let store = b
                .environments
                .get()
                .ok_or_else(|| invalid("environment store unavailable"))?;
            return Ok(json!({"environments": store.list()}));
        }
        Method::EnvironmentsDelete => {
            b.delete_environment(
                text(&p, "id")?,
                p["revision"]
                    .as_u64()
                    .ok_or_else(|| invalid("revision required"))?,
            )
            .await
            .map_err(failure)?;
            return Ok(json!({"deleted":true}));
        }
        Method::EnvironmentsSave => {
            let store = b
                .environments
                .get()
                .ok_or_else(|| invalid("environment store unavailable"))?;
            let secret = p
                .get("secret")
                .map(|s| -> Result<_, ErrorObjectOwned> {
                    let value = match s.get("value") {
                        None | Some(Value::Null) => None,
                        Some(value) => Some(
                            value
                                .as_str()
                                .ok_or_else(|| invalid("secret value must be a string"))?,
                        ),
                    };
                    Ok((text(s, "name")?, value))
                })
                .transpose()?;
            let binding = store
                .save(
                    text(&p, "id")?,
                    text(&p, "name")?,
                    p.get("config")
                        .cloned()
                        .ok_or_else(|| invalid("config required"))?,
                    secret,
                    p["revision"]
                        .as_u64()
                        .ok_or_else(|| invalid("revision required"))?,
                )
                .map_err(invalid)?;
            return serde_json::to_value(binding).map_err(invalid);
        }
        Method::ConfigGet => {
            let product = ctx_product(ctx)?;
            return Ok(crate::config_view(&product));
        }
        Method::ConfigUpdate => return config_update(ctx, &p).await,
        Method::SecretsList => {
            if let Some(id) = p
                .get("environment_id")
                .and_then(Value::as_str)
                .filter(|id| *id != "default")
            {
                let store = b
                    .environments
                    .get()
                    .ok_or_else(|| invalid("environment store unavailable"))?;
                let binding = store.bind(id).map_err(invalid)?;
                let secrets: Vec<_> = store
                    .credentials(&binding)
                    .map_err(invalid)?
                    .keys()
                    .map(|name| json!({"name":name,"source":"environment"}))
                    .collect();
                return Ok(json!({"secrets":secrets}));
            }
            let secrets: Vec<Value> = flow_backend::secrets::list_secrets()
                .into_iter()
                .map(|(name, source)| json!({"name": name, "source": source}))
                .collect();
            return Ok(json!({"secrets": secrets}));
        }
        Method::SecretsSet => return secrets_set(ctx, &p).await,
        Method::SecretsDelete => return secrets_delete(ctx, &p).await,
        Method::NodetypesList => {
            return Ok(json!({"node_types": crate::node_types()}));
        }
        Method::ServicesList => {
            let services = b.services.lock().await;
            let ui=b.service_ui.lock().await;
            return Ok(json!({"services":services.iter().map(|(id,s)|json!({"plugin_id":id,"instance":s.instance,"alive":s.alive(),"binding":s.binding.get().map(|(run_id,node_id,_)|json!({"run_id":run_id,"node_id":node_id})),"ui":ui.iter().find(|(_, (instance,_))|instance==&s.instance).map(|(key,_)|key)})).collect::<Vec<_>>()}));
        }
        Method::ServicesEvents => {
            if !caller.permission_admin {return Err(invalid("service events require admin"));}
            let services = b.services.lock().await;
            let service = services.get(text(&p,"plugin_id")?).ok_or_else(||invalid("service unavailable"))?.clone();
            drop(services);
            return service.events_since(text(&p,"instance")?, text(&p,"after")?.parse().map_err(invalid)?).map_err(invalid);
        }
        Method::ServicesStart => {
            if !caller.permission_admin {return Err(invalid("service start requires admin"));}
            let id = text(&p,"plugin_id")?;
            let mut services = b.services.lock().await;
            if !b.accepts_services(){return Err(invalid("host is shutting down"));}
            if let Some(service) = services.get(id).filter(|s|s.alive()) {
                if p.get("binding").is_some(){return Err(invalid("service already running; stop before rebinding"));}
                return Ok(json!({"instance":service.instance}));
            }
            if services.len() >= 32 && !services.contains_key(id) {return Err(invalid("service limit reached"));}
            let plugin=flow_backend::plugin_registry::discover_plugins().into_iter().find(|p|p.manifest.plugin.id==id).ok_or_else(||invalid("plugin not installed"))?;
            let declaration=plugin.manifest.service.ok_or_else(||invalid("plugin has no service role"))?;
            let html=if let Some(path)=&declaration.web_ui {
                let root=plugin.source.canonicalize().map_err(invalid)?;
                let path=root.join(path).canonicalize().map_err(invalid)?;
                if !path.starts_with(&root){return Err(invalid("web-ui path escapes plugin"));}
                let metadata=std::fs::metadata(&path).map_err(invalid)?;
                if !metadata.is_file() || metadata.len()>262144 {return Err(invalid("web-ui must be a single HTML file under 256 KiB"));}
                use std::io::Read;
                let mut html=String::new();
                std::fs::File::open(path).map_err(invalid)?.take(262145).read_to_string(&mut html).map_err(invalid)?;
                if html.len()>262144 {return Err(invalid("web-ui exceeds 256 KiB"));}
                Some(html)
            }else{None};
            let entry=std::path::PathBuf::from(&plugin.manifest.plugin.entry);
            let entry=if entry.is_absolute(){entry}else{plugin.source.join(entry)};
            let binding=if let Some(value)=p.get("binding") {
                let run_id=text(value,"run_id")?.to_owned();let node_id=text(value,"node_id")?.to_owned();
                let run=b.run_snapshot(&run_id).await.map_err(failure)?.ok_or_else(||invalid("binding run not found"))?;
                if run.terminal(){return Err(invalid("binding run is terminal"));}
                let node=run.nodes.get(&node_id).ok_or_else(||invalid("binding node not active"))?;
                Some((run_id,node_id,node.dispatch_id.clone()))
            }else{None};
            let public_binding=binding.clone();
            let host=flow_backend::service_host::handler(b,id.to_owned(),declaration.peers,binding);
            let service_slot=Arc::new(std::sync::OnceLock::new());
            let host=flow_backend::service_host::with_subscriptions(host,service_slot.clone());
            let service=flow_backend::resident_service::ResidentService::start_installed_with_host(entry,declaration.methods,host).await.map_err(invalid)?;
            if let Some(binding)=public_binding {let _=service.binding.set(binding);}
            let _=service_slot.set(Arc::downgrade(&service));
            service.notify_ready().map_err(invalid)?;
            let instance=service.instance.clone();
            let mut ui=b.service_ui.lock().await;
            if let Some(previous)=services.get(id){ui.retain(|_,(i,_)|i!=&previous.instance);}
            if let Some(html)=html {
                // An unguessable read-only resource handle, never an RPC credential.
                let key=format!("{}-{}",instance,service.resource_token());
                ui.insert(key,(instance.clone(),html));
            }
            drop(ui);
            if let Some(previous)=services.insert(id.into(),service){previous.shutdown().await;}
            return Ok(json!({"instance":instance}));
        }
        Method::ServicesStop => {
            if !caller.permission_admin {return Err(invalid("service stop requires admin"));}
            let mut services=b.services.lock().await;
            let id=text(&p,"plugin_id")?;
            let service=services.get(id).ok_or_else(||invalid("service unavailable"))?;
            if service.instance!=text(&p,"instance")? {return Err(invalid("stale service instance"));}
            let service=services.remove(id).unwrap();drop(services);
            b.service_ui.lock().await.retain(|_,(i,_)|i!=&service.instance);
            service.shutdown().await;
            return Ok(json!({"stopped":true}));
        }
        Method::ServicesCall => {
            if !caller.permission_admin {return Err(invalid("service calls require admin until per-service grants are configured"));}
            let service=b.services.lock().await.get(text(&p,"plugin_id")?).ok_or_else(||invalid("service unavailable"))?.clone();
            let value=service.call(text(&p,"instance")?,text(&p,"method")?,p["params"].clone()).await.map_err(invalid)?;
            return Ok(json!({"value":value}));
        }
        Method::PluginsList => {
            return Ok(json!({"plugins": b.plugins_view().await}));
        }
        Method::PluginsRecheck => return plugins_recheck(ctx).await,
        // ---- 09 权限模型 ----
        Method::RunApprove | Method::RunDeny => {
            let allow = method == Method::RunApprove;
            let scope = match p.get("scope").and_then(Value::as_str) {
                None | Some("once") => flow_dto::permission::ApprovalScope::Once,
                Some("run") => flow_dto::permission::ApprovalScope::Run,
                Some(other) => {
                    return Err(invalid(format!("scope must be once | run, got {other}")))
                }
            };
            b.run_decide(
                flow_backend::journal_permissions::ApprovalCommand {
                    run_id: text(&p, "run_id")?,
                    node_id: text(&p, "node_id")?,
                    operation_id: text(&p, "operation_id")?,
                    request_hash: text(&p, "request_hash")?,
                    allow,
                    scope,
                    reason: p.get("reason").and_then(Value::as_str),
                    by: caller.principal.clone(),
                },
                request.ok_or_else(|| invalid("request_id required"))?,
            )
            .await
        }
        Method::PermissionsGrant => {
            let capability_ids = match p.get("capability_ids") {
                None | Some(Value::Null) => None,
                Some(Value::Array(items)) => Some(
                    items
                        .iter()
                        .map(|item| {
                            item.as_str()
                                .map(str::to_string)
                                .ok_or_else(|| invalid("capability_ids must be strings"))
                        })
                        .collect::<std::result::Result<Vec<_>, _>>()?,
                ),
                Some(_) => return Err(invalid("capability_ids must be an array")),
            };
            b.plugin_grant(
                text(&p, "plugin_id")?,
                capability_ids,
                p.get("legacy_confirm")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                request.ok_or_else(|| invalid("request_id required"))?,
            )
            .await
        }
        Method::RunApprovalsRevoke => {
            b.run_approval_revoke(
                text(&p, "run_id")?,
                text(&p, "node_id")?,
                text(&p, "digest")?,
                caller.principal.clone(),
                request.ok_or_else(|| invalid("request_id required"))?,
            )
            .await
        }
        Method::ApprovalsList => {
            return Ok(b
                .approvals_view(
                    p.get("run_id").and_then(Value::as_str),
                    p.get("all").and_then(Value::as_bool).unwrap_or(false),
                )
                .await)
        }
        Method::PermissionsEffective => {
            return b
                .permissions_effective_view(
                    p.get("workflow_id").and_then(Value::as_str),
                    optional_version(&p)?,
                    p.get("node_id").and_then(Value::as_str),
                    p.get("run_id").and_then(Value::as_str),
                )
                .await
                .map_err(failure)
        }
    }
    // 写命令的共同出口：回执统一在这里转成 JSON，错误统一在这里按
    // journal 错误类型分层——读面各自 return，不经过这条路径。
    .map_err(failure)?;
    serde_json::to_value(receipt).map_err(invalid)
}

/// 插件升级重校验：全部已发布工作流按**当前**已部署 manifest 重验。
///
/// 返回违规清单（工作流/版本/节点/原因）。发布期校验只覆盖发布时刻；
/// 插件升级收紧闭集后，存量定义可能已非法——这个方法把漂移显式化，
/// 不等它在凌晨三点的 run 里爆。
async fn plugins_recheck(ctx: &Context) -> Result<Value, ErrorObjectOwned> {
    let dynamic = flow_backend::plugin_registry::dynamic_node_types();
    let forms = flow_backend::plugin_registry::dynamic_params_forms();
    let b = &ctx.backend;
    let workflows = flow_backend::journal_views::list_workflows(b)
        .await
        .map_err(view_failure)?;
    let mut violations: Vec<Value> = Vec::new();
    for summary in workflows {
        let workflow_id = summary.workflow_id.clone();
        let latest = summary.latest_version;
        for version in 1..=latest {
            let Ok(version_value) =
                flow_backend::journal_views::get_version(b, &workflow_id, Some(version)).await
            else {
                continue;
            };
            let nodes = version_value.definition["nodes"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            for node_value in nodes {
                let Ok(node) = serde_json::from_value::<flow_engine::Node>(node_value) else {
                    continue;
                };
                if dynamic.contains(&node.node_type) {
                    if let Err(reason) =
                        flow_backend::plugin_registry::validate_dynamic_params(&node, &forms)
                    {
                        violations.push(json!({
                            "workflow_id": workflow_id,
                            "version": version,
                            "node_id": node.id,
                            "node_type": node.node_type,
                            "reason": reason,
                        }));
                    }
                } else if flow_engine::NodeType::parse(&node.node_type).is_none() {
                    violations.push(json!({
                        "workflow_id": workflow_id,
                        "version": version,
                        "node_id": node.id,
                        "node_type": node.node_type,
                        "reason": "节点类型的插件已不存在（manifest 下架）",
                    }));
                }
            }
        }
    }
    Ok(json!({"violations": violations}))
}

/// 观测分页：run 存在性以 projection 为准；store 未启用时返回空页 + 丢失
/// 标记（history_incomplete）。
///
/// 「丢失」必须显式出现在响应里而不是伪装成空列表：客户端据此知道这段
/// 历史不可信，不能拿它做完整性判断。
async fn observations_page(b: &JournalBackend, p: &Value) -> Result<Value, ErrorObjectOwned> {
    let run_id = text(p, "run_id")?.to_owned();
    // 存在性以投影为准：投影是「这个 run 被系统承认过」的权威判据。
    if b.store
        .get_summary("run", &run_id)
        .await
        .map_err(invalid)?
        .1
        .is_none()
    {
        return Err(ErrorObjectOwned::owned(-32011, "run not found", None::<()>));
    }
    // 观测存储未启用（可选组件）时返回空页 + 丢失标记，而不是报错：
    // 客户端要能继续用同一个会话读其他方法。
    let Some(store) = b.observations.clone() else {
        return Ok(json!({
            "records": [],
            "loss": {"history_incomplete": true},
            "loss_scope": "store",
        }));
    };
    let from = decimal_param(p, "from_seq")?;
    let dispatch = p["dispatch_id"].as_str().map(str::to_owned);
    // 上限 256：单页过大既拖慢投影查询，也让前端一次要处理太多记录。
    let limit = p["limit"].as_u64().unwrap_or(100).min(256) as usize;
    // 磁盘扫描类操作放到阻塞线程池，避免占住异步 worker 把整个 RPC 面
    // 卡住。
    tokio::task::spawn_blocking(move || {
        let records = store
            .page(&run_id, dispatch.as_deref(), from, limit)
            .map_err(invalid)?;
        Ok(json!({
            "records": records,
            // 两级丢失范围分开报：dispatch 级（某次派发内）与 run 级
            // （整条 run）的丢失含义不同，客户端处理方式也不同。
            "loss": store.scoped_loss(&run_id, dispatch.as_deref()),
            "loss_scope": if dispatch.is_some() { "dispatch" } else { "run" },
            "store_loss": store.loss(),
        }))
    })
    .await
    .map_err(invalid)?
}

/// workflow.list / run.list / legacy.list：projection 快照 + 游标。
///
/// 响应带 `snapshot_cursor`（journal_id + lsn）：客户端据此判断自己看到的
/// 是不是同一份数据，跨 journal 的游标不能混用。
async fn snapshot_list(
    b: &JournalBackend,
    method: &str,
    p: &Value,
) -> Result<Value, ErrorObjectOwned> {
    // 实体类别直接取方法名的首段：三个方法共享同一套快照语义。
    let kind = method.split('.').next().unwrap();
    let after = p["after"].as_str().unwrap_or("");
    let (lsn, values) = b
        .store
        .list(
            kind,
            after,
            // 页大小上限 256：快照读会一次性物化整页，过大的页会拖慢投影。
            p["limit"].as_u64().unwrap_or(64).min(256) as usize,
        )
        .await
        .map_err(storage_failure)?;
    Ok(json!({
        "snapshot_cursor": {"journal_id": b.journal.id(), "lsn": lsn.to_string()},
        "values": values,
    }))
}

/// workflow.get / run.get / legacy.get：单键快照。
///
/// 键名按实体类别决定（run_id / key / workflow_id），但错误语义一致。
async fn snapshot_get(
    b: &JournalBackend,
    method: &str,
    p: &Value,
) -> Result<Value, ErrorObjectOwned> {
    let kind = method.split('.').next().unwrap();
    let key = text(
        p,
        match kind {
            "run" => "run_id",
            "legacy" => "key",
            _ => "workflow_id",
        },
    )?;
    let (lsn, value) = b.store.get(kind, key).await.map_err(storage_failure)?;
    Ok(json!({
        "snapshot_cursor": {"journal_id": b.journal.id(), "lsn": lsn.to_string()},
        "value": value,
    }))
}

/// run.events.page / run.audit.page：游标化 journal 事件/审计分页。
///
/// 与快照面不同，这里读的是 journal 原始事件：事件流是既成事实，不能
/// 被投影重写。
async fn events_page(
    b: &JournalBackend,
    method: &str,
    p: &Value,
) -> Result<Value, ErrorObjectOwned> {
    let run_id = text(p, "run_id")?.to_owned();
    // upper（本次页的 LSN 上界）先从投影取，保证整页读同一个快照。
    let (upper, run) = b
        .store
        .get_summary("run", &run_id)
        .await
        .map_err(storage_failure)?;
    if run.is_none() {
        return Err(ErrorObjectOwned::owned(-32011, "run not found", None::<()>));
    }
    // 事件面与审计面是两种不同的流：前者是业务进展，后者是执行器审计事实。
    // permissions 模式（09 §10.3）过滤权限事件闭集（谁在何时批了什么）。
    let filter = match method {
        "run.events.page" => Filter::Events,
        "run.audit.page" if p.get("filter").and_then(Value::as_str) == Some("permission") => {
            Filter::Permission
        }
        _ => Filter::Audit,
    };
    let identity = b.journal.id().to_owned();
    // 无游标 = 从第一页开始；游标自带过滤条件与位置，必须与本次请求的
    // run/filter 一致，否则会串页。
    let cursor = match p.get("cursor").filter(|v| !v.is_null()) {
        Some(v) => serde_json::from_value::<Cursor>(v.clone()).map_err(invalid)?,
        None => Cursor::first(identity.clone(), run_id.clone(), filter, upper),
    };
    let limit = p
        .get("limit")
        .map(|v| v.as_u64().ok_or_else(|| invalid("invalid limit")))
        .transpose()?
        .unwrap_or(256)
        .min(256) as usize;
    let root = b.journal.root().to_path_buf();
    // 文件扫描放阻塞线程池：这一段是同步 IO，压在异步 worker 上会拖慢
    // 整个 RPC 面。
    let page = tokio::task::spawn_blocking(move || {
        flow_journal::page::page(&root, &identity, &run_id, filter, cursor, upper, limit)
    })
    .await
    .map_err(invalid)?
    .map_err(|e| failure(e.into()))?;
    serde_json::to_value(page).map_err(invalid)
}

/// permissions.audit.page：数据集级权限审计分页。
///
/// 回答「**谁**在**何时**批准了**哪些**命令、依据哪些能力项」：按
/// run_id / decision / actor(kind) 过滤权限事件闭集。实现按 run 分页
/// （复用 run 级 Permission 过滤器）：无 run_id 时从在册 run 里逐个收集
/// 到页满；游标 = 下一批 (run_id, lsn)。
async fn permissions_audit_page(b: &JournalBackend, p: &Value) -> Result<Value, ErrorObjectOwned> {
    let decision = p.get("decision").and_then(Value::as_str);
    if let Some(decision) = decision {
        if !matches!(decision, "allow" | "deny") {
            return Err(invalid("decision must be allow | deny"));
        }
    }
    let run_id = p.get("run_id").and_then(Value::as_str);
    let limit = p
        .get("limit")
        .map(|v| v.as_u64().ok_or_else(|| invalid("invalid limit")))
        .transpose()?
        .unwrap_or(100)
        .min(256) as usize;
    // 候选 run：指定 run 或全部在册 run（有权限事实的只占少数，逐 run 扫描
    // 由 Permission 过滤器短路）。
    let runs: Vec<String> = match run_id {
        Some(run_id) => vec![run_id.to_owned()],
        None => b.run_ids_view().await,
    };
    let identity = b.journal.id().to_owned();
    let root = b.journal.root().to_path_buf();
    let decision = decision.map(str::to_owned);
    // 数据集级权限事实（run 外）：游标 run_id 为 null 时只续读数据集段。
    let dataset_only = p
        .get("cursor")
        .is_some_and(|c| !c.is_null() && c["run_id"].is_null());
    if dataset_only && run_id.is_none() && decision.is_none() {
        let from_lsn = p["cursor"]["lsn"].as_u64().unwrap_or(1);
        let dataset = tokio::task::spawn_blocking({
            let root = root.clone();
            let identity = identity.clone();
            move || {
                flow_journal::page::dataset_permission_events(&root, &identity, from_lsn, limit + 1)
            }
        })
        .await
        .map_err(invalid)?
        .map_err(|e| failure(e.into()))?;
        let complete = dataset.len() <= limit;
        let events: Vec<Value> = dataset
            .into_iter()
            .take(limit)
            .map(|positioned| serde_json::to_value(&positioned).map_err(invalid))
            .collect::<Result<_, _>>()?;
        return Ok(json!({"events": events, "complete": complete}));
    }
    let mut events: Vec<Value> = Vec::new();
    let mut exhausted = true;
    for run_id in runs {
        let Some((upper, summary)) = b.store.get_summary("run", &run_id).await.ok() else {
            continue;
        };
        if summary.is_none() {
            continue;
        }
        let mut cursor = match p.get("cursor").filter(|v| !v.is_null()) {
            // 游标只认 (run_id, lsn) 续读位置：跨 run 续读由 items 顺序保证。
            Some(v) => {
                let value = v.clone();
                let wanted_run = value["run_id"].as_str().unwrap_or(&run_id).to_owned();
                let from_lsn = value["lsn"].as_u64().unwrap_or(1);
                Cursor::first(identity.clone(), wanted_run, Filter::Permission, upper)
                    .with_next_lsn(from_lsn)
            }
            None => Cursor::first(identity.clone(), run_id.clone(), Filter::Permission, upper),
        };
        loop {
            let page = tokio::task::spawn_blocking({
                let root = root.clone();
                let identity = identity.clone();
                let run_id = run_id.clone();
                let cursor = cursor.clone();
                move || {
                    flow_journal::page::page(
                        &root,
                        &identity,
                        &run_id,
                        Filter::Permission,
                        cursor,
                        upper,
                        256,
                    )
                }
            })
            .await
            .map_err(invalid)?
            .map_err(|e| failure(e.into()))?;
            for positioned in &page.events {
                // decision 过滤只对裁决事件有意义：给出过滤时只返回匹配的
                // permission_decided；未给过滤时返回全部权限事实。
                let record = &positioned.event.payload["record"];
                let matched = match &decision {
                    Some(decision) => {
                        matches!(
                            positioned.event.kind,
                            flow_journal::EventKind::PermissionDecided
                        ) && record["decision"].as_str() == Some(decision.as_str())
                    }
                    None => true,
                };
                if matched {
                    events.push(serde_json::to_value(positioned).map_err(invalid)?);
                }
            }
            if events.len() >= limit {
                exhausted = false;
                break;
            }
            match page.next_cursor {
                Some(next) => cursor = next,
                None => break,
            }
        }
        if !exhausted {
            break;
        }
    }
    events.truncate(limit);
    // 追加数据集级权限事实（无 run 范围、无 decision 过滤时）：审批审计之外
    // 的「谁改了权限 / 谁授予了插件」同属权限审计面（09 §10.3）。
    if run_id.is_none() && decision.is_none() && events.len() < limit {
        let remaining = limit - events.len();
        let dataset = tokio::task::spawn_blocking({
            let root = root.clone();
            let identity = identity.clone();
            move || {
                flow_journal::page::dataset_permission_events(&root, &identity, 1, remaining + 1)
            }
        })
        .await
        .map_err(invalid)?
        .map_err(|e| failure(e.into()))?;
        // 数据集段截断时整页不算读完（续读游标由 CLI 从末条 run_id=null 事件推导）。
        exhausted &= dataset.len() <= remaining;
        for positioned in dataset.into_iter().take(remaining) {
            events.push(serde_json::to_value(&positioned).map_err(invalid)?);
        }
    }
    Ok(json!({
        "events": events,
        "complete": exhausted,
        // 导出提示：JSONL = 逐行 serde_json::to_string(events[i])（CLI 同款）。
    }))
}

/// 产品面：工作流清单视图。
async fn workflow_list_view(b: &JournalBackend) -> Result<Value, ErrorObjectOwned> {
    let workflows = flow_backend::journal_views::list_workflows(b)
        .await
        .map_err(view_failure)?;
    Ok(json!({"workflows": workflows}))
}

/// 产品面：工作流版本视图（附 published_version 供编辑器渲染徽标）。
///
/// published_version 单独查而不是从版本列表里推断：编辑器要据此显示
/// 「已发布」徽标，推断值在并发发布时可能指向错误版本。
async fn workflow_get_view(b: &JournalBackend, p: &Value) -> Result<Value, ErrorObjectOwned> {
    let version = flow_backend::journal_views::get_version(
        b,
        text(p, "workflow_id")?,
        optional_version(p)?
            .map(|v| i64::try_from(v).map_err(invalid))
            .transpose()?,
    )
    .await
    .map_err(view_failure)?;
    let published = flow_backend::journal_views::latest_published(b, text(p, "workflow_id")?)
        .await
        .map_err(view_failure)?;
    let mut value = serde_json::to_value(version).map_err(invalid)?;
    value["published_version"] = json!(published);
    Ok(value)
}

/// 产品面：工作流版本列表。
async fn workflow_versions(b: &JournalBackend, p: &Value) -> Result<Value, ErrorObjectOwned> {
    let versions = flow_backend::journal_views::version_metadata(b, text(p, "workflow_id")?)
        .await
        .map_err(view_failure)?;
    Ok(json!({"versions": versions}))
}

/// 产品面：run 详情 + live 标记。
///
/// live 标记表示「这个 run 还在推进」，是前端决定要不要继续订阅的依据；
/// 它是瞬时判断，不参与权威状态。
async fn run_get_view(b: &JournalBackend, p: &Value) -> Result<Value, ErrorObjectOwned> {
    let run_id = text(p, "run_id")?;
    let run = flow_backend::journal_views::get_run(b, run_id)
        .await
        .map_err(view_failure)?;
    let live = flow_backend::journal_views::is_live(b, run_id).await;
    let plugin_projections = flow_backend::journal_views::run_plugin_projections(b, run_id).await;
    let environment = b
        .run_snapshot(run_id)
        .await
        .map_err(failure)?
        .and_then(|r| r.execution.and_then(|s| s.environment));
    Ok(
        json!({"run": run, "live": live, "plugin_projections": plugin_projections, "environment": environment}),
    )
}

/// 产品面：run 清单（status/source 过滤 + 分页）。
///
/// 过滤值先校验再查：非法枚举值直接 `-32602`，否则会被当成「无过滤」，
/// 返回一份与请求语义不符的清单。
async fn run_list_view(b: &JournalBackend, p: &Value) -> Result<Value, ErrorObjectOwned> {
    if let Some(status) = p.get("status") {
        if !status
            .as_str()
            .is_some_and(flow_backend::RunStatus::is_valid_str)
        {
            return Err(invalid("invalid run status"));
        }
    }
    if let Some(source) = p.get("source") {
        if !source
            .as_str()
            .is_some_and(flow_backend::RunSource::is_valid_str)
        {
            return Err(invalid("invalid run source"));
        }
    }
    let runs = flow_backend::journal_views::list_runs(
        b,
        p["workflow_id"].as_str(),
        p["status"].as_str(),
        p["source"].as_str(),
        p["before_run_id"].as_str(),
        // 页大小夹在 1..=500：0 会让「按默认页大小」与「显式传 0」无法
        // 区分，上限则防止一次拉走整库。
        p["limit"].as_i64().unwrap_or(50).clamp(1, 500),
    )
    .await
    .map_err(view_failure)?;
    Ok(json!({"runs": runs}))
}

/// 产品面：run 事件视图（from_seq 起的事件流）。
///
/// 与 run.events.page 的差别在于本视图额外支持 `from_seq` 起点，适合
/// 「接上一次的进度继续看」的场景。
async fn events_view(b: &JournalBackend, p: &Value) -> Result<Value, ErrorObjectOwned> {
    let run_id = text(p, "run_id")?.to_owned();
    let (upper, run) = b
        .store
        .get_summary("run", &run_id)
        .await
        .map_err(storage_failure)?;
    if run.is_none() {
        return Err(ErrorObjectOwned::owned(-32011, "run not found", None::<()>));
    }
    // from_seq 与平台序号参数规范同口径（十进制字符串）：传 JSON 数字
    // 必须显式报错，静默当 0 会从头发送全部事件。
    let from = decimal_param(p, "from_seq")?;
    let identity = b.journal.id().to_owned();
    let root = b.journal.root().to_path_buf();
    let cursor = match p.get("cursor").filter(|v| !v.is_null()) {
        Some(v) => serde_json::from_value::<Cursor>(v.clone()).map_err(invalid)?,
        None => Cursor::first(identity.clone(), run_id.clone(), Filter::Events, upper),
    };
    let mut page = tokio::task::spawn_blocking(move || {
        flow_journal::page::page(
            &root,
            &identity,
            &run_id,
            Filter::Events,
            cursor,
            upper,
            256,
        )
    })
    .await
    .map_err(invalid)?
    .map_err(|e| failure(e.into()))?;
    page.events.retain(|e| e.event.run_seq >= from);
    serde_json::to_value(page).map_err(invalid)
}

/// schedule/webhook/template CRUD：cron 与模板片段校验后走统一产品命令
/// （回执序列化与 journal 错误分层在此完成）。
///
/// 前置校验放在服务端边缘（cron 合法性、片段完整性）：这些错误在写命令
/// 落 journal 之前就能判定，早失败比留下半条命令再回滚便宜得多。
async fn product_command_route(
    ctx: &Context,
    method: &str,
    p: &Value,
) -> Result<Value, ErrorObjectOwned> {
    if method.starts_with("schedule.") {
        // 只在参数里带了 cron 时才校验：update 可能只改启用状态。
        if let Some(cron) = p.get("cron") {
            crate::validate_cron(
                cron.as_str()
                    .ok_or_else(|| invalid("cron must be a string"))?,
            )?;
        }
    }
    // Preserve the existing normalized wire fingerprint when a complete fragment
    // is supplied. Partial updates are merged and validated by the backend.
    // 完整片段先规范化再入库：规范化后的形状就是幂等指纹的输入，
    // 同一份逻辑定义的两种写法必须折叠成同一个指纹。
    let fragment = if method.starts_with("template.")
        && method != "template.delete"
        && p.get("nodes").is_some()
        && p.get("edges").is_some()
    {
        Some(
            flow_engine::template::normalize_fragment(&p["nodes"], &p["edges"])
                .map_err(|e| failure(e.into()))?,
        )
    } else {
        None
    };
    let mut args = p.clone();
    // object 性由 admit_call 前置保证，但不得跨调用帧依赖该检查——
    // 直接在消费点拒绝，防止未来路由改动把客户端输入变成 panic。
    let Some(map) = args.as_object_mut() else {
        return Err(invalid("params must be an object"));
    };
    // request_id 从命令载荷里剔除：它是幂等键，不是命令内容——留在载荷里
    // 会让同一次写在不同键名下产生不同指纹。
    map.remove("request_id");
    if let Some((nodes, edges)) = fragment {
        map.insert("nodes".into(), nodes);
        map.insert("edges".into(), edges);
    }
    // 边缘一次 struct 化；args 保留原始 wire 形状作为命令幂等指纹。
    let command =
        flow_backend::ProductCommand::parse(method, &args).map_err(|e| failure(e.into()))?;
    let receipt = ctx
        .backend
        .product_command(command, &args, text(p, "request_id")?)
        .await
        .map_err(failure)?;
    serde_json::to_value(receipt).map_err(invalid)
}

/// 产品面：schedule 清单。
///
/// 每条 schedule 附服务端算好的 next_fire_at（见 [`crate::schedule_value`]），
/// 前端据此显示「下次触发时间」，不必在客户端实现 cron 解析。
async fn schedule_list(b: &JournalBackend, p: &Value) -> Result<Value, ErrorObjectOwned> {
    let schedules = flow_backend::journal_views::list_schedules(b, p["workflow_id"].as_str())
        .await
        .map_err(view_failure)?;
    let list: Vec<Value> = schedules.iter().map(crate::schedule_value).collect();
    Ok(json!({"schedules": list}))
}

/// config.update：读改写统一配置面（merge-patch → 落盘 → 热更新）。
///
/// 顺序是「读当前 → merge → 落盘 → 换内存」：落盘失败时内存保持原样，
/// 不会出现「内存已改、磁盘没改」的分裂状态。
async fn config_update(ctx: &Context, p: &Value) -> Result<Value, ErrorObjectOwned> {
    let product = ctx_product(ctx)?;
    let patch = p["patch"].clone();
    let cs = product
        .config
        .as_ref()
        .ok_or_else(|| invalid("此实例未启用配置文件读写，无法修改配置"))?;
    // 写锁覆盖 merge 与落盘全程：两个并发 update 若都先读后写，后写者会
    // 覆盖前写者的改动（丢更新）。
    let mut current = cs.config.write().unwrap_or_else(|p| p.into_inner());
    let base = current.clone();
    let merged =
        flow_config::Config::merge_patch(&base, &patch).map_err(|e| invalid(e.to_string()))?;
    // 落点只认启动时发现的配置文件，其次默认落点（FLOW_CONFIG 声明值或
    // 平台配置目录）；绝不用进程 CWD——换目录启动会丢配置。
    let existing = cs.path.read().unwrap_or_else(|p| p.into_inner()).clone();
    let (write_path, first_write) = match existing {
        Some(path) => (path, false),
        None => (
            cs.default_path.clone().ok_or_else(|| {
                invalid("无法确定配置文件落点：请通过 --config 或 FLOW_CONFIG 指定路径")
            })?,
            true,
        ),
    };
    // 父目录可能还不存在（首次写入默认落点）：先建目录再写文件。
    if let Some(parent) = write_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| invalid(format!("配置写入失败：{e}")))?;
        }
    }
    merged
        .save(&write_path)
        .map_err(|e| invalid(format!("配置写入失败：{e}")))?;
    // 首次写入后回填路径：config.get 的 config_path 要立刻显示真实落点，
    // 否则用户不知道配置被写到哪了。
    if first_write {
        *cs.path.write().unwrap_or_else(|p| p.into_inner()) = Some(write_path);
    }
    *current = merged;
    drop(current);
    Ok(crate::config_view(&product))
}

/// secrets.set：名称/长度校验后写持久密钥存储。
///
/// 写入成功只回名字与来源（stored），**绝不回显密钥值**：回显即等于把
/// 凭据复制进客户端日志与前端状态。
async fn secrets_set(ctx: &Context, p: &Value) -> Result<Value, ErrorObjectOwned> {
    let name = text(p, "name")?.to_owned();
    let value = text(p, "value")?.to_owned();
    crate::validate_secret_name(&name)?;
    if value.is_empty() {
        return Err(invalid("密钥值不能为空"));
    }
    // 8 KiB 上限：密钥文件会被整份读进内存并参与摘要，无上限会让一次
    // 请求就能把内存打满。
    if value.len() > 8192 {
        return Err(invalid("密钥值过长（上限 8 KiB）"));
    }
    let product = ctx_product(ctx)?;
    let store = product
        .secrets
        .as_ref()
        .ok_or_else(|| invalid("密钥存储未启用（需要 storage.data_dir 可写）"))?;
    store
        .set(&name, &value)
        .map_err(|e| ErrorObjectOwned::owned(-32603, format!("密钥写入失败：{e}"), None::<()>))?;
    Ok(json!({"name": name, "source": "stored"}))
}

/// secrets.delete：删除持久密钥。
///
/// 只删持久化的那份：env 里的同名密钥不受影响（引擎取值是 stored 优先、
/// env 兜底，删除后会自动回落到 env）。
async fn secrets_delete(ctx: &Context, p: &Value) -> Result<Value, ErrorObjectOwned> {
    let name = text(p, "name")?.to_owned();
    crate::validate_secret_name(&name)?;
    let product = ctx_product(ctx)?;
    let Some(store) = &product.secrets else {
        return Err(invalid("密钥存储未启用（需要 storage.data_dir 可写）"));
    };
    let deleted = store
        .delete(&name)
        .map_err(|e| ErrorObjectOwned::owned(-32603, format!("密钥删除失败：{e}"), None::<()>))?;
    Ok(json!({"name": name, "deleted": deleted}))
}

/// 产品装配取用（config/secrets 方法用）。未装配时报「未启用」。
///
/// 装配缺失不是服务端故障，是「这个实例没有产品面」——用 `-32602` 而不是
/// `-32603`，客户端据此判断「换个实例再试」而不是「重试没用」。
fn ctx_product(ctx: &Context) -> Result<Arc<crate::AppState>, ErrorObjectOwned> {
    ctx.product
        .clone()
        .ok_or_else(|| invalid("此实例未装配产品面（config/secrets 不可用）"))
}

/// run.subscribe：tail journal 按 run 推 v2 原形事件（PositionedEvent）。
/// 事件只发 journal v2 原形；/journal 工作区与主工作台 monitor 都直接
/// 消费这一格式。
///
/// 订阅路径刻意保持「简单」：固定 50ms 轮询，没有补帧状态机、没有首帧
/// 快照。订阅状态机一旦出错，表现为「前端事件丢一半」这类极难复现的
/// 问题；轮询的代价只是 CPU 与「从 0 开始要重放全程」。
///
/// # Errors
/// 订阅被拒绝时通过 `pending.reject` 回错误对象（`-32001` unauthorized /
/// `-32602` invalid params）；进入推流后不再回错误，只能断流。
async fn subscribe_run_events(
    params: jsonrpsee::types::Params<'_>,
    pending: jsonrpsee::server::PendingSubscriptionSink,
    ctx: &Context,
) {
    let p: Value = match params.parse() {
        Ok(p) => p,
        Err(e) => {
            pending.reject(e).await;
            return;
        }
    };
    // 订阅也要鉴权，且口径与普通方法一致：token 是唯一屏障，
    // 「订阅入口不走 admit_call」不构成例外理由。
    let supplied = p["_token"].as_str().unwrap_or("");
    if !constant_time_eq(supplied, &ctx.token) {
        pending
            .reject(ErrorObjectOwned::owned(-32001, "unauthorized", None::<()>))
            .await;
        return;
    }
    // event_format 必须恰好是 "v2"：只有一种事件格式，才能保证前端不会
    // 悄悄按旧格式解析。
    if p["event_format"] != "v2" {
        pending.reject(invalid("event_format must be v2")).await;
        return;
    }
    let run_id = match text(&p, "run_id") {
        Ok(id) => id.to_owned(),
        Err(e) => {
            pending.reject(e).await;
            return;
        }
    };
    let from_seq = match decimal_param(&p, "from_seq") {
        Ok(n) => n,
        Err(e) => {
            pending.reject(e).await;
            return;
        }
    };
    let audit = p["mode"] == "audit";
    // mode 只能取 audit / control：未知的 mode 若被静默当成 control，
    // 调用方会以为自己订阅了审计流却收到业务流。
    if p.get("mode").is_some() && !matches!(p["mode"].as_str(), Some("audit" | "control")) {
        pending.reject(invalid("invalid subscription mode")).await;
        return;
    }
    let from_lsn = match decimal_param(&p, "from_lsn") {
        Ok(n) => n,
        Err(e) => {
            pending.reject(e).await;
            return;
        }
    };
    // 接受订阅后才开始推流：accept 之前的一切校验失败都应表现为
    // 「订阅被拒绝」，而不是「订阅成功但一条事件都没有」。
    let sink = match pending.accept().await {
        Ok(s) => s,
        Err(_) => return,
    };
    let mut tail = flow_journal::tail::TailReader::for_run(
        ctx.backend.journal.root(),
        ctx.backend.journal.id(),
        &run_id,
    );
    // scanned 记录已扫描到的 LSN：既是 tail 续读位置，也是「本轮无需
    // 再睡」的判据。
    let mut scanned = 0;
    loop {
        if sink.is_closed() {
            return;
        }
        // 每轮先取一次投影快照：它给出本轮的 LSN 上界（upper），保证
        // 这一轮读到的都是已提交的事实。
        let (upper, row) = match ctx.backend.store.get_summary("run", &run_id).await {
            Ok(v) => v,
            Err(error) => {
                tracing::warn!(run = %run_id, %error, "run.subscribe summary read failed; stream ends");
                return;
            }
        };
        // run 不存在（没这行）直接结束：不是错误，因为订阅一个不存在的
        // run 本来就无从推送。
        let Some(row) = row else { return };
        let terminal = matches!(
            row["status"].as_str(),
            Some("succeeded" | "failed" | "cancelled")
        );
        let id = run_id.clone();
        // 阻塞扫描放到线程池：journal 文件是同步 IO，压在异步 worker 上
        // 会拖慢整个 RPC 面。
        let batch = tokio::task::spawn_blocking(move || -> flow_journal::Result<_> {
            let mut records = Vec::new();
            let mut bytes = 0;
            let mut latest = scanned;
            // 单批最多累积 MAX_LINE_BYTES：再大就会被 jsonrpsee 的响应
            // 体积上限拒收。
            while bytes < flow_journal::MAX_LINE_BYTES {
                let Some((tx, location)) = tail.next(upper)? else {
                    // A filtered reader may exhaust this snapshot without returning
                    // a transaction at upper. Progress is scan work, not event delivery.
                    latest = upper;
                    break;
                };
                latest = tx.lsn;
                bytes += location.bytes as usize;
                for (index, event) in tx.events.into_iter().enumerate() {
                    // 两种流各自的推进基准：control 按 run_seq（业务事件序），
                    // audit 按 lsn（写入序）。
                    if event.run_id.as_deref() == Some(&id)
                        && (if audit {
                            tx.lsn > from_lsn && event.is_audit()
                        } else {
                            event.run_seq > from_seq
                        })
                    {
                        records.push(flow_journal::page::PositionedEvent {
                            lsn: tx.lsn,
                            event_index: index,
                            event,
                        });
                    }
                }
            }
            Ok((tail, records, latest))
        })
        .await;
        let (reader, records, latest) = match batch {
            Ok(Ok(v)) => v,
            Ok(Err(error)) => {
                tracing::warn!(run = %run_id, %error, "run.subscribe page scan failed; stream ends");
                return;
            }
            Err(join) => {
                tracing::warn!(run = %run_id, %join, "run.subscribe page scan task failed; stream ends");
                return;
            }
        };
        tail = reader;
        scanned = latest;
        for record in records {
            let payload: Value = match serde_json::to_value(&record) {
                Ok(v) => v,
                Err(error) => {
                    tracing::warn!(run = %run_id, %error, "run.subscribe event encode failed; stream ends");
                    return;
                }
            };
            // 单帧超过上限时先发一帧明确的 stream_error 再断流：
            // 客户端据此知道「事件流因体积中断」，而不是「事件流已结束」。
            if flow_journal::codec::bounded_json(&payload, flow_journal::MAX_LINE_BYTES - 1024)
                .is_err()
            {
                if let Ok(message) = jsonrpsee::server::SubscriptionMessage::new(
                    "run.event",
                    sink.subscription_id(),
                    &json!({"type": "stream_error", "code": "RESPONSE_TOO_LARGE"}),
                ) {
                    let _ =
                        tokio::time::timeout(std::time::Duration::from_secs(2), sink.send(message))
                            .await;
                }
                return;
            }
            let message = match jsonrpsee::server::SubscriptionMessage::new(
                "run.event",
                sink.subscription_id(),
                &payload,
            ) {
                Ok(m) => m,
                Err(_) => return,
            };
            // 发送带 2s 超时：客户端不消费时不能把推送协程永久挂住。
            if !matches!(
                tokio::time::timeout(std::time::Duration::from_secs(2), sink.send(message)).await,
                Ok(Ok(()))
            ) {
                return;
            }
        }
        if scanned >= upper {
            // 终态 run 的 control 流可以自行结束（后面不会再有业务事件）；
            // audit 流不结束——迟到的审计记录理论上仍可能出现。
            if terminal && !audit {
                return;
            }
            tokio::select! {
                _ = sink.closed() => return,
                _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
            }
        }
    }
}

/// 开发形态服务：只允许绑定 loopback 地址。
///
/// loopback 限制是开发模式的护栏：token 是单用户凭据，暴露到公网等于
/// 把本机的工作流引擎开放给所有人。产品形态走 [`serve_product`]，绑定
/// 地址由调用方自己决定（外层负责 TLS 与认证）。
///
/// # Errors
/// 地址不是 loopback 或绑定失败时返回 Err。
pub async fn serve(
    backend: Arc<JournalBackend>,
    token: String,
    addr: SocketAddr,
) -> Result<(jsonrpsee::server::ServerHandle, SocketAddr), Box<dyn std::error::Error>> {
    if !addr.ip().is_loopback() {
        return Err("development JSONL RPC requires a loopback address".into());
    }
    let module = module(backend, token)?;
    serve_module(module, addr).await
}

/// 产品形态服务：非 loopback 绑定由调用方决定（token 认证仍在）。
///
/// 注意「绑定放开」不等于「鉴权放开」：token 校验仍在每个方法的前置层
/// 里，外层暴露面必须自备 TLS（DESIGN §0 C7）。
///
/// # Errors
/// 绑定失败时返回 Err。
pub async fn serve_product(
    module: RpcModule<Context>,
    addr: SocketAddr,
) -> Result<(jsonrpsee::server::ServerHandle, SocketAddr), Box<dyn std::error::Error>> {
    serve_module(module, addr).await
}

/// 绑定端口并启动 jsonrpsee 服务，返回句柄与实际监听地址。
///
/// 返回实际地址（而非请求地址）是为了支持端口 0：测试 harness 需要拿到
/// 真实端口才能连接。
///
/// # Errors
/// 端口绑定失败时返回 Err。
async fn serve_module(
    module: RpcModule<Context>,
    addr: SocketAddr,
) -> Result<(jsonrpsee::server::ServerHandle, SocketAddr), Box<dyn std::error::Error>> {
    let server = bind_product_server(addr).await?;
    let addr = server.local_addr()?;
    Ok((server.start(module), addr))
}

/// 构建 jsonrpsee 服务端（尚未开始监听）。
///
/// 体积上限 9 MiB：与命令指纹的 9 MiB 上限对齐——超过指纹上限的请求
/// 无法被幂等地记录，与其让服务端收下再报错，不如在传输层直接拒掉。
/// 连接数上限 64 是单机单用户场景的合理上限，挡住失控客户端的连接风暴。
///
/// # Errors
/// 构造或绑定失败时返回 Err。
pub(crate) async fn bind_product_server(
    addr: SocketAddr,
) -> Result<jsonrpsee::server::Server, Box<dyn std::error::Error>> {
    let config = jsonrpsee::server::ServerConfig::builder()
        .max_request_body_size(9 * 1024 * 1024)
        .max_response_body_size(9 * 1024 * 1024)
        .max_connections(64)
        .build();
    let server = jsonrpsee::server::Server::builder()
        .set_config(config)
        .build(addr)
        .await?;
    Ok(server)
}

#[cfg(test)]
mod tests {
    use super::*;
    /// 错误码必须由**类型**决定，而不是由文案决定。
    ///
    /// 这条规则一旦破例，客户端的按码分支会在某次措辞调整后静默失效。
    #[test]
    fn error_codes_follow_types_instead_of_message_text() {
        assert_eq!(
            failure(flow_journal::Error::NotFound("任意文案".into()).into()).code(),
            -32011
        );
        assert_eq!(
            failure(flow_journal::Error::Invalid("workflow not found".into()).into()).code(),
            -32602
        );
        assert_eq!(
            view_failure(flow_backend::ViewError::NotFound("new wording".into())).code(),
            -32011
        );
        assert_eq!(
            view_failure(flow_backend::ViewError::Limit("budget".into())).code(),
            -32021
        );
    }

    /// 存储层错误映射同样不依赖数据库文案。
    #[test]
    fn storage_limit_mapping_does_not_depend_on_database_messages() {
        assert_eq!(
            storage_failure(flow_backend::StorageError::ResponseTooLarge).code(),
            -32021
        );
    }

    /// 序号参数必须是规范的十进制字符串：数字形态、带符号、带前导零的写法
    /// 一律拒绝，否则同一个序号会有多种表示，幂等与去重都会对不上。
    #[test]
    fn sequence_parameters_are_lossless_canonical_decimal_strings() {
        assert_eq!(decimal_param(&json!({}), "from_lsn").unwrap(), 0);
        assert_eq!(
            decimal_param(&json!({"from_lsn":"18446744073709551615"}), "from_lsn").unwrap(),
            u64::MAX
        );
        for value in [
            json!(1),
            json!(null),
            json!("+1"),
            json!("01"),
            json!("-1"),
            json!("18446744073709551616"),
        ] {
            assert!(decimal_param(&json!({"from_lsn":value}), "from_lsn").is_err());
        }
    }
}

async fn subscribe_service_events(
    params: jsonrpsee::types::Params<'_>,
    pending: jsonrpsee::PendingSubscriptionSink,
    ctx: &Context,
) {
    let admitted = admit_call(ctx, "services.events", params).await;
    let (p, _, caller) = match admitted {
        Ok(value) => value,
        Err(error) => {
            pending.reject(error).await;
            return;
        }
    };
    if !caller.permission_admin {
        pending
            .reject(invalid("service events require admin"))
            .await;
        return;
    }
    let parsed = (|| -> Result<_, ErrorObjectOwned> {
        Ok((
            text(&p, "plugin_id")?.to_owned(),
            text(&p, "instance")?.to_owned(),
            text(&p, "after")?.parse::<u64>().map_err(invalid)?,
        ))
    })();
    let (id, instance, mut cursor) = match parsed {
        Ok(v) => v,
        Err(e) => {
            pending.reject(e).await;
            return;
        }
    };
    let Some(service) = ctx.backend.services.lock().await.get(&id).cloned() else {
        pending.reject(invalid("service unavailable")).await;
        return;
    };
    let mut receiver = service.events.subscribe();
    let initial = match service.events_since(&instance, cursor) {
        Ok(v) => v,
        Err(e) => {
            pending.reject(invalid(e)).await;
            return;
        }
    };
    let Ok(sink) = pending.accept().await else {
        return;
    };
    let mut queue =
        std::collections::VecDeque::from(initial["events"].as_array().cloned().unwrap_or_default());
    loop {
        if sink.is_closed() {
            return;
        }
        let value = if let Some(value) = queue.pop_front() {
            value
        } else {
            if !service.alive() {
                json!({"type":"disconnected"})
            } else {
                match tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv()).await
                {
                    Ok(Ok(value)) => value,
                    Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => {
                        json!({"type":"resync_required"})
                    }
                    Ok(Err(_)) => json!({"type":"disconnected"}),
                    Err(_) => continue,
                }
            }
        };
        let terminal = value.get("type").is_some();
        if !terminal {
            let sequence = value["sequence"]
                .as_str()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            if sequence <= cursor {
                continue;
            }
            cursor = sequence;
        }
        let Ok(message) = jsonrpsee::server::SubscriptionMessage::new(
            "services.event",
            sink.subscription_id(),
            &value,
        ) else {
            return;
        };
        if !matches!(
            tokio::time::timeout(std::time::Duration::from_secs(2), sink.send(message)).await,
            Ok(Ok(()))
        ) {
            return;
        }
        if terminal {
            return;
        }
    }
}
