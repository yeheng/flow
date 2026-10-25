//! JSONL triggers: stable fire identity and run creation share one authoritative command.
//!
//! 本模块负责三条入站触发路径：cron 定时触发（扫描型）、webhook 触发
//! （管理令牌 + 幂等键）、以及业务调用面 `/invoke`（业务密钥 + 每 run 能力
//! 令牌）。三者最终都落到同一条权威命令（`trigger_start` / `invoke_start`），
//! 幂等与去重因此由 journal 侧统一负责。
//!
//! 它守护的核心不变量：**同一次触发只会产生一个 run**。cron 用「触发时刻
//! 字符串」、webhook/业务调用用 `Idempotency-Key` 作为去重身份；没有稳定
//! 身份的重试会造成重复执行。
//!
//! 能力令牌（run_token）是「一个 run 一个令牌」的窄能力：它只允许读取该
//! run 的结果、事件、发信号与取消，绝不泄漏管理令牌，也不授权操作其他 run。
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Local, Utc};
use flow_backend::journal::{JournalBackend, JournalError};
use serde_json::Value;
use std::sync::Arc;

/// 扫描一次到期的 cron schedule 并触发。
///
/// 幂等键 = 该 cron 时隙的 RFC3339 时刻字符串，因此重复扫描同一时隙只会
/// 产生一次 run；反过来说，时隙字符串也必须是**稳定**的，扫描器重启、
/// 时钟回拨都不能让它变。
pub async fn fire_due(backend: &JournalBackend, now: DateTime<Local>) {
    for config in backend.trigger_configs("schedule").await {
        // disabled 的 schedule 显式跳过：配置里的 enabled 是权威开关，
        // 不能因为「上次触发过」而误判。
        if config["enabled"] != true {
            continue;
        }
        let Some(id) = config["id"].as_str() else {
            continue;
        };
        let Ok(cron) = config["cron_expr"]
            .as_str()
            .unwrap_or("")
            .parse::<cron_parser::Schedule>()
        else {
            // 坏 cron 的静默跳过曾让 schedule 永不触发且无迹可查——至少留一条日志。
            tracing::warn!(schedule_id = %id, "schedule cron invalid; fire skipped");
            continue;
        };
        // 向前挪 1 纳秒：取「now 之前最近的一个时隙」时必须包含 now 本身，
        // 否则恰好落在时隙边界的那次触发会被推迟到下一个周期。
        let Some(fire) = cron.previous_before(&(now + chrono::Duration::nanoseconds(1))) else {
            continue;
        };
        // Older configurations have no activation timestamp; their creation time
        // is the lower bound. Never interpret a historical cron slot as a new fire.
        let active_since = config["active_since"]
            .as_str()
            .or_else(|| config["created_at"].as_str())
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok());
        // 生效时刻之前的时隙一律不算触发：刚创建的 schedule 若 cron 表达式
        // 指向过去（例如每小时第 0 分而创建于第 30 分），补跑会造成「一上来
        // 就触发一次」的历史时隙。
        if active_since.is_none_or(|since| fire < since) {
            continue;
        }
        // 去重身份用 UTC 化的时隙时刻字符串：同一时隙无论在哪个时区扫描，
        // 得到的键都相同。
        let key = fire.with_timezone(&Utc).to_rfc3339();
        if let Err(error) = backend
            .trigger_start("schedule", id, &key, Value::Null)
            .await
        {
            tracing::warn!(schedule_id=%id,%error,"JSONL scheduled command did not become visible");
        }
    }
}
/// journal 侧触发器扫描循环；tick 由 `[server].journal_trigger_tick_secs` 配置。
///
/// tick 间隔决定了 cron 的触发精度上限（误差 ≤ tick）：间隔必须明显小于
/// 最短 cron 周期，否则「每小时」这类表达式会漏掉某些时隙。
pub fn start(
    backend: Arc<JournalBackend>,
    tick: std::time::Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // interval 的首次 tick 立即触发：启动后不必等满一个周期才开始工作。
        let mut tick = tokio::time::interval(tick);
        loop {
            tick.tick().await;
            fire_due(&backend, Local::now()).await;
        }
    })
}
/// 触发器 HTTP 面共享状态：journal 句柄、管理令牌、下载并发闸门。
struct HookState {
    backend: Arc<JournalBackend>,
    token: String,
    downloads: Arc<tokio::sync::Semaphore>,
}

/// 装配触发器 HTTP 路由。
///
/// 路由分两类：`/hooks/{id}` 是管理面（管理令牌），`/invoke` 与
/// `/invocations` 是业务面（业务密钥 → 每 run 能力令牌）。两类的鉴权
/// 强度不同，CORS 头也只对业务面开放（前端直接调业务面）。
pub fn router(backend: Arc<JournalBackend>, token: String) -> Router {
    Router::new()
        .route("/hooks/{id}", post(hook))
        .route("/invoke/{id}", post(invoke).options(preflight))
        .route(
            "/invocations/{id}",
            get(invocation_result).options(preflight),
        )
        .route(
            "/invocations/{id}/events",
            get(invocation_events).options(preflight),
        )
        .route(
            "/invocations/{id}/values/{output}",
            get(invocation_value).options(preflight),
        )
        .route(
            "/invocations/{id}/signals/{node}",
            post(invocation_signal).options(preflight),
        )
        .route(
            "/invocations/{id}/cancel",
            post(invocation_cancel).options(preflight),
        )
        // CORS 只加在业务面路径上：管理面（/hooks）不该被浏览器页面跨域调用。
        .layer(axum::middleware::from_fn(
            |request: axum::extract::Request, next: axum::middleware::Next| async move {
                let business = request.uri().path().starts_with("/invoke/")
                    || request.uri().path().starts_with("/invocations/");
                let mut response = next.run(request).await;
                if business {
                    let headers = response.headers_mut();
                    for (key, value) in [
                        ("cache-control", "no-store"),
                        ("access-control-allow-origin", "*"),
                        ("access-control-allow-methods", "GET, POST, OPTIONS"),
                        (
                            "access-control-allow-headers",
                            "Authorization, Content-Type, Idempotency-Key, X-Correlation-Id",
                        ),
                    ] {
                        headers.insert(
                            axum::http::HeaderName::from_static(key),
                            axum::http::HeaderValue::from_static(value),
                        );
                    }
                }
                response
            },
        ))
        // 8 MiB body 上限：与 §11.5 的 webhook 载荷上限一致，超出会被
        // axum 在读进内存前挡掉。
        .layer(DefaultBodyLimit::max(8 * 1024 * 1024))
        .with_state(Arc::new(HookState {
            backend,
            token,
            downloads: Arc::new(tokio::sync::Semaphore::new(8)),
        }))
}

/// CORS 预检响应。
///
/// 固定 204 + 允许头由中间件统一设置，这里只负责让预检短路返回。
async fn preflight() -> StatusCode {
    StatusCode::NO_CONTENT
}

/// 取出 `Authorization: Bearer <token>` 中的 token 部分。
///
/// 缺失或格式不符一律返回空串——调用方拿空串去比对，失败路径与「token
/// 不符」完全一致，不泄露「头是否存在」这一信息。
fn bearer(headers: &HeaderMap) -> &str {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("")
}

/// 常时字符串相等（不按首个差异字节提前返回）。
fn equal(a: &str, b: &str) -> bool {
    a.len() == b.len() && a.bytes().zip(b.bytes()).fold(0u8, |d, (a, b)| d | (a ^ b)) == 0
}

/// 取 `Idempotency-Key` 头作为幂等键。
///
/// 上限 128 字节与 RPC 面的 request_id 口径一致；空键视为缺失——空串做
/// 去重键会让所有请求塌缩成同一次。
fn request_key(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty() && s.len() <= 128)
}

/// HMAC-SHA256 capabilities bind one run and one operation-independent namespace.
/// The management token is never sent to an invocation client.
///
/// 用管理令牌派生「每 run 一个」的窄能力令牌：业务方拿到它只能操作这一个
/// run，而管理令牌本身永不离开服务端。派生用 HMAC 而不是裸哈希，避免
/// 令牌泄露后可被反推出管理令牌。实现走 RustCrypto 的 `hmac` crate
/// （与 RFC 2104 逐字节一致，含超长密钥先摘要的规则）；输出形状由
/// `token_tests::run_token_matches_legacy_hmac_bytewise` 钉死，保证已
/// 发出的能力令牌跨版本不失效。
fn run_token(secret: &str, run: &str) -> String {
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;
    // HMAC 接受任意长度密钥，new_from_slice 对 HMAC 永远成功。
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    // 域分隔串 + run_id：能力令牌与任何其他用途的 HMAC 不共享同一密钥流。
    mac.update(b"flow.invocation.v1\0");
    mac.update(run.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// 校验请求携带的能力令牌是否匹配该 run。
fn run_authorized(state: &HookState, id: &str, headers: &HeaderMap) -> bool {
    equal(bearer(headers), &run_token(&state.token, id))
}

/// 把 journal 错误映射成 HTTP 状态码。
///
/// 映射按错误**类型**而非文案：文案会变，按文案分支会让客户端在改文案后
/// 悄悄走错分支。
fn invocation_error(error: JournalError) -> Response {
    match error {
        JournalError::Journal(flow_journal::Error::Conflict(message)) => {
            (StatusCode::CONFLICT, message).into_response()
        }
        JournalError::Journal(flow_journal::Error::Invalid(message)) => {
            (StatusCode::BAD_REQUEST, message).into_response()
        }
        JournalError::Journal(flow_journal::Error::Limit(message)) => {
            (StatusCode::TOO_MANY_REQUESTS, message).into_response()
        }
        // 其余错误（存储不可用等）一律 503：可重试，客户端应退避重试。
        _ => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

/// `POST /invoke/{id}`：业务调用面入口。
///
/// 鉴权用「业务密钥」（配置里只存哈希），而非管理令牌：调用方是外部业务，
/// 它的权限范围就是这一个 hook。幂等键取 `Idempotency-Key`，重试必须复用
/// 同一个键。
async fn invoke(
    State(state): State<Arc<HookState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let key = bearer(&headers);
    let hash = flow_journal::codec::digest(key.as_bytes());
    // 匹配 hook 配置里保存的密钥摘要：服务端不保存明文业务密钥，
    // 泄露配置不等于泄露可用的调用凭据。
    let config = state
        .backend
        .trigger_configs("webhook")
        .await
        .into_iter()
        .find(|c| c["token"] == id);
    // 密钥长度下限与摘要比对都要满足：短密钥即便摘要对上也说明配置有问题。
    if key.len() < 32
        || !config.as_ref().is_some_and(|c| {
            c["invoke_key_hash"]
                .as_str()
                .is_some_and(|expected| equal(expected, &hash))
        })
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    // 幂等键强制：没有稳定身份的调用无法去重，重试会重复创建 run。
    let Some(request) = request_key(&headers) else {
        return (StatusCode::BAD_REQUEST, "Idempotency-Key required").into_response();
    };
    // 关联 ID 缺省用幂等键：业务方没给追踪 ID 时，仍能用同一串在日志里
    // 把「调用 → run → 事件」串起来。
    let correlation = headers
        .get("x-correlation-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or(request);
    let input = if body.is_empty() {
        Value::Null
    } else {
        match serde_json::from_slice(&body) {
            Ok(v) => v,
            Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        }
    };
    match state
        .backend
        .invoke_start(&id, request, input, correlation, &hash)
        .await
    {
        Ok(receipt) => {
            // 回执里必须有 run_id：没有它就无法给出后续的结果 URL，
            // 此时宁可报 503 让调用方带同一个幂等键重试。
            let Some(id) = receipt.result["run_id"].as_str() else {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            };
            // 202：run 已受理但尚未完成。返回的 run_token 是该 run 的窄能力
            // 令牌，调用方用它读结果/事件，不再需要持有业务密钥。
            (StatusCode::ACCEPTED,Json(serde_json::json!({"run_id":id,"run_token":run_token(&state.token,id),"result_url":format!("/invocations/{id}"),"events_url":format!("/invocations/{id}/events"),"receipt":receipt}))).into_response()
        }
        Err(e) => invocation_error(e),
    }
}
/// `GET /invocations/{id}/values/{output}`：下载该 run 的公开输出值。
///
/// 能力令牌只允许下载 run 的**公开结果**；定义、预备凭据与中间值一律拒绝
/// ——它们是管理面的追踪材料，不属于「调用方该看到的东西」。
async fn invocation_value(
    State(state): State<Arc<HookState>>,
    Path((id, output)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if !run_authorized(&state, &id, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    // End users may download the public result, not definitions, prepared
    // credentials or arbitrary intermediate values from the management trace.
    match state.backend.run_snapshot(&id).await {
        Ok(Some(run)) if matches!(run.output.as_ref(),Some(flow_journal::StoredValue::Ref(v)) if v.output_id==output) =>
            {}
        // 引用存在但不是 run 的公开输出 → 403（无权），而非 404。
        Ok(Some(_)) => return StatusCode::FORBIDDEN.into_response(),
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => return invocation_error(e),
    }
    crate::journal_download::download_authorized(&state.backend, &state.downloads, id, output).await
}
/// `GET /invocations/{id}`：run 的结果摘要。
///
/// 输出过大时不给内联正文，只给 `output_ref` + 下载 URL：内联大值会让
/// 响应体积失控，也会把用户数据复制进每个轮询响应里。
async fn invocation_result(
    State(state): State<Arc<HookState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if !run_authorized(&state, &id, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match state.backend.run_snapshot(&id).await {
        Ok(Some(run)) => {
            let output = run
                .output
                .as_ref()
                .map(|v| {
                    flow_journal::value::materialize(
                        state.backend.journal.root(),
                        state.backend.journal.durable_lsn(),
                        v,
                        8 * 1024 * 1024,
                    )
                })
                .transpose();
            match output{
                Ok(output)=>Json(serde_json::json!({"run_id":id,"status":run.status,"complete":run.terminal(),"output":output,"error":run.error.as_ref().map(|_|"WORKFLOW_FAILED"),"workflow_version":run.workflow_version,"correlation_id":run.execution.as_ref().and_then(|e|e.correlation_id.as_ref())})).into_response(),
                // 超过内联上限：改用引用 + 下载 URL，语义仍是「结果可得」。
                Err(flow_journal::Error::Limit(_))=>(StatusCode::PAYLOAD_TOO_LARGE,Json(serde_json::json!({"run_id":id,"status":run.status,"complete":run.terminal(),"output_ref":run.output,"download_url":run.output.as_ref().and_then(|v|match v {flow_journal::StoredValue::Ref(v)=>Some(format!("/invocations/{id}/values/{}",v.output_id)),_=>None})}))).into_response(),
                Err(_)=>StatusCode::SERVICE_UNAVAILABLE.into_response(),
            }
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => invocation_error(e),
    }
}
/// `GET /invocations/{id}/events` 的查询参数。
///
/// 游标与 after_lsn 都是字符串形态：LSN 是 u64，JSON 数字会丢精度，
/// 字符串是唯一无损的表示。
#[derive(serde::Deserialize)]
struct EventQuery {
    cursor: Option<String>,
    after_lsn: Option<String>,
}
/// `GET /invocations/{id}/events`：run 的事件流（分页）。
///
/// 能力令牌只能看到「业务可观测」的事件子集，payload 也被裁剪成安全字段：
/// 事件原文可能包含节点参数、预备凭据等管理面信息。
async fn invocation_events(
    State(state): State<Arc<HookState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<EventQuery>,
) -> Response {
    if !run_authorized(&state, &id, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    // 上界取当前 durable_lsn：保证这一页是一个一致快照，不会读到
    // 「半条事务」。
    let upper = state.backend.journal.durable_lsn();
    let cursor = match query.cursor {
        Some(raw) => match serde_json::from_str(&raw) {
            Ok(c) => c,
            Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        },
        None => {
            let mut cursor = flow_journal::page::Cursor::first(
                state.backend.journal.id().into(),
                id.clone(),
                flow_journal::page::Filter::Events,
                upper,
            );
            if let Some(raw) = query.after_lsn {
                let Ok(after) = raw.parse::<u64>() else {
                    return StatusCode::BAD_REQUEST.into_response();
                };
                // 必须是最小的十进制规范写法且不超过当前上界：否则同一个
                // after_lsn 会有多种写法，缓存与去重都对不上。
                if after.to_string() != raw || after > upper {
                    return StatusCode::BAD_REQUEST.into_response();
                }
                // 游标是「不包含下界」语义，因此从 after+1 开始。
                let Some(next) = after.checked_add(1) else {
                    return StatusCode::BAD_REQUEST.into_response();
                };
                cursor.next_lsn = next;
            }
            cursor
        }
    };
    match flow_journal::page::page(
        state.backend.journal.root(),
        state.backend.journal.id(),
        &id,
        flow_journal::page::Filter::Events,
        cursor,
        upper,
        100,
    ) {
        Ok(mut page) => {
            use flow_journal::EventKind;
            // 白名单：只保留业务进展类事件，执行器内部的审计事实
            // （含更多细节）不属于调用方可观测范围。
            page.events.retain(|v| {
                matches!(
                    v.event.kind,
                    EventKind::RunStarted
                        | EventKind::RunCompleted
                        | EventKind::RunFailed
                        | EventKind::RunCancelled
                        | EventKind::DispatchStarted
                        | EventKind::NodeCompleted
                        | EventKind::NodeFailed
                        | EventKind::NodeSkipped
                        | EventKind::WaitRegistered
                        | EventKind::WaitResolved
                        | EventKind::SignalReceived
                )
            });
            for positioned in &mut page.events {
                let event = &mut positioned.event;
                // payload 裁剪：只留下业务需要的字段，失败统一成
                // EXECUTION_FAILED（不泄露内部错误细节）。
                event.payload = match event.kind {
                    EventKind::RunStarted => {
                        serde_json::json!({"workflow_version":event.payload["workflow_version"]})
                    }
                    EventKind::WaitRegistered => {
                        serde_json::json!({"wait":{"kind":event.payload["wait"]["kind"],"wake_at":event.payload["wait"]["wake_at"]}})
                    }
                    EventKind::NodeSkipped => serde_json::json!({"reason":event.payload["reason"]}),
                    EventKind::NodeFailed | EventKind::RunFailed => {
                        serde_json::json!({"error":"EXECUTION_FAILED"})
                    }
                    _ => serde_json::json!({}),
                };
            }
            Json(page).into_response()
        }
        Err(e) => invocation_error(e.into()),
    }
}
/// `POST /invocations/{id}/signals/{node}`：向人工节点投递输入。
///
/// 只能作用于 human_task / input 节点：能力令牌不该成为驱动外部系统
/// （脚本、HTTP 调用等）的旁路——那属于管理面裁决的职责。
async fn invocation_signal(
    State(state): State<Arc<HookState>>,
    Path((id, node)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !run_authorized(&state, &id, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(request) = request_key(&headers) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let input = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    // A business capability cannot adjudicate external operations, including through run.signal's legacy alias.
    // 节点类型必须从权威 run 定义里读出来：不能信客户端声称的节点名，
    // 否则就能借信号路径绕过裁决。
    let run = match state.backend.run_snapshot(&id).await {
        Ok(Some(r)) => r,
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    let definition = match flow_journal::value::materialize(
        state.backend.journal.root(),
        state.backend.journal.durable_lsn(),
        &run.definition,
        flow_journal::MAX_LINE_BYTES,
    )
    .and_then(|v| flow_engine::journal_state::validate_definition(&v))
    {
        Ok(d) => d,
        Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };
    if !definition.node_type(&node).is_some_and(|k| {
        matches!(
            k,
            flow_engine::NodeType::HumanTask | flow_engine::NodeType::Input
        )
    }) {
        return StatusCode::FORBIDDEN.into_response();
    }
    match state
        .backend
        .run_signal(&id, &node, input, Some(request))
        .await
    {
        Ok(r) => Json(r).into_response(),
        Err(e) => invocation_error(e),
    }
}
/// `POST /invocations/{id}/cancel`：取消该 run（级联取消子 run）。
async fn invocation_cancel(
    State(state): State<Arc<HookState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if !run_authorized(&state, &id, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    // 取消同样要求幂等键：重复取消要能被识别为同一次操作。
    let Some(request) = request_key(&headers) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    match state.backend.run_cancel(&id, Some(request)).await {
        Ok(r) => Json(r).into_response(),
        Err(e) => invocation_error(e),
    }
}
/// `POST /hooks/{id}`：webhook 触发面（管理令牌）。
///
/// 幂等键由调用方给出；没有它就不受理——否则一次网络重试就会多跑一次
/// workflow。
async fn hook(
    State(state): State<Arc<HookState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // 管理令牌与 RPC 面同一个 token：单用户凭据，不是多租户授权。
    // 常时比较复用 equal()：安全原语在本文件只许有一份实现。
    if !equal(bearer(&headers), &state.token) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(request) = headers
        .get("idempotency-key")
        .and_then(|h| h.to_str().ok())
        .filter(|s| !s.is_empty() && s.len() <= 128)
    else {
        return (StatusCode::BAD_REQUEST, "Idempotency-Key required").into_response();
    };
    let input = if body.is_empty() {
        Value::Null
    } else {
        match serde_json::from_slice(&body) {
            Ok(v) => v,
            Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        }
    };
    match state
        .backend
        .trigger_start("webhook", &id, request, input)
        .await
    {
        Ok(receipt) => Json(receipt).into_response(),
        Err(error) => invocation_error(error),
    }
}

#[cfg(test)]
mod token_tests {
    use super::*;

    /// 换用 hmac crate 前的手写参考实现（RFC 2104 构造，域分隔串与消息
    /// 布局与线上格式一致）。保留在这里钉死 run_token 的输出字节：已发给
    /// 调用方的能力令牌跨版本必须继续有效，任何改动这个断言的提交都是在
    /// 吊销在途凭据。
    fn legacy_run_token(secret: &str, run: &str) -> String {
        use sha2::{Digest, Sha256};
        let mut key = [0u8; 64];
        let raw = secret.as_bytes();
        if raw.len() > 64 {
            key[..32].copy_from_slice(&Sha256::digest(raw));
        } else {
            key[..raw.len()].copy_from_slice(raw);
        }
        let mut inner = Sha256::new();
        inner.update(key.map(|b| b ^ 0x36));
        inner.update(b"flow.invocation.v1\0");
        inner.update(run.as_bytes());
        let mut outer = Sha256::new();
        outer.update(key.map(|b| b ^ 0x5c));
        outer.update(inner.finalize());
        hex::encode(outer.finalize())
    }

    /// 密钥长度覆盖三档（短 / 恰好 64 字节块 / 超长先摘要），与 hmac crate
    /// 逐字节一致；不同 run 派生不同令牌。
    #[test]
    fn run_token_matches_legacy_hmac_bytewise() {
        let keys = [
            "short-secret-value".to_string(),
            "e".repeat(64),
            "z".repeat(200),
        ];
        for secret in &keys {
            for run in ["r1", "run-01923f4e-uuid"] {
                assert_eq!(run_token(secret, run), legacy_run_token(secret, run));
            }
        }
        assert_ne!(run_token("s", "a"), run_token("s", "b"));
        assert_ne!(run_token("s", "a"), run_token("t", "a"));
    }
}
