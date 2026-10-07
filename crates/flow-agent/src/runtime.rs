//! agent 上联运行时（三期 §1.4/§1.6）：Ready → Suspect → Reconnecting →
//! Reconciling → Ready/Closed。
//!
//! 上联任一通道失效 → Suspect：停止新绑定；本地执行器保留（等待 ack 的
//! 自然阻塞即"冻结"）。重连只恢复通信与对账能力，不恢复旧派发的执行权
//! ——由主进程 ResumeReply 裁决后按裁决继续/取消。drain 停止接新派发、
//! 取消在飞、转发最后确认后退出。

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use flow_engine::execution_protocol::contract::{ExecutorInvocation, DRAIN_GRACE_MS};
use flow_engine::execution_protocol::message::Message;
use flow_engine::execution_protocol::remote::{ResumeAction, ResumeItem, RESUME_PAGE_MAX};
use flow_engine::execution_protocol::transport::FrameTransport;

use crate::pool::{self, ExecutorHandle};
use crate::relay::{Relay, RelayError, UPLINK_OUT_QUEUE};
use crate::tls;

pub struct AgentConfig {
    pub agent_id: String,
    pub control_addr: String,
    pub data_addr: String,
    pub ca_cert: PathBuf,
    pub cert: PathBuf,
    pub key: PathBuf,
    /// 执行器召唤方式：合并二进制自召唤（缺省）或显式独立二进制。
    pub executor: ExecutorInvocation,
    pub slots: u32,
}

/// agent 运行入口：循环「连接 → 服务 → 断线 → 退避重连」，直到 shutdown。
/// 返回进程退出原因。
pub async fn run_agent(config: AgentConfig, shutdown: CancellationToken) -> String {
    let agent_boot_id = flow_engine::execution_protocol::transport::fresh_boot_id();
    let executor_handles: Arc<tokio::sync::Mutex<BTreeMap<String, ExecutorHandle>>> =
        Arc::new(tokio::sync::Mutex::new(BTreeMap::new()));
    // 上联转发管道常驻：断线只换 serve 会话的 transport，执行器与桥不拆
    //（Suspect 语义——本地执行器保留，等 Resume 裁决）。relay 槽随会话换新。
    let relay_slot: Arc<parking_lot::Mutex<Arc<parking_lot::Mutex<Relay>>>> = Arc::new(
        parking_lot::Mutex::new(Arc::new(parking_lot::Mutex::new(Relay::new(
            config.agent_id.clone(),
            agent_boot_id.clone(),
            String::new(),
        )))),
    );
    let mut uplink_rx = spawn_uplink_pipeline(executor_handles.clone(), relay_slot.clone());
    let mut backoff = Duration::from_millis(500);
    loop {
        if shutdown.is_cancelled() {
            cancel_all(&executor_handles).await;
            return "shutdown".into();
        }
        // Reconnecting：建立双 TLS + 握手 + DataBind。
        let connection = tokio::select! {
            _ = shutdown.cancelled() => {cancel_all(&executor_handles).await; return "shutdown".into();},
            result = tokio::time::timeout(Duration::from_secs(15), connect(&config, &agent_boot_id)) => result.unwrap_or_else(|_| Err("agent connect timeout".into())),
        };
        match connection {
            Ok((mut transport, link_session_id, journal_id, master_epoch)) => {
                tracing::info!(%link_session_id, "agent uplink ready");
                backoff = Duration::from_millis(500);
                let relay = Arc::new(parking_lot::Mutex::new(Relay::new(
                    config.agent_id.clone(),
                    agent_boot_id.clone(),
                    link_session_id.clone(),
                )));
                *relay_slot.lock() = relay.clone();
                // Reconciling：上报 Resume 清单（分页），应用裁决。
                if !executor_handles.lock().await.is_empty() {
                    if let Err(error) = resume(&mut transport, &relay, &executor_handles).await {
                        tracing::warn!(%error, "resume failed; cancelling local executors");
                        cancel_all(&executor_handles).await;
                        transport.close();
                        continue;
                    }
                }
                // Ready：转发循环（断线返回）。
                let reason = serve(
                    &config,
                    &agent_boot_id,
                    &link_session_id,
                    &journal_id,
                    master_epoch,
                    transport,
                    relay,
                    executor_handles.clone(),
                    &mut uplink_rx.rx,
                    shutdown.clone(),
                )
                .await;
                if reason == "drained" {
                    // drain 是运维主动下线：不再重连。
                    return reason;
                }
                if shutdown.is_cancelled() {
                    return "shutdown".into();
                }
                tracing::warn!(%reason, "agent uplink lost; entering Suspect/Reconnecting");
            }
            Err(error) => {
                tracing::warn!(%error, "agent connect failed; retrying with backoff");
            }
        }
        tokio::select! { _ = shutdown.cancelled() => {}, _ = tokio::time::sleep(backoff) => {} }
        backoff = (backoff * 2).min(Duration::from_secs(15));
    }
}

/// 建立上联：control TLS → AgentHello → AgentWelcome → data TLS → DataBind。
async fn connect(
    config: &AgentConfig,
    agent_boot_id: &str,
) -> std::result::Result<(FrameTransport, String, String, u64), String> {
    let connector = tls::client_config_from_files(&config.ca_cert, &config.cert, &config.key)
        .map_err(|e| format!("tls config: {e}"))?;
    // control
    let control_stream = tokio::net::TcpStream::connect(&config.control_addr)
        .await
        .map_err(|e| format!("control connect: {e}"))?;
    let mut control = connector
        .connect(
            rustls::pki_types::ServerName::try_from("flow-server".to_string())
                .map_err(|e| format!("sni: {e}"))?,
            control_stream,
        )
        .await
        .map_err(|e| format!("control tls: {e}"))?;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    // 简化握手：control 首帧 AgentHello（复用帧编解码器直接写）。
    let hello = Message::AgentHello {
        agent_boot_id: agent_boot_id.to_string(),
        build: crate::AGENT_BUILD.into(),
        capabilities: crate::AGENT_CAPABILITIES
            .iter()
            .map(|s| s.to_string())
            .collect(),
        slots: config.slots,
    };
    let frame = flow_engine::execution_protocol::frame::encode_frame(&hello)
        .map_err(|e| format!("encode hello: {e}"))?;
    control
        .write_all(&frame)
        .await
        .map_err(|e| format!("send hello: {e}"))?;
    // 读 Welcome（裸帧）。
    let mut decoder = flow_engine::execution_protocol::frame::FrameDecoder::new(
        flow_engine::execution_protocol::CONTROL_MAX_FRAME,
    );
    let welcome = loop {
        let mut chunk = [0u8; 16 * 1024];
        let n = control
            .read(&mut chunk)
            .await
            .map_err(|e| format!("read welcome: {e}"))?;
        if n == 0 {
            return Err("control closed before welcome".into());
        }
        decoder
            .feed(&chunk[..n])
            .map_err(|e| format!("welcome frame: {e}"))?;
        if let Some(json) = decoder.pop_frame().map_err(|e| e.to_string())? {
            let value: serde_json::Value =
                serde_json::from_slice(&json).map_err(|e| e.to_string())?;
            break Message::from_envelope(value).map_err(|e| e.to_string())?;
        }
    };
    let Message::AgentWelcome {
        journal_id,
        master_epoch,
        link_session_id,
        data_credential,
        ..
    } = welcome
    else {
        return Err("expected AgentWelcome".into());
    };
    // journal_id / master_epoch 是权威身份：执行器握手与 ValueRef 盖章都以它
    // 为准（主进程 reducer 会拒绝 journal_id 不符的发布）。
    // data
    let data_stream = tokio::net::TcpStream::connect(&config.data_addr)
        .await
        .map_err(|e| format!("data connect: {e}"))?;
    let data = connector
        .connect(
            rustls::pki_types::ServerName::try_from("flow-server".to_string())
                .map_err(|e| format!("sni: {e}"))?,
            data_stream,
        )
        .await
        .map_err(|e| format!("data tls: {e}"))?;
    let mut transport = FrameTransport::spawn_streams(control, data, 128);
    // 会话建立（发 Welcome 已收，data 绑定）。
    let link = handshake(
        &mut transport,
        agent_boot_id,
        &link_session_id,
        data_credential,
    )
    .await?;
    Ok((transport, link, journal_id, master_epoch))
}

/// 上联会话建立（R0 in-process 复用）：发 DataBind 完成双通道绑定。
pub async fn handshake(
    transport: &mut FrameTransport,
    agent_boot_id: &str,
    link_session_id: &str,
    data_credential: String,
) -> std::result::Result<String, String> {
    transport
        .send(Message::DataBind {
            agent_boot_id: agent_boot_id.to_string(),
            link_session_id: link_session_id.to_string(),
            credential: data_credential,
        })
        .await
        .map_err(|e| format!("data bind: {e}"))?;
    Ok(link_session_id.to_string())
}

/// 对账：分页上报 Resume 清单并应用裁决（取消项回收本地执行器）。
pub async fn resume(
    transport: &mut FrameTransport,
    relay: &Arc<parking_lot::Mutex<Relay>>,
    handles: &Arc<tokio::sync::Mutex<BTreeMap<String, ExecutorHandle>>>,
) -> std::result::Result<(), String> {
    // Freeze pagination before applying decisions, which may remove handles.
    let snapshot: Vec<(String, ResumeItem)> = {
        let handles = handles.lock().await;
        // 重挂归属：新 link 会话的 relay 重新登记本地绑定（内层执行器
        // 消息才能按已确认路由包装）。
        for (dispatch, handle) in handles.iter() {
            relay
                .lock()
                .bind(dispatch, &handle.executor_id, &handle.executor_boot_id);
        }
        let mut facts = Vec::new();
        for (dispatch, handle) in handles.iter() {
            let result = handle.result.lock().await.clone();
            facts.push((
                dispatch.clone(),
                ResumeItem {
                    dispatch_id: dispatch.clone(),
                    executor_boot_id: handle.executor_boot_id.clone(),
                    state: if result.is_some() {
                        "result_ready".into()
                    } else {
                        "executing".into()
                    },
                    durable_audit_seq: handle.last_acked.load(Ordering::Relaxed),
                    result_id: result.as_ref().map(|(id, _)| id.clone()),
                    result_last_audit_seq: result.as_ref().map(|(_, seq)| *seq),
                    lost_data: handle.lost_data.load(Ordering::Relaxed),
                    forced_kill: handle.forced_kill.load(Ordering::Relaxed),
                },
            ));
        }
        facts
    };
    let mut page = 0u32;
    loop {
        let start = page as usize * RESUME_PAGE_MAX;
        let end = (start + RESUME_PAGE_MAX).min(snapshot.len());
        let more = end < snapshot.len();
        transport
            .send(Message::Resume {
                items: snapshot[start..end]
                    .iter()
                    .map(|(_, item)| item.clone())
                    .collect(),
                page,
                more,
            })
            .await
            .map_err(|e| format!("send resume: {e}"))?;
        let reply = loop {
            let (_, message) = transport
                .recv()
                .await
                .map_err(|e| format!("resume reply: {e}"))?;
            if let Message::ResumeReply { .. } = message {
                break message;
            }
        };
        if let Message::ResumeReply {
            decisions,
            page: reply_page,
        } = reply
        {
            debug_assert_eq!(reply_page, page);
            for decision in &decisions {
                if matches!(
                    decision.action,
                    ResumeAction::AlreadyCommitted
                        | ResumeAction::CancelAndDrain
                        | ResumeAction::ReconcileRequired
                ) {
                    if let Some(handle) = handles.lock().await.get(&decision.dispatch_id) {
                        if decision.action == ResumeAction::AlreadyCommitted {
                            handle.committed.store(true, Ordering::Release);
                        }
                        handle.cancel.cancel();
                    }
                    relay.lock().unbind(&decision.dispatch_id);
                    handles.lock().await.remove(&decision.dispatch_id);
                }
            }
        }
        page += 1;
        if !more {
            return Ok(());
        }
    }
}

async fn cancel_all(handles: &Arc<tokio::sync::Mutex<BTreeMap<String, ExecutorHandle>>>) {
    for handle in handles.lock().await.values() {
        handle.cancel.cancel();
    }
    handles.lock().await.clear();
}

/// Ready 状态转发服务：断线时返回原因（执行器保留等对账）。
///
/// 上联管道（桥 + relay 包装）由调用方常驻持有，`uplink_rx` 跨会话复用；
/// 本函数只消费当前 transport。`journal_id` / `master_epoch` 来自本会话
/// Welcome，BindExecutor 时下发给执行器握手（ValueRef 盖章身份）。
///
/// `_agent_boot_id` / `_link_session_id` 由调用方在别处消费（relay 槽换新），
/// 这里保留形状签名避免两套入口分流。
#[allow(clippy::too_many_arguments)]
pub async fn serve(
    config: &AgentConfig,
    _agent_boot_id: &str,
    _link_session_id: &str,
    journal_id: &str,
    master_epoch: u64,
    mut transport: FrameTransport,
    relay: Arc<parking_lot::Mutex<Relay>>,
    handles: Arc<tokio::sync::Mutex<BTreeMap<String, ExecutorHandle>>>,
    uplink_rx: &mut mpsc::Receiver<Message>,
    shutdown: CancellationToken,
) -> String {
    let mut draining = None;
    let mut pending: Option<(mpsc::Sender<Message>, Message)> = None;
    let mut housekeeping = tokio::time::interval(Duration::from_millis(20));
    loop {
        tokio::select! {
            permit = async { pending.as_ref().unwrap().0.clone().reserve_owned().await }, if pending.is_some() => {
                let (_, message) = pending.take().unwrap();
                match permit {
                    Ok(permit) => { permit.send(message); }
                    Err(_) => return "executor inbox closed".into(),
                }
            }
            _ = housekeeping.tick() => {
                let mut guard = handles.lock().await;
                let finished: Vec<_> = guard.iter().filter(|(_, handle)| handle.done.load(Ordering::Acquire)
                    && (handle.committed.load(Ordering::Acquire) || handle.forced_kill.load(Ordering::Relaxed)))
                    .map(|(id, _)| id.clone()).collect();
                for id in finished {
                    guard.remove(&id);
                    relay.lock().unbind(&id);
                }
                let all_done = guard.values().all(|handle| handle.done.load(Ordering::Acquire));
                let count = guard.len() as u32;
                drop(guard);
                if draining.is_some_and(|deadline| all_done || tokio::time::Instant::now() >= deadline) {
                    cancel_all(&handles).await;
                    let _ = transport.send(Message::DrainComplete { drained: count }).await;
                    transport.flush_and_close(Duration::from_millis(DRAIN_GRACE_MS)).await;
                    return "drained".into();
                }
            }
            _ = shutdown.cancelled() => {
                cancel_all(&handles).await;
                transport.close();
                return "shutdown".into();
            }
            outbound = uplink_rx.recv() => {
                match outbound {
                    Some(message) => {
                        // Queued frames may have been wrapped before reconnection.
                        let message = if let Message::Routed { inner, .. } = message {
                            let Some(id) = inner.dispatch_id().map(str::to_owned) else {continue};
                            match relay.lock().wrap_to_master(&id, *inner) { Ok(message) => message, Err(RelayError::Unbound(_)) => continue, Err(error) => return error.to_string() }
                        } else { message };
                        if transport.send(message).await.is_err() {
                            tracing::info!("agent serve exiting: uplink send failed");
                            return "uplink send failed".into();
                        }
                    }
                    None => {
                        tracing::info!("agent serve exiting: uplink outbound closed");
                        return "uplink outbound closed".into();
                    }
                }
            }
            received = transport.recv(), if pending.is_none() => {
                match received {
                    Ok((_channel, message)) => match message {
                        Message::Routed { .. } => {
                            let unwrapped = relay.lock().unwrap_from_master(&message);
                            match unwrapped {
                                Ok((dispatch_id, inner)) => {
                                    let handles_guard = handles.lock().await;
                                    if let Some(handle) = handles_guard.get(&dispatch_id) {
                                        if matches!(inner, Message::Cancel { .. }) { handle.cancel.cancel(); }
                                        else {
                                            if matches!(inner, Message::ResultCommitted { .. }) {handle.committed.store(true, Ordering::Release);}
                                            pending = Some((handle.inbox.clone(), inner)); }
                                    }
                                }
                                Err(RelayError::Unbound(_)) => {
                                    // 错路由：明确拒绝（不静默执行）。
                                    let _ = transport.try_send(Message::Reject {
                                        reason: "routed frame for unbound dispatch".into(),
                                    });
                                }
                                Err(error) => {
                                    let _ = transport.try_send(Message::Reject {
                                        reason: format!("{error}"),
                                    });
                                    return format!("route verify failed: {error}");
                                }
                            }
                        }
                        Message::BindExecutor { executor_id, dispatch_id } => {
                            if draining.is_some() || handles.lock().await.values().filter(|handle| !handle.done.load(Ordering::Acquire) && !handle.committed.load(Ordering::Acquire)).count() >= config.slots as usize {
                                if transport.send(Message::BindExecutorAck { executor_id, executor_boot_id: String::new(), dispatch_id, ok: false, error: Some("agent draining or at capacity".into()) }).await.is_err() { return "bind reject send failed".into(); }
                                continue;
                            }
                            let bound = pool::bind_executor(
                                &config.executor,
                                journal_id,
                                master_epoch,
                                &relay,
                                &executor_id,
                                &dispatch_id,
                            )
                            .await;
                            match bound {
                                Ok(handle) => {
                                    let boot = handle.executor_boot_id.clone();
                                    handles.lock().await.insert(dispatch_id.clone(), handle);
                                    let sent = transport.send(Message::BindExecutorAck {
                                        executor_id,
                                        executor_boot_id: boot,
                                        dispatch_id,
                                        ok: true,
                                        error: None,
                                    }).await;
                                    if sent.is_err() { return "bind acknowledgement send failed".into(); }
                                }
                                Err(error) => {
                                    let sent = transport.send(Message::BindExecutorAck {
                                        executor_id,
                                        executor_boot_id: String::new(),
                                        dispatch_id,
                                        ok: false,
                                        error: Some(error.to_string()),
                                    }).await;
                                    if sent.is_err() { return "bind acknowledgement send failed".into(); }
                                }
                            }
                        }
                        Message::Drain { grace_ms } => {
                            draining = Some(tokio::time::Instant::now() + Duration::from_millis(grace_ms.min(DRAIN_GRACE_MS)));
                        }
                        Message::ResumeReply { decisions, .. } => {
                            for decision in decisions {
                                if matches!(
                                    decision.action,
                                    ResumeAction::CancelAndDrain | ResumeAction::ReconcileRequired
                                ) {
                                    if let Some(handle) = handles.lock().await.get(&decision.dispatch_id) {
                                        handle.cancel.cancel();
                                    }
                                    relay.lock().unbind(&decision.dispatch_id);
                                    handles.lock().await.remove(&decision.dispatch_id);
                                }
                            }
                        }
                        Message::CapacityReport { .. } | Message::Heartbeat { .. } => {}
                        Message::Reject { reason } => {
                            tracing::warn!(%reason, "master rejected");
                        }
                        other => {
                            let reason = format!("unexpected master message {}", other.type_name());
                            tracing::info!(%reason, "agent serve exiting");
                            return reason;
                        }
                    }
                    Err(error) => {
                        let reason = format!("uplink recv failed: {error}");
                        tracing::info!(%reason, "agent serve exiting");
                        return reason;
                    }
                }
            }
        }
    }
}

/// fair_mux 的动态桥：把各执行器出站通道聚合成单流（轮转公平）。
///
/// 游标 = 上一轮服务的 dispatch；每轮从游标之后开始整圈扫描，取到一帧
/// 即更新游标——洪泛执行器不能靠 BTreeMap 排序靠前独占共享上联
/// （与 relay::fair_mux 的游标语义一致；有对应洪泛回归测试）。
async fn fair_mux_bridge(
    handles: Arc<tokio::sync::Mutex<BTreeMap<String, ExecutorHandle>>>,
    mux_in: mpsc::Sender<Message>,
) {
    use std::ops::Bound;
    let mut cursor: Option<String> = None;
    loop {
        let mut served: Option<String> = None;
        let mut outgoing = None;
        if mux_in.is_closed() {
            return;
        }
        {
            let mut guard = handles.lock().await;
            for pass in 0..2 {
                let range: (Bound<String>, Bound<String>) = match (&cursor, pass) {
                    (None, _) => (Bound::Unbounded, Bound::Unbounded),
                    // 整圈 = 「游标之后」+「从头到游标（含自身）」。
                    // 第二趟必须 Included：Excluded 会把游标键排除在两趟之外，
                    // 单执行器场景一条消息之后即永久饿死。
                    (Some(key), 0) => (Bound::Excluded(key.clone()), Bound::Unbounded),
                    (Some(key), _) => (Bound::Unbounded, Bound::Included(key.clone())),
                };
                for (dispatch, handle) in guard.range_mut(range) {
                    if let Ok(message) = handle.out.try_recv() {
                        outgoing = Some(message);
                        served = Some(dispatch.clone());
                        break;
                    }
                }
                if served.is_some() || cursor.is_none() {
                    break;
                }
            }
        }
        if let Some(message) = outgoing {
            if mux_in.send(message).await.is_err() {
                return;
            }
        }
        if served.is_some() {
            cursor = served;
        } else {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

/// 上联转发管道：执行器出站 → 轮转桥 → relay 包装 → uplink 队列。
/// 返回给 serve 会话消费的出站端；桥与包装任务随管道常驻（断线不拆），
/// relay 槽里的会话身份由调用方在重连时换新。
struct UplinkPipeline {
    rx: mpsc::Receiver<Message>,
    tasks: [tokio::task::JoinHandle<()>; 2],
}
impl Drop for UplinkPipeline {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

fn spawn_uplink_pipeline(
    handles: Arc<tokio::sync::Mutex<BTreeMap<String, ExecutorHandle>>>,
    relay_slot: Arc<parking_lot::Mutex<Arc<parking_lot::Mutex<Relay>>>>,
) -> UplinkPipeline {
    let (uplink_out, uplink_rx) = mpsc::channel::<Message>(UPLINK_OUT_QUEUE);
    let (mux_in, mux_in_rx) = mpsc::channel::<Message>(UPLINK_OUT_QUEUE);
    let bridge = tokio::spawn(fair_mux_bridge(handles, mux_in));
    let wrapper = tokio::spawn(async move {
        // mux → 包络 → uplink_out（按 dispatch 包装，relay 取当前会话那份）。
        let mut mux_in_rx = mux_in_rx;
        while let Some(message) = mux_in_rx.recv().await {
            let Some(dispatch_id) = message.dispatch_id().map(str::to_owned) else {
                continue;
            };
            let relay = relay_slot.lock().clone();
            let wrapped = relay.lock().wrap_to_master(&dispatch_id, message);
            match wrapped {
                Ok(frame) => {
                    if uplink_out.send(frame).await.is_err() {
                        return;
                    }
                }
                Err(RelayError::Unbound(_)) => continue,
                Err(error) => {
                    tracing::warn!(%error, "wrap failed");
                }
            }
        }
    });
    UplinkPipeline {
        rx: uplink_rx,
        tasks: [bridge, wrapper],
    }
}

/// 测试/进程内入口：跳过 TLS 建流，直接进入服务循环（R0）。
/// 重连场景（R2）：handles 非空时先发送 Resume 清单并应用裁决。
/// `journal_id` / `master_epoch` 必须与主进程 AgentManager 一致——进程内
/// 测试同样走「执行器 ValueRef 盖章身份会被 reducer 校验」的真路径。
pub async fn serve_for_tests(
    config: &AgentConfig,
    journal_id: &str,
    master_epoch: u64,
    transport: FrameTransport,
    relay: Arc<parking_lot::Mutex<Relay>>,
    handles: Arc<tokio::sync::Mutex<BTreeMap<String, ExecutorHandle>>>,
    shutdown: CancellationToken,
) -> String {
    let relay_slot = Arc::new(parking_lot::Mutex::new(relay.clone()));
    let mut uplink_rx = spawn_uplink_pipeline(handles.clone(), relay_slot.clone());
    let session_relay: Arc<parking_lot::Mutex<Relay>> = relay_slot.lock().clone();
    serve(
        config,
        "test-boot",
        "test-link",
        journal_id,
        master_epoch,
        transport,
        session_relay,
        handles,
        &mut uplink_rx.rx,
        shutdown,
    )
    .await
}

/// 重连会话（R2 测试/进程内）：AgentHello → Welcome → DataBind →
/// Resume（若有在飞）→ serve。返回 (link_session_id, 退出原因)。
pub async fn reconnect_session(
    config: &AgentConfig,
    mut transport: FrameTransport,
    agent_boot_id: &str,
    slots: u32,
    handles: Arc<tokio::sync::Mutex<BTreeMap<String, ExecutorHandle>>>,
    shutdown: CancellationToken,
) -> (String, String) {
    // 握手（与二进制 connect 相同序列）。
    if transport
        .send(Message::AgentHello {
            agent_boot_id: agent_boot_id.to_string(),
            build: crate::AGENT_BUILD.into(),
            capabilities: crate::AGENT_CAPABILITIES
                .iter()
                .map(|s| s.to_string())
                .collect(),
            slots,
        })
        .await
        .is_err()
    {
        return (String::new(), "send hello failed".into());
    }
    let welcome = loop {
        match transport.recv().await {
            Ok((_, message)) => {
                if let Message::AgentWelcome { .. } = message {
                    break message;
                }
            }
            Err(e) => return (String::new(), format!("welcome: {e}")),
        }
    };
    let Message::AgentWelcome {
        journal_id,
        master_epoch,
        link_session_id,
        data_credential,
        ..
    } = welcome
    else {
        return (String::new(), "expected welcome".into());
    };
    if transport
        .send(Message::DataBind {
            agent_boot_id: agent_boot_id.to_string(),
            link_session_id: link_session_id.clone(),
            credential: data_credential,
        })
        .await
        .is_err()
    {
        return (link_session_id, "send data bind failed".into());
    }
    let relay = Arc::new(parking_lot::Mutex::new(Relay::new(
        config.agent_id.clone(),
        agent_boot_id.to_string(),
        link_session_id.clone(),
    )));
    if !handles.lock().await.is_empty() {
        if let Err(error) = resume(&mut transport, &relay, &handles).await {
            tracing::warn!(%error, "resume failed in reconnect_session");
        }
    }
    let relay_slot = Arc::new(parking_lot::Mutex::new(relay.clone()));
    let mut uplink_rx = spawn_uplink_pipeline(handles.clone(), relay_slot);
    let reason = serve(
        config,
        agent_boot_id,
        &link_session_id,
        &journal_id,
        master_epoch,
        transport,
        relay,
        handles,
        &mut uplink_rx.rx,
        shutdown,
    )
    .await;
    (link_session_id, reason)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::ExecutorHandle;
    use crate::relay::PER_EXECUTOR_QUEUE;
    use std::sync::atomic::{AtomicBool, AtomicU64};

    fn handle(dispatch: &str) -> (ExecutorHandle, mpsc::Sender<Message>) {
        let (inbox, _inbox_rx) = mpsc::channel(64);
        let (out_tx, out) = mpsc::channel(PER_EXECUTOR_QUEUE);
        (
            ExecutorHandle {
                dispatch_id: dispatch.into(),
                executor_id: format!("exec-{dispatch}"),
                executor_boot_id: format!("boot-{dispatch}"),
                inbox,
                out,
                cancel: CancellationToken::new(),
                last_acked: Arc::new(AtomicU64::new(0)),
                result: Arc::new(tokio::sync::Mutex::new(None)),
                forced_kill: Arc::new(AtomicBool::new(false)),
                lost_data: Arc::new(AtomicBool::new(false)),
                done: Arc::new(AtomicBool::new(false)),
                committed: Arc::new(AtomicBool::new(false)),
            },
            out_tx,
        )
    }

    fn beat(dispatch: &str) -> Message {
        Message::Heartbeat {
            dispatch_id: Some(dispatch.into()),
            phase: "test".into(),
        }
    }

    /// 回归（轮转区间）：第二趟回绕必须**含游标自身**——两趟都用 Excluded
    /// 时，单执行器在第一条消息后永久饿死；BTreeMap 头部执行器洪泛时，
    /// 排在其后的执行器也必须每轮拿到发送机会。
    #[tokio::test]
    async fn bridge_round_robins_under_flood_and_single_executor_not_starved() {
        let handles: Arc<tokio::sync::Mutex<BTreeMap<String, ExecutorHandle>>> =
            Arc::new(tokio::sync::Mutex::new(BTreeMap::new()));
        let (a, a_tx) = handle("a");
        let (b, b_tx) = handle("b");
        handles.lock().await.insert("a".into(), a);
        handles.lock().await.insert("b".into(), b);
        let (mux_in, mut mux_rx) = mpsc::channel::<Message>(256);
        let bridge = tokio::spawn(fair_mux_bridge(handles, mux_in));

        // 洪泛：a 塞满自己的出站队列；b 只发一条。
        for _ in 0..PER_EXECUTOR_QUEUE {
            a_tx.send(beat("a")).await.unwrap();
        }
        b_tx.send(beat("b")).await.unwrap();

        // 前 8 条里必须出现 b 的帧（洪泛不能独占）。
        let mut saw_b = false;
        for _ in 0..8 {
            let message = tokio::time::timeout(Duration::from_secs(2), mux_rx.recv())
                .await
                .expect("bridge stalled")
                .expect("bridge exited");
            if matches!(&message, Message::Heartbeat { dispatch_id: Some(d), .. } if d == "b") {
                saw_b = true;
                break;
            }
        }
        assert!(saw_b, "洪泛执行器 a 独占了上联，b 被饿死");

        // 单执行器场景：b 的队列再塞两条，必须都被取走（游标回绕含自身）。
        b_tx.send(beat("b")).await.unwrap();
        b_tx.send(beat("b")).await.unwrap();
        for _ in 0..2 {
            tokio::time::timeout(Duration::from_secs(2), mux_rx.recv())
                .await
                .expect("single executor starved after cursor wrap")
                .expect("bridge exited");
        }
        bridge.abort();
    }
    #[tokio::test]
    async fn resume_pages_survive_removal_and_committed_handles_are_reaped() {
        let handles = Arc::new(tokio::sync::Mutex::new(BTreeMap::new()));
        let mut committed = Vec::new();
        let mut cancels = Vec::new();
        for index in 0..RESUME_PAGE_MAX + 3 {
            let id = format!("d-{index:05}");
            let (handle, _out) = handle(&id);
            committed.push(handle.committed.clone());
            cancels.push(handle.cancel.clone());
            handles.lock().await.insert(id, handle);
        }
        let relay = Arc::new(parking_lot::Mutex::new(Relay::new(
            "agent".into(),
            "boot".into(),
            "session".into(),
        )));
        let (ca, cb) = tokio::io::duplex(1024 * 1024);
        let (da, db) = tokio::io::duplex(1024 * 1024);
        let mut agent = FrameTransport::spawn_streams(ca, da, 16);
        let mut master = FrameTransport::spawn_streams(cb, db, 16);
        let server = tokio::spawn(async move {
            let mut reported = std::collections::BTreeSet::new();
            loop {
                let (_, message) = master.recv().await.unwrap();
                let Message::Resume { items, page, more } = message else {
                    panic!("expected resume")
                };
                for item in &items {
                    assert!(reported.insert(item.dispatch_id.clone()));
                }
                master
                    .send(Message::ResumeReply {
                        page,
                        decisions: items
                            .into_iter()
                            .map(
                                |item| flow_engine::execution_protocol::remote::ResumeDecision {
                                    dispatch_id: item.dispatch_id,
                                    action: ResumeAction::AlreadyCommitted,
                                },
                            )
                            .collect(),
                    })
                    .await
                    .unwrap();
                if !more {
                    break;
                }
            }
            reported.len()
        });
        tokio::time::timeout(Duration::from_secs(2), resume(&mut agent, &relay, &handles))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(server.await.unwrap(), RESUME_PAGE_MAX + 3);
        assert!(handles.lock().await.is_empty());
        assert!(committed.iter().all(|flag| flag.load(Ordering::Acquire)));
        assert!(cancels.iter().all(CancellationToken::is_cancelled));
    }

    #[tokio::test]
    async fn saturated_uplink_does_not_hold_executor_map_lock() {
        let handles = Arc::new(tokio::sync::Mutex::new(BTreeMap::new()));
        let (entry, source) = handle("a");
        handles.lock().await.insert("a".into(), entry);
        let (sink, mut received) = mpsc::channel(1);
        sink.send(Message::Ready).await.unwrap();
        source.send(Message::Ready).await.unwrap();
        let bridge = tokio::spawn(fair_mux_bridge(handles.clone(), sink));
        // Let the bridge consume the frame and encounter outbound backpressure.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let guard = tokio::time::timeout(Duration::from_millis(200), handles.lock())
            .await
            .expect("backpressure must not block cancel/drain map access");
        assert!(guard["a"].out.is_empty());
        drop(guard);
        received.recv().await.unwrap();
        assert!(matches!(received.recv().await, Some(Message::Ready)));
        bridge.abort();
    }
}
