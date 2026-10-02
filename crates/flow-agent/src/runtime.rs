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
    pub executor_bin: PathBuf,
    pub slots: u32,
}

/// agent 运行入口：循环「连接 → 服务 → 断线 → 退避重连」，直到 shutdown。
/// 返回进程退出原因。
pub async fn run_agent(config: AgentConfig, shutdown: CancellationToken) -> String {
    let agent_boot_id = flow_engine::execution_protocol::transport::fresh_boot_id();
    let executor_handles: Arc<tokio::sync::Mutex<BTreeMap<String, ExecutorHandle>>> =
        Arc::new(tokio::sync::Mutex::new(BTreeMap::new()));
    let mut backoff = Duration::from_millis(500);
    loop {
        if shutdown.is_cancelled() {
            return "shutdown".into();
        }
        // Reconnecting：建立双 TLS + 握手 + DataBind。
        match connect(&config, &agent_boot_id).await {
            Ok((mut transport, link_session_id)) => {
                tracing::info!(%link_session_id, "agent uplink ready");
                backoff = Duration::from_millis(500);
                let relay = Arc::new(std::sync::Mutex::new(Relay::new(
                    config.agent_id.clone(),
                    agent_boot_id.clone(),
                    link_session_id.clone(),
                )));
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
                    transport,
                    relay,
                    executor_handles.clone(),
                    shutdown.clone(),
                )
                .await;
                if shutdown.is_cancelled() {
                    return "shutdown".into();
                }
                tracing::warn!(%reason, "agent uplink lost; entering Suspect/Reconnecting");
            }
            Err(error) => {
                tracing::warn!(%error, "agent connect failed; retrying with backoff");
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(15));
    }
}

/// 建立上联：control TLS → AgentHello → AgentWelcome → data TLS → DataBind。
async fn connect(
    config: &AgentConfig,
    agent_boot_id: &str,
) -> std::result::Result<(FrameTransport, String), String> {
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
    let _ = (journal_id, master_epoch);
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
    Ok((transport, link))
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
    relay: &Arc<std::sync::Mutex<Relay>>,
    handles: &Arc<tokio::sync::Mutex<BTreeMap<String, ExecutorHandle>>>,
) -> std::result::Result<(), String> {
    let mut page = 0u32;
    loop {
        let snapshot: Vec<(String, ResumeItem)> = {
            let handles = handles.lock().await;
            // 重挂归属：新 link 会话的 relay 重新登记本地绑定（内层执行器
            // 消息才能按已确认路由包装）。
            for (dispatch, handle) in handles.iter() {
                relay
                    .lock()
                    .unwrap()
                    .bind(dispatch, &handle.executor_id, &handle.executor_boot_id);
            }
            let mut facts = Vec::new();
            for (index, (dispatch, handle)) in handles.iter().enumerate() {
                if index < page as usize * RESUME_PAGE_MAX {
                    continue;
                }
                if facts.len() >= RESUME_PAGE_MAX {
                    break;
                }
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
                        lost_data: false,
                        forced_kill: false,
                    },
                ));
            }
            facts
        };
        let more = snapshot.len() == RESUME_PAGE_MAX;
        transport
            .send(Message::Resume {
                items: snapshot.into_iter().map(|(_, item)| item).collect(),
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
                    ResumeAction::CancelAndDrain | ResumeAction::ReconcileRequired
                ) {
                    if let Some(handle) = handles.lock().await.get(&decision.dispatch_id) {
                        handle.cancel.cancel();
                    }
                    relay.lock().unwrap().unbind(&decision.dispatch_id);
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
    for (_, handle) in handles.lock().await.iter() {
        handle.cancel.cancel();
    }
    handles.lock().await.clear();
}

/// Ready 状态转发服务：断线时返回原因（执行器保留等对账）。
pub async fn serve(
    config: &AgentConfig,
    _agent_boot_id: &str,
    _link_session_id: &str,
    mut transport: FrameTransport,
    relay: Arc<std::sync::Mutex<Relay>>,
    handles: Arc<tokio::sync::Mutex<BTreeMap<String, ExecutorHandle>>>,
    shutdown: CancellationToken,
) -> String {
    let (uplink_out, mut uplink_rx) = mpsc::channel::<Message>(UPLINK_OUT_QUEUE);
    // fair mux 动态输入：用转发桥任务把各执行器出站接入共享仲裁。
    let (mux_in, mux_in_rx) = mpsc::channel::<Message>(UPLINK_OUT_QUEUE);
    tokio::spawn(fair_mux_bridge(handles.clone(), mux_in));
    let relay_out = relay.clone();
    tokio::spawn(async move {
        // mux → 包络 → uplink_out（按 dispatch 包装）。
        let mut mux_in_rx = mux_in_rx;
        while let Some(message) = mux_in_rx.recv().await {
            let Some(dispatch_id) = message.dispatch_id().map(str::to_owned) else {
                continue;
            };
            let wrapped = relay_out
                .lock()
                .unwrap()
                .wrap_to_master(&dispatch_id, message);
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
    let mut draining = false;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                cancel_all(&handles).await;
                transport.close();
                return "shutdown".into();
            }
            outbound = uplink_rx.recv() => {
                match outbound {
                    Some(message) => {
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
            received = transport.recv() => {
                match received {
                    Ok((_channel, message)) => match message {
                        Message::Routed { .. } => {
                            let unwrapped = relay.lock().unwrap().unwrap_from_master(&message);
                            match unwrapped {
                                Ok((dispatch_id, inner)) => {
                                    let handles_guard = handles.lock().await;
                                    if let Some(handle) = handles_guard.get(&dispatch_id) {
                                        let _ = handle.inbox.try_send(inner);
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
                            if draining {
                                let _ = transport.try_send(Message::BindExecutorAck {
                                    executor_id,
                                    executor_boot_id: String::new(),
                                    dispatch_id,
                                    ok: false,
                                    error: Some("draining".into()),
                                });
                                continue;
                            }
                            let bound = pool::bind_executor(
                                &config.executor_bin,
                                &config.agent_id,
                                0,
                                &relay,
                                &executor_id,
                                &dispatch_id,
                            )
                            .await;
                            match bound {
                                Ok(handle) => {
                                    let boot = handle.executor_boot_id.clone();
                                    handles.lock().await.insert(dispatch_id.clone(), handle);
                                    let _ = transport.try_send(Message::BindExecutorAck {
                                        executor_id,
                                        executor_boot_id: boot,
                                        dispatch_id,
                                        ok: true,
                                        error: None,
                                    });
                                }
                                Err(error) => {
                                    let _ = transport.try_send(Message::BindExecutorAck {
                                        executor_id,
                                        executor_boot_id: String::new(),
                                        dispatch_id,
                                        ok: false,
                                        error: Some(error.to_string()),
                                    });
                                }
                            }
                        }
                        Message::Drain { grace_ms } => {
                            draining = true;
                            cancel_all(&handles).await;
                            let _ = transport.try_send(Message::DrainComplete { drained: 0 });
                            let _ = grace_ms;
                            transport.close();
                            return "drained".into();
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
                                    relay.lock().unwrap().unbind(&decision.dispatch_id);
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
                    },
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
async fn fair_mux_bridge(
    handles: Arc<tokio::sync::Mutex<BTreeMap<String, ExecutorHandle>>>,
    mux_in: mpsc::Sender<Message>,
) {
    // 简化：轮询各执行器 out（try_recv）并轮转发送，保证任意执行器
    // 每轮至多一帧（洪泛不能独占共享上联出站）。
    loop {
        let mut progressed = false;
        let mut guard = handles.lock().await;
        for handle in guard.values_mut() {
            match handle.out.try_recv() {
                Ok(message) => {
                    if mux_in.send(message).await.is_err() {
                        return;
                    }
                    progressed = true;
                    break; // 轮转：下一个执行器优先
                }
                Err(_) => continue,
            }
        }
        drop(guard);
        if !progressed {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

/// 测试/进程内入口：跳过 TLS 建流，直接进入服务循环（R0）。
/// 重连场景（R2）：handles 非空时先发送 Resume 清单并应用裁决。
pub async fn serve_for_tests(
    config: &AgentConfig,
    transport: FrameTransport,
    relay: Arc<std::sync::Mutex<Relay>>,
    handles: Arc<tokio::sync::Mutex<BTreeMap<String, ExecutorHandle>>>,
    shutdown: CancellationToken,
) -> String {
    serve(
        config,
        "test-boot",
        "test-link",
        transport,
        relay,
        handles,
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
    let relay = Arc::new(std::sync::Mutex::new(Relay::new(
        config.agent_id.clone(),
        agent_boot_id.to_string(),
        link_session_id.clone(),
    )));
    if !handles.lock().await.is_empty() {
        if let Err(error) = resume(&mut transport, &relay, &handles).await {
            tracing::warn!(%error, "resume failed in reconnect_session");
        }
    }
    let reason = serve(
        config,
        agent_boot_id,
        &link_session_id,
        transport,
        relay,
        handles,
        shutdown,
    )
    .await;
    (link_session_id, reason)
}
