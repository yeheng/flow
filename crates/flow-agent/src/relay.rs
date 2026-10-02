//! 路由中继核心（R0-01）：包络校验、有界公平转发与 Resume 事实收集。
//!
//! agent 是受信任中继：只转发，不生成确认。data 上联按 dispatch 有界
//! 公平发送（轮转仲裁），控制帧只发小消息；所有队列按帧数与字节双重
//! 有界（缓冲复制计入 agent 实际 RSS 预算）。

use std::collections::BTreeMap;

use tokio::sync::mpsc;

use flow_engine::execution_protocol::message::Message;
use flow_engine::execution_protocol::remote::{routable, RouteEnvelope};

/// 单个执行器转发队列的帧数上界（满了→停止读该执行器→其持久窗口自然
/// 背压生产，二期 §3.5；agent 不提前 ACK）。
pub const PER_EXECUTOR_QUEUE: usize = 8;
/// 上联总出站帧预算（跨执行器）。
pub const UPLINK_OUT_QUEUE: usize = 256;

/// 已确认绑定（agent 视角）：dispatch → 执行器身份。
#[derive(Debug, Clone)]
pub struct Binding {
    pub executor_id: String,
    pub executor_boot_id: String,
}

/// 中继状态机：归属校验 + 包络工厂 + Resume 事实。
pub struct Relay {
    agent_id: String,
    agent_boot_id: String,
    link_session_id: String,
    bindings: BTreeMap<String, Binding>,
}

#[derive(Debug, thiserror::Error)]
pub enum RelayError {
    #[error("route envelope mismatch: {0}")]
    Envelope(String),
    #[error("unbound dispatch: {0}")]
    Unbound(String),
    #[error("not routable: {0}")]
    NotRoutable(String),
}

impl Relay {
    pub fn new(agent_id: String, agent_boot_id: String, link_session_id: String) -> Self {
        Self {
            agent_id,
            agent_boot_id,
            link_session_id,
            bindings: BTreeMap::new(),
        }
    }

    /// BindExecutor 确认后登记归属。
    pub fn bind(&mut self, dispatch_id: &str, executor_id: &str, executor_boot_id: &str) {
        self.bindings.insert(
            dispatch_id.to_string(),
            Binding {
                executor_id: executor_id.to_string(),
                executor_boot_id: executor_boot_id.to_string(),
            },
        );
    }

    pub fn unbind(&mut self, dispatch_id: &str) {
        self.bindings.remove(dispatch_id);
    }

    pub fn binding(&self, dispatch_id: &str) -> Option<&Binding> {
        self.bindings.get(dispatch_id)
    }

    pub fn dispatches(&self) -> impl Iterator<Item = &String> {
        self.bindings.keys()
    }

    /// 主→执行器方向：校验包络并剥出内层（内层编码原样转发）。
    pub fn unwrap_from_master(&self, message: &Message) -> Result<(String, Message), RelayError> {
        let Message::Routed { envelope, inner } = message else {
            return Err(RelayError::NotRoutable(message.type_name().into()));
        };
        self.verify(envelope)?;
        let dispatch_id = envelope
            .dispatch_id
            .clone()
            .ok_or_else(|| RelayError::Envelope("missing dispatch_id".into()))?;
        Ok((dispatch_id, *inner.clone()))
    }

    /// 执行器→主方向：按已确认归属构造包络（不信任执行器自报机器身份）。
    pub fn wrap_to_master(&self, dispatch_id: &str, inner: Message) -> Result<Message, RelayError> {
        if !routable(&inner) {
            return Err(RelayError::NotRoutable(inner.type_name().into()));
        }
        let binding = self
            .binding(dispatch_id)
            .ok_or_else(|| RelayError::Unbound(dispatch_id.into()))?;
        Ok(Message::Routed {
            envelope: RouteEnvelope {
                agent_id: self.agent_id.clone(),
                agent_boot_id: self.agent_boot_id.clone(),
                link_session_id: self.link_session_id.clone(),
                executor_id: binding.executor_id.clone(),
                executor_boot_id: binding.executor_boot_id.clone(),
                dispatch_id: Some(dispatch_id.to_string()),
            },
            inner: Box::new(inner),
        })
    }

    fn verify(&self, envelope: &RouteEnvelope) -> Result<(), RelayError> {
        if envelope.agent_id != self.agent_id
            || envelope.agent_boot_id != self.agent_boot_id
            || envelope.link_session_id != self.link_session_id
        {
            return Err(RelayError::Envelope(format!(
                "link identity mismatch: envelope({},{},{}) vs relay({},{},{})",
                envelope.agent_id,
                envelope.agent_boot_id,
                envelope.link_session_id,
                self.agent_id,
                self.agent_boot_id,
                self.link_session_id
            )));
        }
        let dispatch_id = envelope
            .dispatch_id
            .as_deref()
            .ok_or_else(|| RelayError::Envelope("missing dispatch".into()))?;
        let binding = self
            .bindings
            .get(dispatch_id)
            .ok_or_else(|| RelayError::Unbound(dispatch_id.into()))?;
        if envelope.executor_id != binding.executor_id
            || envelope.executor_boot_id != binding.executor_boot_id
        {
            return Err(RelayError::Envelope("executor identity mismatch".into()));
        }
        Ok(())
    }
}

/// 多执行器 → 单上联的轮转公平仲裁：每轮每个执行器至多发一帧，洪泛
/// 任务不能独占共享出站（三期 §1.2）。
pub async fn fair_mux(inputs: Vec<mpsc::Receiver<Message>>, out: mpsc::Sender<Message>) {
    let mut inputs = inputs;
    let mut cursor = 0usize;
    loop {
        let mut pending = Vec::new();
        // 每轮从游标起整圈扫描：每个通道至多取 1 帧（真轮转）。
        for offset in 0..inputs.len() {
            let index = (cursor + offset) % inputs.len();
            if let Ok(message) = inputs[index].try_recv() {
                pending.push(message);
            }
        }
        if pending.is_empty() {
            // 无待发：让出（退出由上层关闭 out 触发 send 失败）。
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            continue;
        }
        cursor = (cursor + 1) % inputs.len().max(1);
        for message in pending {
            if out.send(message).await.is_err() {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn relay_with_binding() -> Relay {
        let mut relay = Relay::new("agent-1".into(), "boot-1".into(), "link-1".into());
        relay.bind("d-1", "exec-1", "eboot-1");
        relay
    }

    fn envelope(agent: &str, boot: &str, link: &str, exec: &str, eboot: &str, d: &str) -> Message {
        Message::Routed {
            envelope: RouteEnvelope {
                agent_id: agent.into(),
                agent_boot_id: boot.into(),
                link_session_id: link.into(),
                executor_id: exec.into(),
                executor_boot_id: eboot.into(),
                dispatch_id: Some(d.into()),
            },
            inner: Box::new(Message::Cancel {
                command_id: "c".into(),
                dispatch_id: d.into(),
            }),
        }
    }

    #[test]
    fn wrong_route_rejected() {
        let relay = relay_with_binding();
        // 错 agent / 错 boot / 错 link / 错执行器 / 未绑定。
        assert!(relay.unwrap_from_master(&envelope("agent-2", "boot-1", "link-1", "exec-1", "eboot-1", "d-1")).is_err());
        assert!(relay.unwrap_from_master(&envelope("agent-1", "boot-2", "link-1", "exec-1", "eboot-1", "d-1")).is_err());
        assert!(relay.unwrap_from_master(&envelope("agent-1", "boot-1", "link-2", "exec-1", "eboot-1", "d-1")).is_err());
        assert!(relay.unwrap_from_master(&envelope("agent-1", "boot-1", "link-1", "exec-9", "eboot-1", "d-1")).is_err());
        assert!(relay.unwrap_from_master(&envelope("agent-1", "boot-1", "link-1", "exec-1", "eboot-1", "d-9")).is_err());
        // 正确包络通过且内层原样。
        let (dispatch, inner) = relay
            .unwrap_from_master(&envelope("agent-1", "boot-1", "link-1", "exec-1", "eboot-1", "d-1"))
            .unwrap();
        assert_eq!(dispatch, "d-1");
        assert_eq!(inner.type_name(), "Cancel");
    }

    #[test]
    fn wrap_uses_confirmed_binding_only() {
        let relay = relay_with_binding();
        let wrapped = relay
            .wrap_to_master(
                "d-1",
                Message::AuditAck {
                    dispatch_id: "d-1".into(),
                    durable_audit_seq: 3,
                    durable_bytes: 10,
                    commit_lsn: 5,
                },
            )
            .unwrap();
        match wrapped {
            Message::Routed { envelope, .. } => {
                assert_eq!(envelope.executor_boot_id, "eboot-1");
            }
            _ => panic!("expected routed"),
        }
        assert!(relay.wrap_to_master("d-unknown", Message::Ready).is_err());
        // 管理帧不可被包装转发。
        assert!(relay
            .wrap_to_master("d-1", Message::Drain { grace_ms: 1 })
            .is_err());
    }

    #[tokio::test]
    async fn fair_mux_round_robins_under_flood() {
        let (flood_tx, flood_rx) = mpsc::channel(64);
        let (quiet_tx, quiet_rx) = mpsc::channel(64);
        let (out_tx, mut out_rx) = mpsc::channel(16);
        tokio::spawn(fair_mux(vec![flood_rx, quiet_rx], out_tx));
        for i in 0..16 {
            let _ = flood_tx.send(Message::Heartbeat {
                dispatch_id: Some(format!("flood-{i}")),
                phase: "f".into(),
            })
            .await;
        }
        let _ = quiet_tx
            .send(Message::Heartbeat {
                dispatch_id: Some("quiet".into()),
                phase: "q".into(),
            })
            .await;
        let mut seen_flood = 0;
        let mut seen_quiet_before_flood_done = false;
        for _ in 0..8 {
            if let Some(Message::Heartbeat { dispatch_id, .. }) = out_rx.recv().await {
                if dispatch_id.as_deref().is_some_and(|d| d.starts_with("flood")) {
                    seen_flood += 1;
                } else {
                    seen_quiet_before_flood_done = true;
                }
            }
        }
        assert!(seen_quiet_before_flood_done || seen_flood < 8, "quiet task must not starve");
    }
}
