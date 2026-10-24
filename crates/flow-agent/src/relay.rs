//! 路由中继核心（R0-01）：包络校验、有界公平转发与 Resume 事实收集。
//!
//! agent 是受信任中继：只转发，不生成确认。data 上联按 dispatch 有界
//! 公平发送（轮转仲裁），控制帧只发小消息；所有队列按帧数与字节双重
//! 有界（缓冲复制计入 agent 实际 RSS 预算）。

use std::collections::BTreeMap;

use flow_engine::execution_protocol::message::Message;
use flow_engine::execution_protocol::remote::{routable, RouteEnvelope};

/// 单个执行器转发队列的帧数上界（满了→停止读该执行器→其持久窗口自然
/// 背压生产，二期 §3.5；agent 不提前 ACK）。
pub const PER_EXECUTOR_QUEUE: usize = 8;
/// 上联总出站帧预算（跨执行器）。
pub const UPLINK_OUT_QUEUE: usize = 256;
/// 上联共享出站字节预算（T08）：限制 wrapper→serve 队列里滞留的估算
/// 编码字节总量（不含 transport 出站队列——那层另按帧数有界）。
pub const UPLINK_OUT_BYTES: u64 = 8 * 1024 * 1024;
/// 字节公平轮转的每轮额度（T08）：估算编码字节；纯帧数公平会被大帧
/// 洪泛任务变相独占。
pub const FAIR_TURN_BYTES: u64 = 64 * 1024;
/// 每轮帧数上限：小帧洪泛（心跳/观测）不能在一轮内排干队列饿死后继。
pub const FAIR_TURN_FRAMES: usize = 4;

/// 已确认绑定（agent 视角）：dispatch → 执行器身份。
#[derive(Debug, Clone)]
/// 已确认绑定（agent 视角）：dispatch → 执行器身份。
pub struct Binding {
    /// 执行器 id。
    pub executor_id: String,
    /// 执行器本次启动的标识（重启即换）。
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
/// 中继可以拒绝的三类情况。
pub enum RelayError {
    #[error("route envelope mismatch: {0}")]
    Envelope(String),
    #[error("unbound dispatch: {0}")]
    Unbound(String),
    #[error("not routable: {0}")]
    NotRoutable(String),
}

impl Relay {
    /// 创建一个中继状态机：初始没有任何归属绑定。
    ///
    /// 绑定只能由 `bind`（即 BindExecutor 确认后）建立，这是「只按已确认
    /// 路由转发」的前提。
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

    /// 解除一个派发的归属绑定（执行器回收后调用）。
    ///
    /// 解绑后该 dispatch 的上行包络不再被接受——执行器已经没了，
    /// 再收到它的帧说明有串线，必须拒绝而不是转发。
    pub fn unbind(&mut self, dispatch_id: &str) {
        self.bindings.remove(dispatch_id);
    }

    /// 查询某个派发的已确认归属。
    pub fn binding(&self, dispatch_id: &str) -> Option<&Binding> {
        self.bindings.get(dispatch_id)
    }

    /// 遍历当前所有已绑定派发。
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

    /// 消耗式解包（T08 生产路径）：校验后移动内层消息，不做深拷贝。
    /// 借用式 [`Relay::unwrap_from_master`] 仅为兼容保留。
    pub fn unwrap_from_master_owned(
        &self,
        message: Message,
    ) -> Result<(String, Message), RelayError> {
        let Message::Routed { envelope, inner } = message else {
            return Err(RelayError::NotRoutable(message.type_name().into()));
        };
        self.verify(&envelope)?;
        let dispatch_id = envelope
            .dispatch_id
            .clone()
            .ok_or_else(|| RelayError::Envelope("missing dispatch_id".into()))?;
        Ok((dispatch_id, *inner))
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

    /// 校验包络是否与本中继的会话身份及已确认归属一致。
    ///
    /// 这一步是 agent 的**安全闸**：包络里的 agent/boot/link 三个身份必须
    /// 与本连接相符，执行器身份必须与已确认绑定相符。任何一项不符都说明
    /// 帧来自别的会话或已被撤销的绑定，必须拒绝。
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

/// 消息估算编码字节数（T08 调度与预算口径）：不序列化、零分配地遍历
/// 大载荷字段。调度公平性与队列预算共用同一估算；精确计量属 T01 基线
/// 后续验证项（跨进程序列化无法零成本获得，v5 帧编码时自然携带精确值）。
pub fn estimate_weight(message: &Message) -> u64 {
    let raw = match message {
        Message::Routed { inner, .. } => 256 + estimate_weight(inner),
        Message::TransferChunk { bytes, .. } => 512 + bytes.len() as u64,
        Message::AuditBatch { records, .. } => records
            .iter()
            .map(|record| 512 + record.kind.len() as u64 + estimate_value(&record.payload))
            .sum(),
        Message::ObservabilityBatch { lines, .. } => {
            512 + lines
                .iter()
                .map(|line| 256 + line.message.len() as u64)
                .sum::<u64>()
        }
        Message::Result { .. } => 1024,
        _ => 1024,
    };
    raw.min(flow_engine::execution_protocol::DATA_MAX_FRAME as u64)
}

/// 估算一个 JSON 值的编码字节数（同样零分配遍历）。
fn estimate_value(value: &serde_json::Value) -> u64 {
    match value {
        serde_json::Value::String(text) => 16 + text.len() as u64,
        serde_json::Value::Object(map) => map
            .iter()
            .map(|(key, value)| 16 + key.len() as u64 + estimate_value(value))
            .sum(),
        serde_json::Value::Array(items) => items.iter().map(estimate_value).sum(),
        _ => 16,
    }
}

/// 预算口径的估算字节数：按 2× 计入 JSON 转义扩张。这是软收紧不是硬
/// 上限——控制字符转义（\uXXXX）最坏可达 6×，低估的绝对上界由帧级约束
/// 兜底：单帧 ≤ DATA_MAX_FRAME、各队列按帧数有界（PER_EXECUTOR_QUEUE /
/// UPLINK_OUT_QUEUE），字节数不会超出帧数 × 帧上限。
pub fn weight_budgeted(message: &Message) -> u64 {
    estimate_weight(message) * 2
}

/// 共享出站字节预算（T08）：acquire 随消息入队前占用，release 于消息
/// 离开 agent（交 transport 出站队列或发送失败）后归还；预算随消息
/// 所有权流转，不用无界队列化解背压。
pub struct UplinkBudget {
    cap: u32,
    semaphore: std::sync::Arc<tokio::sync::Semaphore>,
}

impl UplinkBudget {
    /// 创建共享出站字节预算。
    ///
    /// 上限至少为 1：预算为 0 会让任何消息都无法出站，等于把 agent 变成黑洞。
    pub fn new(cap: u64) -> std::sync::Arc<Self> {
        let cap = u32::try_from(cap)
            .expect("uplink budget exceeds semaphore limit")
            .max(1);
        std::sync::Arc::new(Self {
            cap,
            semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(cap as usize)),
        })
    }

    /// 占用出站字节预算；预算不足时挂起等待（形成背压）。
    pub async fn acquire(&self, bytes: u64) -> tokio::sync::OwnedSemaphorePermit {
        self.semaphore
            .clone()
            .acquire_many_owned(bytes.min(self.cap as u64).max(1) as u32)
            .await
            .expect("uplink budget never closed")
    }

    /// 未归还的出站字节许可（已入队、尚未交到上联的字节数）——
    /// CapacityReport.pending_bytes 的来源。
    pub fn pending_bytes(&self) -> u64 {
        (self.cap as u64).saturating_sub(self.semaphore.available_permits() as u64)
    }
}

#[cfg(test)]
/// 中继归属校验与预算归还的回归测试。
///
/// 这些用例守的是 agent 的安全边界：错会话、错执行器、未绑定的包络
/// 必须一律被拒；以及字节预算在消息被丢弃时必须归还（否则会逐渐
/// 饿死出站）。
mod tests {
    use super::*;

    #[tokio::test]
    async fn dropped_and_failed_queue_messages_return_budget() {
        let budget = UplinkBudget::new(8);
        for _ in 0..32 {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let permit = budget.acquire(8).await;
            tx.send((Message::Ready, permit)).await.unwrap();
            drop(rx);
            let permit =
                tokio::time::timeout(std::time::Duration::from_millis(100), budget.acquire(8))
                    .await
                    .unwrap();
            drop(tx.send((Message::Ready, permit)).await.unwrap_err());
        }
        assert_eq!(budget.semaphore.available_permits(), 8);
    }

    #[tokio::test]
    async fn pending_bytes_tracks_outstanding_permits() {
        let budget = UplinkBudget::new(1024);
        assert_eq!(budget.pending_bytes(), 0);
        let permit = budget.acquire(300).await;
        assert_eq!(budget.pending_bytes(), 300);
        drop(permit);
        assert_eq!(budget.pending_bytes(), 0);
    }

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
        assert!(relay
            .unwrap_from_master(&envelope(
                "agent-2", "boot-1", "link-1", "exec-1", "eboot-1", "d-1"
            ))
            .is_err());
        assert!(relay
            .unwrap_from_master(&envelope(
                "agent-1", "boot-2", "link-1", "exec-1", "eboot-1", "d-1"
            ))
            .is_err());
        assert!(relay
            .unwrap_from_master(&envelope(
                "agent-1", "boot-1", "link-2", "exec-1", "eboot-1", "d-1"
            ))
            .is_err());
        assert!(relay
            .unwrap_from_master(&envelope(
                "agent-1", "boot-1", "link-1", "exec-9", "eboot-1", "d-1"
            ))
            .is_err());
        assert!(relay
            .unwrap_from_master(&envelope(
                "agent-1", "boot-1", "link-1", "exec-1", "eboot-1", "d-9"
            ))
            .is_err());
        // 正确包络通过且内层原样。
        let (dispatch, inner) = relay
            .unwrap_from_master(&envelope(
                "agent-1", "boot-1", "link-1", "exec-1", "eboot-1", "d-1",
            ))
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
}
