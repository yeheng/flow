//! 物理帧编解码（postcard 单流）。
//!
//! 唯一帧格式：`u32 body_len | postcard(Message)`。变体索引即帧类型；
//! TransferChunk 的字节由 postcard 原生承载（len varint + 原始字节，无
//! base64、无 JSON meta 与 blob 的拼接）。一条流内的帧序即双方提交序。
//!
//! 读端处理半帧/粘帧：长度头一到就校验上限（分配任何载荷缓冲前拒绝非法
//! 长度），解码后按消息类型复核 control/data 预算，并要求 postcard 恰好
//! 消费完整个帧体（尾随字节 = 协议错误，防帧内走私）。
//!
//! 对抗性输入（验收项）：postcard 对 String/bytes 的长度声明先做
//! 边界检查再取字节、对超出的 seq 长度声明抑制 size_hint——巨型声明
//! 在小帧内立即报错，不会先按声明分配（见 message.rs 的对应测试）。
//!
//! 为什么编解码必须自带预算复核：帧长是接收方唯一无法伪造的成本闸门
//! （声明多大就得读多大）。若这里放过超限帧，一个 200 MiB 的节点输出
//! 就能让接收方在分配阶段被拖垮，而不是在预算检查处被明确拒绝。

use super::contract::{CONTROL_MAX_FRAME, DATA_MAX_FRAME};
use super::message::{Message, ProtocolError};

/// 长度头字节数（`u32 body_len`）。
pub const FRAME_LENGTH_BYTES: usize = 4;

/// 消息级帧预算：大载荷类型（data 语义）放宽到 DATA_MAX_FRAME，其余
/// （control 语义）收紧到 CONTROL_MAX_FRAME——分类只服务预算上限，
/// 旧版的通道概念已随单流化删除。
///
/// `Routed` 递归取内层预算：外层包络不改变内层消息的语义，否则
/// `Routed{Execute}` 能把节点定义塞到 DATA_MAX_FRAME，绕开 Execute 侧
/// CONTROL_MAX_FRAME/2 的预检意图。编解码两端同函数，天然对称。
fn frame_limit(message: &Message) -> usize {
    match message {
        Message::TransferChunk { .. }
        | Message::ObservabilityBatch { .. }
        | Message::AuditBatch { .. } => DATA_MAX_FRAME,
        Message::Routed { inner, .. } => frame_limit(inner),
        _ => CONTROL_MAX_FRAME,
    }
}

/// 最小合法帧体：一个变体索引字节（如 Ready）。0 字节是独立的格式
/// 违规（不是大小问题），拒绝时给独立的错误语义。
const MIN_FRAME_BODY: usize = 1;

/// 长度头声明的 body 尺寸校验（feed/pop 共用）：空体明确拒绝，超上限
/// 在分配任何载荷缓冲前拒绝。
fn check_body_len(body_len: usize) -> Result<(), ProtocolError> {
    if body_len < MIN_FRAME_BODY {
        return Err(ProtocolError::Malformed("empty frame body".into()));
    }
    if body_len > DATA_MAX_FRAME {
        return Err(ProtocolError::FrameTooLarge(body_len, DATA_MAX_FRAME));
    }
    Ok(())
}

/// 单流帧编码：`u32 body_len | postcard(Message)`。
///
/// 长度头用大端：帧头是协议里唯一跨字节序解释的部分（帧体本身是
/// postcard 的 LE varint），固定为大端让抓包与跨语言排障工具不必猜
/// 主机字节序。
///
/// # Errors
/// postcard 编码失败返回 [`ProtocolError::Malformed`]；编码后超过该消息
/// 类型的帧预算返回 [`ProtocolError::FrameTooLarge`]（不截断）。
pub fn encode_frame(message: &Message) -> Result<Vec<u8>, ProtocolError> {
    let body = postcard::to_allocvec(message)
        .map_err(|e| ProtocolError::Malformed(format!("postcard: {e}")))?;
    // 预算按消息语义分类：control 收紧、data 放宽，但两者都是硬上限。
    let limit = frame_limit(message);
    if body.len() > limit {
        return Err(ProtocolError::FrameTooLarge(body.len(), limit));
    }
    let mut frame = Vec::with_capacity(FRAME_LENGTH_BYTES + body.len());
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

/// 流式帧解码器：喂入任意切分的字节，弹出完整消息。
#[derive(Debug)]
pub struct FrameDecoder {
    buffer: Vec<u8>,
}

impl Default for FrameDecoder {
    /// 与 [`FrameDecoder::new`] 等价：解码器无内部状态，默认构造即空缓冲。
    fn default() -> Self {
        Self::new()
    }
}

impl FrameDecoder {
    /// 新建空解码器（无缓冲）。
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    /// 追加字节。长度头一到就校验上限（分配任何载荷缓冲前拒绝）。
    ///
    /// 半帧与粘帧都由调用方随意切分喂入：解码器只负责累积字节，
    /// 完整帧的判定与弹出在 [`FrameDecoder::pop`]。
    pub fn feed(&mut self, bytes: &[u8]) -> Result<(), ProtocolError> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.buffer.extend_from_slice(bytes);
        // 长度头一旦到齐就立刻校验：此时还没有任何按声明长度分配的缓冲，
        // 恶意长度声明无法放大内存占用。
        if self.buffer.len() >= FRAME_LENGTH_BYTES {
            check_body_len(self.head_length())?;
        }
        // 缓冲不会一直长：pop 会摘走完整帧，这里只回收「半帧残留 +
        // 超大声明」造成的过量容量，避免长期会话的 RSS 被历史大帧撑住。
        let cap = FRAME_LENGTH_BYTES + DATA_MAX_FRAME + 1024;
        if self.buffer.capacity() > cap {
            self.buffer.shrink_to(cap);
        }
        Ok(())
    }

    /// 读取长度头声明的 body 尺寸（大端 u32）。
    ///
    /// 调用前置条件：缓冲里至少已有 [`FRAME_LENGTH_BYTES`] 字节。
    fn head_length(&self) -> usize {
        u32::from_be_bytes([
            self.buffer[0],
            self.buffer[1],
            self.buffer[2],
            self.buffer[3],
        ]) as usize
    }

    /// 弹出一个完整消息；无完整帧返回 None。解码在缓冲内进行，仅在
    /// 帧完整、postcard 恰好消费完帧体且预算通过后才从缓冲摘除。
    ///
    /// 摘除（`drain`）刻意放在所有校验之后：任何一条校验失败时缓冲
    /// 原样保留，会话随即因错误终止——半个被消费的帧留在缓冲里没有
    /// 意义，保留它只会掩盖「协议已经坏了」这一事实。
    pub fn pop(&mut self) -> Result<Option<Message>, ProtocolError> {
        if self.buffer.len() < FRAME_LENGTH_BYTES {
            return Ok(None);
        }
        let body_len = self.head_length();
        // 长度头已到齐即复核上限：feed 时校验过一次，这里防止在两次
        // feed 之间长度语义被绕过（例如上层换了解码器实例）。
        check_body_len(body_len)?;
        let total = FRAME_LENGTH_BYTES + body_len;
        if self.buffer.len() < total {
            return Ok(None);
        }
        let body = &self.buffer[FRAME_LENGTH_BYTES..total];
        let (message, rest) = postcard::take_from_bytes::<Message>(body)
            .map_err(|e| ProtocolError::Malformed(format!("postcard: {e}")))?;
        // 帧体必须被恰好消费完：尾随字节意味着「合法消息 + 走私数据」，
        // 是典型的帧内走私形态，必须按协议错误拒绝而不是忽略。
        if !rest.is_empty() {
            return Err(ProtocolError::Malformed(
                "frame body has trailing bytes".into(),
            ));
        }
        // 长度头声明的上限是全局硬顶（DATA_MAX_FRAME），这里再按消息
        // 语义复核收紧后的预算：control 消息不能借声明长度塞进 data
        // 级的字节量。
        let limit = frame_limit(&message);
        if body_len > limit {
            return Err(ProtocolError::FrameTooLarge(body_len, limit));
        }
        self.buffer.drain(..total);
        Ok(Some(message))
    }
}

#[cfg(test)]
mod tests {
    use super::super::fixtures;
    use super::*;

    #[test]
    fn roundtrip_all_fixture_messages() {
        for message in fixtures::valid_messages() {
            let frame = encode_frame(&message).expect("encode");
            let mut decoder = FrameDecoder::new();
            // 半帧 + 粘帧喂入
            decoder.feed(&frame[..3]).unwrap();
            decoder.feed(&frame[3..]).unwrap();
            let decoded = decoder.pop().expect("pop").expect("frame");
            assert_eq!(decoded, message);
        }
    }

    #[test]
    fn transfer_chunk_carries_raw_bytes_not_base64() {
        let message = Message::TransferChunk {
            dispatch_id: "d".into(),
            transfer_id: "in:1".into(),
            offset: 0,
            bytes: vec![0u8, 255, 1, 2, 3],
            digest: "digest".into(),
        };
        let frame = encode_frame(&message).unwrap();
        // 原始字节作为 body 直邻出现（无 base64 膨胀）。
        assert!(frame.windows(5).any(|w| w == [0u8, 255, 1, 2, 3]));
        let mut decoder = FrameDecoder::new();
        decoder.feed(&frame).unwrap();
        let decoded = decoder.pop().unwrap().unwrap();
        assert_eq!(decoded, message);
    }

    /// 远程中继：Routed 内层 TransferChunk 的字节同样原样承载（嵌套枚举
    /// 序列化，无专用帧 kind 与 meta 拼接）。
    #[test]
    fn routed_transfer_chunk_carries_raw_bytes() {
        use super::super::remote::RouteEnvelope;
        let inner = Message::TransferChunk {
            dispatch_id: "d".into(),
            transfer_id: "in:op".into(),
            offset: 0,
            bytes: vec![7u8; 4096],
            digest: "digest".into(),
        };
        let message = Message::Routed {
            envelope: RouteEnvelope {
                agent_id: "a".into(),
                agent_boot_id: "b".into(),
                link_session_id: "l".into(),
                executor_id: "e".into(),
                executor_boot_id: "eb".into(),
                dispatch_id: Some("d".into()),
            },
            inner: Box::new(inner),
        };
        let frame = encode_frame(&message).unwrap();
        assert!(frame.windows(64).any(|w| w.iter().all(|b| *b == 7)));
        let mut decoder = FrameDecoder::new();
        decoder.feed(&frame).unwrap();
        let decoded = decoder.pop().unwrap().unwrap();
        assert_eq!(decoded, message);
    }

    /// 对抗性验收：损坏帧必须明确拒绝（不静默、不崩溃、不先分配）。
    #[test]
    fn corrupt_frames_are_rejected() {
        for frame in fixtures::corrupt_frames() {
            let mut decoder = FrameDecoder::new();
            decoder.feed(&frame).unwrap();
            assert!(decoder.pop().is_err(), "损坏帧必须被拒绝: {frame:02x?}");
        }
    }

    #[test]
    fn sticky_and_split_frames() {
        let messages = fixtures::valid_messages();
        let a = encode_frame(&messages[0]).unwrap();
        let b = encode_frame(&messages[1]).unwrap();
        // 粘帧：两帧一次喂入 → 依次弹出。
        let mut decoder = FrameDecoder::new();
        decoder.feed(&[a.clone(), b.clone()].concat()).unwrap();
        let first = decoder.pop().unwrap().unwrap();
        let second = decoder.pop().unwrap().unwrap();
        assert_eq!(first, messages[0]);
        assert_eq!(second, messages[1]);
        // 半帧：逐字节喂入也能重组。
        let mut decoder = FrameDecoder::new();
        for byte in &a {
            decoder.feed(std::slice::from_ref(byte)).unwrap();
        }
        let decoded = decoder.pop().unwrap().unwrap();
        assert_eq!(decoded, messages[0]);
    }

    /// 帧内走私：合法消息后附加任意字节（帧长含垃圾）必须拒绝。
    #[test]
    fn trailing_bytes_inside_frame_rejected() {
        let mut frame = encode_frame(&Message::Ready).unwrap();
        frame.extend_from_slice(b"junk");
        let body_len = (frame.len() - FRAME_LENGTH_BYTES) as u32;
        frame[..FRAME_LENGTH_BYTES].copy_from_slice(&body_len.to_be_bytes());
        let mut decoder = FrameDecoder::new();
        decoder.feed(&frame).unwrap();
        let error = decoder.pop().unwrap_err();
        assert!(error.to_string().contains("trailing"), "{error}");
    }

    /// 控制帧预算复核：data 语义消息按 CONTROL_MAX_FRAME 编码会被拒。
    #[test]
    fn control_budget_enforced_by_message_type() {
        // 大 AuditBatch（data 语义）在 DATA_MAX_FRAME 内放行；
        // 它塞进 Routed 也仍是 data 语义；Ready 是 control 语义但天然小。
        let big = Message::TransferChunk {
            dispatch_id: "d".into(),
            transfer_id: "t".into(),
            offset: 0,
            bytes: vec![1u8; 256 * 1024],
            digest: String::new(),
        };
        assert!(encode_frame(&big).is_ok());
        // 构造一个 control 语义超限消息：Hello.capabilities 巨大。
        let fat = Message::Hello {
            version: super::super::contract::PROTOCOL_VERSION,
            boot_id: "b".into(),
            build: "x".into(),
            capabilities: vec!["c".repeat(200 * 1024); 1],
            session_token: None,
        };
        assert!(matches!(
            encode_frame(&fat),
            Err(ProtocolError::FrameTooLarge(_, CONTROL_MAX_FRAME))
        ));
    }

    /// 嵌套与否预算判定一致：Routed 递归取内层预算，控制帧预算不可被
    /// 外层包络绕过（否则 Routed{Execute} 能塞 256 KiB 节点定义）。
    #[test]
    fn routed_budget_follows_inner_message() {
        use super::super::message::{ExecuteTask, TaskValue};
        use super::super::remote::RouteEnvelope;
        use serde_json::json;
        let envelope = RouteEnvelope {
            agent_id: "a".into(),
            agent_boot_id: "b".into(),
            link_session_id: "l".into(),
            executor_id: "e".into(),
            executor_boot_id: "eb".into(),
            dispatch_id: Some("d".into()),
        };
        // control 语义超限消息（256 KiB 节点定义）：裸帧拒绝。
        let fat_execute = Message::Execute {
            command_id: "cmd".into(),
            dispatch_id: "d".into(),
            task: Box::new(ExecuteTask {
                run_id: "r".into(),
                node_id: "n".into(),
                node_execution_id: "ne".into(),
                attempt: 1,
                node: json!({"id": "n", "type": "script", "params": {"code": "x".repeat(256 * 1024)}}),
                input: TaskValue::Local,
                predecessors: vec![],
                prepared: None,
                previous_outcome: None,
                observability: false,
            }),
        };
        assert!(matches!(
            encode_frame(&fat_execute),
            Err(ProtocolError::FrameTooLarge(_, CONTROL_MAX_FRAME))
        ));
        // 同一条消息套上 Routed 包络必须同样按内层预算拒绝。
        let routed_fat = Message::Routed {
            envelope: envelope.clone(),
            inner: Box::new(fat_execute),
        };
        assert!(matches!(
            encode_frame(&routed_fat),
            Err(ProtocolError::FrameTooLarge(_, CONTROL_MAX_FRAME))
        ));
        // data 语义内层（TransferChunk）套包络仍按 DATA 放行。
        let routed_chunk = Message::Routed {
            envelope,
            inner: Box::new(Message::TransferChunk {
                dispatch_id: "d".into(),
                transfer_id: "t".into(),
                offset: 0,
                bytes: vec![1u8; 256 * 1024],
                digest: String::new(),
            }),
        };
        assert!(encode_frame(&routed_chunk).is_ok());
    }

    /// 空帧体是格式违规（最小合法体 = 1 字节变体索引）：报独立的
    /// Malformed 语义，而不是 FrameTooLarge(0, …)。
    #[test]
    fn empty_frame_body_is_malformed_not_too_large() {
        let mut decoder = FrameDecoder::new();
        let error = decoder.feed(&[0, 0, 0, 0]).unwrap_err();
        assert!(matches!(error, ProtocolError::Malformed(_)), "{error}");
        assert!(error.to_string().contains("empty frame body"), "{error}");
    }
}
