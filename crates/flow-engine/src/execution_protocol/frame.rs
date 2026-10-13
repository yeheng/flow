//! 物理帧编解码（v4 单流，T12）。
//!
//! 唯一帧格式：`u32 body_len | u8 kind | u32 meta_len | JSON meta | raw blob`。
//! `body_len` 计 kind + meta_len + meta + blob（不含 4 字节长度头）。带二进制
//! 体的类型（`TransferChunk`）的载荷作为 raw blob 承载；其余类型 kind=0、
//! meta 即完整 JSON 信封。一条流内的帧序即双方提交序。
//!
//! 读端处理半帧/粘帧：长度头一到就校验上限（分配前拒绝非法长度），并按
//! 消息通道复核 control/data 预算。

use super::contract::{CONTROL_MAX_FRAME, DATA_MAX_FRAME};
use super::message::{Channel, Message, ProtocolError};

/// 长度头字节数（`u32 body_len`）。
pub const FRAME_LENGTH_BYTES: usize = 4;
/// kind(1) + meta_len(4)。
const KIND_META_HEADER: usize = 5;

/// 单流帧编码：kind/meta 由消息声明，raw blob 承载二进制体。
pub fn encode_frame(message: &Message) -> Result<Vec<u8>, ProtocolError> {
    let (kind_byte, meta, blob) = message.to_frame_parts()?;
    let body_len = KIND_META_HEADER + meta.len() + blob.len();
    let limit = frame_limit(message.channel());
    if body_len > limit {
        return Err(ProtocolError::FrameTooLarge(body_len, limit));
    }
    let mut frame = Vec::with_capacity(FRAME_LENGTH_BYTES + body_len);
    frame.extend_from_slice(&(body_len as u32).to_be_bytes());
    frame.push(kind_byte);
    frame.extend_from_slice(&(meta.len() as u32).to_be_bytes());
    frame.extend_from_slice(&meta);
    frame.extend_from_slice(&blob);
    Ok(frame)
}

pub(crate) fn frame_limit(channel: Channel) -> usize {
    match channel {
        Channel::Control => CONTROL_MAX_FRAME,
        Channel::Data => DATA_MAX_FRAME,
    }
}

/// 流式帧解码器：喂入任意切分的字节，弹出完整消息。
#[derive(Debug)]
pub struct FrameDecoder {
    buffer: Vec<u8>,
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
        }
    }

    /// 追加字节。长度头一到就校验上限（分配任何载荷缓冲前拒绝）。
    pub fn feed(&mut self, bytes: &[u8]) -> Result<(), ProtocolError> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.buffer.extend_from_slice(bytes);
        if self.buffer.len() >= FRAME_LENGTH_BYTES {
            let body_len = self.head_length();
            if !(KIND_META_HEADER..=DATA_MAX_FRAME).contains(&body_len) {
                return Err(ProtocolError::FrameTooLarge(body_len, DATA_MAX_FRAME));
            }
        }
        let cap = FRAME_LENGTH_BYTES + DATA_MAX_FRAME + 1024;
        if self.buffer.capacity() > cap {
            self.buffer.shrink_to(cap);
        }
        Ok(())
    }

    fn head_length(&self) -> usize {
        u32::from_be_bytes([self.buffer[0], self.buffer[1], self.buffer[2], self.buffer[3]]) as usize
    }

    /// 弹出一个完整消息（含通道）；无完整帧返回 None。
    pub fn pop(&mut self) -> Result<Option<(Channel, Message)>, ProtocolError> {
        if self.buffer.len() < FRAME_LENGTH_BYTES {
            return Ok(None);
        }
        let body_len = self.head_length();
        if !(KIND_META_HEADER..=DATA_MAX_FRAME).contains(&body_len) {
            return Err(ProtocolError::FrameTooLarge(body_len, DATA_MAX_FRAME));
        }
        if self.buffer.len() < FRAME_LENGTH_BYTES + body_len {
            return Ok(None);
        }
        let frame: Vec<u8> = self
            .buffer
            .drain(..FRAME_LENGTH_BYTES + body_len)
            .collect();
        let kind_byte = frame[FRAME_LENGTH_BYTES];
        let meta_len = u32::from_be_bytes([
            frame[FRAME_LENGTH_BYTES + 1],
            frame[FRAME_LENGTH_BYTES + 2],
            frame[FRAME_LENGTH_BYTES + 3],
            frame[FRAME_LENGTH_BYTES + 4],
        ]) as usize;
        let meta_start = FRAME_LENGTH_BYTES + KIND_META_HEADER;
        if meta_start + meta_len > frame.len() {
            return Err(ProtocolError::Malformed(
                "frame meta length exceeds body".into(),
            ));
        }
        let meta = &frame[meta_start..meta_start + meta_len];
        let blob = &frame[meta_start + meta_len..];
        let message = Message::from_frame_parts(kind_byte, meta, blob)?;
        // 通道预算复核（control 收紧到 CONTROL_MAX_FRAME）。
        let limit = frame_limit(message.channel());
        if body_len > limit {
            return Err(ProtocolError::FrameTooLarge(body_len, limit));
        }
        Ok(Some((message.channel(), message)))
    }
}

#[cfg(test)]
mod tests {
    use super::super::fixtures;
    use super::super::message::KIND_JSON;
    use super::*;

    #[test]
    fn roundtrip_all_fixture_messages() {
        for message in fixtures::valid_messages() {
            let frame = encode_frame(&message).expect("encode");
            let mut decoder = FrameDecoder::new();
            // 半帧 + 粘帧喂入
            decoder.feed(&frame[..3]).unwrap();
            decoder.feed(&frame[3..]).unwrap();
            let (channel, decoded) = decoder.pop().expect("pop").expect("frame");
            assert_eq!(channel, message.channel());
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
        // 原始字节作为 blob 直接出现（无 base64 膨胀）。
        assert!(frame.windows(5).any(|w| w == [0u8, 255, 1, 2, 3]));
        let mut decoder = FrameDecoder::new();
        decoder.feed(&frame).unwrap();
        let (_, decoded) = decoder.pop().unwrap().unwrap();
        assert_eq!(decoded, message);
    }

    #[test]
    fn routed_transfer_chunk_carries_raw_bytes() {
        // 远程中继：Routed 内层 TransferChunk 也必须走 raw（帧内 blob），
        // 不经 JSON / base64。
        use super::super::message::Message as M;
        use super::super::remote::RouteEnvelope;
        let inner = M::TransferChunk {
            dispatch_id: "d".into(),
            transfer_id: "in:op".into(),
            offset: 0,
            bytes: vec![7u8; 4096],
            digest: "digest".into(),
        };
        let message = M::Routed {
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
        // 大块 raw 直接出现（无 base64 膨胀；4096 个 0x07 连续）。
        assert!(frame.windows(64).any(|w| w.iter().all(|b| *b == 7)));
        let mut decoder = FrameDecoder::new();
        decoder.feed(&frame).unwrap();
        let (_, decoded) = decoder.pop().unwrap().unwrap();
        assert_eq!(decoded, message);
    }

    #[test]
    fn rejects_unknown_kind_and_bad_meta_length() {
        // 未知 kind 字节。
        let mut frame = Vec::new();
        let meta = b"{}";
        let body_len = KIND_META_HEADER + meta.len();
        frame.extend_from_slice(&(body_len as u32).to_be_bytes());
        frame.push(99u8);
        frame.extend_from_slice(&(meta.len() as u32).to_be_bytes());
        frame.extend_from_slice(meta);
        let mut decoder = FrameDecoder::new();
        decoder.feed(&frame).unwrap();
        assert!(decoder.pop().is_err(), "unknown kind must be rejected");

        // meta_len 超出帧边界。
        let mut frame = Vec::new();
        let body_len = KIND_META_HEADER; // 声明没有 meta
        frame.extend_from_slice(&(body_len as u32).to_be_bytes());
        frame.push(KIND_JSON);
        frame.extend_from_slice(&(9999u32).to_be_bytes());
        let mut decoder = FrameDecoder::new();
        decoder.feed(&frame).unwrap();
        assert!(decoder.pop().is_err(), "meta overflow must be rejected");
    }

    #[test]
    fn rejects_malformed_envelopes() {
        for bad in fixtures::malformed_envelopes() {
            let json = serde_json::to_vec(&bad).unwrap();
            let mut frame = Vec::new();
            let body_len = KIND_META_HEADER + json.len();
            frame.extend_from_slice(&(body_len as u32).to_be_bytes());
            frame.push(KIND_JSON);
            frame.extend_from_slice(&(json.len() as u32).to_be_bytes());
            frame.extend_from_slice(&json);
            let mut decoder = FrameDecoder::new();
            decoder.feed(&frame).unwrap();
            assert!(
                decoder.pop().is_err(),
                "malformed envelope must be rejected: {bad}"
            );
        }
        let unknown = serde_json::to_vec(&fixtures::unknown_type_envelope()).unwrap();
        let mut frame = Vec::new();
        let body_len = KIND_META_HEADER + unknown.len();
        frame.extend_from_slice(&(body_len as u32).to_be_bytes());
        frame.push(KIND_JSON);
        frame.extend_from_slice(&(unknown.len() as u32).to_be_bytes());
        frame.extend_from_slice(&unknown);
        let mut decoder = FrameDecoder::new();
        decoder.feed(&frame).unwrap();
        assert!(decoder.pop().is_err());
    }

    #[test]
    fn sticky_and_split_frames() {
        let messages = fixtures::valid_messages();
        let a = encode_frame(&messages[0]).unwrap();
        let b = encode_frame(&messages[1]).unwrap();
        // 粘帧：两帧一次喂入 → 依次弹出。
        let mut decoder = FrameDecoder::new();
        decoder.feed(&[a.clone(), b.clone()].concat()).unwrap();
        let (_, first) = decoder.pop().unwrap().unwrap();
        let (_, second) = decoder.pop().unwrap().unwrap();
        assert_eq!(first, messages[0]);
        assert_eq!(second, messages[1]);
        // 半帧：逐字节喂入也能重组。
        let mut decoder = FrameDecoder::new();
        for byte in &a {
            decoder.feed(std::slice::from_ref(byte)).unwrap();
        }
        let (_, decoded) = decoder.pop().unwrap().unwrap();
        assert_eq!(decoded, messages[0]);
    }
}
