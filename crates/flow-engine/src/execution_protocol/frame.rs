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

    /// 弹出一个完整消息（含通道）；无完整帧返回 None。帧在缓冲内原地
    /// 解析（不复制帧体），仅在帧完整且校验通过后才从缓冲摘除。
    pub fn pop(&mut self) -> Result<Option<(Channel, Message)>, ProtocolError> {
        if self.buffer.len() < FRAME_LENGTH_BYTES {
            return Ok(None);
        }
        let body_len = self.head_length();
        if !(KIND_META_HEADER..=DATA_MAX_FRAME).contains(&body_len) {
            return Err(ProtocolError::FrameTooLarge(body_len, DATA_MAX_FRAME));
        }
        let total = FRAME_LENGTH_BYTES + body_len;
        if self.buffer.len() < total {
            return Ok(None);
        }
        let kind_byte = self.buffer[FRAME_LENGTH_BYTES];
        let meta_len = u32::from_be_bytes([
            self.buffer[FRAME_LENGTH_BYTES + 1],
            self.buffer[FRAME_LENGTH_BYTES + 2],
            self.buffer[FRAME_LENGTH_BYTES + 3],
            self.buffer[FRAME_LENGTH_BYTES + 4],
        ]) as usize;
        // meta 必须完整落在本帧 body 内（body_len 已含 kind+meta_len+meta+blob）。
        if meta_len > body_len - KIND_META_HEADER {
            return Err(ProtocolError::Malformed(
                "frame meta length exceeds body".into(),
            ));
        }
        let meta_start = FRAME_LENGTH_BYTES + KIND_META_HEADER;
        let meta = &self.buffer[meta_start..meta_start + meta_len];
        let blob = &self.buffer[meta_start + meta_len..total];
        let message = Message::from_frame_parts(kind_byte, meta, blob)?;
        // 通道预算复核（control 收紧到 CONTROL_MAX_FRAME）。
        let limit = frame_limit(message.channel());
        if body_len > limit {
            return Err(ProtocolError::FrameTooLarge(body_len, limit));
        }
        self.buffer.drain(..total);
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
    fn json_frame_with_blob_is_rejected() {
        // kind=0 帧携带非空 raw blob：blob 会被静默丢弃（v4 前），必须拒绝。
        let mut frame = Vec::new();
        let meta = serde_json::to_vec(&serde_json::json!({"v": 4, "type": "Ready"})).unwrap();
        let body_len = KIND_META_HEADER + meta.len() + 4;
        frame.extend_from_slice(&(body_len as u32).to_be_bytes());
        frame.push(KIND_JSON);
        frame.extend_from_slice(&(meta.len() as u32).to_be_bytes());
        frame.extend_from_slice(&meta);
        frame.extend_from_slice(b"junk");
        let mut decoder = FrameDecoder::new();
        decoder.feed(&frame).unwrap();
        assert!(
            decoder.pop().is_err(),
            "JSON frame carrying a blob must be rejected"
        );
    }

    #[test]
    fn routed_transfer_meta_must_be_self_consistent() {
        // kind=2 的外层 meta / body.envelope / 内层三处 dispatch_id 不一致
        // 时必须拒绝（路由键与载荷归属不得分离）。
        use super::super::message::Message as M;
        use super::super::remote::RouteEnvelope;
        let inner = M::TransferChunk {
            dispatch_id: "d".into(),
            transfer_id: "t".into(),
            offset: 0,
            bytes: vec![1u8, 2, 3],
            digest: "digest".into(),
        };
        let good = M::Routed {
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
        // 篡改外层 meta 的 dispatch_id（与内层错位）。
        let (kind_byte, meta, blob) = good.to_frame_parts().unwrap();
        assert_eq!(kind_byte, super::super::message::KIND_ROUTED_TRANSFER_CHUNK);
        let mut value: serde_json::Value = serde_json::from_slice(&meta).unwrap();
        value["dispatch_id"] = serde_json::json!("other");
        let tampered = serde_json::to_vec(&value).unwrap();
        assert!(M::from_frame_parts(kind_byte, &tampered, &blob).is_err());
        // 篡改 body.envelope.dispatch_id。
        let mut value: serde_json::Value = serde_json::from_slice(&meta).unwrap();
        value["body"]["envelope"]["dispatch_id"] = serde_json::json!("other");
        let tampered = serde_json::to_vec(&value).unwrap();
        assert!(M::from_frame_parts(kind_byte, &tampered, &blob).is_err());
        // 未篡改：往返成立。
        assert_eq!(M::from_frame_parts(kind_byte, &meta, &blob).unwrap(), good);
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
