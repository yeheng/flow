//! 物理帧编解码（二期 §3.3）。
//!
//! 每帧为 `u32 大端长度 + UTF-8 JSON`，长度只计算 JSON 部分。读端处理
//! 半帧/粘帧：先校验长度上限再保留缓冲，防止非法长度触发无界分配。

use super::contract::{CONTROL_MAX_FRAME, DATA_MAX_FRAME, FRAME_HEADER_BYTES};
use super::message::{Channel, Message, ProtocolError};

/// 编码一帧。上限按 channel 取 control/data；JSON 部分超限即拒绝，
/// 不静默截断。
pub fn encode_frame(message: &Message) -> Result<Vec<u8>, ProtocolError> {
    let json = serde_json::to_vec(&message.to_envelope())
        .map_err(|e| ProtocolError::Malformed(format!("encode: {e}")))?;
    let limit = match message.channel() {
        Channel::Control => CONTROL_MAX_FRAME,
        Channel::Data => DATA_MAX_FRAME,
    };
    if json.len() > limit {
        return Err(ProtocolError::FrameTooLarge(json.len(), limit));
    }
    let mut frame = Vec::with_capacity(json.len() + FRAME_HEADER_BYTES);
    frame.extend_from_slice(&(json.len() as u32).to_be_bytes());
    frame.extend_from_slice(&json);
    Ok(frame)
}

/// 流式帧解码器：喂入任意切分的字节，弹出完整帧的 JSON 字节。
/// 头 4 字节一到就校验长度上限，非法长度在分配任何载荷缓冲前拒绝。
#[derive(Debug)]
pub struct FrameDecoder {
    limit: usize,
    buffer: Vec<u8>,
    validated: bool,
}

impl FrameDecoder {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            buffer: Vec::new(),
            validated: false,
        }
    }

    /// 追加字节。粘帧由缓冲自然承载；`pop_frame` 逐帧弹出。
    pub fn feed(&mut self, bytes: &[u8]) -> Result<(), ProtocolError> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.buffer.extend_from_slice(bytes);
        self.validated = false;
        // 校验当前缓冲头部声明的长度：必须在解析任何载荷前完成。
        self.ensure_head_valid()?;
        if self.buffer.capacity() > self.limit + FRAME_HEADER_BYTES + 1024 {
            self.buffer.shrink_to(self.limit + FRAME_HEADER_BYTES);
        }
        Ok(())
    }

    fn ensure_head_valid(&mut self) -> Result<(), ProtocolError> {
        if self.validated || self.buffer.len() < FRAME_HEADER_BYTES {
            return Ok(());
        }
        let length = self.head_length().expect("checked above");
        if length == 0 {
            return Err(ProtocolError::Malformed("empty frame".into()));
        }
        if length > self.limit {
            return Err(ProtocolError::FrameTooLarge(length, self.limit));
        }
        self.validated = true;
        Ok(())
    }

    fn head_length(&self) -> Option<usize> {
        if self.buffer.len() < FRAME_HEADER_BYTES {
            return None;
        }
        Some(u32::from_be_bytes([
            self.buffer[0],
            self.buffer[1],
            self.buffer[2],
            self.buffer[3],
        ]) as usize)
    }

    /// 弹出一个已解码帧的 JSON 字节（无完整帧则 None）。非法长度在此时
    /// 也会被拒绝（半帧头到达后的下一次调用）。
    pub fn pop_frame(&mut self) -> Result<Option<Vec<u8>>, ProtocolError> {
        self.ensure_head_valid()?;
        let Some(length) = self.head_length() else {
            return Ok(None);
        };
        if self.buffer.len() < FRAME_HEADER_BYTES + length {
            return Ok(None);
        }
        let json = self.buffer[FRAME_HEADER_BYTES..FRAME_HEADER_BYTES + length].to_vec();
        self.buffer.drain(..FRAME_HEADER_BYTES + length);
        self.validated = false;
        Ok(Some(json))
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
            let mut decoder = FrameDecoder::new(DATA_MAX_FRAME);
            decoder.feed(&frame).expect("feed");
            let json = decoder.pop_frame().expect("frame").expect("json");
            let decoded =
                Message::from_envelope(serde_json::from_slice(&json).expect("envelope json"))
                    .expect("decode");
            assert_eq!(decoded, message);
        }
    }

    #[test]
    fn half_and_sticky_frames() {
        let frame = encode_frame(&fixtures::valid_messages()[0]).unwrap();
        let mut decoder = FrameDecoder::new(DATA_MAX_FRAME);
        // 半帧：无输出
        decoder.feed(&frame[..frame.len() / 2]).unwrap();
        assert!(decoder.pop_frame().unwrap().is_none());
        // 剩余 + 下一帧粘在一起
        let frame2 = encode_frame(&fixtures::valid_messages()[1]).unwrap();
        let mut stuck = frame[frame.len() / 2..].to_vec();
        stuck.extend_from_slice(&frame2);
        decoder.feed(&stuck).unwrap();
        assert_eq!(decoder.pop_frame().unwrap().unwrap(), &frame[4..]);
        assert_eq!(decoder.pop_frame().unwrap().unwrap(), &frame2[4..]);
        assert!(decoder.pop_frame().unwrap().is_none());
    }

    #[test]
    fn illegal_length_rejected_before_allocation() {
        let mut decoder = FrameDecoder::new(CONTROL_MAX_FRAME);
        assert!(matches!(
            decoder.feed(&u32::MAX.to_be_bytes()),
            Err(ProtocolError::FrameTooLarge(_, _))
        ));
        let mut small = FrameDecoder::new(CONTROL_MAX_FRAME);
        let oversized = (CONTROL_MAX_FRAME as u32 + 1).to_be_bytes().to_vec();
        assert!(matches!(
            small.feed(&oversized),
            Err(ProtocolError::FrameTooLarge(_, _))
        ));
        // 零长度帧同样非法
        let mut zero = FrameDecoder::new(CONTROL_MAX_FRAME);
        assert!(matches!(
            zero.feed(&0u32.to_be_bytes()),
            Err(ProtocolError::Malformed(_))
        ));
    }

    #[test]
    fn unknown_and_malformed_envelopes_rejected() {
        for bad in fixtures::malformed_envelopes() {
            assert!(Message::from_envelope(bad.clone()).is_err());
        }
        assert!(matches!(
            Message::from_envelope(fixtures::unknown_type_envelope()),
            Err(ProtocolError::UnknownMessage(_))
        ));
    }

    #[test]
    fn control_limit_enforced_per_channel() {
        let big = Message::ObservabilityBatch {
            dispatch_id: None,
            lines: vec![super::super::message::ObservationLine {
                level: "info".into(),
                stream: "stdout".into(),
                message: "x".repeat(200_000),
            }],
        };
        // data 通道允许（1 MiB）。
        encode_frame(&big).expect("data channel accepts");
        let big_control = Message::Reject {
            reason: "x".repeat(100_000),
        };
        assert!(matches!(
            encode_frame(&big_control),
            Err(ProtocolError::FrameTooLarge(_, _))
        ));
    }

    #[test]
    fn audit_record_encoding_is_deterministic_and_counted() {
        let record = fixtures::audit_record();
        let a = record.encoded_bytes();
        let b = record.encoded_bytes();
        assert_eq!(a, b);
        assert!(a > 0);
        // 修改 payload 改变计费字节
        let mut other = record.clone();
        other.payload["extra"] = serde_json::json!("more bytes");
        assert!(other.encoded_bytes() > a);
    }
}
