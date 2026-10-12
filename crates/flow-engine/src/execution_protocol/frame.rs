//! 物理帧编解码（二期 §3.3；v3 单流标签扩展）。
//!
//! 两种承载、两种帧格式，共享同一 JSON 信封：
//!
//! - **单流（执行器本地 IPC，v3）**：`u32 大端长度 + u8 通道标签 + JSON`，
//!   长度只计标签 + JSON。一条流内的帧序即双方提交序，跨通道乱序不存在。
//! - **双流（agent 上联，二期格式不变）**：`u32 大端长度 + JSON`，control 与
//!   data 各占一条物理流，靠流本身区分通道（二期 §3.1 原语义）。
//!
//! 读端处理半帧/粘帧：先校验长度上限再保留缓冲，防止非法长度触发无界分配。

use super::contract::{CONTROL_MAX_FRAME, DATA_MAX_FRAME, FRAME_HEADER_BYTES};
use super::message::{Channel, Message, ProtocolError};

/// 双流帧上限按 channel 取 control/data；JSON 部分超限即拒绝，
/// 不静默截断。
pub fn encode_frame(message: &Message) -> Result<Vec<u8>, ProtocolError> {
    let json = serde_json::to_vec(&message.to_envelope())
        .map_err(|e| ProtocolError::Malformed(format!("encode: {e}")))?;
    let limit = frame_limit(message.channel());
    if json.len() > limit {
        return Err(ProtocolError::FrameTooLarge(json.len(), limit));
    }
    let mut frame = Vec::with_capacity(json.len() + FRAME_HEADER_BYTES);
    frame.extend_from_slice(&(json.len() as u32).to_be_bytes());
    frame.extend_from_slice(&json);
    Ok(frame)
}

/// 单流帧：长度计标签 + JSON；标签取自消息声明的通道。
pub fn encode_tagged_frame(message: &Message) -> Result<Vec<u8>, ProtocolError> {
    let json = serde_json::to_vec(&message.to_envelope())
        .map_err(|e| ProtocolError::Malformed(format!("encode: {e}")))?;
    let channel = message.channel();
    let limit = frame_limit(channel);
    if json.len() + 1 > limit {
        return Err(ProtocolError::FrameTooLarge(json.len() + 1, limit));
    }
    let payload_len = json.len() + 1;
    let mut frame = Vec::with_capacity(payload_len + FRAME_HEADER_BYTES);
    frame.extend_from_slice(&(payload_len as u32).to_be_bytes());
    frame.push(channel.tag());
    frame.extend_from_slice(&json);
    Ok(frame)
}

fn frame_limit(channel: Channel) -> usize {
    match channel {
        Channel::Control => CONTROL_MAX_FRAME,
        Channel::Data => DATA_MAX_FRAME,
    }
}

/// 流式帧解码器：喂入任意切分的字节，弹出完整帧的 JSON 字节。
/// 头 4 字节一到就校验长度上限，非法长度在分配任何载荷缓冲前拒绝。
#[derive(Debug)]
pub struct FrameDecoder {
    limit: usize,
    buffer: Vec<u8>,
    validated: bool,
    tagged: bool,
}

impl FrameDecoder {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            buffer: Vec::new(),
            validated: false,
            tagged: false,
        }
    }

    /// 单流模式：上限按标签分流（control 收紧到 CONTROL_MAX_FRAME，
    /// 分配缓冲的全局硬顶取 data 上限）。标签字节到达即校验所属通道的
    /// 上限，不等到整帧缓冲完成。
    pub fn new_tagged() -> Self {
        Self {
            limit: DATA_MAX_FRAME + 1,
            buffer: Vec::new(),
            validated: false,
            tagged: true,
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
        if self.tagged {
            // 标签字节（第 5 字节）一到即按所属通道收紧上限。
            if self.buffer.len() < FRAME_HEADER_BYTES + 1 {
                return Ok(());
            }
            let tag = self.buffer[FRAME_HEADER_BYTES];
            let channel = Channel::from_tag(tag)
                .ok_or_else(|| ProtocolError::Malformed(format!("unknown channel tag {tag}")))?;
            let effective = frame_limit(channel) + 1;
            if length > effective {
                return Err(ProtocolError::FrameTooLarge(length, effective));
            }
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

    fn split_payload(&mut self, length: usize) -> Option<Vec<u8>> {
        if self.buffer.len() < FRAME_HEADER_BYTES + length {
            return None;
        }
        let payload = self.buffer[FRAME_HEADER_BYTES..FRAME_HEADER_BYTES + length].to_vec();
        self.buffer.drain(..FRAME_HEADER_BYTES + length);
        self.validated = false;
        Some(payload)
    }

    /// 弹出一个已解码帧的 JSON 字节（无完整帧则 None）。非法长度在此时
    /// 也会被拒绝（半帧头到达后的下一次调用）。
    pub fn pop_frame(&mut self) -> Result<Option<Vec<u8>>, ProtocolError> {
        self.ensure_head_valid()?;
        let Some(length) = self.head_length() else {
            return Ok(None);
        };
        Ok(self.split_payload(length))
    }

    /// 单流模式弹出：返回 (通道, JSON 字节)。通道由帧内标签声明。
    pub fn pop_tagged_frame(&mut self) -> Result<Option<(Channel, Vec<u8>)>, ProtocolError> {
        self.ensure_head_valid()?;
        let Some(length) = self.head_length() else {
            return Ok(None);
        };
        let Some(payload) = self.split_payload(length) else {
            return Ok(None);
        };
        let channel = Channel::from_tag(payload[0])
            .ok_or_else(|| ProtocolError::Malformed(format!("unknown channel tag {}", payload[0])))?;
        Ok(Some((channel, payload[1..].to_vec())))
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
    fn tagged_roundtrip_all_fixture_messages() {
        for message in fixtures::valid_messages() {
            let frame = encode_tagged_frame(&message).expect("encode");
            let mut decoder = FrameDecoder::new_tagged();
            // 半帧 + 粘帧喂入
            decoder.feed(&frame[..3]).unwrap();
            decoder.feed(&frame[3..]).unwrap();
            let (channel, json) = decoder
                .pop_tagged_frame()
                .expect("frame")
                .expect("json");
            assert_eq!(channel, message.channel());
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
    fn tagged_illegal_length_and_tag_rejected() {
        // 声明超过 data 硬顶的长度：头部即拒
        let mut decoder = FrameDecoder::new_tagged();
        assert!(matches!(
            decoder.feed(&(DATA_MAX_FRAME as u32 + 2).to_be_bytes()),
            Err(ProtocolError::FrameTooLarge(_, _))
        ));
        // 未知标签：标签字节到达即拒
        let mut decoder = FrameDecoder::new_tagged();
        decoder.feed(&8u32.to_be_bytes()).unwrap();
        assert!(matches!(
            decoder.feed(&[9u8]),
            Err(ProtocolError::Malformed(_))
        ));
        // control 帧声明 data 级长度：标签字节一到即按 128 KiB 拒
        let mut decoder = FrameDecoder::new_tagged();
        let length = (DATA_MAX_FRAME - 1024) as u32;
        decoder.feed(&length.to_be_bytes()).unwrap();
        assert!(matches!(
            decoder.feed(&[Channel::Control.tag()]),
            Err(ProtocolError::FrameTooLarge(_, _))
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
        encode_tagged_frame(&big).expect("tagged data channel accepts");
        let big_control = Message::Reject {
            reason: "x".repeat(200_000),
        };
        assert!(matches!(
            encode_frame(&big_control),
            Err(ProtocolError::FrameTooLarge(_, _))
        ));
        assert!(matches!(
            encode_tagged_frame(&big_control),
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
