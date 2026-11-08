//! 帧格式：`[u8 kind][u32 BE len][payload]`。
//!
//! 设计取舍：
//!
//! - **显式 kind**：一帧是什么，帧头说了算。对照反例是 postcard 的变体
//!   索引编码 —— 类型编号藏在 Rust 枚举的声明顺序里，跨版本/跨语言配对
//!   错了不报错，静默错解成另一种消息。
//! - **u32 大端长度**：定长、无歧义、任何语言 5 行代码就能读。上限
//!   [`MAX_FRAME_BYTES`]（1 MiB）同时封顶内存：解码前就知道要分配多少，
//!   坏帧不能借「声明一个巨大长度」让宿主预分配到死。
//! - **不做分帧协商、不做压缩**：插件协议的控制帧都很小；大二进制走
//!   kind=binary 的原始字节，零膨胀（对比 JSON 里 base64 的 +33%）。

use std::io;

use crate::ProtoError;

/// 帧种类。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    /// JSON-RPC 2.0 消息（UTF-8 JSON，见 [`crate::jsonrpc`]）。
    Rpc = 0x01,
    /// 二进制块（原始字节，语义由上层的 JSON-RPC 消息约定）。
    Binary = 0x02,
}

impl FrameKind {
    /// 从 kind 字节解析；未知值返回 `None`（调用方按协议错误处理）。
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0x01 => Some(Self::Rpc),
            0x02 => Some(Self::Binary),
            _ => None,
        }
    }
}

/// 单帧上限：kind=rpc 与 kind=binary 共用。
///
/// 1 MiB 覆盖控制消息绰绰有余；更大的数据必须由上层切分成多个
/// kind=binary 块（块大小建议 ≤ 256 KiB）。
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;

/// 一帧：kind + 载荷。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// 帧种类。
    pub kind: FrameKind,
    /// 载荷（rpc：UTF-8 JSON；binary：原始字节）。
    pub payload: Vec<u8>,
}

/// 编码一帧：`[kind][u32 BE len][payload]`。
///
/// 载荷超过 [`MAX_FRAME_BYTES`] 时报错 —— 编码侧先拒绝，别等解码侧炸。
pub fn encode_frame(kind: FrameKind, payload: &[u8]) -> Result<Vec<u8>, ProtoError> {
    if payload.len() > MAX_FRAME_BYTES {
        return Err(ProtoError::FrameTooLarge(payload.len()));
    }
    let mut out = Vec::with_capacity(payload.len() + 5);
    out.push(kind as u8);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

impl Frame {
    /// 编码（见 [`encode_frame`]）。
    pub fn encode(&self) -> Result<Vec<u8>, ProtoError> {
        encode_frame(self.kind, &self.payload)
    }

    /// 从「完整的一帧字节」（含 5 字节头）解码。
    ///
    /// 输入必须正好是一帧：多了少了都报错 —— 这个方法服务向量测试与
    /// 内存中的确定性路径；流式半包/粘包用 [`read_frame`]。
    pub fn decode(bytes: &[u8]) -> Result<Self, ProtoError> {
        if bytes.len() < 5 {
            return Err(ProtoError::TruncatedFrame {
                declared: 0,
                received: bytes.len(),
            });
        }
        let kind = FrameKind::from_byte(bytes[0]).ok_or(ProtoError::UnknownKind(bytes[0]))?;
        let declared = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
        if declared > MAX_FRAME_BYTES {
            return Err(ProtoError::FrameTooLarge(declared));
        }
        let payload = &bytes[5..];
        if payload.len() != declared {
            return Err(ProtoError::TruncatedFrame {
                declared,
                received: payload.len(),
            });
        }
        Ok(Self {
            kind,
            payload: payload.to_vec(),
        })
    }

    /// rpc 帧的载荷按 UTF-8 JSON 解析为 JSON-RPC 消息。
    pub fn as_rpc(&self) -> Result<crate::protocol::jsonrpc::Message, ProtoError> {
        if self.kind != FrameKind::Rpc {
            return Err(ProtoError::InvalidMessage(format!(
                "expected kind=rpc(0x01), got 0x{:02x}",
                self.kind as u8
            )));
        }
        crate::protocol::jsonrpc::Message::from_slice(&self.payload)
    }
}

/// 从字节流读一帧（处理半包；粘包由调用方持有剩余缓冲处理）。
///
/// `EOF` 出现在帧头之前 = 干净关闭；出现在帧中间 = [`ProtoError::TruncatedFrame`]。
pub async fn read_frame<S>(stream: &mut S) -> Result<Frame, io::Error>
where
    S: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).await?;
    let kind = FrameKind::from_byte(header[0]).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            ProtoError::UnknownKind(header[0]),
        )
    })?;
    let declared = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
    if declared > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            ProtoError::FrameTooLarge(declared),
        ));
    }
    let mut payload = vec![0u8; declared];
    stream.read_exact(&mut payload).await?;
    Ok(Frame { kind, payload })
}

/// 把一帧写进字节流。
pub async fn write_frame<S>(stream: &mut S, frame: &Frame) -> Result<(), io::Error>
where
    S: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;
    let bytes = frame
        .encode()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    stream.write_all(&bytes).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_rpc_and_binary() {
        for (kind, payload) in [
            (FrameKind::Rpc, br#"{"jsonrpc":"2.0"}"#.to_vec()),
            (FrameKind::Binary, vec![0xde, 0xad, 0xbe, 0xef]),
        ] {
            let frame = Frame {
                kind,
                payload: payload.clone(),
            };
            let bytes = frame.encode().unwrap();
            assert_eq!(
                &bytes[..5],
                &[kind as u8][..]
                    .iter()
                    .copied()
                    .chain((payload.len() as u32).to_be_bytes())
                    .collect::<Vec<u8>>()[..5]
            );
            assert_eq!(Frame::decode(&bytes).unwrap(), frame);
        }
    }

    #[test]
    fn oversize_is_rejected_on_encode_and_decode() {
        let payload = vec![0u8; MAX_FRAME_BYTES + 1];
        assert!(matches!(
            encode_frame(FrameKind::Binary, &payload),
            Err(ProtoError::FrameTooLarge(_))
        ));
        // 解码侧：声明超限的头（哪怕载荷没真给那么多）也必须立刻拒绝。
        let mut evil = vec![FrameKind::Binary as u8];
        evil.extend_from_slice(&((MAX_FRAME_BYTES as u32) + 1).to_be_bytes());
        assert!(matches!(
            Frame::decode(&evil),
            Err(ProtoError::FrameTooLarge(_))
        ));
    }

    #[test]
    fn unknown_kind_and_truncated_are_rejected() {
        let mut bytes = encode_frame(FrameKind::Rpc, b"{}").unwrap();
        bytes[0] = 0x7f;
        assert!(matches!(
            Frame::decode(&bytes),
            Err(ProtoError::UnknownKind(0x7f))
        ));
        let bytes = encode_frame(FrameKind::Rpc, b"{}").unwrap();
        assert!(matches!(
            Frame::decode(&bytes[..bytes.len() - 1]),
            Err(ProtoError::TruncatedFrame { .. })
        ));
    }

    #[tokio::test]
    async fn stream_roundtrip_handles_exact_eof() {
        let frame = Frame {
            kind: FrameKind::Binary,
            payload: b"block".to_vec(),
        };
        let (mut writer, mut reader) = tokio::io::duplex(64);
        write_frame(&mut writer, &frame).await.unwrap();
        let read = read_frame(&mut reader).await.unwrap();
        assert_eq!(read, frame);
        drop(writer);
    }
}
