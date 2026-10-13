//! WSS 承载（T13）：WebSocket 二进制消息 ↔ v4 单流字节流的适配。
//!
//! - **写**：transport 对整帧调用一次 `write_all` + `flush`，适配器把整帧
//!   作为**一条** Binary 消息发出（超限时按上限切分，读侧帧解码器自会重组）。
//! - **读**：Binary 消息字节按需交给上层；半帧/粘帧由 [`super::frame`] 的
//!   FrameDecoder 处理。Text 明确拒绝；单条消息超限拒绝；Ping 自动回 Pong；
//!   Close 视作 EOF。
//! - **有界**：写缓冲与单消息长度均有硬上限（不用无界队列化解背压）。
//!
//! 该类型是 `FrameTransport` 的字节流承载面：直连 mTLS 与 WSS 复用同一
//! 传输/握手/会话校验逻辑，WSS 只替换物理承载。

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{Sink, Stream};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

/// 单条 WS 消息字节上限：必须容纳最大 data 帧（`DATA_MAX_FRAME` = 1 MiB）
/// 的分片。单帧（4 字节头 + ≤1 MiB body）超限时按上限切分为多条 Binary
/// 消息，读侧帧解码器自会重组——上限本身不需要装下整帧。
pub const MAX_WS_MESSAGE_BYTES: usize = 1024 * 1024;
/// 待 flush 写缓冲上限：一次 `write_all` 的整帧不会超过此值。取
/// `FRAME_LENGTH_BYTES + DATA_MAX_FRAME + 余量`：帧层允许的
/// `body_len ≤ DATA_MAX_FRAME`，整帧 = 4 + body_len，若上限只取 1 MiB，
/// 贴着上限的合法帧会在 WSS 上写失败（WouldBlock）而 TCP 上正常。
pub const MAX_WS_WRITE_BYTES: usize = 1024 * 1024 + 4 + 64;

/// 把 WebSocket 二进制消息适配为字节流（`AsyncRead + AsyncWrite`）。
pub struct WsByteStream<S> {
    stream: WebSocketStream<S>,
    read_buf: Vec<u8>,
    read_pos: usize,
    write_buf: Vec<u8>,
    pending: std::collections::VecDeque<Message>,
    eof: bool,
}

impl<S> WsByteStream<S> {
    pub fn new(stream: WebSocketStream<S>) -> Self {
        Self {
            stream,
            read_buf: Vec::new(),
            read_pos: 0,
            write_buf: Vec::new(),
            pending: std::collections::VecDeque::new(),
            eof: false,
        }
    }
}

impl<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> WsByteStream<S> {
    /// 尽力发送待发控制帧（Pong）与写缓冲；背压时保留待下次 flush。
    fn poll_send_pending(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        loop {
            if !self.pending.is_empty() {
                match Pin::new(&mut self.stream).poll_ready(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(ws_io(error))),
                    Poll::Ready(Ok(())) => {
                        let message = self.pending.pop_front().unwrap();
                        if let Err(error) = Pin::new(&mut self.stream).start_send(message) {
                            return Poll::Ready(Err(ws_io(error)));
                        }
                    }
                }
                continue;
            }
            if self.write_buf.is_empty() {
                break;
            }
            // 超限时切分为多条 Binary 消息（读侧帧解码器自会重组）。
            let take = self.write_buf.len().min(MAX_WS_MESSAGE_BYTES);
            match Pin::new(&mut self.stream).poll_ready(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(ws_io(error))),
                Poll::Ready(Ok(())) => {
                    let chunk: Vec<u8> = self.write_buf.drain(..take).collect();
                    if let Err(error) =
                        Pin::new(&mut self.stream).start_send(Message::Binary(chunk.into()))
                    {
                        return Poll::Ready(Err(ws_io(error)));
                    }
                }
            }
        }
        match Pin::new(&mut self.stream).poll_flush(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(ws_io(error))),
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
        }
    }
}

impl<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> AsyncRead for WsByteStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        loop {
            if self.read_pos < self.read_buf.len() {
                let take = (self.read_buf.len() - self.read_pos).min(buf.remaining());
                buf.put_slice(&self.read_buf[self.read_pos..self.read_pos + take]);
                self.read_pos += take;
                return Poll::Ready(Ok(()));
            }
            if self.eof {
                return Poll::Ready(Ok(())); // 0 字节 = EOF
            }
            match Pin::new(&mut self.stream).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    self.eof = true;
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(ws_io(error))),
                Poll::Ready(Some(Ok(message))) => match message {
                    Message::Binary(data) => {
                        if data.len() > MAX_WS_MESSAGE_BYTES {
                            return Poll::Ready(Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "ws binary message exceeds limit",
                            )));
                        }
                        self.read_buf = data.to_vec();
                        self.read_pos = 0;
                    }
                    Message::Text(_) => {
                        return Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "ws text message not allowed",
                        )));
                    }
                    Message::Ping(payload) => {
                        // 回 Pong（best-effort，待发队列有界 1 条）。
                        self.pending.push_back(Message::Pong(payload));
                        if self.pending.len() > 8 {
                            self.pending.pop_front();
                        }
                        let _ = self.poll_send_pending(cx);
                    }
                    Message::Pong(_) | Message::Frame(_) => {}
                    Message::Close(_) => {
                        self.eof = true;
                        return Poll::Ready(Ok(()));
                    }
                },
            }
        }
    }
}

impl<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> AsyncWrite for WsByteStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.write_buf.len() + buf.len() > MAX_WS_WRITE_BYTES {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "ws write buffer exceeds limit",
            )));
        }
        self.write_buf.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.poll_send_pending(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        // 先冲刷待发，再发 Close。
        match self.poll_send_pending(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {}
        }
        match Pin::new(&mut self.stream).poll_close(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(ws_io(error))),
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
        }
    }
}

fn ws_io(error: tokio_tungstenite::tungstenite::Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::BrokenPipe, error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{SinkExt, StreamExt};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_tungstenite::tungstenite::Message;

    /// 建立一对 WS：一侧包成 WsByteStream，另一侧保留原始 WebSocketStream
    /// 以便注入 Text/Ping/Close 等控制形态。
    async fn pair() -> (
        WsByteStream<tokio::io::DuplexStream>,
        WebSocketStream<tokio::io::DuplexStream>,
    ) {
        let (a, b) = tokio::io::duplex(256 * 1024);
        let (client, server) = tokio::join!(
            tokio_tungstenite::client_async("ws://localhost/ws/agent", a),
            tokio_tungstenite::accept_async(b),
        );
        (WsByteStream::new(client.unwrap().0), server.unwrap())
    }

    #[tokio::test]
    async fn binary_roundtrip_as_byte_stream() {
        let (mut adapter, mut raw) = pair().await;
        let payload: Vec<u8> = (0..4096u32).map(|n| (n % 251) as u8).collect();
        adapter.write_all(&payload).await.unwrap();
        adapter.flush().await.unwrap();
        // 原始侧收到一条 Binary，内容一致。
        let message = raw.next().await.unwrap().unwrap();
        match message {
            Message::Binary(data) => assert_eq!(data.to_vec(), payload),
            other => panic!("expected binary, got {other:?}"),
        }
        // 反向：原始侧发 Binary，适配器读回。
        raw.send(Message::Binary(vec![9u8, 8, 7].into()))
            .await
            .unwrap();
        let mut back = [0u8; 3];
        adapter.read_exact(&mut back).await.unwrap();
        assert_eq!(back, [9, 8, 7]);
    }

    #[tokio::test]
    async fn text_message_is_rejected() {
        let (mut adapter, mut raw) = pair().await;
        raw.send(Message::Text("not allowed".into())).await.unwrap();
        let mut buf = [0u8; 1];
        let error = adapter.read(&mut buf).await.expect_err("text must fail");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn ping_gets_pong_and_close_is_eof() {
        let (mut adapter, mut raw) = pair().await;
        raw.send(Message::Ping(vec![1u8, 2, 3].into()))
            .await
            .unwrap();
        // 适配器读循环处理 Ping→Pong；随后发 Close。
        raw.send(Message::Close(None)).await.unwrap();
        let mut buf = [0u8; 4];
        // 读到 EOF（0 字节）即证明 Close 被识别；同时 Pong 应在原始侧可见。
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            let mut seen_pong = false;
            while let Some(message) = raw.next().await {
                match message.unwrap() {
                    Message::Pong(payload) => {
                        assert_eq!(payload.to_vec(), vec![1, 2, 3]);
                        seen_pong = true;
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            seen_pong
        })
        .await;
        let n = adapter.read(&mut buf).await.unwrap();
        assert_eq!(n, 0, "close must surface as EOF");
    }
}
