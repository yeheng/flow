//! WSS 承载（T13）：WebSocket 二进制消息 ↔ 单流字节流（postcard 帧）的适配。
//!
//! - **写**：transport 对整帧调用一次 `write_all` + `flush`，适配器把整帧
//!   作为**一条** Binary 消息发出（超限时按上限切分，读侧帧解码器自会重组）。
//! - **读**：Binary 消息字节按需交给上层；半帧/粘帧由 [`super::frame`] 的
//!   FrameDecoder 处理。Text 明确拒绝；单条消息超限拒绝；Ping 自动回 Pong；
//!   Close 视作 EOF。
//! - **有界**：写缓冲与单消息长度均有硬上限（不用无界队列化解背压）；
//!   超出单帧写上限是硬错误（[`std::io::ErrorKind::InvalidInput`]），不是
//!   背压信号——帧上限是硬边界，调用方必须自己切分。
//!
//! 该类型是 `FrameTransport` 的字节流承载面：直连 mTLS 与 WSS 复用同一
//! 传输/握手/会话校验逻辑，WSS 只替换物理承载。
//!
//! 守护的核心不变量：WS 只是承载，不改变帧语义——一条 Binary 消息
//! 可以承载半帧、多帧或帧的一部分，帧边界完全由上层帧解码器重建；
//! 任何一条 WS 消息的长度上限都必须与帧上限解耦（否则贴着帧上限的
//! 合法帧会在 WSS 上失败、在 TCP 上正常）。

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

/// 待发控制帧（Pong）队列上限：有界，满则丢最旧——Pong 是活性提示，
/// 不是正确性路径。丢弃是安全的（对端下一个 Ping 仍会被应答）。
const MAX_PENDING_CONTROL: usize = 8;

/// 把 WebSocket 二进制消息适配为字节流（`AsyncRead + AsyncWrite`）。
pub struct WsByteStream<S> {
    stream: WebSocketStream<S>,
    read_buf: Vec<u8>,
    read_pos: usize,
    write_buf: Vec<u8>,
    pending: std::collections::VecDeque<Message>,
    /// sink 内可能还有未冲刷字节（start_send 后 poll_flush 曾为 Pending）。
    /// 有了它，poll_read 入口才知道要重试冲刷——否则排队中的 Pong 会
    /// 停在 sink 缓冲里，直到下一个 Ping 或业务写才被带出去。
    write_dirty: bool,
    eof: bool,
}

impl<S> WsByteStream<S> {
    /// 包装一条已建立的 WebSocket 流为字节流适配器。
    ///
    /// 适配器不改变握手语义：WS 层的握手/鉴权在上游完成，这里只
    /// 负责字节与消息的双向搬运。
    pub fn new(stream: WebSocketStream<S>) -> Self {
        Self {
            stream,
            read_buf: Vec::new(),
            read_pos: 0,
            write_buf: Vec::new(),
            pending: std::collections::VecDeque::new(),
            write_dirty: false,
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
                        self.write_dirty = true;
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
                    self.write_dirty = true;
                }
            }
        }
        match Pin::new(&mut self.stream).poll_flush(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(ws_io(error))),
            Poll::Ready(Ok(())) => {
                self.write_dirty = false;
                Poll::Ready(Ok(()))
            }
        }
    }
}

impl<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> AsyncRead for WsByteStream<S> {
    /// 从 WS 消息里取字节喂给上层帧解码器。
    ///
    /// 半帧/粘帧都允许：这里只保证「字节流顺序正确」，帧边界由上层
    /// 重建。Text 消息明确拒绝（协议只用 Binary），Close 视作 EOF。
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        loop {
            // 先冲待发控制帧（Pong）与未冲刷字节：不依赖下一个 Ping 或
            // 业务写触发 flush。poll_ready 为 Pending 时保留待发并已注册
            // waker，就绪后被唤醒的 poll_read 会在此把它冲出去。
            if !self.pending.is_empty() || self.write_dirty {
                let _ = self.poll_send_pending(cx);
            }
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
                        // 回 Pong（best-effort）：待发队列有界
                        // [`MAX_PENDING_CONTROL`] 条，满则丢最旧。
                        self.pending.push_back(Message::Pong(payload));
                        if self.pending.len() > MAX_PENDING_CONTROL {
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
    /// 把字节写入待发缓冲（后续由 [`Self::poll_flush`] 按 WS 消息切分发出）。
    ///
    /// 契约：调用方一次写入的是**一整帧**（transport 对整帧调用一次
    /// `write_all`），因此缓冲上限只需容纳一个最大帧。
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        // AsyncWrite 契约：缓冲不足不是 WouldBlock（那是「稍后重试同一写入」
        // 的背压语义，但本适配器永远不会变出更多缓冲），而是硬错误。
        // 帧层保证单帧 ≤ MAX_WS_WRITE_BYTES（write_all 恰好一次装下），
        // 走到这里只可能是调用方违反单帧上限。
        if self.write_buf.len() + buf.len() > MAX_WS_WRITE_BYTES {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "ws write exceeds MAX_WS_WRITE_BYTES (one frame per write_all)",
            )));
        }
        self.write_buf.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    /// 冲刷待发缓冲与控制帧（Pong）。
    ///
    /// 写侧拥塞时返回 `Pending` 并保留待发内容，由 waker 唤醒后重试。
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.poll_send_pending(cx)
    }

    /// 关闭：先冲刷待发内容，再发 WS Close 帧。
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

/// 把 tungstenite 错误折成 I/O 错误（统一映射为断链语义）。
///
/// 统一成 `BrokenPipe` 而不是逐个透传 tungstenite 的错误种类：上层只
/// 需要知道「连接断了」，再细分错误类型没有可操作性。
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

    /// 超出单帧写上限是硬错误（InvalidInput），不是 WouldBlock 背压信号：
    /// 缓冲永远不会变大，报背压等于把调用方挂在一个永不完成的重试上。
    #[tokio::test]
    async fn oversized_write_is_a_hard_error() {
        let (mut adapter, _raw) = pair().await;
        let too_big = vec![0u8; MAX_WS_WRITE_BYTES + 1];
        let error = adapter.write(&too_big).await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        // 上限内的写入不受影响。
        assert_eq!(adapter.write(&[1u8, 2, 3]).await.unwrap(), 3);
    }

    /// Ping 在写侧拥塞（poll_ready = Pending）期间到达：Pong 只能排队；
    /// 写侧恢复后，即便没有下一个 Ping、也没有业务写，poll_read 也必须
    /// 把排队中的 Pong 冲出去（否则对端等到下一个 Ping 才看到响应）。
    #[tokio::test]
    async fn queued_pong_is_flushed_without_next_ping() {
        let (a, b) = tokio::io::duplex(4096);
        let gate = Gate::default();
        gate.open();
        let (client, server) = tokio::join!(
            tokio_tungstenite::client_async(
                "ws://localhost/ws/agent",
                GatedStream::new(a, gate.clone())
            ),
            tokio_tungstenite::accept_async(b),
        );
        let mut adapter = WsByteStream::new(client.unwrap().0);
        let mut raw = server.unwrap();
        // 关闸制造写侧拥塞：业务帧滞留，start_send 的 WouldBlock 使
        // poll_ready 进入 Pending 重试态。
        gate.close();
        adapter.write_all(&[7u8; 32]).await.unwrap();
        let flush =
            tokio::time::timeout(std::time::Duration::from_millis(50), adapter.flush()).await;
        assert!(flush.is_err(), "写侧拥塞下 flush 必须停在 pending");
        // Ping 在写侧不可用时到达：Pong 只能排队，此刻发不出去。
        raw.send(Message::Ping(vec![1u8, 2, 3].into()))
            .await
            .unwrap();
        let read = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            adapter.read(&mut [0u8; 1]),
        )
        .await;
        assert!(read.is_err(), "无入站数据时读必须停在 pending");
        // 开闸（写侧恢复）：不发第二个 Ping、也不做业务写，排队的 Pong
        // 必须随 poll_read 的入口冲刷发出。
        gate.open();
        let _ = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            adapter.read(&mut [0u8; 1]),
        )
        .await;
        let first = tokio::time::timeout(std::time::Duration::from_secs(2), raw.next())
            .await
            .expect("滞留的业务帧随开闸写出")
            .unwrap()
            .unwrap();
        assert!(matches!(first, Message::Binary(data) if data.len() == 32));
        let pong = tokio::time::timeout(std::time::Duration::from_secs(2), raw.next())
            .await
            .expect("排队的 Pong 必须发出，不依赖下一个 Ping")
            .unwrap()
            .unwrap();
        assert!(matches!(pong, Message::Pong(payload) if payload.to_vec() == vec![1, 2, 3]));
    }

    /// 可控背压的字节流：gate 关闭时 poll_write 返回 Pending 并登记 waker，
    /// 模拟底层写侧拥塞（WS 层 poll_ready 停在 Pending 重试态）。
    struct GatedStream {
        inner: tokio::io::DuplexStream,
        gate: Gate,
    }

    impl GatedStream {
        /// 包装一条 duplex 流并挂上闸门。
        fn new(inner: tokio::io::DuplexStream, gate: Gate) -> Self {
            Self { inner, gate }
        }
    }

    #[derive(Clone, Default)]
    struct Gate {
        open: std::sync::Arc<std::sync::atomic::AtomicBool>,
        waker: std::sync::Arc<std::sync::Mutex<Option<std::task::Waker>>>,
    }

    impl Gate {
        /// 开闸并唤醒等待者（写侧恢复可用）。
        fn open(&self) {
            self.open.store(true, std::sync::atomic::Ordering::SeqCst);
            self.wake();
        }
        /// 关闸：写侧转为拥塞（poll_write 返回 Pending）。
        fn close(&self) {
            self.open.store(false, std::sync::atomic::Ordering::SeqCst);
        }
        /// 唤醒最后一个登记的 waker（闸门状态变化后必须调用）。
        fn wake(&self) {
            if let Some(waker) = self.waker.lock().unwrap().take() {
                waker.wake();
            }
        }
    }

    impl tokio::io::AsyncRead for GatedStream {
        /// 读方向不受闸门影响（只关写侧）。
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl tokio::io::AsyncWrite for GatedStream {
        /// 闸门关闭时返回 Pending 并登记 waker，模拟底层写侧拥塞。
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            if !self.gate.open.load(std::sync::atomic::Ordering::SeqCst) {
                *self.gate.waker.lock().unwrap() = Some(cx.waker().clone());
                return Poll::Pending;
            }
            Pin::new(&mut self.inner).poll_write(cx, buf)
        }

        /// 闸门关闭时同样返回 Pending（写侧整体不可用）。
        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            if !self.gate.open.load(std::sync::atomic::Ordering::SeqCst) {
                *self.gate.waker.lock().unwrap() = Some(cx.waker().clone());
                return Poll::Pending;
            }
            Pin::new(&mut self.inner).poll_flush(cx)
        }

        /// 关闸不影响 shutdown（用于收尾测试）。
        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }
}
