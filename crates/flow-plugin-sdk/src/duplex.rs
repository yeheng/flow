//! Full-duplex JSON-RPC pump. Exactly one reader and one writer own the stream.
use crate::protocol::envelope::{read_frame, write_frame};
use crate::{Frame, FrameKind, Message};
use std::io;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

pub async fn pump<S>(
    stream: S,
    mut outgoing: mpsc::Receiver<Message>,
    incoming: mpsc::Sender<Message>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut reader, mut writer) = tokio::io::split(stream);
    let read = async {
        loop {
            let frame = read_frame(&mut reader).await?;
            if frame.kind != FrameKind::Rpc {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "service expects RPC frames",
                ));
            }
            let message = frame
                .as_rpc()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            incoming.send(message).await.map_err(|_| {
                io::Error::new(io::ErrorKind::BrokenPipe, "incoming consumer closed")
            })?;
        }
    };
    let write = async {
        while let Some(message) = outgoing.recv().await {
            let payload = message.to_vec().map_err(io::Error::other)?;
            write_frame(
                &mut writer,
                &Frame {
                    kind: FrameKind::Rpc,
                    payload,
                },
            )
            .await?;
        }
        Ok(())
    };
    tokio::select! {r=read=>r,r=write=>r}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Notification;
    #[tokio::test]
    async fn receives_while_sending_without_cancelling_partial_frames() {
        let (host, mut child) = tokio::io::duplex(32);
        let (tx, rx) = mpsc::channel(2);
        let (incoming, mut messages) = mpsc::channel(2);
        let task = tokio::spawn(pump(host, rx, incoming));
        let event = Message::Notification(Notification {
            jsonrpc: "2.0",
            method: "event".into(),
            params: None,
        });
        let bytes = Frame {
            kind: FrameKind::Rpc,
            payload: event.to_vec().unwrap(),
        }
        .encode()
        .unwrap();
        use tokio::io::AsyncWriteExt;
        child.write_all(&bytes[..3]).await.unwrap();
        tx.send(event.clone()).await.unwrap();
        assert_eq!(
            read_frame(&mut child).await.unwrap().as_rpc().unwrap(),
            event
        );
        child.write_all(&bytes[3..]).await.unwrap();
        assert_eq!(messages.recv().await.unwrap(), event);
        drop(tx);
        task.await.unwrap().unwrap();
    }
}
