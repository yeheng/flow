//! 执行器会话运行时：握手、单任务串行循环、心跳、取消与父死亡检测
//! （二期 §3.2/§3.7，I03）。
//!
//! 一个执行进程同一时刻只承载一个任务：空闲槽位的新派发或当前派发的
//! 重复消息；正常 ResultCommitted 前不复用槽位（二期 §3.4）。任一通道
//! EOF/错误即按失联处理：停止当前任务、尽力封口后退出进程，槽位由主\n//! 进程整体回收重建。

use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use flow_engine::execution_protocol::contract::bounded_control_text;
use flow_engine::execution_protocol::contract::STARTUP_TIMEOUT_MS;
use flow_engine::execution_protocol::loopback::connect_loopback;
use flow_engine::execution_protocol::message::Message;
use flow_engine::execution_protocol::transport::{
    executor_handshake, FrameTransport, NegotiatedLimits, SessionIdentity,
};
#[cfg(unix)]
use flow_engine::execution_protocol::unix::channels_from_raw;

use crate::task::{TaskEnd, TaskRunner};
use crate::transfer::IncomingTransfers;
use crate::{EXECUTOR_BUILD, EXECUTOR_CAPABILITIES};

/// 通道引导方式：unix 由主进程 pre_exec 落位 FD 槽位；其余平台经
/// [`FLOW_EXECUTOR_CONNECT_ADDR`] 回连 loopback（见 execution_protocol::loopback）。
pub enum ChannelBootstrap {
    #[cfg(unix)]
    FdSlots {
        control_fd: i32,
        data_fd: i32,
    },
    Loopback {
        addr: String,
    },
}

pub struct ExecutorConfig {
    pub bootstrap: ChannelBootstrap,
    /// 会话心跳与限额来自 Welcome。
    pub queue_depth: usize,
}

struct CurrentTask {
    dispatch_id: String,
    mail: mpsc::Sender<Message>,
    ack_tx: watch::Sender<(u64, u64)>,
    cancel: CancellationToken,
    done: tokio::sync::oneshot::Receiver<TaskEnd>,
    phase: String,
}

/// 执行器主循环。返回进程退出码语义：0 = 干净结束，非 0 = 需要回收。
pub async fn run_executor(config: ExecutorConfig) -> i32 {
    let mut transport = match &config.bootstrap {
        #[cfg(unix)]
        ChannelBootstrap::FdSlots {
            control_fd,
            data_fd,
        } => match channels_from_raw(*control_fd, *data_fd) {
            Ok(pair) => FrameTransport::spawn(pair, config.queue_depth),
            Err(error) => {
                eprintln!("flow-executor: channel fds unavailable: {error}");
                return 71;
            }
        },
        ChannelBootstrap::Loopback { addr } => {
            let connected = tokio::time::timeout(
                Duration::from_millis(STARTUP_TIMEOUT_MS),
                connect_loopback(addr),
            )
            .await;
            let (control, data) = match connected {
                Ok(Ok(pair)) => pair,
                Ok(Err(error)) => {
                    eprintln!("flow-executor: loopback connect failed: {error}");
                    return 71;
                }
                Err(_) => {
                    eprintln!("flow-executor: loopback connect timeout");
                    return 71;
                }
            };
            FrameTransport::spawn_streams(control, data, config.queue_depth)
        }
    };
    let (identity, limits) = match tokio::time::timeout(
        Duration::from_millis(STARTUP_TIMEOUT_MS * 2),
        executor_handshake(&mut transport, EXECUTOR_BUILD, EXECUTOR_CAPABILITIES),
    )
    .await
    {
        Ok(Ok(ok)) => ok,
        Ok(Err(error)) => {
            eprintln!("flow-executor: handshake rejected: {error}");
            return 72;
        }
        Err(_) => {
            eprintln!("flow-executor: handshake timeout");
            return 73;
        }
    };
    tracing::info!(
        session = %identity.session_id,
        executor = %identity.executor_id,
        "executor session ready"
    );
    let limits_ref = limits;
    // 心跳：存活与进度提示，不是执行结果或持久提交证明。
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Message>(config.queue_depth);
    let heartbeat_out = outbound_tx.clone();
    let heartbeat_handle = tokio::spawn(heartbeat_loop(
        heartbeat_out,
        identity.session_id.clone(),
        Duration::from_millis(limits_ref.heartbeat_ms),
    ));
    let mut current: Option<CurrentTask> = None;
    let mut exit_code = 0;
    // 转投队列（FIFO）：避免单槽覆盖丢失——双通道帧可能连续到达，
    // 单槽在 permit 获取窗口内被覆盖会把整条 TransferChunk 静默丢掉，
    // 执行器死等 InputReady 永不到来（材料不齐），主进程等到 deadline。
    let mut pending: std::collections::VecDeque<(mpsc::Sender<Message>, Message)> =
        std::collections::VecDeque::new();
    // 无主 transfer 帧：先于 Execute 到达（双通道乱序）时缓存，任务建立后补投。
    let mut orphan_transfers: std::collections::VecDeque<Message> =
        std::collections::VecDeque::new();
    loop {
        if !pending.is_empty() {
            // 先把队首能立即投递的消息确定性转发（有容量即送，无需等待）。
            let (mail, message) = pending.front().unwrap().clone();
            match mail.try_send(message) {
                Ok(()) => {
                    pending.pop_front();
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    exit_code = 75;
                    break;
                }
            }
        }
        tokio::select! {
            biased;
            _ = async {
                if pending.is_empty() { std::future::pending().await } else { let (mail, _) = pending.front().unwrap(); mail.clone().reserve_owned().await.map(|_| ()) }
            }, if !pending.is_empty() => {
                // 容量可用：回到循环顶部确定性转发。
                continue;
            }
            outbound = outbound_rx.recv() => {
                match outbound {
                    Some(message) => {
                        if transport.send(message).await.is_err() {
                            // 主进程不可达：停止一切，退出等待回收。
                            eprintln!("flow-executor: master unreachable; stopping");
                            if let Some(task) = &current { task.cancel.cancel(); }
                            exit_code = 74;
                            break;
                        }
                    }
                    None => break,
                }
            }
            task_done = async {
                match current.as_mut() {
                    Some(task) => (&mut task.done).await,
                    None => std::future::pending().await,
                }
            } => {
                if task_done.is_err() {
                    // A task panic closes its completion channel; do not spin forever.
                    if let Some(task) = &current { task.cancel.cancel(); }
                    exit_code = 75;
                    break;
                }
                if let Ok(end) = task_done {
                    match end {
                        TaskEnd::Committed => {
                            tracing::info!("task committed; slot reusable");
                            current = None;
                        }
                        TaskEnd::Recycle => {
                            tracing::warn!("task stopped uncommitted; exiting for recycle");
                            exit_code = 0; // 预期内回收，非错误
                            current = None;
                            break;
                        }
                    }
                }
            }
            received = transport.recv(), if pending.is_empty() => {
                match received {
                    Ok((channel, message)) => {
                        let _ = channel;
                        if let Some(code) = handle_message(&mut current, &mut pending, &mut orphan_transfers, message, &outbound_tx, &identity, limits_ref) {
                            exit_code = code;
                            break;
                        }
                    }
                    Err(error) => {
                        // 任一通道 EOF/错误：失联证据，不证明进程停止；停止执行。
                        eprintln!("flow-executor: channel failed; stopping: {error}");
                        if let Some(task) = &current { task.cancel.cancel(); }
                        exit_code = 0;
                        break;
                    }
                }
            }
        }
    }
    if let Some(task) = &current {
        task.cancel.cancel();
        // 尽力封口通知：取消后的 Stopped（通道可能已死，忽略结果）。
        let stopped = Message::Stopped {
            dispatch_id: task.dispatch_id.clone(),
            sealed: false,
            last_audit_seq: 0,
            pending_operation: None,
        };
        let _ = transport.send(stopped).await;
    }
    heartbeat_handle.abort();
    transport.close();
    transport.join().await;
    exit_code
}

/// 处理一条入站消息。返回 Some(exit_code) 表示应退出进程。
fn handle_message(
    current: &mut Option<CurrentTask>,
    pending: &mut std::collections::VecDeque<(mpsc::Sender<Message>, Message)>,
    orphan_transfers: &mut std::collections::VecDeque<Message>,
    message: Message,
    outbound: &mpsc::Sender<Message>,
    identity: &SessionIdentity,
    limits: NegotiatedLimits,
) -> Option<i32> {
    match message {
        Message::Execute {
            command_id,
            dispatch_id,
            task,
        } => {
            if current.is_some() {
                // 执行进程单任务：占用期拒绝新派发（主进程调度错误）。
                let _ = outbound.try_send(Message::Reject {
                    reason: bounded_control_text("executor busy; dispatch rejected".into()),
                });
                return None;
            }
            // 派发归属校验：会话上下文绑定（journal/epoch 由握手固定）。
            if task.run_id.is_empty() || task.node_id.is_empty() {
                let _ = outbound.try_send(Message::Reject {
                    reason: bounded_control_text("execute task identity missing".into()),
                });
                return None;
            }
            let _ = identity;
            let (mail_tx, mail_rx) = mpsc::channel(64);
            let (ack_tx, _) = watch::channel((0u64, 0u64));
            let runner_ack = ack_tx.clone();
            let cancel = CancellationToken::new();
            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
            let runner = TaskRunner {
                face: None,
                command_id,
                dispatch_id: dispatch_id.clone(),
                task,
                outbound: outbound.clone(),
                mail: mail_rx,
                ack_tx: runner_ack,
                cancel: cancel.clone(),
                journal_id: identity.journal_id.clone(),
                window_bytes: limits.window_bytes,
                transfers: IncomingTransfers::default(),
                last_result: None,
            };
            tokio::spawn(async move {
                let end = runner.run().await;
                let _ = done_tx.send(end);
            });
            let mail_tx_for_orphans = mail_tx.clone();
            *current = Some(CurrentTask {
                dispatch_id,
                mail: mail_tx,
                ack_tx,
                cancel,
                done: done_rx,
                phase: "starting".into(),
            });
            // 双通道乱序（二期 §3.6）：TransferChunk/InputReady 可能先于
            // Execute 到达——建立任务后补投缓存的无主 transfer 帧，否则
            // 输入字节永久丢失，执行器死等 InputReady 的配套块。
            let current_dispatch = current.as_ref().unwrap().dispatch_id.clone();
            while orphan_transfers
                .front()
                .is_some_and(|m| m.dispatch_id() == Some(current_dispatch.as_str()))
            {
                let orphan = orphan_transfers.pop_front().unwrap();
                pending.push_back((mail_tx_for_orphans.clone(), orphan));
            }
            None
        }
        Message::Cancel { dispatch_id, .. } => {
            if let Some(task) = current {
                if task.dispatch_id == dispatch_id {
                    task.cancel.cancel();
                    task.phase = "cancelling".into();
                }
            }
            None
        }
        Message::AuditAck {
            dispatch_id,
            durable_audit_seq,
            durable_bytes,
            ..
        } => {
            if let Some(task) = current {
                if task.dispatch_id == dispatch_id {
                    let _ = task.ack_tx.send((durable_audit_seq, durable_bytes));
                }
            }
            None
        }
        message @ (Message::ResultCommitted { .. } | Message::OperationPermit { .. }) => {
            let dispatch_id = match &message {
                Message::ResultCommitted { dispatch_id, .. } => dispatch_id.clone(),
                Message::OperationPermit { dispatch_id, .. } => dispatch_id.clone(),
                _ => unreachable!(),
            };
            if let Some(task) = current {
                if task.dispatch_id == dispatch_id {
                    pending.push_back((task.mail.clone(), message));
                }
            }
            None
        }
        Message::TransferChunk { .. } | Message::InputReady { .. } => {
            let dispatch_id = message.dispatch_id().unwrap_or_default().to_string();
            // 仅当帧属于当前派发时直接转投；Execute 未到（双通道乱序）或
            // 已不属于当前任务时缓存为无主帧，绝不静默丢弃。
            match current {
                Some(task) if task.dispatch_id == dispatch_id => {
                    pending.push_back((task.mail.clone(), message));
                }
                _ => orphan_transfers.push_back(message),
            }
            None
        }
        Message::Heartbeat { .. } => None,
        Message::Reject { reason } => {
            tracing::warn!(%reason, "master rejected; exiting");
            Some(75)
        }
        other => {
            // 未知/错位消息：协议错误，明确拒绝后退出。
            let _ = outbound.try_send(Message::Reject {
                reason: bounded_control_text(format!("unexpected message {}", other.type_name())),
            });
            tracing::warn!("unexpected message {}", other.type_name());
            Some(76)
        }
    }
}

async fn heartbeat_loop(outbound: mpsc::Sender<Message>, _session_id: String, interval: Duration) {
    let mut ticker = tokio::time::interval(interval.max(Duration::from_millis(100)));
    loop {
        ticker.tick().await;
        if outbound
            .send(Message::Heartbeat {
                dispatch_id: None,
                phase: "alive".into(),
            })
            .await
            .is_err()
        {
            return;
        }
    }
}
