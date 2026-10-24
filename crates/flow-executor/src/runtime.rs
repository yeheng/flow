//! 执行器会话运行时：握手、单任务串行循环、心跳、取消与父死亡检测
//! （二期 §3.2/§3.7，I03）。
//!
//! 一个执行进程同一时刻只承载一个任务：空闲槽位的新派发或当前派发的
//! 重复消息；正常 ResultCommitted 前不复用槽位（二期 §3.4）。任一通道
//! EOF/错误即按失联处理：停止当前任务、尽力封口后退出进程，槽位由主\n//! 进程整体回收重建。
//!
//! 会话层的核心不变量是「一个槽位一条命」：派发归属严格按
//! dispatch_id 校验，ResultCommitted 之前不释放槽位，通道失联立即取消
//! 当前任务并退出进程。它守不住的只有一件事——进程崩溃——那由主进程的
//! kill/reap 兜底。
//!
//! 另一个不变量是「心跳不是承诺」：心跳只表示进程活着，绝不表示任何
//! 结果已持久。持久性由 AuditAck（审计）与 ResultCommitted（提交）两
//! 条独立确认链保证。

use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use flow_engine::execution_protocol::contract::bounded_control_text;
use flow_engine::execution_protocol::contract::{DRAIN_GRACE_MS, STARTUP_TIMEOUT_MS};
use flow_engine::execution_protocol::loopback::connect_loopback;
use flow_engine::execution_protocol::message::Message;
use flow_engine::execution_protocol::transport::{
    executor_handshake, FrameTransport, NegotiatedLimits, SessionIdentity,
};
#[cfg(unix)]
use flow_engine::execution_protocol::unix::channel_from_raw;

use crate::task::{TaskEnd, TaskRunner};
use crate::transfer::IncomingTransfers;
use crate::{EXECUTOR_BUILD, EXECUTOR_CAPABILITIES};

/// 通道引导方式：unix 由主进程 argv `--channel-fd` 指定 socketpair 原号；
/// 其余平台经 [`FLOW_EXECUTOR_CONNECT_ADDR`] 单连接回连 loopback。
///
/// 两种形态的安全边界不同：FD 形态靠内核的 FD 传递能力（其他进程拿不
/// 到这个句柄），回连形态靠会话令牌回验（防同机抢连）。
pub enum ChannelBootstrap {
    /// socketpair 子端的原始 FD 号（主进程 spawn 时传入 argv）。
    #[cfg(unix)]
    Fd {
        /// 已继承的通道 FD 号。
        channel_fd: i32,
    },
    /// 回连地址（loopback TCP）。
    Loopback {
        /// 主进程监听的回连地址。
        addr: String,
    },
}

/// 执行器会话配置（由引导阶段决定，运行期不再变）。
pub struct ExecutorConfig {
    /// 通道引导方式。
    pub bootstrap: ChannelBootstrap,
    /// Only an agent's reconnectable remote uplink needs application replay.
    ///
    /// 仅远端 agent 上联需要应用层重放：链路可能半死（连着但不回消息），
    /// 本地 socketpair 形态靠幂等重传与通道断开即可收敛。
    pub remote_replay: bool,
    /// 会话心跳与限额来自 Welcome。
    pub queue_depth: usize,
    /// loopback 回连的会话令牌（主进程 spawn 时经 env 注入；Hello 回验）。
    /// socketpair 形态走 FD 能力，为 None。
    pub session_token: Option<String>,
}

/// 当前占用槽位的任务上下文（会话内至多一个）。
struct CurrentTask {
    /// 派发归属：所有入站消息按它过滤。
    dispatch_id: String,
    /// 本派发的入站邮箱（传输块、许可、ACK 转发目标）。
    mail: mpsc::Sender<Message>,
    /// 审计确认的转发通道。
    ack_tx: watch::Sender<(u64, u64)>,
    /// 取消令牌：会话取消/失联时唤醒任务。
    cancel: CancellationToken,
    /// 任务完成通知（携带 TaskEnd）。
    done: tokio::sync::oneshot::Receiver<TaskEnd>,
}

/// 执行器主循环。返回进程退出码语义：0 = 干净结束，非 0 = 需要回收。
///
/// 退出码是给主进程看的分类信号（引导失败 71 / 握手被拒 72 / 握手超时
/// 73 / 主进程失联 74 / 协议错位 75-76），主进程据此决定是重试派发
/// 还是回收槽位。
pub async fn run_executor(config: ExecutorConfig) -> i32 {
    let mut transport = match &config.bootstrap {
        #[cfg(unix)]
        ChannelBootstrap::Fd { channel_fd } => match channel_from_raw(*channel_fd) {
            Ok(stream) => FrameTransport::spawn_local(stream, config.queue_depth),
            Err(error) => {
                // 通道拿不到 = 无法与主进程通信，进程没有任何存在意义。
                eprintln!("flow-executor: channel fd unavailable: {error}");
                return 71;
            }
        },
        ChannelBootstrap::Loopback { addr } => {
            // 回连带超时：主进程没监听时进程不能无限挂着占端口。
            let connected = tokio::time::timeout(
                Duration::from_millis(STARTUP_TIMEOUT_MS),
                connect_loopback(addr),
            )
            .await;
            let stream = match connected {
                Ok(Ok(stream)) => stream,
                Ok(Err(error)) => {
                    eprintln!("flow-executor: loopback connect failed: {error}");
                    return 71;
                }
                Err(_) => {
                    eprintln!("flow-executor: loopback connect timeout");
                    return 71;
                }
            };
            FrameTransport::spawn(stream, config.queue_depth)
        }
    };
    let (identity, limits) = match tokio::time::timeout(
        Duration::from_millis(STARTUP_TIMEOUT_MS * 2),
        executor_handshake(
            &mut transport,
            EXECUTOR_BUILD,
            EXECUTOR_CAPABILITIES,
            config.session_token.clone(),
        ),
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
    // 转投队列（FIFO）：避免单槽覆盖丢失——任务 mail 通道容量有限，
    // 单槽在 permit 获取窗口内被覆盖会把整条 TransferChunk 静默丢掉，
    // 执行器死等 InputReady 永不到来（材料不齐），主进程等到 deadline。
    //
    // 队列先进先出：传输块与许可必须按到达顺序交给任务，乱序会让
    // 装配器看到「缺口」并直接拒绝。
    let mut pending: std::collections::VecDeque<(mpsc::Sender<Message>, Message)> =
        std::collections::VecDeque::new();
    // 出站积压（T03）：transport 队列满时暂存重试，不让阻塞中的 send 卡住
    // 整个 select——入站 Cancel/ACK 与任务完成始终可被处理；积压占用期间
    // 不再拉取新出站消息（自然背压到心跳/任务生产方）。
    let mut send_backlog =
        flow_engine::execution_protocol::transport::SendBacklog::with_capacity(4);
    loop {
        // 每轮先冲刷积压：积压未清时不再拉取新的出站消息，形成对
        // 心跳与任务生产方的自然背压。
        if send_backlog.flush(&transport).is_err() {
            eprintln!("flow-executor: master unreachable; stopping");
            if let Some(task) = &current {
                task.cancel.cancel();
            }
            exit_code = 74;
            break;
        }
        if !pending.is_empty() {
            // 先把队首能立即投递的消息确定性转发（有容量即送，无需等待）。
            let (mail, message) = pending.front().unwrap().clone();
            match mail.try_send(message) {
                Ok(()) => {
                    pending.pop_front();
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    // 任务邮箱已关：它已经结束或被取消，不再需要本会话。
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
            outbound = outbound_rx.recv(), if send_backlog.is_empty() => {
                match outbound {
                    Some(message) => {
                        if send_backlog.push(&transport, message).is_err() {
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
                    //
                    // 任务 panic 与「任务失败」不同：这里无法区分是哪种，
                    // 只能按平台故障整体回收。
                    if let Some(task) = &current { task.cancel.cancel(); }
                    exit_code = 75;
                    break;
                }
                if let Ok(end) = task_done {
                    match end {
                        TaskEnd::Committed => {
                            // 已提交：主进程已持久，槽位可复用（普通节点）。
                            tracing::info!("task committed; slot reusable");
                            current = None;
                        }
                        TaskEnd::CommittedRecycle => {
                            // agent 任务已提交，但进程级状态不可继承：
                            // 提交后立刻退役整个会话。
                            tracing::info!("agent task committed; retiring executor session");
                            current = None;
                            break;
                        }
                        TaskEnd::Recycle => {
                            tracing::warn!("task stopped uncommitted; exiting for recycle");
                            exit_code = 0; // 预期内回收，非错误
                            // 不清 current：循环尾的封口 Stopped 依赖它携带
                            // 派发归属，提前清掉会让回收对 master 完全静默
                            // （只能靠空闲超时才发现）。
                            break;
                        }
                    }
                }
            }
                received = transport.recv(), if pending.is_empty() => {
                match received {
                    Ok(message) => {
                        if let Some(code) = handle_message(&mut current, &mut pending, message, &outbound_tx, &identity, limits_ref, config.remote_replay) {
                            exit_code = code;
                            break;
                        }
                    }
                    Err(error) => {
                        // 任一通道 EOF/错误：失联证据，不证明进程停止；停止执行。
                        //
                        // 失联不是业务失败：此时既不知道结果是否已提交，
                        // 也不能重发，只能让主进程按 uncertain 裁决。
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
        // 尽力封口通知：取消后的 Stopped（通道可能已死/拥塞，有界等待）。
        //
        // sealed=false 表示这不是一条封口完成的事实：主进程不能把它
        // 当作「审计前缀已封存」，只能当作「执行器知道出事了」。
        let stopped = Message::Stopped {
            dispatch_id: task.dispatch_id.clone(),
            sealed: false,
            last_audit_seq: 0,
            pending_operation: None,
        };
        let _ = tokio::time::timeout(
            Duration::from_millis(DRAIN_GRACE_MS),
            transport.send(stopped),
        )
        .await;
    }
    // 退出前排干在途出站（I24：已排队未送达的帧——含 Result/Reject——
    // 不能随进程退出丢弃）：先收尽 outbound_rx 残留进 backlog，再冲刷，
    // 最后 close。任务已结束，不会再有新帧，排空是有界的。
    while let Ok(message) = outbound_rx.try_recv() {
        if send_backlog.push(&transport, message).is_err() {
            break;
        }
    }
    let _ = send_backlog.flush(&transport);
    heartbeat_handle.abort();
    transport.close();
    transport.join().await;
    exit_code
}

/// 处理一条入站消息。返回 Some(exit_code) 表示应退出进程。
///
/// 职责边界：只做归属判定与转发，不解释业务语义（那是 task.rs 的事）。
/// 任何无法归属到当前派发的帧一律丢弃或拒绝，绝不跨派发投递。
fn handle_message(
    current: &mut Option<CurrentTask>,
    pending: &mut std::collections::VecDeque<(mpsc::Sender<Message>, Message)>,
    message: Message,
    outbound: &mpsc::Sender<Message>,
    identity: &SessionIdentity,
    limits: NegotiatedLimits,
    remote_replay: bool,
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
            // 解箱：TaskRunner 持有裸 ExecuteTask（下游全是借用，无字段移出）。
            let task = *task;
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
                remote_replay,
                transfers: IncomingTransfers::default(),
                last_result: None,
            };
            tokio::spawn(async move {
                let end = runner.run().await;
                let _ = done_tx.send(end);
            });
            *current = Some(CurrentTask {
                dispatch_id,
                mail: mail_tx,
                ack_tx,
                cancel,
                done: done_rx,
            });
            None
        }
        Message::Cancel { dispatch_id, .. } => {
            // 取消只对当前派发生效：串台的取消必须忽略，否则会误杀
            // 别的派发。
            if let Some(task) = current {
                if task.dispatch_id == dispatch_id {
                    task.cancel.cancel();
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
            // ACK 同样按派发归属过滤：串台的 ACK 会让错误的审计前缀
            // 被当作已持久。
            if let Some(task) = current {
                if task.dispatch_id == dispatch_id {
                    let _ = task.ack_tx.send((durable_audit_seq, durable_bytes));
                }
            }
            None
        }
        message @ (Message::ResultCommitted { .. } | Message::OperationPermit { .. }) => {
            // 这两类消息由任务侧等待（wait_committed / authorize_operation），
            // 会话层只负责按派发归属转投递。
            let dispatch_id = message.dispatch_id().unwrap_or_default().to_string();
            if let Some(task) = current {
                if task.dispatch_id == dispatch_id {
                    pending.push_back((task.mail.clone(), message));
                }
            }
            None
        }
        Message::TransferChunk { .. } | Message::InputReady { .. } => {
            // 单流承载下 Execute 恒先于其传输帧到达；不属于当前派发的
            // 传输帧只可能是前序派发的迟到残留（取消/回收竞态），丢弃计数。
            let dispatch_id = message.dispatch_id().unwrap_or_default().to_string();
            match current {
                Some(task) if task.dispatch_id == dispatch_id => {
                    pending.push_back((task.mail.clone(), message));
                }
                _ => tracing::debug!(
                    dispatch = %dispatch_id,
                    kind = message.type_name(),
                    "stale transfer frame dropped"
                ),
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

/// 心跳循环：按协商间隔发存活信号。
///
/// 心跳只是存活/进度提示，**不是**执行结果或持久提交证明；主进程只
/// 据此判断会话是否还活着。
async fn heartbeat_loop(outbound: mpsc::Sender<Message>, _session_id: String, interval: Duration) {
    // 间隔下限 100ms：心跳太密会挤占 data 帧带宽，太疏会让主进程误判
    // 失联。
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
