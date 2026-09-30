//! 共享事件订阅轮询（`flow-pg/src/subscribe.rs`）：进程内唯一的轮询任务维护全局游标，
//! 把共享日志的增量扇出给所有本地订阅者——查询次数与订阅者数无关。
//!
//! 正确性不依赖 LISTEN/NOTIFY：通知只是低延迟唤醒提示，丢失由 subscribe_poll
//! 兜底轮询兜住。唤醒分两条路径：NOTIFY 载荷带 run_id，只定向抓取被点名的
//! run（突发事件按 run 合并成一次读取）；兜底轮询到期才全量扫描候选视野，
//! 兼顾游标回收与「可能丢失通知的短生命周期 run」。游标的生命周期与候选视野绑定：
//! - 终态事件已转发（或状态投影已终结且日志追平）的 run 标记 done，不再查询；
//! - done 游标保留到该 run 退出候选视野（ended_at 回看窗口过期）后随 GC 移除。
//!
//! 这既挡住了窗口期内重复查询导致的全量重放，又保证游标集合随 run 生命周期
//! 自清理——不需要「已处理集合超阈值整体清空」这种补丁。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use flow_engine::Envelope;

use crate::config::PgConfig;
use crate::metadata::PgStore;
use crate::sink::PgRunSink;

const BROADCAST_CAPACITY: usize = 1024;

/// 单次 NOTIFY 唤醒里最多合并多少个 run id。封顶的理由见 `poll_loop`：
/// 不封顶则持续事件流下排空永不结束，兜底全量扫描被饿死。
const NOTIFY_DRAIN_MAX: usize = 128;

/// 已终结 run 的回看窗口：至少覆盖数个兜底轮询周期，
/// 保证「两次扫描之间走完一生」的短 run 的终态事件被游标追平。
fn recent_window(poll: Duration) -> Duration {
    (poll * 3).max(Duration::from_secs(30))
}

/// 一个 run 的转发进度。`last_seq` 是已确认转发的最大 seq。
#[derive(Default)]
struct Cursor {
    last_seq: u64,
    done: bool,
}

/// 共享订阅 hub：一个轮询任务 + 一个进程内 broadcast 扇出。
pub struct EventHub {
    /// `parking_lot::Mutex` 而非 tokio 的：`subscribe()` 是同步函数，全仓库
    /// 调用点（`PgEngine::subscribe_events`、`PgChildLauncher` 的等待循环）
    /// 都在同步上下文里，不值得为它把签名改成 async。
    /// 槽内是 `Option` 而非直接持有 sender：只有 `stop()` 把它 take 掉，
    /// broadcast 才会因为「最后一个 sender 被销毁」而关闭。否则 `stop()` 之后
    /// 订阅者永远收不到 `RecvError::Closed`——挂在 `run_tail` /
    /// `broadcast_tail` 的 select 里不退出，每次订阅在停机时留一个永不
    /// 退出的 task。
    tx: parking_lot::Mutex<Option<broadcast::Sender<Envelope>>>,
    stop: CancellationToken,
    task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl EventHub {
    /// 启动共享轮询任务（随 PgEngine 生命周期运行，`stop` 时退出）。
    pub fn start(pool: sqlx::PgPool, cfg: PgConfig) -> Arc<EventHub> {
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let stop = CancellationToken::new();
        // 任务句柄**构造时**就放进槽位，不能延后到第一次使用时——锁被争用时会
        // 静默丢弃 JoinHandle（= detach 该任务），`stop()` 于是报告「已停止」
        // 而 poll_loop 还在跑，且再也无法取消。
        let task = tokio::sync::Mutex::new(Some(tokio::spawn(poll_loop(
            pool,
            cfg,
            tx.clone(),
            stop.clone(),
        ))));
        Arc::new(EventHub {
            tx: parking_lot::Mutex::new(Some(tx)),
            stop,
            task,
        })
    }

    /// 订阅增量事件流（实时尾部；历史事件用 run.events 补齐）。
    ///
    /// `stop()` 之后拿不到新的接收端（sender 已被 take 掉）；已经存在的接收端
    /// 会立即收到 `Closed`。
    pub fn subscribe(&self) -> broadcast::Receiver<Envelope> {
        self.tx
            .lock()
            .as_ref()
            .expect("EventHub 已 stop：不应再建立新订阅")
            .subscribe()
    }

    /// 停止轮询并等待任务退出，随后关闭 broadcast（唤醒所有挂起的订阅者）。
    pub async fn stop(&self) {
        self.stop.cancel();
        if let Some(task) = self.task.lock().await.take() {
            let _ = task.await;
        }
        // 轮询任务已退出，这里销毁最后一个 sender：订阅流自然结束
        self.tx.lock().take();
    }
}

async fn poll_loop(
    pool: sqlx::PgPool,
    cfg: PgConfig,
    tx: broadcast::Sender<Envelope>,
    stop: CancellationToken,
) {
    let store = PgStore::new(pool.clone());
    let window = recent_window(cfg.subscribe_poll);
    // 先挂监听再进入循环，缩小「查询后、LISTEN 生效前」的通知丢失窗口
    //（残余窗口由兜底轮询兜住）
    let mut notify = match crate::event_notifications(&pool).await {
        Ok(rx) => Some(rx),
        Err(err) => {
            tracing::warn!("pg 事件监听不可用，退化为纯兜底轮询：{err}");
            None
        }
    };
    let mut cursors: HashMap<String, Cursor> = HashMap::new();
    // 首轮全量扫描建立游标视野；之后 NOTIFY 只定向抓取被点名的 run，
    // 兜底轮询到期才再做全量扫描（捕捉可能丢失通知的短生命周期 run）。
    scan_candidates(&store, &tx, &mut cursors, window).await;
    // 兜底全量扫描的到期点按「距上次全量扫描多久」计算，不被 NOTIFY 唤醒
    // 重置：持续事件流下每次唤醒都重置定时器，会饿死兜底扫描（游标回收、
    // done 标记、漏通知追平全靠它）。
    let mut last_full_scan = Instant::now();
    loop {
        if stop.is_cancelled() {
            break;
        }
        let until_full = cfg.subscribe_poll.saturating_sub(last_full_scan.elapsed());
        let wait = tokio::time::sleep(until_full);
        tokio::pin!(wait);
        // 只在兜底扫描到期时置位：唯一 break 出口就是到期分支，先赋值再跳出
        let full_scan_due;
        loop {
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = &mut wait => {
                    // 兜底全量扫描：通知丢失/未开启时按候选视野追平
                    full_scan_due = true;
                    break;
                }
                note = async { notify.as_mut().unwrap().recv().await }, if notify.is_some() => {
                    match note {
                        Some(run_id) => {
                            // 定向抓取：只读 NOTIFY 点名的 run，顺带排空通道积压
                            // （突发事件同一 run 的多个通知合并成一次读取）。
                            //
                            // 排空**必须封顶**：不封顶时持续事件流下通道永不空，
                            // `scan_targeted` 不返回 → 兜底全量扫描的到期检查永远
                            // 到不了 → 游标得不到回收（`cursors.retain`）而无界增长，
                            // 短生命周期 run 也拿不到终态追平。
                            let mut ids = HashSet::new();
                            ids.insert(run_id);
                            if let Some(rx) = notify.as_mut() {
                                while ids.len() < NOTIFY_DRAIN_MAX {
                                    match rx.try_recv() {
                                        Ok(more) => {
                                            ids.insert(more);
                                        }
                                        Err(_) => break,
                                    }
                                }
                            }
                            scan_targeted(&store, &tx, &mut cursors, &ids).await;
                        }
                        // 监听任务退出：摘掉监听臂，退化为纯兜底轮询
                        None => notify = None,
                    }
                }
            }
        }
        if full_scan_due {
            scan_candidates(&store, &tx, &mut cursors, window).await;
            last_full_scan = Instant::now();
        }
    }
}

/// 一次定向抓取：只读取通知涉及的 run 的日志增量（NOTIFY 快路径）。
/// 没有候选视野表可查，不套用「状态投影已终结且追平」的 done 捷径——
/// 被通知才读，查询次数由事件数决定；done 标记与游标回收留给全量扫描。
async fn scan_targeted(
    store: &PgStore,
    tx: &broadcast::Sender<Envelope>,
    cursors: &mut HashMap<String, Cursor>,
    run_ids: &HashSet<String>,
) {
    for id in run_ids {
        let cursor = cursors.entry(id.clone()).or_default();
        if cursor.done {
            continue;
        }
        let events = match PgRunSink::read_events(store.pool(), id, Some(cursor.last_seq + 1)).await
        {
            Ok(events) => events,
            Err(err) => {
                tracing::debug!(run_id = %id, error = %err, "订阅增量读取失败，下轮重试");
                continue;
            }
        };
        for envelope in events {
            // 先取走后面还要用的两个字段，再把 envelope 整个 move 进 send
            // （否则每事件深拷贝一次 payload，send 完就丢）
            cursor.last_seq = envelope.seq;
            if envelope.event.is_run_terminal() {
                cursor.done = true;
            }
            // 没有本地订阅者时 send 返回 Err，游标照常推进
            let _ = tx.send(envelope);
        }
    }
}

/// 一轮全量扫描：把候选 run 的日志增量按游标转发出去，然后回收退出视野的游标。
async fn scan_candidates(
    store: &PgStore,
    tx: &broadcast::Sender<Envelope>,
    cursors: &mut HashMap<String, Cursor>,
    window: Duration,
) {
    let watched: HashMap<String, bool> = match store.watch_candidates(window).await {
        Ok(list) => list.into_iter().collect(),
        Err(err) => {
            tracing::warn!("订阅候选查询失败，本轮跳过：{err}");
            return;
        }
    };
    // 候选 = 当前视野 ∪ 已有游标：活跃 run 超出 LIMIT 时游标也不能丢
    let mut ids: Vec<String> = watched.keys().cloned().collect();
    for id in cursors.keys() {
        if !watched.contains_key(id) {
            ids.push(id.clone());
        }
    }
    for id in ids {
        let cursor = cursors.entry(id.clone()).or_default();
        if cursor.done {
            continue;
        }
        let events =
            match PgRunSink::read_events(store.pool(), &id, Some(cursor.last_seq + 1)).await {
                Ok(events) => events,
                Err(err) => {
                    tracing::debug!(run_id = %id, error = %err, "订阅增量读取失败，下轮重试");
                    continue;
                }
            };
        let drained = events.is_empty();
        for envelope in events {
            cursor.last_seq = envelope.seq;
            if envelope.event.is_run_terminal() {
                cursor.done = true;
            }
            // 没有本地订阅者时 send 返回 Err，游标照常推进
            let _ = tx.send(envelope);
        }
        // 状态投影已终结且日志追平：不必再查
        if !cursor.done && drained && watched.get(&id) == Some(&true) {
            cursor.done = true;
        }
    }
    // 视野内（或尚未追平）的游标保留；已追平且退出视野的游标随 run 生命周期自清理。
    // done 游标在视野内保留，是为了挡住 ended_at 窗口内重复查询导致的全量重放。
    cursors.retain(|id, cursor| watched.contains_key(id) || !cursor.done);
}
