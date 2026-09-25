//! 共享事件订阅轮询（DISTRIBUTED.md §8）：进程内唯一的轮询任务维护全局游标，
//! 把共享日志的增量扇出给所有本地订阅者——查询次数与订阅者数无关。
//!
//! 正确性不依赖 LISTEN/NOTIFY：通知只是低延迟唤醒提示，丢失由 subscribe_poll
//! 兜底轮询兜住。游标的生命周期与候选视野绑定：
//! - 终态事件已转发（或状态投影已终结且日志追平）的 run 标记 done，不再查询；
//! - done 游标保留到该 run 退出候选视野（ended_at 回看窗口过期）后随 GC 移除。
//!
//! 这既挡住了窗口期内重复查询导致的全量重放，又保证游标集合随 run 生命周期
//! 自清理——不需要「已处理集合超阈值整体清空」这种补丁。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use flow_engine::Envelope;

use crate::config::PgConfig;
use crate::metadata::PgStore;
use crate::sink::PgRunSink;

const BROADCAST_CAPACITY: usize = 1024;

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
    tx: broadcast::Sender<Envelope>,
    stop: CancellationToken,
    task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl EventHub {
    /// 启动共享轮询任务（随 PgEngine 生命周期运行，`stop` 时退出）。
    pub fn start(pool: sqlx::PgPool, cfg: PgConfig) -> Arc<EventHub> {
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let stop = CancellationToken::new();
        let hub = Arc::new(EventHub {
            tx: tx.clone(),
            stop: stop.clone(),
            task: tokio::sync::Mutex::new(None),
        });
        let handle = tokio::spawn(poll_loop(pool, cfg, tx, stop));
        if let Ok(mut slot) = hub.task.try_lock() {
            *slot = Some(handle);
        }
        hub
    }

    /// 订阅增量事件流（实时尾部；历史事件用 run.events 补齐）。
    pub fn subscribe(&self) -> broadcast::Receiver<Envelope> {
        self.tx.subscribe()
    }

    /// 停止轮询并等待任务退出。
    pub async fn stop(&self) {
        self.stop.cancel();
        if let Some(task) = self.task.lock().await.take() {
            let _ = task.await;
        }
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
    loop {
        if stop.is_cancelled() {
            break;
        }
        scan(&store, &tx, &mut cursors, window).await;
        // 等待下一次扫描：NOTIFY 唤醒（快路径）或兜底轮询到期。
        // 通知在扫描期间积压在 channel 里，不会丢失唤醒。
        let wait = tokio::time::sleep(cfg.subscribe_poll);
        tokio::pin!(wait);
        loop {
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = &mut wait => break,
                note = async { notify.as_mut().unwrap().recv().await }, if notify.is_some() => {
                    match note {
                        Some(_) => {
                            // 排空积压通知再扫描：突发事件合并成一次扫描，
                            // 避免扫描次数随事件数线性增长
                            if let Some(rx) = notify.as_mut() {
                                while rx.try_recv().is_ok() {}
                            }
                            break;
                        }
                        // 监听任务退出：摘掉监听臂，退化为纯兜底轮询
                        None => notify = None,
                    }
                }
            }
        }
    }
}

/// 一轮扫描：把候选 run 的日志增量按游标转发出去，然后回收退出视野的游标。
async fn scan(
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
            // 没有本地订阅者时 send 返回 Err，游标照常推进
            let _ = tx.send(envelope.clone());
            cursor.last_seq = envelope.seq;
            if envelope.event.is_run_terminal() {
                cursor.done = true;
            }
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
