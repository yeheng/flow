//! 指定 run 的「回放 + 实时追流」订阅流（DISTRIBUTED.md §8）：
//! 从 seq=1 回放完整日志，再接实时增量，按 seq 去重；
//! **只吐严格连续的事件**——出现缺口先补齐，补齐失败等重试定时器/新事件唤醒，
//! 绝不跳过缺口静默丢事件；run 终态事件转发后流自然结束。
//!
//! SQLite 与 Postgres 两个后端共用这一段：`AnyBackend::subscribe(Some(run_id))`
//! 的公共契约在两臂必须一致，语义分叉会泄漏给 RPC 调用方。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use flow_engine::Envelope;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::StreamExt;
use tokio::sync::broadcast;

use crate::BackendError;

/// 补齐失败后的重试间隔。已终结 run 没有新事件唤醒，靠它保证回放最终完成。
const FILL_RETRY: Duration = Duration::from_secs(10);

/// 事件读取面：两个后端各自适配自己的权威日志（文件 / 共享日志表）。
pub(crate) trait EventReader: Send + Sync + 'static {
    fn read_events<'a>(
        &'a self,
        run_id: &'a str,
        from_seq: Option<u64>,
    ) -> BoxFuture<'a, Result<Vec<Envelope>, BackendError>>;
}

/// 指定 run 的回放 + 追流状态机，包装成流。
pub(crate) fn run_tail(
    reader: Arc<dyn EventReader>,
    rx: broadcast::Receiver<Envelope>,
    run_id: String,
) -> BoxStream<'static, Envelope> {
    let tail = RunTail::new(reader, rx, run_id);
    futures::stream::unfold(tail, |mut tail| async move {
        tail.advance().await.map(|env| (env, tail))
    })
    .boxed()
}

/// 状态机的相位。
///
/// 这里此前是五个 bool（`backfilled`/`needs_fill`/`fill_failed`/`done`/`closed`），
/// 32 种组合里约一半非法——而且非法组合是可构造的：旧代码先置 `needs_fill`
/// 再置 `done`，得到「已结束但仍有缺口」。相位是一个值，不变量由类型保证：
/// - 缺口**不是**状态，是 `events` 里最小 seq 与 `last_seq` 的关系（`has_gap`）；
/// - 回放是否完成过由 `Replaying → Streaming` 的迁移表达，不是独立开关；
/// - 补齐失败 = `Filling`，与「要补齐」是同一个相位，不存在
///   「有缺口但没在补」或「没缺口却在退避」这两种矛盾组合。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// 首次全量回放（seq=1 起）尚未成功过。
    Replaying,
    /// 回放完成，追流中。有缺口时转 `Filling`。
    Streaming,
    /// 正在等一个缺口被补齐；`retry_at` 到点后重试补齐。
    Filling { retry_at: tokio::time::Instant },
    /// 终态事件已转发，流结束。
    Done,
    /// 共享流已关闭（进程停机），尽力补齐剩余后收尾。
    Closed,
}

impl Phase {
    /// 还有机会拿到数据（终态/已关闭之外的一切相位）。
    fn is_live(self) -> bool {
        !matches!(self, Phase::Done | Phase::Closed)
    }
    /// 是否处于「不能空转、要等唤醒」的相位。
    fn is_filling(self) -> bool {
        matches!(self, Phase::Filling { .. })
    }
}

struct RunTail {
    reader: Arc<dyn EventReader>,
    rx: broadcast::Receiver<Envelope>,
    run_id: String,
    /// 已确认转发的最大 seq。
    last_seq: u64,
    /// 按 seq 去重、有序暂存（补齐段与实时段会重叠）。
    events: BTreeMap<u64, Envelope>,
    phase: Phase,
}

impl RunTail {
    fn new(
        reader: Arc<dyn EventReader>,
        rx: broadcast::Receiver<Envelope>,
        run_id: String,
    ) -> RunTail {
        RunTail {
            reader,
            rx,
            run_id,
            last_seq: 0,
            events: BTreeMap::new(),
            phase: Phase::Replaying,
        }
    }

    /// 回放起点：已完成过回放则从 `last_seq+1` 续，否则从 1 全量。
    fn fill_from(&self) -> u64 {
        match self.phase {
            Phase::Replaying => 1,
            _ => self.last_seq + 1,
        }
    }

    /// 暂存区里是否还有 `last_seq` 之后的缺口（连续段之外的事件）。
    fn has_gap(&self) -> bool {
        self.events
            .keys()
            .next()
            .is_some_and(|seq| *seq > self.last_seq + 1)
    }

    /// 进入补齐相位；已在补齐则保留原退避（不重置计时器）。
    fn ensure_filling(&mut self) {
        if self.phase.is_filling() {
            return;
        }
        self.phase = Phase::Filling {
            retry_at: tokio::time::Instant::now() + FILL_RETRY,
        };
    }

    async fn advance(&mut self) -> Option<Envelope> {
        loop {
            // 1) 排水：只吐严格连续的前缀。重复（补齐段与实时段重叠）丢弃；
            //    缺口绝不跳过——转 Filling 去补齐，否则事件被静默吞掉。
            match self.events.keys().next().copied() {
                Some(seq) if seq <= self.last_seq => {
                    self.events.remove(&seq);
                    continue;
                }
                Some(seq) if seq == self.last_seq + 1 => {
                    let envelope = self.events.remove(&seq).unwrap();
                    self.last_seq = seq;
                    if envelope.event.is_run_terminal() {
                        self.phase = Phase::Done;
                    }
                    return Some(envelope);
                }
                // 缺口：绝不跳，转 Filling 去补齐（已在 Filling 就不重置退避）
                Some(_) => self.ensure_filling(),
                None => {}
            }
            if !self.phase.is_live() {
                return None;
            }

            // 2) 补齐。回放期（Replaying）读全量；追流期读 last_seq+1 起的增量。
            //    失败不重试过热：转 Filling 等唤醒，绝不在缺口未补齐时继续吐事件。
            if !self.phase.is_filling() {
                let from = self.fill_from();
                match self.reader.read_events(&self.run_id, Some(from)).await {
                    Ok(list) => {
                        let replay_from_start = from == 1;
                        let log_is_empty = list.is_empty();
                        let mut added = false;
                        for envelope in list {
                            if self.events.insert(envelope.seq, envelope).is_none() {
                                added = true;
                            }
                        }
                        if self.phase == Phase::Replaying {
                            self.phase = Phase::Streaming;
                        }
                        // 防御 1：seq=1 起的回放一条事件都没有——run 不存在（PG reader
                        // 报 RunNotFound）或初始化中断的空日志（SQLite 臂崩溃窗口：
                        // event.jsonl 已建、run_started 未落盘，恢复已按 DB 投影标终态）。
                        // 终态事件永远不会出现，等下去只会挂死订阅方，结束流。
                        if replay_from_start && log_is_empty && self.events.is_empty() {
                            tracing::debug!(
                                run_id = %self.run_id,
                                "事件日志为空（run 不存在或初始化中断），结束订阅流"
                            );
                            self.phase = Phase::Done;
                        }
                        // 防御 2：补齐成功却毫无新数据而缺口仍在（日志里根本没有
                        // 缺口段）——继续只能空转，吐完连续段后结束流。
                        if !added && self.has_gap() {
                            tracing::warn!(
                                run_id = %self.run_id,
                                "订阅游标缺口无法从日志补齐，结束订阅流"
                            );
                            self.phase = Phase::Done;
                        }
                    }
                    Err(err) => {
                        // run 不存在是确定事实，不是抖动：缺口永远补不上，
                        // 每 10s 重试只会泄漏一个永不退出的 task。
                        // 结束流而非谎报终结、跳缺口或用调用方无法区分的挂起。
                        if matches!(err, BackendError::RunNotFound(_)) {
                            tracing::debug!(run_id = %self.run_id, "订阅的 run 不存在，结束订阅流");
                            self.phase = Phase::Done;
                            continue;
                        }
                        // 其余失败：不谎报终结、不跳缺口：等重试定时器或下一条事件唤醒
                        tracing::debug!(
                            run_id = %self.run_id,
                            error = %err,
                            "订阅补齐读取失败，稍后重试"
                        );
                        self.ensure_filling();
                    }
                }
                continue;
            }

            // 3) 等待唤醒：新事件（快路径）/ 补齐重试定时器 / 共享流关闭
            let retry_at = match self.phase {
                Phase::Filling { retry_at } => retry_at,
                _ => tokio::time::Instant::now() + FILL_RETRY,
            };
            let sleep = tokio::time::sleep_until(retry_at);
            tokio::pin!(sleep);
            tokio::select! {
                _ = &mut sleep => {
                    // 重试窗口到了：回到「去补齐」
                    self.phase = Phase::Streaming;
                }
                received = self.rx.recv() => {
                    match received {
                        Ok(envelope) if envelope.run_id == self.run_id => {
                            if envelope.seq > self.last_seq {
                                if envelope.seq > self.last_seq + 1 {
                                    self.ensure_filling();
                                }
                                self.events.insert(envelope.seq, envelope);
                            }
                            if self.phase.is_filling() {
                                // 新事件是有效唤醒：立刻重试补齐，不等退避
                                self.phase = Phase::Streaming;
                            }
                        }
                        Ok(_) => {}
                        // 广播追赶不上：丢的是唤醒不是数据，整体补齐
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            self.phase = Phase::Streaming;
                        }
                        // 共享流关闭（进程停机）：尽力补齐剩余后收尾
                        Err(broadcast::error::RecvError::Closed) => {
                            if let Ok(list) = self
                                .reader
                                .read_events(&self.run_id, Some(self.last_seq + 1))
                                .await
                            {
                                for envelope in list {
                                    self.events.insert(envelope.seq, envelope);
                                }
                            }
                            self.phase = Phase::Closed;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
