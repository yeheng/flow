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

struct RunTail {
    reader: Arc<dyn EventReader>,
    rx: broadcast::Receiver<Envelope>,
    run_id: String,
    /// 已确认转发的最大 seq。
    last_seq: u64,
    /// 按 seq 去重、有序暂存（补齐段与实时段会重叠）。
    events: BTreeMap<u64, Envelope>,
    /// 回放（seq=1 起）是否已成功完成过。
    backfilled: bool,
    /// 存在待补齐的缺口（含首次回放）。
    needs_fill: bool,
    /// 上次补齐失败：等重试定时器或新事件唤醒，不空转。
    fill_failed: bool,
    done: bool,
    closed: bool,
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
            backfilled: false,
            needs_fill: true,
            fill_failed: false,
            done: false,
            closed: false,
        }
    }

    async fn advance(&mut self) -> Option<Envelope> {
        loop {
            // 1) 排水：只吐严格连续的前缀。重复（补齐段与实时段重叠）丢弃；
            //    缺口绝不跳过——置 needs_fill 去补齐，否则事件被静默吞掉。
            match self.events.keys().next().copied() {
                Some(seq) if seq <= self.last_seq => {
                    self.events.remove(&seq);
                    continue;
                }
                Some(seq) if seq == self.last_seq + 1 => {
                    let envelope = self.events.remove(&seq).unwrap();
                    self.last_seq = seq;
                    if envelope.event.is_run_terminal() {
                        self.done = true;
                    }
                    return Some(envelope);
                }
                Some(_) => self.needs_fill = true,
                None => {}
            }
            if self.done || self.closed {
                return None;
            }

            // 2) 补齐（backfilled 前是回放全量）。失败不重试过热，
            //    置 fill_failed 后等唤醒——绝不在缺口未补齐时继续吐事件。
            if self.needs_fill && !self.fill_failed {
                let from = if self.backfilled {
                    self.last_seq + 1
                } else {
                    1
                };
                match self.reader.read_events(&self.run_id, Some(from)).await {
                    Ok(list) => {
                        let mut added = false;
                        for envelope in list {
                            if self.events.insert(envelope.seq, envelope).is_none() {
                                added = true;
                            }
                        }
                        self.backfilled = true;
                        self.needs_fill = false;
                        self.fill_failed = false;
                        // 防御：补齐成功却毫无新数据而缺口仍在（日志里根本没有
                        // 缺口段）——继续只能空转，吐完连续段后结束流。
                        if !added
                            && self
                                .events
                                .keys()
                                .next()
                                .is_some_and(|seq| *seq > self.last_seq + 1)
                        {
                            tracing::warn!(
                                run_id = %self.run_id,
                                "订阅游标缺口无法从日志补齐，结束订阅流"
                            );
                            self.done = true;
                        }
                    }
                    Err(err) => {
                        // run 不存在是确定事实，不是抖动：缺口永远补不上，
                        // 每 10s 重试只会泄漏一个永不退出的 task。
                        // 结束流而非谎报终结、跳缺口或用调用方无法区分的挂起。
                        if matches!(err, BackendError::RunNotFound(_)) {
                            tracing::debug!(run_id = %self.run_id, "订阅的 run 不存在，结束订阅流");
                            self.done = true;
                            continue;
                        }
                        // 其余失败：不谎报终结、不跳缺口：等重试定时器或下一条事件唤醒
                        tracing::debug!(
                            run_id = %self.run_id,
                            error = %err,
                            "订阅补齐读取失败，稍后重试"
                        );
                        self.fill_failed = true;
                    }
                }
                continue;
            }

            // 3) 等待唤醒：新事件（快路径）/ 补齐重试定时器 / 共享流关闭
            tokio::select! {
                _ = tokio::time::sleep(FILL_RETRY), if self.needs_fill => {
                    self.fill_failed = false;
                }
                received = self.rx.recv() => {
                    match received {
                        Ok(envelope) if envelope.run_id == self.run_id => {
                            if envelope.seq > self.last_seq {
                                if envelope.seq > self.last_seq + 1 {
                                    self.needs_fill = true;
                                }
                                self.events.insert(envelope.seq, envelope);
                            }
                            self.fill_failed = false;
                        }
                        Ok(_) => {}
                        // 广播追赶不上：丢的是唤醒不是数据，整体补齐
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            self.needs_fill = true;
                            self.fill_failed = false;
                        }
                        // 共享流关闭（进程停机）：尽力补齐剩余后收尾
                        Err(broadcast::error::RecvError::Closed) => {
                            self.closed = true;
                            if let Ok(list) = self
                                .reader
                                .read_events(&self.run_id, Some(self.last_seq + 1))
                                .await
                            {
                                for envelope in list {
                                    self.events.insert(envelope.seq, envelope);
                                }
                                self.needs_fill = false;
                                self.fill_failed = false;
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow_engine::Event;
    use serde_json::Value;

    struct FakeReader {
        log: parking_lot::Mutex<Vec<Envelope>>,
        failures: parking_lot::Mutex<u32>,
        /// run 不存在：确定事实，不是抖动
        missing: bool,
    }

    impl FakeReader {
        fn new(log: Vec<Envelope>, failures: u32) -> Arc<FakeReader> {
            Arc::new(FakeReader {
                log: parking_lot::Mutex::new(log),
                failures: parking_lot::Mutex::new(failures),
                missing: false,
            })
        }

        fn missing() -> Arc<FakeReader> {
            Arc::new(FakeReader {
                log: parking_lot::Mutex::new(Vec::new()),
                failures: parking_lot::Mutex::new(0),
                missing: true,
            })
        }

        fn push(&self, envelope: Envelope) {
            self.log.lock().push(envelope);
        }
    }

    impl EventReader for FakeReader {
        fn read_events<'a>(
            &'a self,
            _run_id: &'a str,
            from_seq: Option<u64>,
        ) -> BoxFuture<'a, Result<Vec<Envelope>, BackendError>> {
            if self.missing {
                return Box::pin(async { Err(BackendError::RunNotFound("run-1".into())) });
            }
            let fail = {
                let mut failures = self.failures.lock();
                if *failures > 0 {
                    *failures -= 1;
                    true
                } else {
                    false
                }
            };
            let from = from_seq.unwrap_or(0);
            let list: Vec<Envelope> = self
                .log
                .lock()
                .iter()
                .filter(|e| e.seq >= from)
                .cloned()
                .collect();
            Box::pin(async move {
                if fail {
                    Err(BackendError::Internal("db 抖动".into()))
                } else {
                    Ok(list)
                }
            })
        }
    }

    fn env(seq: u64, event: Event) -> Envelope {
        Envelope {
            seq,
            ts: chrono::Utc::now(),
            run_id: "run-1".to_string(),
            event,
        }
    }

    fn started(seq: u64) -> Envelope {
        env(
            seq,
            Event::RunStarted {
                workflow_id: "w".into(),
                workflow_version: 1,
                input: Value::Null,
                depth: 0,
            },
        )
    }

    fn node_done(seq: u64) -> Envelope {
        env(
            seq,
            Event::NodeCompleted {
                node_id: "n".into(),
                attempt: 1,
                output: Value::Null,
                duration_ms: 1,
            },
        )
    }

    fn terminal(seq: u64) -> Envelope {
        env(
            seq,
            Event::RunCompleted {
                output: Value::Null,
            },
        )
    }

    async fn collect(stream: BoxStream<'static, Envelope>) -> Vec<u64> {
        let mut out = Vec::new();
        futures::pin_mut!(stream);
        while let Some(envelope) = stream.next().await {
            out.push(envelope.seq);
        }
        out
    }

    /// 订阅一个不存在的 run：必须结束流。修复前 RunNotFound 被当成「db 抖动」，
    /// needs_fill 永远为真 → 每 10s 重试一次，task 永不退出（前端打错 run_id
    /// 就永久泄漏一个订阅 task）。
    #[tokio::test(start_paused = true)]
    async fn missing_run_ends_stream_instead_of_retrying_forever() {
        let reader = FakeReader::missing();
        let (_tx, rx) = broadcast::channel::<Envelope>(16);

        let stream = run_tail(reader, rx, "run-1".into());
        let seqs = tokio::time::timeout(Duration::from_secs(60), collect(stream))
            .await
            .expect("订阅不存在的 run 必须立刻结束（修复前永久挂起）");
        assert!(seqs.is_empty(), "不存在的 run 不应吐出任何事件：{seqs:?}");
    }

    /// 回放历史、实时段去重、终态后自然结束。
    #[tokio::test(start_paused = true)]
    async fn replays_history_dedups_live_and_ends_on_terminal() {
        let log = vec![started(1), node_done(2), terminal(3)];
        let reader = FakeReader::new(log, 0);
        let (tx, rx) = broadcast::channel(16);
        // 实时段带着与回放重叠的重复事件
        tx.send(node_done(2)).unwrap();
        tx.send(terminal(3)).unwrap();

        let stream = run_tail(reader, rx, "run-1".into());
        let seqs = tokio::time::timeout(Duration::from_secs(60), collect(stream))
            .await
            .expect("订阅流未结束");
        assert_eq!(seqs, vec![1, 2, 3]);
    }

    /// 缺口必须补齐后再吐，绝不跳过丢事件（实时段跳号 = 广播 Lagged）。
    #[tokio::test(start_paused = true)]
    async fn gap_is_filled_instead_of_skipped() {
        let log = vec![started(1), node_done(2)];
        let reader = FakeReader::new(log, 0);
        let (tx, rx) = broadcast::channel(16);
        let stream = run_tail(reader.clone(), rx, "run-1".into());
        futures::pin_mut!(stream);

        assert_eq!(stream.next().await.unwrap().seq, 1);
        assert_eq!(stream.next().await.unwrap().seq, 2);

        // 日志续写 3、4，但实时只推 4：缺口 3 必须被补齐
        reader.push(node_done(3));
        reader.push(terminal(4));
        tx.send(terminal(4)).unwrap();

        let seqs: Vec<u64> = tokio::time::timeout(Duration::from_secs(60), async {
            let mut out = Vec::new();
            while let Some(envelope) = stream.next().await {
                out.push(envelope.seq);
            }
            out
        })
        .await
        .expect("订阅流未结束");
        assert_eq!(seqs, vec![3, 4]);
    }

    /// 补齐读失败时不跳缺口、不挂死：重试定时器兜底后再吐。
    #[tokio::test(start_paused = true)]
    async fn fill_failure_retries_without_skipping_or_hanging() {
        let log = vec![started(1), node_done(2)];
        let reader = FakeReader::new(log, 1); // 下一次读失败
        let (tx, rx) = broadcast::channel(16);
        let stream = run_tail(reader.clone(), rx, "run-1".into());
        futures::pin_mut!(stream);

        assert_eq!(stream.next().await.unwrap().seq, 1);
        assert_eq!(stream.next().await.unwrap().seq, 2);

        reader.push(node_done(3));
        reader.push(terminal(4));
        tx.send(terminal(4)).unwrap(); // 缺口 3 + 补齐失败

        let seqs: Vec<u64> = tokio::time::timeout(Duration::from_secs(60), async {
            let mut out = Vec::new();
            while let Some(envelope) = stream.next().await {
                out.push(envelope.seq);
            }
            out
        })
        .await
        .expect("订阅流未结束");
        assert_eq!(seqs, vec![3, 4]);
    }

    /// 已终结 run 订阅：首轮回放读失败也必须靠重试吐完历史并结束，
    /// 不能因没有新事件唤醒而永久挂起。
    #[tokio::test(start_paused = true)]
    async fn terminal_run_replay_survives_transient_read_failure() {
        let log = vec![started(1), terminal(2)];
        let reader = FakeReader::new(log, 1); // 首轮回放失败
        let (_tx, rx) = broadcast::channel::<Envelope>(16);

        let stream = run_tail(reader, rx, "run-1".into());
        let seqs = tokio::time::timeout(Duration::from_secs(60), collect(stream))
            .await
            .expect("订阅流未结束（回放失败后挂死）");
        assert_eq!(seqs, vec![1, 2]);
    }
}
