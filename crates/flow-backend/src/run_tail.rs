//! 指定 run 的「回放 + 实时追流」订阅流（`flow-pg/src/subscribe.rs`）：
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
/// 用相位枚举而非一组 bool：多个 bool 的组合里约一半是非法的，而且非法组合
/// 可构造（先置「要补齐」再置「已结束」就得到「已结束但仍有缺口」）。
/// 相位是一个值，不变量由类型保证：
/// - 缺口**不是**状态，是 `events` 里最小 seq 与 `last_seq` 的关系（`has_gap`）；
/// - 回放是否完成过由 `Replaying → Streaming` 的迁移表达，不是独立开关；
/// - 补齐失败 = `Filling`，与「要补齐」是同一个相位，不存在
///   「有缺口但没在补」或「没缺口却在退避」这两种矛盾组合。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// 首次全量回放（seq=1 起）尚未成功过。
    Replaying,
    /// 回放完成，追流中。已追平（`caught_up`）时不再读，等唤醒。
    Streaming,
    /// 正在等一个缺口被补齐；`retry_at` 到点后重试补齐。
    ///
    /// **只有「一次读取毫无所获而缺口仍在」才进这个相位**（`advance` 第 2 步）。
    /// 缺口本身不是进入理由：相位一旦是 `Filling`，第 2 步的补齐就被整个跳过，
    /// 于是「缺口由实时段先到造成」时会永远在 退避→醒来→再退避 之间打转——
    /// 读不发生、缺口不消失、订阅静默停摆（`gap_created_by_live_segment_*` 守护）。
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
    /// 已追平：上一次补读取没有任何新数据，且暂存区没有缺口。
    ///
    /// **为什么需要它**：没有这个标记时，第 2 步每次 `advance()` 都会重读一次
    /// 读取面。对活着且暂时没有新事件的 run（订阅一个长跑 run 的**常态**）这就
    /// 是无限空转——每个在线观众烧满一个核，且每次重读整份日志
    /// （`live_run_at_tail_*` 守护）。追平后必须落到第 3 步，由新事件或 10s
    /// 兜底定时器唤醒；`Phase` 装不下这个信息：它不是进度，而是「读取结果为空
    /// 且无缺口」这个组合的结论。
    caught_up: bool,
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
            caught_up: false,
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
    ///
    /// 进入即意味着「这一轮不要读了」：调用点必须已经准备好去第 3 步等唤醒。
    fn ensure_filling(&mut self) {
        if self.phase.is_filling() {
            return;
        }
        self.caught_up = false;
        self.phase = Phase::Filling {
            retry_at: tokio::time::Instant::now() + FILL_RETRY,
        };
    }

    async fn advance(&mut self) -> Option<Envelope> {
        loop {
            // 1) 排水：只吐严格连续的前缀。重复（补齐段与实时段重叠）丢弃。
            //    缺口**绝不跳**（下面的臂只放行 last_seq+1，缺口天然挡住后续一切
            //    事件），但**不在这里转 Filling**——补齐发生在第 2 步，提前转相位
            //    只会让第 2 步被跳过，缺口永远补不上（见 Phase::Filling 的注释）。
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
                Some(_) => {}
                None => {}
            }
            if !self.phase.is_live() {
                return None;
            }

            // 2) 补齐。回放期（Replaying）读全量；追流期读 last_seq+1 起的增量。
            //    追平（`caught_up`）后跳过本步去等唤醒：否则活着且暂时没有新事件的
            //    run 会被无限重读——每个在线观众烧满一个核，且每次重读整份日志。
            //    读不到新数据时同样去等：有缺口转 Filling 等退避，无缺口就是追平
            //    等新事件。两条的共同点是**不再读**，绝不 `continue` 回本步。
            let mut fresh_data = false;
            if !self.phase.is_filling() && !self.caught_up {
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
                        fresh_data = added;
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
                            self.caught_up = false;
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
                // 一次读取毫无所获：去第 3 步等唤醒。有缺口 → Filling（保留原退避，
                // 由新事件或定时器唤醒）；无缺口 → 追平。两者都不再读。
                if !fresh_data {
                    if self.has_gap() {
                        self.ensure_filling();
                    }
                    self.caught_up = self.phase.is_live() && !self.has_gap();
                }
                if !self.caught_up {
                    continue;
                }
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
                    // 重试窗口到了：回到「去补齐」。**退避不等于追平**——醒来必须
                    // 真的再读一次，所以 caught_up 与相位一起清掉。
                    self.phase = Phase::Streaming;
                    self.caught_up = false;
                }
                received = self.rx.recv() => {
                    match received {
                        Ok(envelope) if envelope.run_id == self.run_id => {
                            // 新事件是有效唤醒：清掉追平标记回第 2 步重读。它造成的
                            // 缺口是否继续退避，由第 2 步的「读不到新数据」分支决定，
                            // 不在这里提前决定——那正是永久退避的来路。
                            self.caught_up = false;
                            if envelope.seq > self.last_seq {
                                if envelope.seq > self.last_seq + 1 {
                                    self.ensure_filling();
                                }
                                self.events.insert(envelope.seq, envelope);
                            }
                            self.phase = Phase::Streaming;
                        }
                        Ok(_) => {}
                        // 广播追赶不上：丢的是唤醒不是数据，整体补齐
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            self.phase = Phase::Streaming;
                            self.caught_up = false;
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
