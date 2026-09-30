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

/// 订阅一个不存在的 run：必须结束流。`RunNotFound` 是确定事实而非抖动，
/// 若当「db 抖动」处理则退避重试永不退出（前端打错 run_id 就永久泄漏一个
/// 订阅 task）。
#[tokio::test(start_paused = true)]
async fn missing_run_ends_stream_instead_of_retrying_forever() {
    let reader = FakeReader::missing();
    let (_tx, rx) = broadcast::channel::<Envelope>(16);

    let stream = run_tail(reader, rx, "run-1".into());
    let seqs = tokio::time::timeout(Duration::from_secs(60), collect(stream))
        .await
        .expect("订阅不存在的 run 必须立刻结束（当成抖动会永久挂起）");
    assert!(seqs.is_empty(), "不存在的 run 不应吐出任何事件：{seqs:?}");
}

/// 空日志（run 存在但零事件——SQLite 臂「event.jsonl 已建、run_started
/// 未落盘」的崩溃窗口，恢复流程已按 DB 投影标终态）：必须结束流而不是
/// 挂死。终态事件永远不会出现，等下去只会挂死订阅方。
#[tokio::test(start_paused = true)]
async fn empty_log_ends_stream_instead_of_hanging() {
    let reader = FakeReader::new(Vec::new(), 0);
    let (_tx, rx) = broadcast::channel::<Envelope>(16);

    let stream = run_tail(reader, rx, "run-1".into());
    let seqs = tokio::time::timeout(Duration::from_secs(60), collect(stream))
        .await
        .expect("空日志的订阅必须立刻结束（当成抖动会永久挂起）");
    assert!(seqs.is_empty(), "空日志不应吐出任何事件：{seqs:?}");
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

/// 计数 + 让出的一次读取：模拟真实异步 I/O（ Immediately-ready 的假读者
/// 会让空转的任务永不屈服，超时断言就跑不到）。
struct CountingReader {
    log: parking_lot::Mutex<Vec<Envelope>>,
    reads: std::sync::atomic::AtomicUsize,
}

impl CountingReader {
    fn new(log: Vec<Envelope>) -> Arc<CountingReader> {
        Arc::new(CountingReader {
            log: parking_lot::Mutex::new(log),
            reads: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    fn reads(&self) -> usize {
        self.reads.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl EventReader for CountingReader {
    fn read_events<'a>(
        &'a self,
        _run_id: &'a str,
        from_seq: Option<u64>,
    ) -> BoxFuture<'a, Result<Vec<Envelope>, BackendError>> {
        Box::pin(async move {
            tokio::task::yield_now().await;
            self.reads
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let from = from_seq.unwrap_or(0);
            Ok(self
                .log
                .lock()
                .iter()
                .filter(|e| e.seq >= from)
                .cloned()
                .collect())
        })
    }
}

/// 活着的 run（日志已追平、尚无终态）必须阻塞等唤醒，**不得空转读日志**。
///
/// 已有的用例全部以「终态 / run 不存在 / 空日志」收尾，三种都会让
/// `advance()` 走到 `return None`；「追平但仍活着」这条路径没有任何覆盖，
/// 而它恰好是订阅一个长跑 run 的**常态**。空转在那里意味着每个在线观众
/// 无偿占满一个核，并且每次都把整份日志重读一遍。
#[tokio::test]
async fn live_run_at_tail_blocks_instead_of_spinning() {
    let reader = CountingReader::new(vec![started(1), node_done(2)]);
    let (_tx, rx) = broadcast::channel::<Envelope>(16);

    let stream = run_tail(reader.clone(), rx, "run-1".into());
    futures::pin_mut!(stream);
    assert_eq!(stream.next().await.unwrap().seq, 1);
    assert_eq!(stream.next().await.unwrap().seq, 2);

    let before = reader.reads();
    let next = tokio::time::timeout(Duration::from_millis(300), stream.next()).await;
    assert!(
        next.is_err(),
        "日志已追平且 run 未终结：必须阻塞等唤醒，不得空转"
    );
    let during = reader.reads() - before;
    assert!(during <= 1, "追平后重读日志 {during} 次——advance 在空转");
}

/// 缺口由**实时段先到**造成（广播快于读取面）时必须被重试补齐。
///
/// 构造方式只能借道「首读失败」：广播只在第 3 步的 `select!` 里被消费，而第 3 步
/// 只有已经 `Filling` 才会到——先让读取失败一次把相位压过去，此时广播里排队的
/// 终点事件才被取出，缺口 3 就此由实时段造成。
///
/// 失败模式是**永久退避**：`advance()` 每轮先把缺口转回 `Filling`，第 2 步的
/// 补齐于是被整个跳过，10s 定时器醒来又立刻被转回 `Filling`——读永远不会发生，
/// 订阅静默停在这个 run 上，连终态都送不出去。已有用例的读取面都即时成功，
/// 缺口在读取一步就被补掉，广播只是送来的重复，碰不到这条路径。
#[tokio::test(start_paused = true)]
async fn gap_created_by_live_segment_is_filled_by_retry() {
    let log = vec![started(1), node_done(2), node_done(3), terminal(4)];
    let reader = FakeReader::new(log, 1); // 首读失败：进入 Filling，才会消费广播
    let (tx, rx) = broadcast::channel::<Envelope>(16);
    tx.send(terminal(4)).unwrap(); // 实时段先到 4

    let stream = run_tail(reader, rx, "run-1".into());
    let seqs = tokio::time::timeout(Duration::from_secs(120), collect(stream))
        .await
        .expect("缺口必须被重试补齐（永久退避会让订阅静默停摆）");
    assert_eq!(seqs, vec![1, 2, 3, 4]);
}
