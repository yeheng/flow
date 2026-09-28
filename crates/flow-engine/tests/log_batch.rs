//! 批量日志落盘的 driver 契约：exec 任务的日志**只**经 `append_log_batch`
//! 落盘（单条 `append_log` 不得出现）——PG 后端据此用单事务整批写入，
//! 逐条受保护事务会把"日志廉价层"语义在 PG 上抹平（见 trait 注释）。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use flow_engine::{spawn_driver, DbRunStatus, DriverSpec, Envelope, Event, RunEventSink, RunState};
use futures::future::BoxFuture;
use serde_json::json;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Shared {
    events: Mutex<Vec<Envelope>>,
    seq: AtomicU64,
    /// append_log（单条）调用次数：exec 日志路径必须为 0
    single_log_calls: AtomicU64,
    /// append_log_batch 调用次数与经它的日志事件总数
    batch_calls: AtomicU64,
    batch_log_events: AtomicU64,
}

struct CountingSink {
    shared: Arc<Shared>,
}

impl CountingSink {
    fn new() -> (CountingSink, Arc<Shared>) {
        let shared = Arc::new(Shared::default());
        (
            CountingSink {
                shared: shared.clone(),
            },
            shared,
        )
    }

    fn commit(&self, event: Event) -> Envelope {
        let seq = self.shared.seq.fetch_add(1, Ordering::SeqCst) + 1;
        let envelope = Envelope {
            seq,
            ts: chrono::Utc::now(),
            run_id: "r-batch".into(),
            event,
        };
        self.shared.events.lock().unwrap().push(envelope.clone());
        envelope
    }
}

impl RunEventSink for CountingSink {
    fn append<'a>(
        &'a mut self,
        event: Event,
    ) -> BoxFuture<'a, Result<Envelope, flow_engine::EngineError>> {
        Box::pin(async move { Ok(self.commit(event)) })
    }

    fn append_log<'a>(
        &'a mut self,
        event: Event,
    ) -> BoxFuture<'a, Result<Envelope, flow_engine::EngineError>> {
        Box::pin(async move {
            self.shared.single_log_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.commit(event))
        })
    }

    fn append_log_batch<'a>(
        &'a mut self,
        events: Vec<Event>,
    ) -> BoxFuture<'a, Result<Vec<Envelope>, flow_engine::EngineError>> {
        Box::pin(async move {
            self.shared.batch_calls.fetch_add(1, Ordering::SeqCst);
            self.shared
                .batch_log_events
                .fetch_add(events.len() as u64, Ordering::SeqCst);
            let mut envelopes = Vec::with_capacity(events.len());
            for event in events {
                envelopes.push(self.commit(event));
            }
            Ok(envelopes)
        })
    }

    fn append_terminal<'a>(
        &'a mut self,
        event: Event,
    ) -> BoxFuture<'a, Result<Envelope, flow_engine::EngineError>> {
        Box::pin(async move { Ok(self.commit(event)) })
    }

    fn project_status<'a>(
        &'a mut self,
        _status: DbRunStatus,
        _error: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), flow_engine::EngineError>> {
        Box::pin(async { Ok(()) })
    }

    fn poll_inputs<'a>(
        &'a mut self,
    ) -> BoxFuture<'a, Result<Vec<flow_engine::PendingInput>, flow_engine::EngineError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn commit_signal<'a>(
        &'a mut self,
        _input: &'a flow_engine::PendingInput,
        _event: Event,
    ) -> BoxFuture<'a, Result<flow_engine::CommitOutcome, flow_engine::EngineError>> {
        Box::pin(async { Err(flow_engine::EngineError::Node("测试不投递信号".into())) })
    }

    fn reject_signal<'a>(
        &'a mut self,
        _input: &'a flow_engine::PendingInput,
        _reason: &'a str,
    ) -> BoxFuture<'a, Result<(), flow_engine::EngineError>> {
        Box::pin(async { Ok(()) })
    }

    fn consume_cancel<'a>(
        &'a mut self,
        _input: &'a flow_engine::PendingInput,
    ) -> BoxFuture<'a, Result<flow_engine::CommitOutcome, flow_engine::EngineError>> {
        Box::pin(async { Err(flow_engine::EngineError::Node("测试不取消".into())) })
    }

    fn release<'a>(&'a mut self) -> BoxFuture<'a, Result<(), flow_engine::EngineError>> {
        Box::pin(async { Ok(()) })
    }
}

/// 脚本刷 300 条 console.log：全部日志经 append_log_batch 落盘，
/// 单条 append_log 一次都不出现；seq 全程连续。
#[tokio::test]
async fn driver_writes_exec_logs_only_through_batch_interface() {
    let (sink, shared) = CountingSink::new();
    let definition: flow_engine::Definition = serde_json::from_value(json!({
        "nodes": [
            {"id": "s", "type": "start", "params": {}},
            {"id": "n", "type": "script", "params": {
                "code": "for (let i = 0; i < 300; i++) { console.log('line', i); }\nreturn 'done';"
            }},
            {"id": "e", "type": "end", "params": {}}
        ],
        "edges": [
            {"from": "s", "to": "n"},
            {"from": "n", "to": "e"}
        ]
    }))
    .unwrap();

    let handle = spawn_driver(
        DriverSpec {
            run_id: "r-batch".into(),
            definition: Arc::new(definition),
            input: json!({}),
            depth: 0,
            child_launcher: None,
            sink: Box::new(sink),
            events_tx: None,
            cancel: CancellationToken::new(),
            ownership_lost: CancellationToken::new(),
            signal_rx: None,
            inbox_poll: Duration::from_millis(10),
        },
        RunState::new(),
        Default::default(),
    );

    // sink 不做投影：等 Driver 完全退出后直接校验事件日志
    let _ = handle.await;

    let events = shared.events.lock().unwrap().clone();
    let log_count = events
        .iter()
        .filter(|e| matches!(e.event, Event::NodeLog { .. }))
        .count();
    assert_eq!(log_count, 300, "300 条 console.log 全部落盘：{events:?}");
    assert_eq!(
        shared.single_log_calls.load(Ordering::SeqCst),
        0,
        "exec 日志不得走单条 append_log（PG 上逐条=逐事务）"
    );
    assert_eq!(
        shared.batch_log_events.load(Ordering::SeqCst),
        300,
        "全部日志经批接口"
    );
    let batches = shared.batch_calls.load(Ordering::SeqCst);
    assert!(
        batches >= 1 && batches <= 300,
        "批次数应在合理范围：{batches}"
    );

    // seq 连续：日志行与状态事件同一编号空间
    for (index, envelope) in events.iter().enumerate() {
        assert_eq!(envelope.seq, index as u64 + 1);
    }
    let state = RunState::from_events(&events).unwrap();
    assert!(matches!(state.phase, flow_engine::RunPhase::Succeeded));
}

/// 只实现 `append` 的最小 sink：`append_log` / `append_log_batch` 全走 trait
/// 默认（append_log→append，批→逐条循环），不覆写的后端自动继承正确语义。
#[tokio::test]
async fn trait_default_batch_delegates_to_append() {
    use std::sync::atomic::AtomicUsize;

    struct MinimalSink {
        appended: AtomicUsize,
        sink: CountingSink,
    }

    impl MinimalSink {
        fn new() -> MinimalSink {
            MinimalSink {
                appended: AtomicUsize::new(0),
                sink: CountingSink::new().0,
            }
        }
    }

    impl RunEventSink for MinimalSink {
        fn append<'a>(
            &'a mut self,
            event: Event,
        ) -> BoxFuture<'a, Result<Envelope, flow_engine::EngineError>> {
            Box::pin(async move {
                self.appended.fetch_add(1, Ordering::SeqCst);
                Ok(self.sink.commit(event))
            })
        }
        fn append_terminal<'a>(
            &'a mut self,
            event: Event,
        ) -> BoxFuture<'a, Result<Envelope, flow_engine::EngineError>> {
            Box::pin(async move { self.append(event).await })
        }
        fn project_status<'a>(
            &'a mut self,
            _status: DbRunStatus,
            _error: Option<&'a str>,
        ) -> BoxFuture<'a, Result<(), flow_engine::EngineError>> {
            Box::pin(async { Ok(()) })
        }
        fn poll_inputs<'a>(
            &'a mut self,
        ) -> BoxFuture<'a, Result<Vec<flow_engine::PendingInput>, flow_engine::EngineError>>
        {
            Box::pin(async { Ok(Vec::new()) })
        }
        fn commit_signal<'a>(
            &'a mut self,
            _input: &'a flow_engine::PendingInput,
            _event: Event,
        ) -> BoxFuture<'a, Result<flow_engine::CommitOutcome, flow_engine::EngineError>> {
            Box::pin(async { Err(flow_engine::EngineError::Node("测试不投递信号".into())) })
        }
        fn reject_signal<'a>(
            &'a mut self,
            _input: &'a flow_engine::PendingInput,
            _reason: &'a str,
        ) -> BoxFuture<'a, Result<(), flow_engine::EngineError>> {
            Box::pin(async { Ok(()) })
        }
        fn consume_cancel<'a>(
            &'a mut self,
            _input: &'a flow_engine::PendingInput,
        ) -> BoxFuture<'a, Result<flow_engine::CommitOutcome, flow_engine::EngineError>> {
            Box::pin(async { Err(flow_engine::EngineError::Node("测试不取消".into())) })
        }
        fn release<'a>(&'a mut self) -> BoxFuture<'a, Result<(), flow_engine::EngineError>> {
            Box::pin(async { Ok(()) })
        }
    }

    // 空批短路：不触发任何 sink 调用
    let mut sink = MinimalSink::new();
    let envelopes = sink.append_log_batch(Vec::new()).await.unwrap();
    assert!(envelopes.is_empty());
    assert_eq!(sink.appended.load(Ordering::SeqCst), 0);

    // 3 条日志批：默认逐条 → 3 次 append，包络 seq 连续、事件保序
    let events = (0..3)
        .map(|i| Event::NodeLog {
            node_id: "n".into(),
            attempt: 1,
            level: flow_engine::LogLevel::Info,
            stream: flow_engine::LogStream::Stdout,
            message: format!("m{i}"),
        })
        .collect();
    let envelopes = sink.append_log_batch(events).await.unwrap();
    assert_eq!(envelopes.len(), 3);
    assert_eq!(sink.appended.load(Ordering::SeqCst), 3, "默认实现逐条委托");
    for (index, envelope) in envelopes.iter().enumerate() {
        assert_eq!(envelope.seq, index as u64 + 1);
        assert!(matches!(envelope.event, Event::NodeLog { .. }));
    }
}
