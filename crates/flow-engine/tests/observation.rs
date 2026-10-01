use flow_engine::observation::{ObservationOptions, ObservationStore};
use flow_engine::{LogLevel, LogLine, LogStream, NodeLogger};

#[test]
fn bounded_observations_survive_restart_and_record_loss() {
    let root = std::env::temp_dir().join(format!("flow-observation-{}", uuid::Uuid::now_v7()));
    let options = ObservationOptions {
        segment_bytes: 16 * 1024,
        retained_bytes: 32 * 1024,
    };
    let store = ObservationStore::open(&root, options.clone()).unwrap();
    let target = store.logger("r".into(), "d".into());
    let logger = NodeLogger::observation(target, "n".into(), 1);
    for _ in 0..10_000 {
        logger.log(LogLevel::Warn, LogStream::Stderr, "x".repeat(1024));
    }
    store.flush();
    assert!(store.loss().queue_dropped + store.loss().retention_dropped > 0);
    let page = store.page("r", Some("d"), 0, 20).unwrap();
    assert!(!page.is_empty());
    assert!(page.len() <= 20);
    let total = std::fs::read_dir(&root)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|s| s == "jsonl"))
        .map(|e| e.metadata().unwrap().len())
        .sum::<u64>();
    assert!(total <= 32 * 1024);
    let losses = store.loss();
    drop(logger);
    drop(store);
    let store = ObservationStore::open(&root, options).unwrap();
    assert_eq!(store.loss().queue_dropped, losses.queue_dropped);
    assert!(!store.page("r", None, 0, 20).unwrap().is_empty());
    store.logger("r".into(), "d".into()).emit(LogLine {
        node_id: "n".into(),
        attempt: 1,
        level: LogLevel::Info,
        stream: LogStream::Stdout,
        message: "last".into(),
    });
    store.flush();
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn scoped_rejections_survive_restart_and_do_not_leak_between_runs() {
    let root = std::env::temp_dir().join(format!("flow-observation-{}", uuid::Uuid::now_v7()));
    let store = ObservationStore::open(&root, Default::default()).unwrap();
    let logger = store.logger("r".into(), "d".into());
    for _ in 0..3 {
        logger.emit(LogLine {
            node_id: "n".into(),
            attempt: 1,
            level: LogLevel::Info,
            stream: LogStream::Stdout,
            message: "x".repeat(20_000),
        });
    }
    store.logger("other".into(), "d".into());
    store.flush();
    assert_eq!(store.scoped_loss("r", Some("d")).queue_dropped, 3);
    assert_eq!(store.scoped_loss("other", None).queue_dropped, 0);
    drop(logger);
    drop(store);
    let store = ObservationStore::open(&root, Default::default()).unwrap();
    assert_eq!(store.scoped_loss("r", None).queue_dropped, 3);
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}
