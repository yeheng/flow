//! 严格组提交语义（DESIGN §3.2）：
//! - `append` 返回即 durable：任意新读取者立刻能看到完整行，seq 严格连续；
//! - fsync 跨 run 组批：并发追加时批数显著小于事件数（攒批不是摆设）；
//! - 并发多日志互不串扰：单文件 seq 有序、行不互相粘连。
//!
//! fsync 级持久化（断电语义）进程内不可观测，由 backend-e2e 的 SIGKILL 恢复
//! 用例钉住「写序协议 + 崩溃窗口」不变；这里钉组批机制与读回一致性。

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use flow_engine::{commit_stats, read_events, Event, EventLog};
use uuid::Uuid;

/// `commit_stats` 是进程级计数器，同二进制内的用例必须串行取差值，
/// 否则并行用例的 append 会混进统计（跨测试二进制是独立进程，无此问题）。
fn test_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(tokio::sync::Mutex::default)
}

fn temp_root() -> PathBuf {
    let root = std::env::temp_dir().join(format!("flow-group-commit-{}", Uuid::now_v7()));
    std::fs::create_dir_all(&root).expect("创建临时目录失败");
    root
}

fn log_path(root: &Path, run_id: &str) -> PathBuf {
    root.join("runs").join(run_id).join("event.jsonl")
}

fn started(index: usize) -> Event {
    Event::NodeStarted {
        node_id: format!("n{index}"),
        attempt: 1,
        child_run_id: None,
    }
}

/// 8 个日志 × 5 事件并发追加：每个文件 seq 有序、行完整；fsync 必须被组批
/// （批数远小于事件数——攒批是 drain 式的，一攒就是同时在飞的 append）。
#[tokio::test]
async fn concurrent_appends_group_fsyncs_and_stay_ordered() {
    let _guard = test_lock().lock().await;
    let root = temp_root();
    const LOGS: usize = 8;
    const EVENTS: usize = 5;
    let before = commit_stats();

    let mut tasks = Vec::new();
    for log_index in 0..LOGS {
        let root = root.clone();
        tasks.push(tokio::spawn(async move {
            let run_id = format!("run-{log_index}");
            let mut log = EventLog::create(&root, &run_id).await.unwrap();
            for event_index in 0..EVENTS {
                let envelope = log.append(&run_id, started(event_index)).await.unwrap();
                assert_eq!(envelope.seq, event_index as u64 + 1);
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }

    for log_index in 0..LOGS {
        let run_id = format!("run-{log_index}");
        let events = read_events(&log_path(&root, &run_id)).await.unwrap();
        assert_eq!(events.len(), EVENTS, "{run_id} 事件数不符");
        for (position, envelope) in events.iter().enumerate() {
            assert_eq!(envelope.seq, position as u64 + 1, "{run_id} seq 不连续");
            assert_eq!(envelope.run_id, run_id, "{run_id} 行串扰到别的 run");
        }
    }

    let after = commit_stats();
    let batches = after.batches - before.batches;
    let file_syncs = after.file_syncs - before.file_syncs;
    let total = (LOGS * EVENTS) as u64;
    println!("组批统计：{total} 个 append → {batches} 批 / {file_syncs} 次文件 fsync");
    assert_eq!(file_syncs, total, "每事件一个文件 fsync（严格语义不打折）");
    assert!(
        batches * 2 <= total,
        "fsync 必须被组批：{total} 个 append 只该跑几批，实际 {batches} 批"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// append 返回即可被全新读取者完整读到；EventLog::open 续写接在返回的 seq 后。
/// 「返回即 durable」的 fsync 面由 SIGKILL 用例覆盖，这里钉读回一致性。
#[tokio::test]
async fn append_returns_with_line_fully_readable() {
    let _guard = test_lock().lock().await;
    let root = temp_root();
    let mut log = EventLog::create(&root, "r").await.unwrap();

    let first = log.append("r", started(1)).await.unwrap();
    let events = read_events(&log_path(&root, "r")).await.unwrap();
    assert_eq!(events, vec![first.clone()], "append 返回即须读到完整行");

    // 崩溃恢复路径：open 续写必须接在已返回的 seq 之后，不重不漏
    let mut reopened = EventLog::open(&root, "r").await.unwrap();
    let second = reopened
        .append(
            "r",
            Event::NodeCompleted {
                node_id: "n1".into(),
                attempt: 1,
                output: serde_json::Value::Null,
                duration_ms: 3,
            },
        )
        .await
        .unwrap();
    assert_eq!(second.seq, first.seq + 1);

    let events = read_events(&log_path(&root, "r")).await.unwrap();
    assert_eq!(events, vec![first, second]);

    let _ = std::fs::remove_dir_all(&root);
}
