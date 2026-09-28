//! 排他 flock 回归（DESIGN §12.14）：SQLite 后端是单进程设计，第二个实例
//! （同机或跨机共享 data_dir）必须在 open 时立即失败。否则启动恢复
//! （recover_unfinished）会把进程 A 正在驱动的 run 劫持出第二个 Driver——
//! 各自独立 seq 计数器写同一 event.jsonl，必然产生重复 seq 与重复副作用
//! （同一节点的 http_call 重打一次）。事后 seq 校验只能检测，flock 才是预防。

use flow_backend::SqliteBackend;
use flow_test_support::io::TempDir;

fn scratch() -> TempDir {
    TempDir::new("flow-flock")
}

#[tokio::test]
async fn second_instance_on_same_data_dir_is_rejected() {
    let root = scratch();

    let first = SqliteBackend::open(root.path(), root.join("flow.db"))
        .await
        .expect("首个实例应正常打开");

    // 同进程二次 open（独立 fd）同样被拒：flock 按 open file description 判定，
    // 与「另一个进程」路径走同一条防线
    let err = match SqliteBackend::open(root.path(), root.join("flow.db")).await {
        Ok(_) => panic!("共享 data_dir 的第二个实例必须被拒绝"),
        Err(err) => err,
    };
    let message = err.to_string();
    assert!(
        message.contains("另一个 flow 实例"),
        "错误信息应指明 data_dir 被占用，实际：{message}"
    );

    // 释放后（Drop 解锁）可重新打开——重启语义不被破坏
    drop(first);
    SqliteBackend::open(root.path(), root.join("flow.db"))
        .await
        .expect("锁释放后应能重新打开");
}

#[tokio::test]
async fn independent_data_dirs_do_not_interfere() {
    // flock 按 data_dir 粒度：不同目录的多个后端（同进程，如测试场景）互不干扰
    let roots: Vec<TempDir> = (0..2).map(|_| scratch()).collect();
    let mut backends = Vec::new();
    for root in &roots {
        backends.push(
            SqliteBackend::open(root.path(), root.join("flow.db"))
                .await
                .expect("不同 data_dir 应能同时打开"),
        );
    }
    drop(backends);
    // TempDir 的 Drop 各自删目录
}
