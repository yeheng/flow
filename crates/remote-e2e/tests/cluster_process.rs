#![cfg(feature = "journal-cluster")]
use backend_e2e::common::wire as common;
use flow_test_support::io::TempDir;
use serde_json::{json, Value};
use std::process::{Command, Stdio};
use std::time::Duration;

fn spawn(dir: &TempDir, cluster: &str, node: &str) -> common::ServerProc {
    let config = dir.path().join("flow.toml");
    // T7：unix 上进程内执行路径已删除，产品服务端只有 ipc——显式指向工作区
    // 产出的 flow-executor（集群作业因此需要先构建它）。本套件不起 run，
    // 执行器只在启动时完成定位/装配。
    std::fs::write(
        &config,
        format!(
            "[server]\nscheduler_enabled=false\n[execution]\nmode=\"ipc\"\n[journal.cluster]\ncluster_id='{cluster}'\nnode_id='{node}'\nheartbeat_ms=500\nlease_ms=2000\n"
        ),
    )
    .unwrap();
    let mut cmd = Command::new(flow_test_support::io::server_bin());
    cmd.arg("--config")
        .arg(config)
        .env("FLOW_DATA_DIR", dir.path().join("secrets"))
        .env("FLOW_JOURNAL_DATA_DIR", dir.path().join("journal"))
        .env("FLOW_JOURNAL_TOKEN", common::TOKEN)
        .env("FLOW_EXECUTOR_BIN", flow_test_support::io::executor_bin())
        .env(
            "FLOW_CLUSTER_DATABASE_URL",
            std::env::var("FLOW_CLUSTER_TEST_DATABASE_URL").expect("isolated PG required"),
        )
        .env("RUST_LOG", "info")
        .stdin(Stdio::null())
        .stderr(Stdio::inherit());
    let (child, ports) = common::spawn_reporting_ports(
        &mut cmd,
        "FLOW_JOURNAL_ADDR",
        "FLOW_JOURNAL_HTTP_ADDR",
        None,
    )
    .unwrap();
    common::ServerProc::from_parts(child, ports.rpc, ports.http)
}

#[tokio::test]
#[ignore = "requires FLOW_CLUSTER_TEST_DATABASE_URL"]
async fn two_product_nodes_share_registry_and_remote_removal_stops_only_target() {
    let cluster = format!("rpc-{}", uuid::Uuid::now_v7());
    let one = TempDir::new("cluster-node-a");
    let two = TempDir::new("cluster-node-b");
    let server_a = spawn(&one, &cluster, "a");
    let mut server_b = spawn(&two, &cluster, "b");
    let a = server_a.client().await;
    let b = server_b.client().await;
    let status: Value = common::call(&a, "journal.cluster.status", json!({})).await;
    assert_eq!(
        status,
        json!({"enabled":true,"cluster_id":cluster,"node_id":"a"})
    );
    let nodes: Value = common::call(&a, "journal.cluster.nodes", json!({})).await;
    assert_eq!(nodes["nodes"].as_array().unwrap().len(), 2);
    assert_ne!(
        nodes["nodes"][0]["journal_id"],
        nodes["nodes"][1]["journal_id"]
    );
    assert_eq!(
        nodes["nodes"][0]["rpc_url"],
        format!("ws://{}", server_a.addr())
    );
    assert_eq!(
        nodes["nodes"][1]["rpc_url"],
        format!("ws://{}", server_b.addr())
    );
    let _: Value = common::call(&b, "workflow.create", json!({"name":"node b workflow"})).await;
    let independent: Value = common::call(&a, "workflow.list", json!({})).await;
    assert!(independent["workflows"].as_array().unwrap().is_empty());
    let args =
        json!({"node_id":"b","expected_epoch":nodes["nodes"][1]["epoch"],"request_id":"remove-b"});
    let first: Value = common::call(&a, "journal.cluster.remove", args.clone()).await;
    let retry: Value = common::call(&a, "journal.cluster.remove", args).await;
    assert_eq!(first, retry);
    tokio::time::timeout(Duration::from_secs(8), async {
        while server_b.exit_status().is_none() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(server_b.exit_status().unwrap().success());
    let page: Value = common::call(&a, "journal.cluster.nodes", json!({})).await;
    assert_eq!(page["nodes"][1]["status"], "removed");
    assert_eq!(page["nodes"][0]["status"], "online");
    let _: Value = common::call(
        &a,
        "workflow.create",
        json!({"name":"node a remains available"}),
    )
    .await;
}

#[tokio::test]
#[ignore = "requires FLOW_CLUSTER_TEST_DATABASE_URL"]
async fn pg_report_timeout_stops_product_and_leaves_expiring_registry_state() {
    let cluster = format!("rpc-outage-{}", uuid::Uuid::now_v7());
    let dir = TempDir::new("cluster-outage");
    let mut server = spawn(&dir, &cluster, "a");
    let pool = sqlx::PgPool::connect(&std::env::var("FLOW_CLUSTER_TEST_DATABASE_URL").unwrap())
        .await
        .unwrap();
    // Hold the node row: the heartbeat UPDATE must hit its PG statement timeout.
    // This exercises fail-stop without disrupting the database used by other tests.
    let mut lock = pool.begin().await.unwrap();
    sqlx::query("SELECT node_id FROM flow_journal_cluster_nodes WHERE cluster_id=$1 AND node_id='a' FOR UPDATE")
        .bind(&cluster).fetch_one(&mut *lock).await.unwrap();
    tokio::time::timeout(Duration::from_secs(8), async {
        while server.exit_status().is_none() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(server.exit_status().unwrap().success());
    lock.rollback().await.unwrap();
    tokio::time::sleep(Duration::from_millis(2100)).await;
    let expired: bool = sqlx::query_scalar("SELECT lease_until <= clock_timestamp() AND desired_state='enabled' FROM flow_journal_cluster_nodes WHERE cluster_id=$1 AND node_id='a'")
        .bind(cluster).fetch_one(&pool).await.unwrap();
    assert!(expired);
}
