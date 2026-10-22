#![cfg(feature = "journal-cluster")]
use flow_backend::journal::JournalBackend;
use flow_backend::journal_cluster::{ClusterError, Membership, Registry, Snapshot};
use flow_config::JournalClusterConfig;
use std::sync::Arc;
use std::time::Duration;

fn config() -> JournalClusterConfig {
    JournalClusterConfig {
        cluster_id: format!("test-{}", uuid::Uuid::now_v7()),
        node_id: "a".into(),
        heartbeat_ms: 300,
        lease_ms: 900,
        ..Default::default()
    }
}
async fn registry(config: &JournalClusterConfig) -> Arc<Registry> {
    Registry::connect(
        &std::env::var("FLOW_CLUSTER_TEST_DATABASE_URL").expect("isolated PG required"),
        config,
    )
    .await
    .unwrap()
}
async fn register(
    registry: &Registry,
    node: &str,
    journal: &str,
    snapshot: &Snapshot,
) -> flow_backend::journal_cluster::Lease {
    registry
        .register(
            node,
            journal,
            "ws://127.0.0.1:9802",
            "http://127.0.0.1:9803",
            snapshot,
        )
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires FLOW_CLUSTER_TEST_DATABASE_URL"]
async fn concurrent_registration_and_permanent_journal_ownership() {
    let config = config();
    let (one, two) = tokio::join!(registry(&config), registry(&config));
    let snapshot = Snapshot::default();
    let (first, second) = tokio::join!(
        one.register(
            "a",
            "j1",
            "ws://localhost:9802",
            "http://localhost:9803",
            &snapshot
        ),
        two.register(
            "a",
            "j1",
            "ws://localhost:9802",
            "http://localhost:9803",
            &snapshot
        )
    );
    assert_ne!(first.is_ok(), second.is_ok());
    let lease = first.or(second).unwrap();
    assert!(matches!(
        one.register(
            "b",
            "j1",
            "ws://localhost:9802",
            "http://localhost:9803",
            &snapshot
        )
        .await,
        Err(ClusterError::Conflict(_))
    ));
    one.release(&lease).await.unwrap();
    assert!(matches!(
        one.register(
            "a",
            "j2",
            "ws://localhost:9802",
            "http://localhost:9803",
            &snapshot
        )
        .await,
        Err(ClusterError::Conflict(_))
    ));
    let restarted = register(&two, "a", "j1", &snapshot).await;
    assert_eq!(restarted.epoch, lease.epoch + 1);
    assert!(matches!(
        one.publish(&lease, &snapshot).await,
        Err(ClusterError::LeaseLost)
    ));
    one.release(&lease).await.unwrap(); // stale release must not stop the new instance
    assert_eq!(
        two.nodes("", 64).await.unwrap()["nodes"][0]["status"],
        "online"
    );
}

#[tokio::test]
#[ignore = "requires FLOW_CLUSTER_TEST_DATABASE_URL"]
async fn atomic_snapshots_are_monotonic_exact_and_cluster_scoped() {
    let c = config();
    let r = registry(&c).await;
    let lease = register(&r, "a", "j1", &Snapshot::default()).await;
    let big = 9_007_199_254_740_993;
    let snapshot = Snapshot {
        durable_lsn: big,
        applied_lsn: big - 1,
        master_epoch: 1,
        writer_error: Some("paused".into()),
        ..Default::default()
    };
    r.publish(&lease, &snapshot).await.unwrap();
    let page = r.nodes("", 64).await.unwrap();
    assert_eq!(page["nodes"][0]["snapshot"]["durable_lsn"], big.to_string());
    assert_eq!(page["nodes"][0]["status"], "degraded");
    assert!(matches!(
        r.publish(&lease, &Snapshot::default()).await,
        Err(ClusterError::LeaseLost)
    ));
    assert!(matches!(
        r.publish(
            &lease,
            &Snapshot {
                durable_lsn: 1,
                applied_lsn: 2,
                ..Default::default()
            }
        )
        .await,
        Err(ClusterError::Invalid(_))
    ));
    let isolated = registry(&config()).await;
    register(&isolated, "a", "j1", &Snapshot::default()).await;
    assert_eq!(
        isolated.nodes("", 64).await.unwrap()["nodes"][0]["snapshot"]["durable_lsn"],
        "0"
    );
    r.release(&lease).await.unwrap();
    assert!(matches!(
        r.register(
            "a",
            "j1",
            "ws://localhost:9802",
            "http://localhost:9803",
            &Snapshot::default()
        )
        .await,
        Err(ClusterError::Conflict(_))
    ));
}

#[tokio::test]
#[ignore = "requires FLOW_CLUSTER_TEST_DATABASE_URL"]
async fn expired_leases_cannot_renew_and_old_removal_cannot_remove_restart() {
    let r = registry(&config()).await;
    let lease = register(&r, "a", "j1", &Snapshot::default()).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        r.nodes("", 64).await.unwrap()["nodes"][0]["status"],
        "offline"
    );
    assert!(matches!(
        r.publish(&lease, &Snapshot::default()).await,
        Err(ClusterError::LeaseLost)
    ));
    let next = register(&r, "a", "j1", &Snapshot::default()).await;
    assert!(matches!(
        r.remove("a", lease.epoch as u64, "old-remove").await,
        Err(ClusterError::Conflict(_))
    ));
    r.publish(&next, &Snapshot::default()).await.unwrap();
}

#[tokio::test]
#[ignore = "requires FLOW_CLUSTER_TEST_DATABASE_URL"]
async fn removal_is_idempotent_across_api_instances_and_fences_reports() {
    let c = config();
    let one = registry(&c).await;
    let two = registry(&c).await;
    let lease = register(&one, "a", "j1", &Snapshot::default()).await;
    register(&one, "b", "j2", &Snapshot::default()).await;
    let (a, b) = tokio::join!(
        one.remove("a", lease.epoch as u64, "remove-1"),
        two.remove("a", lease.epoch as u64, "remove-1")
    );
    assert_eq!(a.unwrap(), b.unwrap());
    assert!(matches!(
        one.remove("b", 1, "remove-1").await,
        Err(ClusterError::Conflict(_))
    ));
    assert!(matches!(
        one.publish(&lease, &Snapshot::default()).await,
        Err(ClusterError::LeaseLost)
    ));
    assert!(matches!(
        one.register(
            "a",
            "j1",
            "ws://localhost:9802",
            "http://localhost:9803",
            &Snapshot::default()
        )
        .await,
        Err(ClusterError::Conflict(_))
    ));
    let page = two.nodes("", 1).await.unwrap();
    assert_eq!(page["nodes"][0]["status"], "removed");
    assert_eq!(page["next_after"], "a");
    assert_eq!(
        two.nodes("a", 64).await.unwrap()["nodes"][0]["node_id"],
        "b"
    );
}

#[tokio::test]
#[ignore = "requires FLOW_CLUSTER_TEST_DATABASE_URL"]
async fn membership_removal_stops_execution_and_rejects_journal_writes() {
    let mut c = config();
    c.database_url_env = format!("FLOW_CLUSTER_TEST_{}", uuid::Uuid::now_v7().simple());
    std::env::set_var(
        &c.database_url_env,
        std::env::var("FLOW_CLUSTER_TEST_DATABASE_URL").unwrap(),
    );
    let dir = tempfile::tempdir().unwrap();
    let backend = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let member = Membership::join(&backend, &c, "ws://localhost:9802", "http://localhost:9803")
        .await
        .unwrap();
    assert!(backend.install_cluster(member.clone()));
    member
        .registry
        .remove("a", member.lease.epoch as u64, "remove")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), member.lost().cancelled())
        .await
        .unwrap();
    let lsn = backend.journal.durable_lsn();
    assert!(backend
        .journal
        .submit(
            "after-removal".into(),
            vec![flow_journal::Event::new(
                flow_journal::EventKind::Command,
                serde_json::json!({"test":true})
            )],
            flow_journal::QueueClass::Control
        )
        .await
        .is_err());
    assert_eq!(backend.journal.durable_lsn(), lsn);
    backend.close().await.unwrap();
    std::env::remove_var(&c.database_url_env);
}

#[tokio::test]
#[ignore = "requires FLOW_CLUSTER_TEST_DATABASE_URL"]
async fn normal_close_publishes_final_progress_and_releases_lease() {
    let mut c = config();
    c.database_url_env = format!("FLOW_CLUSTER_TEST_{}", uuid::Uuid::now_v7().simple());
    std::env::set_var(
        &c.database_url_env,
        std::env::var("FLOW_CLUSTER_TEST_DATABASE_URL").unwrap(),
    );
    let dir = tempfile::tempdir().unwrap();
    let backend = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let member = Membership::join(&backend, &c, "ws://localhost:9802", "http://localhost:9803")
        .await
        .unwrap();
    assert!(backend.install_cluster(member.clone()));
    backend
        .workflow_create("final commit", Some("final-commit"))
        .await
        .unwrap();
    backend.close().await.unwrap();
    let page = member.registry.nodes("", 64).await.unwrap();
    assert_eq!(page["nodes"][0]["status"], "stopped");
    assert_eq!(
        page["nodes"][0]["snapshot"]["durable_lsn"],
        backend.journal.durable_lsn().to_string()
    );
    assert!(matches!(
        member
            .registry
            .publish(&member.lease, &Snapshot::default())
            .await,
        Err(ClusterError::LeaseLost)
    ));
    std::env::remove_var(&c.database_url_env);
}
