//! PostgreSQL membership and progress reports for independent SQLite journals.
//! No business events, run ownership or log replication are stored here.
use std::collections::BTreeMap;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use flow_config::JournalClusterConfig;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::journal::JournalBackend;

#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error("journal cluster database unavailable: {0}")]
    Database(#[from] sqlx::Error),
    #[error("journal cluster conflict: {0}")]
    Conflict(String),
    #[error("journal cluster lease lost")]
    LeaseLost,
    #[error("journal cluster invalid: {0}")]
    Invalid(String),
}
pub type Result<T> = std::result::Result<T, ClusterError>;

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Snapshot {
    #[serde(with = "flow_journal::decimal")]
    pub durable_lsn: u64,
    #[serde(with = "flow_journal::decimal")]
    pub applied_lsn: u64,
    #[serde(with = "flow_journal::decimal")]
    pub master_epoch: u64,
    pub run_counts: BTreeMap<String, u64>,
    pub writer_error: Option<String>,
}
/// Keep legacy wire fields at the serialization boundary only. There is no
/// independently progressing projection or projection failure in SQLite mode.
impl Serialize for Snapshot {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut wire = serializer.serialize_struct("Snapshot", 7)?;
        wire.serialize_field("durable_lsn", &self.durable_lsn.to_string())?;
        wire.serialize_field("applied_lsn", &self.applied_lsn.to_string())?;
        wire.serialize_field("projected_lsn", &self.durable_lsn.to_string())?;
        wire.serialize_field("master_epoch", &self.master_epoch.to_string())?;
        wire.serialize_field("run_counts", &self.run_counts)?;
        wire.serialize_field("writer_error", &self.writer_error)?;
        wire.serialize_field("projection_error", &Option::<String>::None)?;
        wire.end()
    }
}

impl Snapshot {
    fn counters(&self) -> Result<[i64; 4]> {
        if self.applied_lsn > self.durable_lsn {
            return Err(ClusterError::Invalid(
                "progress exceeds durable journal".into(),
            ));
        }
        let mut result = [0; 4];
        for (slot, value) in result.iter_mut().zip([
            self.durable_lsn,
            self.applied_lsn,
            self.durable_lsn, // Legacy SQL column: projection commits with facts.
            self.master_epoch,
        ]) {
            *slot = i64::try_from(value)
                .map_err(|_| ClusterError::Invalid("counter exceeds PostgreSQL BIGINT".into()))?;
        }
        Ok(result)
    }
    fn health(&self) -> &'static str {
        if self.writer_error.is_some() {
            "degraded"
        } else {
            "online"
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub node_id: String,
    pub journal_id: String,
    pub incarnation_id: String,
    #[serde(with = "flow_journal::decimal")]
    pub epoch: u64,
    #[serde(with = "flow_journal::decimal")]
    pub revision: u64,
    pub desired_state: String,
    pub status: String,
    pub rpc_url: String,
    pub http_url: String,
    pub build: String,
    pub last_seen_at: DateTime<Utc>,
    pub lease_until: DateTime<Utc>,
    pub snapshot: Snapshot,
}

#[derive(Debug, Clone)]
pub struct Lease {
    pub node_id: String,
    pub incarnation_id: String,
    pub epoch: i64,
}

pub struct Registry {
    pool: PgPool,
    cluster_id: String,
    lease_ms: i64,
}
impl Registry {
    pub async fn connect(url: &str, config: &JournalClusterConfig) -> Result<Arc<Self>> {
        config.validate().map_err(ClusterError::Invalid)?;
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_millis(config.heartbeat_ms))
            .after_connect({
                let timeout = config.heartbeat_ms.to_string();
                move |connection, _| {
                    let timeout = timeout.clone();
                    Box::pin(async move {
                        sqlx::query("SELECT set_config('statement_timeout', $1, false)")
                            .bind(timeout)
                            .execute(connection)
                            .await?;
                        Ok(())
                    })
                }
            })
            .connect(url)
            .await?;
        let registry = Arc::new(Self {
            pool,
            cluster_id: config.cluster_id.clone(),
            lease_ms: config.lease_ms as i64,
        });
        let mut tx = registry.pool.begin().await?;
        // Serialize concurrent bootstrap DDL, including on an empty database.
        sqlx::query(
            "SELECT pg_advisory_xact_lock(hashtextextended('flow-journal-cluster-schema-v1', 0))",
        )
        .execute(&mut *tx)
        .await?;
        sqlx::raw_sql(include_str!("../migrations/journal_cluster_v1.sql"))
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(registry)
    }

    pub async fn register(
        &self,
        node_id: &str,
        journal_id: &str,
        rpc_url: &str,
        http_url: &str,
        snapshot: &Snapshot,
    ) -> Result<Lease> {
        validate_endpoint(rpc_url, &["ws", "wss"])?;
        validate_endpoint(http_url, &["http", "https"])?;
        if node_id.is_empty()
            || node_id.len() > 128
            || journal_id.is_empty()
            || journal_id.len() > 128
        {
            return Err(ClusterError::Invalid(
                "invalid node/journal identity".into(),
            ));
        }
        let [durable, applied, projected, master] = snapshot.counters()?;
        let incarnation_id = uuid::Uuid::now_v7().to_string();
        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT INTO flow_journal_cluster_nodes(cluster_id,node_id,journal_id,incarnation_id,epoch,desired_state,observed_state,rpc_url,http_url,build,lease_until,last_seen_at) VALUES($1,$2,$3,$4,0,'enabled','stopped',$5,$6,$7,'-infinity',clock_timestamp()) ON CONFLICT (cluster_id,node_id) DO NOTHING")
            .bind(&self.cluster_id).bind(node_id).bind(journal_id).bind(&incarnation_id)
            .bind(rpc_url).bind(http_url).bind(env!("CARGO_PKG_VERSION")).execute(&mut *tx).await?;
        let current = sqlx::query("SELECT *, lease_until > clock_timestamp() AS live FROM flow_journal_cluster_nodes WHERE cluster_id=$1 AND node_id=$2 FOR UPDATE")
            .bind(&self.cluster_id).bind(node_id).fetch_one(&mut *tx).await?;
        if current.try_get::<String, _>("desired_state")? == "removed" {
            return Err(ClusterError::Conflict(
                "removed node identity cannot register again".into(),
            ));
        }
        if current.try_get::<bool, _>("live")? {
            return Err(ClusterError::Conflict(
                "node already has an active incarnation".into(),
            ));
        }
        if current.try_get::<String, _>("journal_id")? != journal_id {
            return Err(ClusterError::Conflict(
                "node identity belongs to a different journal".into(),
            ));
        }
        for (column, value) in [
            ("durable_lsn", durable),
            ("applied_lsn", applied),
            ("projected_lsn", projected),
            ("master_epoch", master),
        ] {
            if current.try_get::<i64, _>(column)? > value {
                return Err(ClusterError::Conflict(format!(
                    "{column} regressed; restore the original journal before registering"
                )));
            }
        }
        sqlx::query("INSERT INTO flow_journal_cluster_journals(cluster_id,journal_id,node_id) VALUES($1,$2,$3) ON CONFLICT (cluster_id,journal_id) DO NOTHING")
            .bind(&self.cluster_id).bind(journal_id).bind(node_id).execute(&mut *tx).await?;
        let owner: String = sqlx::query_scalar("SELECT node_id FROM flow_journal_cluster_journals WHERE cluster_id=$1 AND journal_id=$2 FOR UPDATE")
            .bind(&self.cluster_id).bind(journal_id).fetch_one(&mut *tx).await?;
        if owner != node_id {
            return Err(ClusterError::Conflict(
                "journal is permanently assigned to another node; logs are not replicas".into(),
            ));
        }
        let epoch: i64 = sqlx::query_scalar("UPDATE flow_journal_cluster_nodes SET incarnation_id=$3,epoch=epoch+1,revision=revision+1,rpc_url=$4,http_url=$5,build=$6,observed_state=$7,lease_until=clock_timestamp()+$8*interval '1 millisecond',last_seen_at=clock_timestamp(),durable_lsn=$9,applied_lsn=$10,projected_lsn=$11,master_epoch=$12,snapshot=$13 WHERE cluster_id=$1 AND node_id=$2 RETURNING epoch")
            .bind(&self.cluster_id).bind(node_id).bind(&incarnation_id).bind(rpc_url).bind(http_url)
            .bind(env!("CARGO_PKG_VERSION")).bind(snapshot.health()).bind(self.lease_ms)
            .bind(durable).bind(applied).bind(projected).bind(master).bind(json!(snapshot))
            .fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok(Lease {
            node_id: node_id.into(),
            incarnation_id,
            epoch,
        })
    }

    /// Reports are accepted only from the live incarnation and cannot rewind any cursor.
    pub async fn publish(&self, lease: &Lease, snapshot: &Snapshot) -> Result<()> {
        let [durable, applied, projected, master] = snapshot.counters()?;
        let affected = sqlx::query("UPDATE flow_journal_cluster_nodes SET revision=revision+1,last_seen_at=clock_timestamp(),lease_until=clock_timestamp()+$5*interval '1 millisecond',observed_state=$6,durable_lsn=$7,applied_lsn=$8,projected_lsn=$9,master_epoch=$10,snapshot=$11 WHERE cluster_id=$1 AND node_id=$2 AND incarnation_id=$3 AND epoch=$4 AND desired_state='enabled' AND lease_until>clock_timestamp() AND durable_lsn<=$7 AND applied_lsn<=$8 AND projected_lsn<=$9 AND master_epoch<=$10")
            .bind(&self.cluster_id).bind(&lease.node_id).bind(&lease.incarnation_id).bind(lease.epoch)
            .bind(self.lease_ms).bind(snapshot.health()).bind(durable).bind(applied).bind(projected)
            .bind(master).bind(json!(snapshot)).execute(&self.pool).await?.rows_affected();
        if affected != 1 {
            return Err(ClusterError::LeaseLost);
        }
        Ok(())
    }

    pub async fn release(&self, lease: &Lease) -> Result<()> {
        sqlx::query("UPDATE flow_journal_cluster_nodes SET revision=revision+1,observed_state='stopped',lease_until=clock_timestamp() WHERE cluster_id=$1 AND node_id=$2 AND incarnation_id=$3 AND epoch=$4 AND desired_state='enabled'")
            .bind(&self.cluster_id).bind(&lease.node_id).bind(&lease.incarnation_id).bind(lease.epoch)
            .execute(&self.pool).await?;
        Ok(())
    }

    pub async fn nodes(&self, after: &str, limit: usize) -> Result<Value> {
        let mut tx = self.pool.begin().await?;
        let server_time: DateTime<Utc> = sqlx::query_scalar("SELECT transaction_timestamp()")
            .fetch_one(&mut *tx)
            .await?;
        let rows = sqlx::query("SELECT *, CASE WHEN desired_state='removed' THEN 'removed' WHEN observed_state='stopped' THEN 'stopped' WHEN lease_until <= transaction_timestamp() THEN 'offline' ELSE observed_state END AS status FROM flow_journal_cluster_nodes WHERE cluster_id=$1 AND node_id>$2 ORDER BY node_id LIMIT $3")
            .bind(&self.cluster_id).bind(after).bind(limit.clamp(1,256) as i64).fetch_all(&mut *tx).await?;
        let nodes = rows
            .into_iter()
            .map(|r| -> Result<Node> {
                Ok(Node {
                    node_id: r.try_get("node_id")?,
                    journal_id: r.try_get("journal_id")?,
                    incarnation_id: r.try_get("incarnation_id")?,
                    epoch: r.try_get::<i64, _>("epoch")? as u64,
                    revision: r.try_get::<i64, _>("revision")? as u64,
                    desired_state: r.try_get("desired_state")?,
                    status: r.try_get("status")?,
                    rpc_url: r.try_get("rpc_url")?,
                    http_url: r.try_get("http_url")?,
                    build: r.try_get("build")?,
                    last_seen_at: r.try_get("last_seen_at")?,
                    lease_until: r.try_get("lease_until")?,
                    snapshot: serde_json::from_value(r.try_get("snapshot")?)
                        .map_err(|_| ClusterError::Invalid("invalid stored snapshot".into()))?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        tx.commit().await?;
        let next_after =
            (nodes.len() == limit.clamp(1, 256)).then(|| nodes.last().unwrap().node_id.clone());
        Ok(
            json!({"cluster_id":self.cluster_id,"server_time":server_time,"nodes":nodes,"next_after":next_after}),
        )
    }

    /// Removal is durable and idempotent across API nodes, and fences delayed reports.
    pub async fn remove(
        &self,
        node_id: &str,
        expected_epoch: u64,
        request_id: &str,
    ) -> Result<Value> {
        if request_id.is_empty() || request_id.len() > 128 {
            return Err(ClusterError::Invalid(
                "request_id must contain 1..128 bytes".into(),
            ));
        }
        let epoch = i64::try_from(expected_epoch)
            .map_err(|_| ClusterError::Invalid("epoch overflow".into()))?;
        let request = json!({"method":"journal.cluster.remove","node_id":node_id,"expected_epoch":expected_epoch.to_string()});
        let mut tx = self.pool.begin().await?;
        // The keyed lock serializes duplicate commands, including before their receipt exists.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(serde_json::to_string(&(&self.cluster_id, request_id)).unwrap())
            .execute(&mut *tx)
            .await?;
        if let Some(row) = sqlx::query("SELECT request,result FROM flow_journal_cluster_commands WHERE cluster_id=$1 AND request_id=$2")
            .bind(&self.cluster_id).bind(request_id).fetch_optional(&mut *tx).await?
        {
            if row.try_get::<Value,_>("request")? != request {
                return Err(ClusterError::Conflict("same request_id with different arguments".into()));
            }
            return Ok(row.try_get("result")?);
        }
        let affected = sqlx::query("UPDATE flow_journal_cluster_nodes SET desired_state='removed',lease_until=clock_timestamp(),revision=revision+1 WHERE cluster_id=$1 AND node_id=$2 AND epoch=$3 RETURNING revision")
            .bind(&self.cluster_id).bind(node_id).bind(epoch).fetch_optional(&mut *tx).await?;
        let Some(row) = affected else {
            return Err(ClusterError::Conflict(
                "node missing or incarnation changed; refresh before removing".into(),
            ));
        };
        let result = json!({"removed":true,"node_id":node_id,"revision":row.try_get::<i64,_>("revision")?.to_string()});
        sqlx::query("INSERT INTO flow_journal_cluster_commands(cluster_id,request_id,request,result) VALUES($1,$2,$3,$4)")
            .bind(&self.cluster_id).bind(request_id).bind(request).bind(&result).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(result)
    }
}

fn validate_endpoint(value: &str, schemes: &[&str]) -> Result<()> {
    let url = reqwest::Url::parse(value)
        .map_err(|_| ClusterError::Invalid("invalid advertised endpoint".into()))?;
    if value.len() > 2048
        || !schemes.contains(&url.scheme())
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.port() == Some(0)
        || url.host_str().is_some_and(|host| {
            host.trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_unspecified())
        })
    {
        return Err(ClusterError::Invalid(
            "advertised endpoint must have a reachable host and no credentials, query or fragment"
                .into(),
        ));
    }
    Ok(())
}

pub struct Membership {
    pub registry: Arc<Registry>,
    pub lease: Lease,
    pub cluster_id: String,
    stop: CancellationToken,
    lost: CancellationToken,
    admission: Arc<flow_journal::writer::WriteAdmission>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    timeout: Duration,
}
impl Membership {
    pub async fn join(
        backend: &Arc<JournalBackend>,
        config: &JournalClusterConfig,
        rpc_url: &str,
        http_url: &str,
    ) -> Result<Arc<Self>> {
        let url = std::env::var(&config.database_url_env).map_err(|_| {
            ClusterError::Invalid(format!("{} is required", config.database_url_env))
        })?;
        let timeout = Duration::from_millis(config.heartbeat_ms);
        let registry = tokio::time::timeout(timeout, Registry::connect(&url, config))
            .await
            .map_err(|_| ClusterError::Invalid("registry bootstrap timed out".into()))??;
        let snapshot = backend.cluster_snapshot().await?;
        let started = Instant::now();
        let lease = tokio::time::timeout(
            timeout,
            registry.register(
                &config.node_id,
                backend.journal.id(),
                config.rpc_url.as_deref().unwrap_or(rpc_url),
                config.http_url.as_deref().unwrap_or(http_url),
                &snapshot,
            ),
        )
        .await
        .map_err(|_| ClusterError::Invalid("registration timed out".into()))??;
        let admission = Arc::new(flow_journal::writer::WriteAdmission::new(
            started + Duration::from_millis(config.lease_ms),
        ));
        backend
            .journal
            .set_write_admission(admission.clone())
            .map_err(|_| {
                ClusterError::Conflict("journal already has a write admission gate".into())
            })?;
        let membership = Arc::new(Self {
            registry,
            lease,
            cluster_id: config.cluster_id.clone(),
            stop: CancellationToken::new(),
            lost: CancellationToken::new(),
            admission,
            task: Mutex::new(None),
            timeout,
        });
        let weak = Arc::downgrade(backend);
        let worker = membership.clone();
        let heartbeat = Duration::from_millis(config.heartbeat_ms);
        let lease_duration = Duration::from_millis(config.lease_ms);
        *membership.task.lock().await = Some(tokio::spawn(async move {
            let mut tick =
                tokio::time::interval_at(tokio::time::Instant::now() + heartbeat, heartbeat);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! { _=worker.stop.cancelled()=>break, _=tick.tick()=>{} }
                let started = Instant::now();
                let report = tokio::time::timeout(worker.timeout, worker.report(&weak)).await;
                if !matches!(report, Ok(Ok(())))
                    || !worker.admission.renew(started + lease_duration)
                {
                    worker.admission.revoke();
                    worker.lost.cancel();
                    if let Some(backend) = weak.upgrade() {
                        backend.cancellation().cancel();
                    }
                    tracing::error!(node_id=%worker.lease.node_id,"journal registry synchronization failed; stopping node");
                    break;
                }
            }
        }));
        Ok(membership)
    }
    async fn report(&self, weak: &Weak<JournalBackend>) -> Result<()> {
        let backend = weak.upgrade().ok_or(ClusterError::LeaseLost)?;
        self.registry
            .publish(&self.lease, &backend.cluster_snapshot().await?)
            .await
    }
    pub fn lost(&self) -> CancellationToken {
        self.lost.clone()
    }
    pub async fn close(&self, snapshot: Option<&Snapshot>) {
        self.stop.cancel();
        if let Some(task) = self.task.lock().await.take() {
            let _ = task.await;
        }
        if !self.lost.is_cancelled() {
            if let Some(snapshot) = snapshot {
                let _ = tokio::time::timeout(
                    self.timeout,
                    self.registry.publish(&self.lease, snapshot),
                )
                .await;
            }
            let _ = tokio::time::timeout(self.timeout, self.registry.release(&self.lease)).await;
        }
        self.admission.revoke();
    }
}

impl JournalBackend {
    pub async fn cluster_snapshot(&self) -> Result<Snapshot> {
        let (applied_lsn, master_epoch, run_counts) = self
            .inspect(|s| {
                // RunStatus 是枚举（无 Ord）：按 wire 字符串聚合（与
                // Snapshot.run_counts 的 String 键一致，也和 journal 里的
                // snake_case 逐字兼容）。
                let mut counts = BTreeMap::new();
                for run in s.runs.values() {
                    *counts.entry(run.status.as_str().to_owned()).or_insert(0) += 1;
                }
                (s.applied_lsn, s.master_epoch, counts)
            })
            .await;
        let stats = self.journal.stats();
        Ok(Snapshot {
            durable_lsn: stats.durable_lsn,
            applied_lsn,
            master_epoch,
            run_counts,
            writer_error: stats.failure,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::validate_endpoint;
    #[test]
    fn obsolete_projection_fields_are_wire_aliases_only() {
        let snapshot: super::Snapshot = serde_json::from_value(serde_json::json!({
            "durable_lsn":"42", "applied_lsn":"40", "master_epoch":"1",
            "run_counts":{}, "writer_error":null,
            "projected_lsn":"17", "projection_error":"obsolete"
        }))
        .unwrap();
        assert_eq!(snapshot.health(), "online");
        assert_eq!(snapshot.counters().unwrap(), [42, 40, 42, 1]);
        let wire = serde_json::to_value(snapshot).unwrap();
        assert_eq!(wire["projected_lsn"], "42");
        assert!(wire["projection_error"].is_null());
    }

    #[test]
    fn advertised_endpoints_reject_wildcard_hosts_and_credentials() {
        for endpoint in [
            "ws://0.0.0.0:9802",
            "ws://[::]:9802",
            "ws://user:password@host:9802",
            "ws://host:0",
            "ws://host?token=secret",
            "http://host:9802",
        ] {
            assert!(
                validate_endpoint(endpoint, &["ws", "wss"]).is_err(),
                "{endpoint}"
            );
        }
        assert!(validate_endpoint("wss://journal-a.example.com/rpc", &["ws", "wss"]).is_ok());
    }
}
