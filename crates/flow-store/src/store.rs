//! 面向产品视图与执行视图的只读查询实现。
//!
//! 与 `flow-journal` 的写入者共用同一个 `flow.sqlite`，但连接以
//! `read_only(true)` 打开，因此**结构上不可能**在这里改到权威数据。
//! 这条约束是刻意的：写者负责「事实 + 当前状态 + 回执」的同事务提交，
//! 读侧一旦也能写，两条路径就会在同一个库上产生无法判定归属的变更。
//!
//! 本模块的两条主线：
//!
//! 1. **一致性读取**：状态与 `committed_lsn` 游标必须在同一个读事务里取到，
//!    否则上层会把两个不同时刻的事实拼成一页。
//! 2. **字节预算**：单次响应的字节数有硬上限，且尽量在 SQLite 内部先算
//!    完再决定要不要搬运，避免「先读进内存再发现太大」。
use serde_json::Value;
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool};
use std::path::Path;

/// 只读查询可能失败的两种原因。
///
/// 刻意不引入更多变体：这个 crate 是读侧投影，任何「业务上不可能」的失败
/// 都被归到 `Database`（协议错误），调用方只需要区分「太大」与「读不动」。
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// 响应体超过预算上限。
    ///
    /// 这不是故障而是契约：调用方应当改用更细的分页/摘要视图，
    /// 而不是等一个更小的结果。
    #[error("RESPONSE_TOO_LARGE")]
    ResponseTooLarge,
    /// SQLite 层的错误，包括被包装成 `Protocol` 的完整性校验失败。
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// 只读查询句柄，包装一个只读 SQLite 连接池。
pub struct Store {
    /// 只读连接池。`max_connections=4` 是为了在读并发与 WAL 读开销之间取平衡。
    pool: SqlitePool,
}

// Startup must scale with live runs even before SQLite has collected statistics.
// 用部分索引 `entity_active_runs` 直取「进行中」的 run：启动时 SQLite 还没有
// 统计信息，全表扫会把全部历史 run 拉进来，成本与已完成的运行量成正比。
// `INDEXED BY` 是显式钉住这条路径——万一索引被误删，这里会直接报错而不是
// 悄悄退化成全表扫。
const ACTIVE_RUNS_SQL:&str="SELECT key FROM current_entities INDEXED BY entity_active_runs WHERE kind='run' AND status IN ('running','awaiting_resume') AND key>? ORDER BY key LIMIT 256";

pub use flow_journal::sqlite::EntityChange;

/// 列表类接口的单次响应字节上限（4 MiB）。
///
/// 与 `READ_BYTES` 分开的原因：列表是「先筛 key 再拼装」的场景，
/// 预算必须覆盖累积后的全部 body；`READ_BYTES` 只约束单条实体读取。
const LIST_BYTES: i64 = 4 * 1024 * 1024;
/// 单条实体（含其附属的节点/尝试/版本）的读取预算，与 journal 的值预算同源。
const READ_BYTES: i64 = flow_journal::VALUE_BUDGET as i64;

/// 运行列表的过滤条件。
///
/// 用 `Option` 表达「不过滤」而不是拼 SQL 片段，把 SQL 构造收敛在
/// `runs_page` 内部，避免调用方各自拼 WHERE 造成口径漂移。
#[derive(Default)]
pub struct RunsQuery<'a> {
    /// 只看某个工作流下的运行。
    pub workflow_id: Option<&'a str>,
    /// 只看某个状态的运行。
    pub status: Option<&'a str>,
    /// 只看某种触发来源（手动/定时/webhook/子工作流…）。
    pub source: Option<&'a str>,
    /// 游标：只返回创建序在这条之前的运行。
    pub before_run_id: Option<&'a str>,
    /// 返回条数上限。
    pub limit: usize,
}

/// 构造产品集合（工作流/模板）的有序查询。
///
/// `by_name` 为真时按名称排序，并强制走部分索引 `entity_name_order`；
/// 否则按创建序倒序。两条路径都必须命中索引——测试 `named_collections_use_the_partial_index_without_temporary_sorting`
/// 会用 `EXPLAIN QUERY PLAN` 断言没有 `TEMP B-TREE`。
fn ordered_query(kind: &str, workflow_id: Option<&str>, by_name: bool) -> QueryBuilder<Sqlite> {
    let name_index = by_name && matches!(kind, "workflow" | "template");
    let mut query = QueryBuilder::<Sqlite>::new(if name_index {
        "SELECT key,length(CAST(body AS BLOB)) AS bytes FROM current_entities INDEXED BY entity_name_order WHERE kind="
    } else {
        "SELECT key,length(CAST(body AS BLOB)) AS bytes FROM current_entities WHERE kind="
    });
    query.push_bind(kind);
    if name_index {
        // SQLite cannot infer this partial-index predicate from kind=? alone.
        query.push(" AND kind IN ('workflow','template')");
    }
    if let Some(id) = workflow_id {
        query.push(" AND workflow_id=").push_bind(id);
    }
    query.push(if by_name {
        " ORDER BY name,key"
    } else {
        " ORDER BY created_order DESC,key DESC"
    });
    query.push(" LIMIT 10001");
    query
}

/// 为运行节点追加「尝试」的过滤条件。
///
/// `all=false` 时只保留**当前拥有该节点的那次尝试**，而不是它的全部历史尝试。
/// 原因是产品视图要回答「这个节点现在怎么样了」，历史尝试属于审计面，
/// 混进来会让节点状态自相矛盾。
fn attempt_filter(q: &mut QueryBuilder<Sqlite>, run: &str, all: bool) {
    q.push("parent=").push_bind(run);
    if !all {
        // Only the attempt that currently owns the node, not its historical attempts.
        q.push(" AND extra IN (SELECT dispatch_id FROM current_entities WHERE kind='run_node' AND parent=")
            .push_bind(run)
            .push(")");
    }
}

/// 解码实体 body，并校验其摘要。
///
/// 摘要来自写入者承诺的 `digest`；不匹配说明库内容与写入时的语义已经脱节
/// （截断、外部改写、迁移缺陷），此时必须报错而不是照抄内容——
/// 读侧一旦把损坏内容当事实返回，后面所有裁决都建立在错误输入上。
fn decode_body(kind: &str, key: &str, body: &str, digest: &str) -> Result<Value, Error> {
    if flow_journal::sqlite::entity_digest(kind, key, body) != digest {
        return Err(sqlx::Error::Protocol("current entity checksum mismatch".into()).into());
    }
    serde_json::from_str(body).map_err(|e| Error::Database(sqlx::Error::Decode(Box::new(e))))
}

impl Store {
    /// Opens only an existing authoritative database. This query handle cannot write it.
    ///
    /// 只打开**已存在**的库：查询侧没有创建语义，一个新建的空库对读者
    /// 毫无意义，只会让「库不存在」和「库是空的」两种故障混为一谈。
    ///
    /// 打开时做两项校验：`user_version` 必须等于当前 `FORMAT`（版本不符说明
    /// 这是另一种存储格式，读出来的列语义都不同）；`journal_id` 必须匹配
    /// （不匹配说明连到了别的实例的库——比读不到库更危险的是读到别人的库）。
    pub async fn open(path: &Path, journal_id: &str) -> Result<Self, Error> {
        use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(SqliteConnectOptions::new().filename(path).read_only(true))
            .await?;
        let version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&pool)
            .await?;
        if version != flow_journal::sqlite::FORMAT {
            return Err(sqlx::Error::Protocol("unsupported SQLite storage format".into()).into());
        }
        let identity: String =
            sqlx::query_scalar("SELECT journal_id FROM store_meta WHERE singleton=1")
                .fetch_one(&pool)
                .await?;
        if identity != journal_id {
            return Err(sqlx::Error::Protocol("foreign SQLite authority".into()).into());
        }
        Ok(Self { pool })
    }

    /// 读取写者已提交的最大 LSN，作为读侧游标的基线。
    ///
    /// 它与状态读必须落在同一事务里，否则游标可能领先于实际读到的状态。
    pub async fn applied_lsn(&self) -> Result<u64, Error> {
        let raw: String =
            sqlx::query_scalar("SELECT committed_lsn FROM store_meta WHERE singleton=1")
                .fetch_one(&self.pool)
                .await?;
        raw.parse()
            .map_err(|_| Error::Database(sqlx::Error::Protocol("invalid commit cursor".into())))
    }
    /// 是否处于「领域模式」（事件驱动归约）而非仅做事实记账。
    ///
    /// 读侧据此判断投影是否已经就绪。
    pub async fn domain_mode(&self) -> Result<bool, Error> {
        sqlx::query_scalar("SELECT domain_mode FROM store_meta WHERE singleton=1")
            .fetch_one(&self.pool)
            .await
            .map_err(Error::from)
    }
    /// State and its cursor share one SQLite read transaction.
    ///
    /// 返回状态与其 LSN，**同事务**读取——这是本 crate 的核心不变量。
    pub async fn get(&self, kind: &str, key: &str) -> Result<(u64, Option<Value>), Error> {
        self.get_entity(kind, key, true, true).await
    }
    /// Read run metadata without assembling its node ledger.
    ///
    /// 只要运行元信息，不要拼装节点台账：台账拼装是本模块最贵的部分，
    /// 列表页与轮询场景通常只需要元信息。
    pub async fn get_summary(&self, kind: &str, key: &str) -> Result<(u64, Option<Value>), Error> {
        self.get_entity(kind, key, false, false).await
    }
    /// 读取一次运行的状态与其 LSN（含节点台账，但只含当前拥有节点的尝试）。
    ///
    /// 与 `get` 的区别在于不返回历史尝试：产品详情页要回答「现在怎么样了」，
    /// 历史尝试属于审计面。
    pub async fn get_execution(&self, key: &str) -> Result<(u64, Option<Value>), Error> {
        self.get_entity("run", key, true, false).await
    }
    /// 读取单个实体，必要时拼装其附属集合。
    ///
    /// `with_nodes` 控制是否展开附属集合（工作流的版本、运行的节点与尝试），
    /// `all_attempts` 控制运行视图是否包含历史尝试。
    ///
    /// 附属集合的展开**先查字节总量再取内容**：先取回再判断大小的话，
    /// 一个超大运行已经把内存吃掉了，此时再报「太大」毫无意义。
    async fn get_entity(
        &self,
        kind: &str,
        key: &str,
        with_nodes: bool,
        all_attempts: bool,
    ) -> Result<(u64, Option<Value>), Error> {
        let mut tx = self.pool.begin().await?;
        // 游标与状态必须在同一个读事务里：先取游标再读状态（两次独立查询）
        // 会跨越一次提交，调用方就会拿着「新状态 + 旧游标」继续推进。
        let lsn: String =
            sqlx::query_scalar("SELECT committed_lsn FROM store_meta WHERE singleton=1")
                .fetch_one(&mut *tx)
                .await?;
        // 在 SQL 里就按大小裁剪：把超限的 body 替换成哨兵串，
        // 这样超限内容根本不会被传回进程，内存峰值天然有界。
        let body_row =
            sqlx::query("SELECT CASE WHEN length(CAST(body AS BLOB))<=? THEN body ELSE 'RESPONSE_TOO_LARGE' END,digest FROM current_entities WHERE kind=? AND key=?")
                .bind(READ_BYTES)
                .bind(kind)
                .bind(key)
                .fetch_optional(&mut *tx)
                .await?;
        let body: Option<String> = body_row.as_ref().map(|r| r.get(0));
        // 哨兵串与真实 body 共用一列，所以必须区分「超限」和「正文恰好等于哨兵」。
        if body.as_deref() == Some("RESPONSE_TOO_LARGE") {
            return Err(Error::ResponseTooLarge);
        }
        let mut value: Option<Value> = body
            .as_ref()
            .map(|s| decode_body(kind, key, s, body_row.as_ref().unwrap().get("digest")))
            .transpose()?;
        if kind == "workflow" && with_nodes {
            if let Some(workflow) = value.as_mut() {
                // 版本集合先量体积：工作流可以积累大量历史版本，
                // 不预检就整体取回，内存峰值不可控。
                let size:i64=sqlx::query_scalar("SELECT COALESCE(SUM(length(CAST(body AS BLOB))),0) FROM current_entities WHERE kind='workflow_version' AND parent=?").bind(key).fetch_one(&mut *tx).await?;
                if size > READ_BYTES {
                    return Err(Error::ResponseTooLarge);
                }
                let rows=sqlx::query("SELECT key,body,digest FROM current_entities WHERE kind='workflow_version' AND parent=? ORDER BY key").bind(key).fetch_all(&mut *tx).await?;
                for row in rows {
                    let version = decode_body(
                        "workflow_version",
                        row.get("key"),
                        row.get("body"),
                        row.get("digest"),
                    )?;
                    // 以版本号（而非实体 key）作为下标：调用方按版本号寻址定义。
                    let key = version["version"]
                        .as_u64()
                        .ok_or_else(|| sqlx::Error::Protocol("invalid definition version".into()))?
                        .to_string();
                    workflow["versions"][key] = version;
                }
            }
        }
        if kind == "run" && with_nodes {
            if let Some(run) = value.as_mut() {
                // 节点台账与尝试台账合并计量，且尝试部分复用同一套过滤条件，
                // 保证「量的」和「取的」是同一批记录。
                let node_size: i64 = sqlx::query_scalar("SELECT COALESCE(SUM(length(CAST(body AS BLOB))+length(CAST(key AS BLOB))+4),0) FROM current_entities WHERE kind='run_node' AND parent=?")
                    .bind(key).fetch_one(&mut *tx).await?;
                let mut size_query = QueryBuilder::<Sqlite>::new(
                    "SELECT COALESCE(SUM(length(CAST(body AS BLOB))+length(CAST(key AS BLOB))+4),0) FROM current_entities WHERE kind='run_attempt' AND ",
                );
                attempt_filter(&mut size_query, key, all_attempts);
                let size = node_size
                    + size_query
                        .build_query_scalar::<i64>()
                        .fetch_one(&mut *tx)
                        .await?;
                // 预算里要扣掉运行自身 body 已经占用的部分。
                if size > (READ_BYTES as usize - body.as_ref().map_or(0, String::len)) as i64 {
                    return Err(Error::ResponseTooLarge);
                }
                let nodes = sqlx::query("SELECT slot AS node_id,key,body,digest FROM current_entities WHERE kind='run_node' AND parent=?")
                    .bind(key).fetch_all(&mut *tx).await?;
                let mut map = serde_json::Map::new();
                for node in nodes {
                    let body: String = node.get("body");
                    // 用 slot（节点槽位）而非实体 key 作为对外的节点 id。
                    map.insert(
                        node.get("node_id"),
                        decode_body("run_node", node.get("key"), &body, node.get("digest"))?,
                    );
                }
                let mut attempt_query = QueryBuilder::<Sqlite>::new(
                    "SELECT slot AS node_id,extra AS dispatch_id,key,body,digest FROM current_entities WHERE kind='run_attempt' AND ",
                );
                attempt_filter(&mut attempt_query, key, all_attempts);
                let attempts = attempt_query.build().fetch_all(&mut *tx).await?;
                for attempt in attempts {
                    let node_id: String = attempt.get("node_id");
                    let dispatch_id: String = attempt.get("dispatch_id");
                    let body: String = attempt.get("body");
                    // 尝试挂到它所属的节点下；找不到宿主节点的尝试直接跳过，
                    // 因为没有宿主时无处安放，丢弃比造出孤儿结构更安全。
                    if let Some(node) = map.get_mut(&node_id) {
                        node["attempts"][dispatch_id] = decode_body(
                            "run_attempt",
                            attempt.get("key"),
                            &body,
                            attempt.get("digest"),
                        )?;
                    }
                }
                run["nodes"] = Value::Object(map);
            }
        }
        Ok((
            lsn.parse()
                .map_err(|_| sqlx::Error::Protocol("invalid cursor".into()))?,
            value,
        ))
    }
    /// 只保留当前状态：较旧的修订无法靠「读一个更新的」来重建，
    /// 调用方必须把自己的决策与写者串行化。
    ///
    /// `upper` 是决策边界；如果当前状态的 `last_lsn` 已经越过它，
    /// 说明这条数据比调用方做决策时更新，此时必须报错而不是返回——
    /// 返回一份更新的状态会让调用方以为自己的决策仍然成立。
    pub async fn current_at(
        &self,
        kind: &str,
        key: &str,
        upper: u64,
    ) -> Result<Option<Value>, Error> {
        let row =
            sqlx::query("SELECT body,digest,last_lsn FROM current_entities WHERE kind=? AND key=?")
                .bind(kind)
                .bind(key)
                .fetch_optional(&self.pool)
                .await?;
        row.map(|row| {
            let revision: String = row.get("last_lsn");
            if revision > format!("{upper:020}") {
                return Err(sqlx::Error::Protocol(
                    "current state is newer than decision boundary".into(),
                )
                .into());
            }
            decode_body(kind, key, row.get("body"), row.get("digest"))
        })
        .transpose()
    }
    /// 列出满足条件的实体 key（keyset 分页），不读取 body。
    ///
    /// `upper` 是决策边界：只返回 `last_lsn <= upper` 的实体，保证读到的集合
    /// 与调用方持有的游标一致；否则一次分页会跨越两次提交，页与页之间
    /// 出现「幽灵记录」。
    pub async fn current_keys(
        &self,
        kind: &str,
        parent: Option<&str>,
        after: &str,
        upper: u64,
        limit: usize,
    ) -> Result<Vec<String>, Error> {
        let mut q = QueryBuilder::<Sqlite>::new("SELECT key FROM current_entities WHERE kind=");
        q.push_bind(kind)
            .push(" AND key>")
            .push_bind(after)
            .push(" AND last_lsn<=")
            .push_bind(format!("{upper:020}"));
        if let Some(parent) = parent {
            q.push(" AND parent=").push_bind(parent);
        }
        q.push(" ORDER BY key LIMIT ")
            .push_bind(limit.min(256) as i64);
        q.build_query_scalar()
            .fetch_all(&self.pool)
            .await
            .map_err(Error::from)
    }
    /// 列出某个父实体下的子实体及其最小 `extra`（子实体按 slot 归并）。
    ///
    /// 只返回 `last_lsn <= upper` 的子实体，与 `current_keys` 保持同样的
    /// 决策边界语义。
    pub async fn children(&self, parent: &str, upper: u64) -> Result<Vec<(String, String)>, Error> {
        sqlx::query_as("SELECT slot,MIN(extra) FROM current_entities WHERE kind='child' AND parent=? AND last_lsn<=? GROUP BY slot")
            .bind(parent).bind(format!("{upper:020}")).fetch_all(&self.pool).await.map_err(Error::from)
    }

    /// 读取一次派发的当前归属信息。
    ///
    /// 这是执行面最常用的单点查询：判断某个 dispatch 现在归谁所有。
    pub async fn dispatch_owner(&self, dispatch: &str) -> Result<Option<Value>, Error> {
        Ok(self.get("dispatch", dispatch).await?.1)
    }

    /// 列出进行中（running / awaiting_resume）的运行 id。
    ///
    /// 用于恢复与预热：只关心还没结束的运行，已完成的运行不需要被重新扫一遍。
    pub async fn active_run_ids(&self, after: &str) -> Result<Vec<String>, Error> {
        sqlx::query_scalar(ACTIVE_RUNS_SQL)
            .bind(after)
            .fetch_all(&self.pool)
            .await
            .map_err(Error::from)
    }

    /// 按 key 顺序分页列出实体，返回游标与本页内容。
    pub async fn list(
        &self,
        kind: &str,
        after: &str,
        limit: usize,
    ) -> Result<(u64, Vec<Value>), Error> {
        let mut tx = self.pool.begin().await?;
        let lsn: String =
            sqlx::query_scalar("SELECT committed_lsn FROM store_meta WHERE singleton=1")
                .fetch_one(&mut *tx)
                .await?;
        // Bound bytes in SQLite before transferring bodies. Fetching all candidates
        // first would defeat the response budget for a page of large values.
        // `MATERIALIZED` 强制 CTE 物化：否则 SQLite 会把候选集内联进外层查询，
        // 窗口函数的累计字节就无从在传输前算清。
        let rows = sqlx::query(
            "WITH candidates AS MATERIALIZED (
                SELECT key,length(CAST(body AS BLOB)) AS bytes FROM current_entities
                WHERE kind=? AND key>? ORDER BY key LIMIT ?
             ), bounded AS (
                SELECT key,bytes,SUM(bytes) OVER (ORDER BY key) AS total FROM candidates
             )
             SELECT e.key,CASE WHEN b.total<=? THEN e.body END AS body,e.digest
             FROM bounded b JOIN current_entities e ON e.kind=? AND e.key=b.key
             WHERE b.total<=? OR b.total=b.bytes ORDER BY e.key",
        )
        .bind(kind)
        .bind(after)
        .bind(limit.min(256) as i64)
        .bind(LIST_BYTES)
        .bind(kind)
        .bind(LIST_BYTES)
        .fetch_all(&mut *tx)
        .await?;
        let mut values = Vec::with_capacity(rows.len());
        for row in rows {
            let body: Option<&str> = row.get("body");
            let body = body.ok_or(Error::ResponseTooLarge)?;
            values.push(decode_body(kind, row.get("key"), body, row.get("digest"))?);
        }
        Ok((
            lsn.parse()
                .map_err(|_| sqlx::Error::Protocol("invalid cursor".into()))?,
            values,
        ))
    }
    /// 关闭底层连接池。
    ///
    /// 显式关闭而不是靠 Drop：只读句柄通常在进程收尾阶段释放，
    /// 但提前关闭能让 SQLite 的 WAL 收尾更干净。
    pub async fn close(&self) {
        self.pool.close().await;
    }

    /// Filter, keyset ordering and LIMIT execute in the same SQLite snapshot as the cursor.
    ///
    /// 运行列表的游标是 `(created_order, key)` 复合游标：只用 key 排序会在
    /// 同一毫秒内创建的运行之间产生不稳定分页，用 `created_order` 兜底也
    /// 需要 key 做最终的确定性 tie-break。
    pub async fn runs_page(&self, filter: RunsQuery<'_>) -> Result<(u64, Vec<Value>), Error> {
        let mut tx = self.pool.begin().await?;
        let cursor: Option<String> = match filter.before_run_id {
            Some(id) => {
                // 先把游标 id 解析成创建序；找不到就当作没有游标（从头开始），
                // 而不是报错——游标 id 可能已被清理策略删除。
                sqlx::query_scalar(
                    "SELECT created_order FROM current_entities WHERE kind='run' AND key=?",
                )
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?
            }
            None => None,
        };
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT key,length(CAST(body AS BLOB)) AS bytes FROM current_entities WHERE kind='run'",
        );
        for (field, value) in [
            ("workflow_id", filter.workflow_id),
            ("status", filter.status),
            ("source", filter.source),
        ] {
            if let Some(value) = value {
                query.push(format!(" AND {field}=")).push_bind(value);
            }
        }
        if let Some(created) = &cursor {
            query
                .push(" AND (created_order,key)<(")
                .push_bind(created)
                .push(",")
                .push_bind(filter.before_run_id.unwrap())
                .push(")");
        }
        query
            .push(" ORDER BY created_order DESC,key DESC LIMIT ")
            .push_bind(filter.limit.min(500) as i64);
        let rows = query.build().fetch_all(&mut *tx).await?;
        Self::bodies(&mut tx, "run", rows).await
    }

    /// Product collections retain their existing order without cloning reducer history.
    ///
    /// 产品集合直接读 `current_entities` 的现成顺序，而不是在读侧重放归约历史：
    /// 归约历史是写者的职责，读侧重放会引入第二份语义。
    pub async fn ordered(
        &self,
        kind: &str,
        workflow_id: Option<&str>,
        by_name: bool,
    ) -> Result<Vec<Value>, Error> {
        let mut tx = self.pool.begin().await?;
        let mut query = ordered_query(kind, workflow_id, by_name);
        let rows = query.build().fetch_all(&mut *tx).await?;
        if rows.len() > 10000 {
            return Err(Error::ResponseTooLarge);
        }
        Ok(Self::bodies(&mut tx, kind, rows).await?.1)
    }

    /// 按已选中的行装配 body，并顺带取出同事务的游标。
    ///
    /// 先按 `bytes` 汇总判断总量，超预算直接拒绝，避免逐行拉取大 body；
    /// 逐行再查一次 body 是为了复用同一事务的快照，并顺带校验摘要。
    async fn bodies(
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        kind: &str,
        rows: Vec<sqlx::sqlite::SqliteRow>,
    ) -> Result<(u64, Vec<Value>), Error> {
        let bytes: i64 = rows.iter().map(|r| r.get::<i64, _>("bytes")).sum();
        if bytes > READ_BYTES {
            return Err(Error::ResponseTooLarge);
        }
        let mut values = Vec::with_capacity(rows.len());
        for row in rows {
            let body_row =
                sqlx::query("SELECT body,digest FROM current_entities WHERE kind=? AND key=?")
                    .bind(kind)
                    .bind(row.get::<&str, _>("key"))
                    .fetch_one(&mut **tx)
                    .await?;
            values.push(decode_body(
                kind,
                row.get("key"),
                body_row.get("body"),
                body_row.get("digest"),
            )?);
        }
        let lsn: String =
            sqlx::query_scalar("SELECT committed_lsn FROM store_meta WHERE singleton=1")
                .fetch_one(&mut **tx)
                .await?;
        Ok((
            lsn.parse()
                .map_err(|_| sqlx::Error::Protocol("invalid cursor".into()))?,
            values,
        ))
    }

    /// 按工作流与状态统计运行数量。
    ///
    /// 直接在库上 `GROUP BY`，而不是取回全部运行在内存里计数：
    /// 运行数会随时间无界增长，读全量会让统计接口随使用时长线性变慢。
    pub async fn run_counts(
        &self,
        workflow_id: Option<&str>,
    ) -> Result<Vec<(String, String, i64)>, Error> {
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT workflow_id AS workflow,status AS status,COUNT(*) AS count FROM current_entities WHERE kind='run'",
        );
        if let Some(id) = workflow_id {
            query.push(" AND workflow_id=").push_bind(id);
        }
        query.push(" GROUP BY workflow_id,status");
        Ok(query
            .build()
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|r| (r.get("workflow"), r.get("status"), r.get("count")))
            .collect())
    }
}

#[cfg(test)]
/// 查询计划的回归测试。
///
/// 这些用例不验证返回值，只验证**访问路径**：启动期查询必须命中部分索引而
/// 不是全表扫。随着历史数据增长，一次退化成全表扫足以让启动时间从毫秒级
/// 变成分钟级，而返回值本身看不出任何异常。
mod tests {
    use super::*;
    #[tokio::test]
    async fn startup_seeks_the_active_index_without_scanning_completed_history() {
        let dir = flow_test_support::io::TempDir::new("active-query-plan");
        let writer = flow_journal::Journal::open(dir.path(), Default::default())
            .await
            .unwrap();
        let conn = flow_journal::sqlite::read_connection(dir.path()).unwrap();
        let plan: String = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {ACTIVE_RUNS_SQL}"))
            .unwrap()
            .query_map([""], |r| r.get::<_, String>(3))
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>()
            .join(" ");
        assert!(plan.contains("entity_active_runs"), "{plan}");
        assert!(!plan.contains("TEMP B-TREE"), "{plan}");
        writer.close().await.unwrap();
    }
    #[tokio::test]
    async fn named_collections_use_the_partial_index_without_temporary_sorting() {
        let dir = flow_test_support::io::TempDir::new("name-query-plan");
        let writer = flow_journal::Journal::open(dir.path(), Default::default())
            .await
            .unwrap();
        let conn = flow_journal::sqlite::read_connection(dir.path()).unwrap();
        for kind in ["workflow", "template"] {
            let query = ordered_query(kind, None, true);
            let plan = conn
                .prepare(&format!("EXPLAIN QUERY PLAN {}", query.sql().as_str()))
                .unwrap()
                .query_map([kind], |r| r.get::<_, String>(3))
                .unwrap()
                .map(Result::unwrap)
                .collect::<Vec<_>>()
                .join(" ");
            assert!(plan.contains("entity_name_order"), "{plan}");
            assert!(!plan.contains("TEMP B-TREE"), "{plan}");
        }
        writer.close().await.unwrap();
    }
}
