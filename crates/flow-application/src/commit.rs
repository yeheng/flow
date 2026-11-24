use std::{collections::BTreeSet, future::Future, sync::Arc};

use flow_domain::journal_state::State;
use flow_facts::{Error, Event, EventKind, Transaction};
use tokio::sync::{watch, Mutex, Notify, OwnedMutexGuard};
use tokio_util::sync::CancellationToken;

pub struct DurableTransition {
    pub transaction: Arc<Transaction>,
    pub value_prefix: Vec<Transaction>,
}

pub struct WriteFailure {
    pub error: Error,
    /// A rejected transition is recoverable. A failed writer cannot accept
    /// another decision based on the application's in-memory state.
    pub writer_failed: bool,
}

/// Persists facts, projection changes and staged values as one transition.
pub trait TransitionWriter: Send + 'static {
    type Changes: Send + 'static;
    type Values: Send + 'static;

    fn write(
        self,
        transaction: Transaction,
        changes: Self::Changes,
        values: Self::Values,
    ) -> impl Future<Output = Result<DurableTransition, WriteFailure>> + Send;
}

#[derive(Debug, thiserror::Error)]
pub enum CommitError {
    #[error(transparent)]
    Facts(#[from] Error),
    #[error(transparent)]
    Task(#[from] tokio::task::JoinError),
}

/// The in-memory mirror of the durable state. The coordinator consumes dirty runs.
pub struct CommittedState {
    pub state: State,
    pub pending: bool,
    pub failure: Option<String>,
    pub dirty_runs: BTreeSet<String>,
}

pub struct CommitRuntime {
    pub state: Arc<Mutex<CommittedState>>,
    pub shutdown: CancellationToken,
    pub execution_changed: Arc<Notify>,
    decision: Arc<Mutex<()>>,
    commits: watch::Sender<()>,
}

impl CommitRuntime {
    pub fn new(state: State) -> Self {
        Self {
            state: Arc::new(Mutex::new(CommittedState {
                dirty_runs: state.runs.keys().cloned().collect(),
                state,
                pending: false,
                failure: None,
            })),
            shutdown: CancellationToken::new(),
            execution_changed: Arc::new(Notify::new()),
            decision: Arc::new(Mutex::new(())),
            commits: watch::channel(()).0,
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<()> {
        self.commits.subscribe()
    }

    /// Serialize new decisions and fail closed if an earlier commit stopped
    /// before publishing its state or left the writer permanently unavailable.
    pub async fn begin(&self) -> Result<OwnedMutexGuard<()>, String> {
        let decision = self.decision.clone().lock_owned().await;
        let committed = self.state.lock().await;
        if let Some(error) = &committed.failure {
            return Err(error.clone());
        }
        if committed.pending {
            return Err("commit task stopped before publishing state".into());
        }
        Ok(decision)
    }

    /// Wait out an accepted transition during shutdown, even after a failure.
    pub async fn quiesce(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.decision.lock().await
    }

    pub async fn preview<T>(
        &self,
        transactions: &[Transaction],
        project: impl FnOnce(&State) -> flow_facts::Result<T>,
    ) -> flow_facts::Result<T> {
        self.state
            .lock()
            .await
            .state
            .preview_batch(transactions, project)
    }

    /// An accepted commit keeps the decision lock even if its caller goes away.
    /// Publish state and notifications only after the writer reports durability.
    pub async fn persist<W: TransitionWriter>(
        &self,
        decision: OwnedMutexGuard<()>,
        writer: W,
        transaction: Transaction,
        changes: W::Changes,
        values: W::Values,
    ) -> Result<u64, CommitError> {
        let state = self.state.clone();
        let changed = self.execution_changed.clone();
        let shutdown = self.shutdown.clone();
        let commits = self.commits.clone();
        self.state.lock().await.pending = true;
        let result = tokio::spawn(async move {
            let written = writer.write(transaction, changes, values).await;
            let mut committed = state.lock().await;
            let (result, fatal) = match written {
                Ok(transition) => (apply_commit(&mut committed, transition), true),
                Err(failure) => (Err(failure.error), failure.writer_failed),
            };
            committed.pending = false;
            if let Err(error) = &result {
                if fatal {
                    committed.failure = Some(error.to_string());
                    shutdown.cancel();
                }
            }
            drop(committed);
            if result.is_ok() {
                commits.send_replace(());
            }
            changed.notify_one();
            drop(decision);
            result
        })
        .await??;
        Ok(result)
    }
}

/// Reserve sequence numbers for any staged chunk facts and the final transition.
pub fn transaction(
    journal_id: &str,
    upper: u64,
    chunks: u64,
    events: Vec<Event>,
) -> flow_facts::Result<Transaction> {
    let lsn = upper
        .checked_add(chunks)
        .and_then(|lsn| lsn.checked_add(1))
        .ok_or_else(|| Error::Invalid("LSN overflow".into()))?;
    Ok(Transaction {
        v: 2,
        journal_id: journal_id.to_owned(),
        lsn,
        tx_id: uuid::Uuid::now_v7().to_string(),
        events,
    })
}

/// 把已提交的事务应用到内存状态，并标记需要重新调度的 run。
fn apply_commit(
    committed: &mut CommittedState,
    commit: DurableTransition,
) -> flow_facts::Result<u64> {
    let tx = &commit.transaction;
    if commit.value_prefix.is_empty() {
        committed.state.apply(tx)?;
    } else {
        // 有正文分块时，先应用分块前缀事务再应用主事务，reducer 才能
        // 看到「值已发布」这一事实。
        let mut transactions = commit.value_prefix;
        transactions.push((**tx).clone());
        committed.state.apply_batch(&transactions)?;
    }
    // 子 run 终态会改变父 run 的等待条件，父 run 必须重新评估。
    // 只看本事务涉及且现已终态的 run：父唤醒只依赖「子变为终态」的
    // 那一刻，而那一刻必然落在本事务的事件里；启动恢复时全部 run 已
    // 进 dirty 集，历史终态无需每笔提交全量重扫（旧实现每笔提交
    // O(resident runs)）。
    for event in &tx.events {
        if let Some(id) = &event.run_id {
            if let Some(parent) = committed
                .state
                .runs
                .get(id)
                .filter(|r| r.terminal())
                .and_then(|r| r.parent.as_ref())
            {
                committed.dirty_runs.insert(parent.run_id.clone());
            }
        }
    }
    trim_hot_state(&mut committed.state);
    for event in &tx.events {
        if let Some(id) = &event.run_id {
            if committed.state.runs.get(id).is_some_and(|r| !r.terminal()) {
                committed.dirty_runs.insert(id.clone());
            } else {
                committed.dirty_runs.remove(id);
            }
            if event.kind == EventKind::RunCancelled {
                // 取消父 run 会级联影响子 run，子 run 必须一起重新评估。
                let children = committed
                    .state
                    .runs
                    .values()
                    .filter(|r| !r.terminal() && r.parent.as_ref().is_some_and(|p| p.run_id == *id))
                    .map(|r| r.run_id.clone())
                    .collect::<Vec<_>>();
                committed.dirty_runs.extend(children);
            }
        }
    }
    Ok(tx.lsn)
}

/// 裁剪协调器的热状态：只保留仍在用的东西。
///
/// 裁剪只影响内存占用，不影响正确性——被裁掉的东西都能从 SQLite 的
/// 当前行按需读回，所以这里可以放心地按「最后两个版本 + 在飞 run」收口。
pub fn trim_hot_state(state: &mut State) {
    for workflow in state.workflows.values_mut() {
        let latest = workflow.versions.last_key_value().map(|(v, _)| *v);
        let published = workflow
            .versions
            .iter()
            .rev()
            .find(|(_, v)| v.published)
            .map(|(v, _)| *v);
        // 只保留最新版本与已发布版本：前者是编辑目标，后者是执行依赖。
        workflow
            .versions
            .retain(|version, _| Some(*version) == latest || Some(*version) == published);
    }
    // 终态 run 不再需要调度，直接丢弃出内存。
    state.runs.retain(|_, run| !run.terminal());
    for run in state.runs.values_mut() {
        for node in run.nodes.values_mut() {
            // 只保留当前 attempt 的账本；旧 attempt 是历史，不参与判定。
            node.attempts.retain(|id, _| *id == node.dispatch_id);
        }
    }
    state.values.trim_cache();
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::sync::oneshot;

    struct ControlledWriter {
        started: oneshot::Sender<()>,
        durable: oneshot::Receiver<()>,
    }

    impl TransitionWriter for ControlledWriter {
        type Changes = u64;
        type Values = ();

        async fn write(
            self,
            transaction: Transaction,
            preview_epoch: u64,
            _: (),
        ) -> Result<DurableTransition, WriteFailure> {
            assert_eq!(preview_epoch, 1);
            self.started.send(()).unwrap();
            self.durable.await.unwrap();
            Ok(DurableTransition {
                transaction: Arc::new(transaction),
                value_prefix: Vec::new(),
            })
        }
    }

    #[tokio::test]
    async fn accepted_transition_publishes_after_durability_even_if_the_request_is_cancelled() {
        let commits = Arc::new(CommitRuntime::new(State {
            journal_id: "workflow-test".into(),
            version: 2,
            ..State::default()
        }));
        let tx = transaction(
            "workflow-test",
            0,
            0,
            vec![Event::new(
                EventKind::MasterEpochStarted,
                json!({"epoch": "1"}),
            )],
        )
        .unwrap();
        let preview = commits
            .preview(std::slice::from_ref(&tx), |s| Ok(s.master_epoch))
            .await
            .unwrap();
        let guard = commits.begin().await.unwrap();
        let mut changed = commits.subscribe();
        let (started, writing) = oneshot::channel();
        let (durable, waiting) = oneshot::channel();
        let caller = tokio::spawn({
            let commits = commits.clone();
            async move {
                commits
                    .persist(
                        guard,
                        ControlledWriter {
                            started,
                            durable: waiting,
                        },
                        tx,
                        preview,
                        (),
                    )
                    .await
            }
        });
        writing.await.unwrap();
        assert_eq!(commits.state.lock().await.state.master_epoch, 0);
        assert!(!changed.has_changed().unwrap());
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert!(commits.decision.try_lock().is_err());
        durable.send(()).unwrap();
        changed.changed().await.unwrap();
        let _decision = commits.quiesce().await;
        assert_eq!(commits.state.lock().await.state.master_epoch, 1);
        assert!(!commits.state.lock().await.pending);
    }
}
