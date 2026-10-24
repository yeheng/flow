//! Transaction-local before-images, restored in reverse mutation order.
//! Audit events save only counters/receipts, not accumulated attempt histories.
//!
//! 本模块提供折叠失败时的回滚能力：每个事件生效前拍下受影响实体的「前像」，
//! 一旦本次转移出错就按相反顺序还原，让失败的转移不留下任何痕迹。
//! 它守护的核心不变量是「一次失败的转移不改变可观察状态」——否则调用方
//! 看到的折叠态会与磁盘上的事实账本不一致，恢复时就会出现半套 run。
//! 只回滚被改动的实体（而不是克隆整份状态），是因为长跑服务里状态体量
//! 与运行历史同阶增长，整份克隆会让每次转移都付出不可接受的开销。
use super::*;

/// 事务内累积的回滚日志：按「前像采集顺序」记录，逆序还原。
///
/// 只记录被改动的实体，因此日志大小与本次转移触及的实体数成正比，
/// 而不是与全局状态成正比。
#[derive(Default)]
pub(super) struct UndoLog(Vec<Undo>);

/// 一条前像：标识被改动的实体，加上改动前的取值。
///
/// `Option<Box<T>>` 的 `None` 表示「该实体此前不存在」，
/// 还原时应当删除而不是写回一个空壳——这是「新建实体被回滚」的正确语义。
/// `Box` 用于避免在日志里再存一份完整实体的所有权拷贝。
enum Undo {
    /// 工作流实体前像。
    Workflow(String, Option<Box<Workflow>>),
    /// run 实体前像（只含摘要，不含节点账本）。
    Run(String, Option<Box<Run>>),
    /// 节点前像；末位布尔表示是否保存了完整节点（跳过事件会整条替换节点）。
    Node((String, String), Option<Box<Node>>, bool),
    /// 单次 dispatch 的 attempt 账本前像。
    Attempt((String, String, String), Option<Box<Attempt>>),
    /// run 的序号与状态前像（仅纯审计事件涉及）。
    RunSequence(String, u64, RunStatus),
    /// 审计条目前像：标识、事件审计序号、原审计序号、原摘要与原 lsn。
    Audit(
        (String, String, String),
        u64,
        u64,
        Option<String>,
        Option<u64>,
    ),
    /// run→workflow 归属关系前像：布尔表示此前是否已存在该归属。
    Membership(String, bool),
    /// 配置类实体（调度/webhook/模板/旧基线）前像。
    Config(Config, String, Option<Value>),
    /// 调度触发水位前像。
    Fire(String, Option<ScheduleFire>),
    /// 命令回执前像。
    Command(String, Option<CommandRecord>),
    /// 值发布缓存前像。
    Value(flow_journal::value::ValueUndo),
    /// 写者纪元前像。
    Epoch(u64),
}

/// 配置类容器的种类。
///
/// 用枚举而不是四份独立分支，是为了让回滚逻辑对所有配置容器共用同一条
/// 路径——新增一类配置时只需加一个变体，忘记加回滚分支的风险被消除。
enum Config {
    /// 调度配置。
    Schedule,
    /// webhook 配置。
    Webhook,
    /// 节点模板配置。
    Template,
    /// 旧基线导入配置。
    Legacy,
}

impl Config {
    /// 取出该种类对应的配置容器引用。
    fn map<'a>(&self, state: &'a mut State) -> &'a mut BTreeMap<String, Value> {
        match self {
            Self::Schedule => &mut state.schedules,
            Self::Webhook => &mut state.webhooks,
            Self::Template => &mut state.templates,
            Self::Legacy => &mut state.legacy,
        }
    }
}

impl UndoLog {
    /// 记录一条命令回执的前像。
    ///
    /// 幂等窗口超窗淘汰也会走这里：淘汰掉的回执必须同样可回滚，
    /// 否则一次失败的事务会把别人的回执永久删掉。
    pub(super) fn command(&mut self, key: String, previous: Option<CommandRecord>) {
        self.0.push(Undo::Command(key, previous));
    }

    // 采集必须发生在每个事件之前：包括本事务里更早的事件刚刚创建的实体
    // （那时的前像就是「不存在」）。不维护分容器索引，是因为索引一旦与
    // 实际改动脱节，回滚就会漏还原或错还原。
    // 载荷缺少必需字段、无法反序列化时返回错误。
    /// Capture immediately before each event, including when earlier events in
    /// this transaction created the entity. No separate per-map undo indexes.
    pub(super) fn capture(&mut self, state: &State, event: &Event) -> Result<()> {
        let p = &event.payload;
        match event.kind {
            EventKind::WorkflowCreated
            | EventKind::WorkflowUpdated
            | EventKind::WorkflowPublished
            | EventKind::WorkflowDeleted => {
                let id = string(p, "workflow_id")?;
                self.0.push(Undo::Workflow(
                    id.clone(),
                    state.workflows.get(&id).cloned().map(Box::new),
                ));
            }
            EventKind::RunStarted => {
                let id = string(p, "workflow_id")?;
                self.0.push(Undo::Membership(
                    id.clone(),
                    state.run_workflows.contains(&id),
                ));
            }
            EventKind::ScheduleChanged => {
                let id = string(p, "id")?;
                // 调度变更会同时动到「配置」和「触发水位」两处状态，
                // 两处都要各自拍前像，否则回滚后会留下水位与配置不一致。
                self.0.push(Undo::Fire(
                    id.clone(),
                    state.schedule_fires.get(&id).cloned(),
                ));
                self.0.push(Undo::Config(
                    Config::Schedule,
                    id.clone(),
                    state.schedules.get(&id).cloned(),
                ));
            }
            EventKind::WebhookChanged => {
                let id = string(p, "token")?;
                self.0.push(Undo::Config(
                    Config::Webhook,
                    id.clone(),
                    state.webhooks.get(&id).cloned(),
                ));
            }
            EventKind::TemplateChanged => {
                let id = string(p, "id")?;
                self.0.push(Undo::Config(
                    Config::Template,
                    id.clone(),
                    state.templates.get(&id).cloned(),
                ));
            }
            EventKind::LegacyImport => {
                let id = string(p, "key")?;
                self.0.push(Undo::Config(
                    Config::Legacy,
                    id.clone(),
                    state.legacy.get(&id).cloned(),
                ));
            }
            // 纪元只有一个标量，旧值本身就是完整前像。
            EventKind::MasterEpochStarted => self.0.push(Undo::Epoch(state.master_epoch)),
            EventKind::ValueChunk | EventKind::ValuePublished => {
                // 发布缓存也是折叠态的一部分：发布失败必须把缓存还原，
                // 否则会留下「引用了未发布值」的悬空状态。
                self.0
                    .push(Undo::Value(state.values.save(&string(p, "output_id")?)));
            }
            EventKind::Command => {
                let c: CommandRecord = serde_json::from_value(p.clone())?;
                // 调度类命令还会推进该槽位的触发水位，水位也要能回滚。
                if let Some(key) = c.scope.strip_prefix("run.start:schedule:") {
                    self.0.push(Undo::Fire(
                        key.into(),
                        state.schedule_fires.get(key).cloned(),
                    ));
                }
                if let Some(id) = c.request_id {
                    let key = command_key(&c.scope, &id);
                    self.command(key.clone(), state.commands.get(&key).cloned());
                }
            }
            // Keep this exhaustive: new event kinds must choose their rollback
            // strategy at compile time, even when only generic run/node state changes.
            EventKind::SegmentStarted
            | EventKind::SegmentSealed
            | EventKind::RunCancelled
            | EventKind::RunCompleted
            | EventKind::RunFailed
            | EventKind::DispatchStarted
            | EventKind::InputPrepared
            | EventKind::OperationIntent
            | EventKind::OperationAuthorized
            | EventKind::OperationOutcome
            | EventKind::NodeCompleted
            | EventKind::NodeFailed
            | EventKind::NodeSkipped
            | EventKind::WaitRegistered
            | EventKind::WaitResolved
            | EventKind::SignalReceived
            | EventKind::Adjudicated
            | EventKind::LateAudit
            | EventKind::OperationRetryAuthorized => {}
        }
        // 不带 run 身份的事件（数据集级事件）到不了 run/node 这一层，
        // 它们的前像已在上面的分支里采集完毕。
        let Some(id) = &event.run_id else {
            return Ok(());
        };
        let run = state.runs.get(id);
        let node_id = event.node_id.as_deref().or_else(|| p["node_id"].as_str());
        if event.is_audit_only() {
            if let Some(run) = run {
                // 纯审计事件只动 run 的序号/状态与 attempt 里的审计计数，
                // 不动节点执行状态——因此只记这些最小前像，回滚代价与
                // 已累积的运行历史无关。
                self.0
                    .push(Undo::RunSequence(id.clone(), run.last_run_seq, run.status));
                if let (Some(node), Some(dispatch)) = (node_id, &event.dispatch_id) {
                    if let Some(attempt) =
                        run.nodes.get(node).and_then(|n| n.attempts.get(dispatch))
                    {
                        self.0.push(Undo::Audit(
                            (id.clone(), node.into(), dispatch.clone()),
                            event.audit_seq,
                            attempt.audit_seq,
                            attempt.audit_digests.get(&event.audit_seq).cloned(),
                            attempt.audit_lsns.get(&event.audit_seq).copied(),
                        ));
                    }
                }
            }
        } else {
            // 业务事件：run 前像只存摘要（不含节点账本），
            // 节点前像同样只存摘要（不含 attempt 账本），
            // 各自更细的部分由下面的 Node/Attempt 前像负责还原。
            self.0
                .push(Undo::Run(id.clone(), run.map(|r| Box::new(r.summary()))));
            if let Some(node_id) = node_id {
                let node = run.and_then(|r| r.nodes.get(node_id));
                // NodeSkipped 会整条替换节点记录，所以那时必须保存完整节点；
                // 其余事件只改执行字段，保存摘要即可。
                let full = event.kind == EventKind::NodeSkipped;
                self.0.push(Undo::Node(
                    (id.clone(), node_id.into()),
                    node.map(|n| Box::new(if full { n.clone() } else { n.summary() })),
                    full,
                ));
                if let Some(dispatch) = &event.dispatch_id {
                    // attempt 账本单独拍前像：它承载审计证据链，
                    // 不能随节点摘要一起丢掉。
                    self.0.push(Undo::Attempt(
                        (id.clone(), node_id.into(), dispatch.clone()),
                        node.and_then(|n| n.attempts.get(dispatch))
                            .cloned()
                            .map(Box::new),
                    ));
                }
            }
        }
        Ok(())
    }

    /// 逆序还原所有前像，把状态恢复到本次转移之前。
    ///
    /// 逆序是必须的：先改的实体会被后改的实体依赖（例如先改 run 再改节点），
    /// 逆序还原才能让每一步还原都看到与当时一致的前置状态。
    ///
    /// # Errors
    /// 还原所需的宿主实体已经消失时返回错误——这说明折叠过程本身有缺陷，
    /// 继续还原会造出不一致的状态，宁可让调用方把整次转移视为失败。
    pub(super) fn restore(mut self, state: &mut State) -> Result<()> {
        while let Some(undo) = self.0.pop() {
            match undo {
                Undo::Workflow(id, previous) => {
                    restore_one(&mut state.workflows, id, previous.map(|v| *v))
                }
                Undo::Run(id, previous) => {
                    // 还原 run 时保留当前的 nodes 容器：节点与 attempt 各自
                    // 有独立前像，会在之后单独还原；这里只负责 run 自身的字段。
                    let previous = previous.map(|mut previous| {
                        if let Some(current) = state.runs.get_mut(&id) {
                            previous.nodes = std::mem::take(&mut current.nodes);
                        }
                        *previous
                    });
                    restore_one(&mut state.runs, id, previous);
                }
                Undo::Node((run, node), previous, full) => {
                    if let Some(run) = state.runs.get_mut(&run) {
                        // 摘要前像不含 attempts 账本，还原时保留当前账本，
                        // 由 Attempt 前像负责还原其中的条目。
                        let previous = previous.map(|mut previous| {
                            if !full {
                                if let Some(current) = run.nodes.get_mut(&node) {
                                    previous.attempts = std::mem::take(&mut current.attempts);
                                }
                            }
                            *previous
                        });
                        restore_one(&mut run.nodes, node, previous);
                    } else if previous.is_some() {
                        // 节点原本存在却找不到宿主 run：折叠内部状态已错乱，
                        // 不能悄悄跳过——跳过会留下已改写的节点。
                        return Err(invalid("rollback lost node owner"));
                    }
                }
                Undo::Attempt((run, node, dispatch), previous) => {
                    if let Some(node) = state
                        .runs
                        .get_mut(&run)
                        .and_then(|r| r.nodes.get_mut(&node))
                    {
                        restore_one(&mut node.attempts, dispatch, previous.map(|v| *v));
                    } else if previous.is_some() {
                        // attempt 原本存在却找不到宿主节点：与上面同理，
                        // 必须让整次转移失败而不是留下半还原状态。
                        return Err(invalid("rollback lost attempt owner"));
                    }
                }
                Undo::RunSequence(id, seq, status) => {
                    // 纯审计事件改过 run 的序号与状态，必须一并还原；
                    // run 消失说明状态已错乱，直接报错。
                    let run = state
                        .runs
                        .get_mut(&id)
                        .ok_or_else(|| invalid("rollback lost audit run"))?;
                    run.last_run_seq = seq;
                    run.status = status;
                }
                Undo::Audit((run, node, dispatch), seq, previous_seq, digest, lsn) => {
                    // 还原审计计数与该序号上的摘要/lsn：审计序号是单调的证据链，
                    // 残留一条会让「下一个审计序号」的判定错位。
                    let attempt = state
                        .runs
                        .get_mut(&run)
                        .and_then(|r| r.nodes.get_mut(&node))
                        .and_then(|n| n.attempts.get_mut(&dispatch))
                        .ok_or_else(|| invalid("rollback lost audit attempt"))?;
                    attempt.audit_seq = previous_seq;
                    restore_one(&mut attempt.audit_digests, seq, digest);
                    restore_one(&mut attempt.audit_lsns, seq, lsn);
                }
                Undo::Membership(id, existed) => {
                    // 归属关系是集合：只有本次新增的归属需要撤销，
                    // 此前就存在的归属不能被本次回滚删掉。
                    if !existed {
                        state.run_workflows.remove(&id);
                    }
                }
                // 以下几类都是「键 → 旧值」的一次性替换，语义一致。
                Undo::Config(kind, id, previous) => restore_one(kind.map(state), id, previous),
                Undo::Fire(id, previous) => restore_one(&mut state.schedule_fires, id, previous),
                Undo::Command(id, previous) => restore_one(&mut state.commands, id, previous),
                Undo::Value(previous) => state.values.restore(previous),
                Undo::Epoch(previous) => state.master_epoch = previous,
            }
        }
        Ok(())
    }
}

/// 把单个键还原到旧值：旧值存在则写回，不存在则删除该键。
///
/// 「旧值不存在」等价于「这个键是本次转移新建的」，因此要删除而不是
/// 写入空值——这是新建实体被回滚时的正确表现。
fn restore_one<K: Ord, V>(map: &mut BTreeMap<K, V>, key: K, previous: Option<V>) {
    if let Some(previous) = previous {
        map.insert(key, previous);
    } else {
        map.remove(&key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// 构造一个最小可用的 run 状态。
    fn run(id: &str) -> Run {
        serde_json::from_value(json!({
            "run_id":id,"workflow_id":"w","workflow_version":1,
            "definition":{"type":"inline","value":{}},"input":{"type":"inline","value":null},
            "source":"schedule","source_detail":"s","parent":null,"depth":0,
            "created_at":"2026-10-24T00:00:00Z","status":"running","output":null,
            "error":null,"last_run_seq":"1","nodes":{}
        }))
        .unwrap()
    }
    /// 构造一个紧接当前水位的事务。
    fn tx(state: &State, events: Vec<Event>) -> Transaction {
        Transaction {
            v: 2,
            journal_id: state.journal_id.clone(),
            lsn: state.applied_lsn + 1,
            tx_id: "test".into(),
            events,
        }
    }
    /// 构造一个挂在指定节点上的 run 级事件。
    fn node_event(kind: EventKind, seq: u64, node: &str, payload: Value) -> Event {
        let mut event = Event::new(kind, payload);
        event.run_id = Some("r".into());
        event.run_seq = seq;
        event.node_id = Some(node.into());
        event
    }

    /// 同一批事件里多次改动同一实体时，预演、检视失败与末尾非法事件
    /// 都必须把每一个实体完整还原到转移前的状态。
    #[test]
    fn repeated_mutations_restore_every_entity_on_preview_inspect_error_and_late_failure() {
        let mut state = State {
            journal_id: "undo".into(),
            applied_lsn: 1,
            ..Default::default()
        };
        state.workflows.insert(
            "w".into(),
            Workflow {
                workflow_id: "w".into(),
                name: "original".into(),
                created_at: "now".into(),
                versions: BTreeMap::new(),
                deleted: false,
            },
        );
        state
            .schedules
            .insert("s".into(), json!({"id":"s","enabled":true}));
        state
            .webhooks
            .insert("token".into(), json!({"token":"token","enabled":true}));
        state
            .templates
            .insert("t".into(), json!({"id":"t","name":"old"}));
        let receipt = json!({"scope":"run.start:schedule:s","request_id":"2026-10-23T00:00:00Z","fingerprint":"old","result":{},"lsn":"1"});
        state.schedule_fires.insert(
            "s".into(),
            serde_json::from_value(json!({"at":"2026-10-23T00:00:00Z","command":receipt})).unwrap(),
        );
        let mut started = Event::new(
            EventKind::RunStarted,
            serde_json::to_value(run("r")).unwrap(),
        );
        started.run_id = Some("r".into());
        started.run_seq = 1;
        let mut dispatched = node_event(
            EventKind::DispatchStarted,
            2,
            "a",
            json!({"node_execution_id":"exec","attempt":"1"}),
        );
        dispatched.dispatch_id = Some("dispatch".into());
        let reference: flow_journal::ValueRef = serde_json::from_value(json!({
            "journal_id":"undo","output_id":"value","codec":"utf8","version":1,"chunk_count":"0","total_bytes":"0",
            "digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        })).unwrap();
        let events = vec![
            Event::new(EventKind::ValuePublished, json!(reference)),
            Event::new(
                EventKind::WorkflowUpdated,
                json!({"workflow_id":"w","version":{"version":1,"definition":{"type":"inline","value":{}},"checksum":"c","published":false,"created_at":"now"}}),
            ),
            Event::new(
                EventKind::WorkflowPublished,
                json!({"workflow_id":"w","version":"1"}),
            ),
            Event::new(EventKind::WorkflowDeleted, json!({"workflow_id":"w"})),
            Event::new(EventKind::ScheduleChanged, json!({"id":"s","deleted":true})),
            Event::new(
                EventKind::ScheduleChanged,
                json!({"id":"s","enabled":false}),
            ),
            Event::new(
                EventKind::WebhookChanged,
                json!({"token":"token","enabled":false}),
            ),
            Event::new(EventKind::TemplateChanged, json!({"id":"t","name":"new"})),
            Event::new(
                EventKind::LegacyImport,
                json!({"key":"legacy","data":{"type":"inline","value":null}}),
            ),
            Event::new(EventKind::MasterEpochStarted, json!({"epoch":"1"})),
            started,
            dispatched,
            node_event(
                EventKind::NodeSkipped,
                3,
                "b",
                json!({"reason":"upstream_failed"}),
            ),
            node_event(
                EventKind::SignalReceived,
                4,
                "c",
                json!({"queued":true,"payload":{"type":"inline","value":42}}),
            ),
            node_event(EventKind::RunCancelled, 5, "", json!({})),
            Event::new(
                EventKind::Command,
                json!({"scope":"run.start:schedule:s","request_id":"2026-10-24T00:00:00Z","fingerprint":"new","result":{"run_id":"r"},"lsn":"0"}),
            ),
        ];
        let before = serde_json::to_value(&state).unwrap();
        let valid = tx(&state, events);
        assert!(state
            .preview(&valid, |s| Ok(s.runs["r"].terminal()))
            .unwrap());
        assert_eq!(serde_json::to_value(&state).unwrap(), before);
        assert!(state
            .values
            .check(&StoredValue::Ref(reference.clone()))
            .is_err());
        assert!(state
            .apply_with(&valid, |_| Err::<(), _>(invalid("index encoding failed")))
            .is_err());
        assert_eq!(serde_json::to_value(&state).unwrap(), before);
        let mut invalid_tx = valid.clone();
        invalid_tx.events.push(Event::new(
            EventKind::WorkflowPublished,
            json!({"workflow_id":"absent","version":"1"}),
        ));
        assert!(state.apply(&invalid_tx).is_err());
        assert_eq!(serde_json::to_value(&state).unwrap(), before);
        assert!(state
            .values
            .check(&StoredValue::Ref(reference.clone()))
            .is_err());
        state.apply(&valid).unwrap();
        assert_eq!(state.applied_lsn, 2);
        assert!(state.runs["r"].terminal());
        assert_eq!(state.runs["r"].nodes.len(), 2);
        assert!(state.run_workflows.contains("w"));
        assert!(state.values.check(&StoredValue::Ref(reference)).is_ok());
        assert_eq!(state.schedule_fires["s"].command.result["run_id"], "r");
    }

    /// 审计序号有缺口时必须失败并完整还原（含压缩与非压缩两种模式），
    /// 且重复回执与 run 序号不得 panic。
    #[test]
    fn audit_gaps_restore_repeated_receipts_and_run_sequences_without_panicking() {
        for compact in [false, true] {
            let mut state = State {
                journal_id: "audit".into(),
                applied_lsn: 1,
                compact_audits: compact,
                ..Default::default()
            };
            state.runs.insert("r".into(), run("r"));
            let mut dispatch = node_event(
                EventKind::DispatchStarted,
                2,
                "a",
                json!({"node_execution_id":"exec","attempt":"1"}),
            );
            dispatch.dispatch_id = Some("dispatch".into());
            state.apply(&tx(&state, vec![dispatch])).unwrap();
            let audit = |seq| {
                let mut event =
                    node_event(EventKind::LateAudit, 0, "a", json!({"evidence":"late"}));
                event.dispatch_id = Some("dispatch".into());
                event.audit_seq = seq;
                event
            };
            let before = serde_json::to_value(&state).unwrap();
            assert!(state
                .apply(&tx(&state, vec![audit(1), audit(2), audit(4)]))
                .is_err());
            assert_eq!(serde_json::to_value(&state).unwrap(), before);
            assert!(state
                .apply_with(&tx(&state, vec![audit(1), audit(2)]), |_| Err::<(), _>(
                    invalid("inspection failed")
                ))
                .is_err());
            assert_eq!(serde_json::to_value(&state).unwrap(), before);
            state.apply(&tx(&state, vec![audit(1), audit(2)])).unwrap();
            assert_eq!(state.runs["r"].nodes["a"].attempts["dispatch"].audit_seq, 2);
        }
    }
}
