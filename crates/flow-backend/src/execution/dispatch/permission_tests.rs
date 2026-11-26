use super::*;

use crate::journal_permissions::ApprovalCommand;
use flow_domain::journal_state::ApprovalStatus;
use flow_dto::permission::{run_approval_digest, RunApproval};
use tokio::sync::Mutex;

static SCENARIO: Mutex<()> = Mutex::const_new(());
const URL: &str = "http://127.0.0.1:1/fixture";

struct Fixture {
    _root: tempfile::TempDir,
    backend: Arc<JournalBackend>,
    runner: DispatchRunner,
    link: DispatchLink,
    commands: mpsc::Receiver<ToSession>,
}

impl Fixture {
    async fn start(net: &str, write: &str) -> Self {
        let root = tempfile::tempdir().unwrap();
        let backend = JournalBackend::open(root.path(), flow_journal::JournalOptions::default())
            .await
            .unwrap();
        let node = json!({"id":"n","type":"http_call","params":{"url":URL}});
        let definition = json!({
            "semantics": 2,
            "nodes": [{"id":"s","type":"start"}, node, {"id":"e","type":"end"}],
            "edges": [{"from":"s","to":"n"},{"from":"n","to":"e"}],
            "permissions": {"version": 1, "mode": "execute", "allow": [
                {"id":"net","action":"net","methods":["GET"],"url_prefix":URL,"approval":net},
                {"id":"write","action":"file_write","path":"fixture.txt","approval":write}
            ]}
        });
        let created = backend
            .workflow_create("permission ordering", None)
            .await
            .unwrap();
        let workflow = created.result["workflow_id"].as_str().unwrap();
        backend
            .workflow_update(workflow, definition, None)
            .await
            .unwrap();
        backend.workflow_publish(workflow, 1, None).await.unwrap();
        let started = backend
            .run_start(workflow, None, json!({}), "manual", None, None)
            .await
            .unwrap();
        let run_id = started.result["run_id"].as_str().unwrap();
        let attempt = Attempt::begin(backend.clone(), run_id.into(), "n".into())
            .await
            .unwrap();
        backend
            .internal_for_run(run_id, |state| {
                let run = &state.runs[run_id];
                let prepared = Prepared {
                    input: run.input.clone(),
                    predecessors: BTreeMap::new(),
                    params: StoredValue::Inline(json!({"url":URL})),
                    engine_build: "fixture".into(),
                    node_semantics: 2,
                };
                let mut event = crate::journal_commands::node_event(
                    run,
                    "n",
                    EventKind::InputPrepared,
                    json!({"prepared":prepared}),
                    false,
                );
                event.audit_seq = 1;
                Ok((vec![event], ()))
            })
            .await
            .unwrap();
        let runner = DispatchRunner::runner(
            attempt,
            serde_json::from_value(node).unwrap(),
            BTreeMap::new(),
            None,
        );
        let (outbound, commands) = mpsc::channel(8);
        let (_upstream, events) = mpsc::channel(8);
        let link = DispatchLink {
            commands: outbound,
            events,
            parking: None,
        };
        Self {
            _root: root,
            backend,
            runner,
            link,
            commands,
        }
    }

    async fn node_request(&mut self, request: &Value) -> Message {
        let bytes = serde_json::to_vec(request).unwrap();
        let fingerprint = hex_sha(&bytes);
        let operation_id = "http".to_string();
        let transfer_id = TransferKey::OperationRequest(operation_id.clone()).to_wire();
        let dispatch_id = self.runner.attempt.dispatch_id.clone();
        let len = bytes.len() as u64;
        for message in [
            Message::TransferChunk {
                dispatch_id: dispatch_id.clone(),
                transfer_id: transfer_id.clone(),
                offset: 0,
                bytes,
                digest: fingerprint.clone(),
            },
            Message::InputReady {
                dispatch_id: dispatch_id.clone(),
                transfer_id: transfer_id.clone(),
                total_bytes: len,
                digest: fingerprint.clone(),
            },
        ] {
            self.runner.handle(message, &mut self.link).await.unwrap();
        }
        Message::RequestOperation {
            dispatch_id,
            operation_id,
            request_fingerprint: fingerprint,
            command_id: transfer_id.clone(),
            transfer_id,
        }
    }

    async fn start_turn(&mut self) {
        let message = self.node_request(&json!({"method":"GET","url":URL})).await;
        self.runner.handle(message, &mut self.link).await.unwrap();
        assert!(matches!(self.command(), Message::OperationPermit { .. }));
    }

    fn tool_request(&self, id: &str) -> Message {
        Message::ToolPermissionRequest {
            dispatch_id: self.runner.attempt.dispatch_id.clone(),
            tool_call_id: id.into(),
            tool: "write".into(),
            params: json!({"path":"fixture.txt","content":"fixture"}),
            cwd: None,
        }
    }

    fn command(&mut self) -> Message {
        match self.commands.try_recv().unwrap() {
            ToSession::Send(message) => *message,
            _ => panic!("expected a worker protocol message"),
        }
    }

    async fn approve_tool(&mut self, call: &str, scope: ApprovalScope) {
        let message = self.tool_request(call);
        self.runner
            .handle_tool_permission(message, &mut self.link, false)
            .await
            .unwrap();
        let run = self.runner.snapshot().await.unwrap();
        let asked = run.nodes["n"].approval.as_ref().unwrap();
        let receipt = self
            .backend
            .run_decide(
                ApprovalCommand {
                    run_id: &run.run_id,
                    node_id: "n",
                    operation_id: &asked.request.operation_id,
                    request_hash: &asked.request.request_hash,
                    allow: true,
                    scope,
                    reason: None,
                    by: Principal::LocalAdmin,
                },
                "approve",
            )
            .await
            .unwrap();
        assert_eq!(receipt.result["decision"], json!("allowed"));
    }
}

async fn reached(point: &str) {
    tokio::time::timeout(Duration::from_secs(10), fault::wait_reached(point))
        .await
        .unwrap();
}

#[tokio::test]
async fn auto_tool_allow_is_suppressed_when_turn_outcome_commits_first() {
    let _scenario = SCENARIO.lock().await;
    let mut fixture = Fixture::start("auto", "auto").await;
    fixture.start_turn().await;
    let message = fixture.tool_request("call");
    let run_id = fixture.runner.attempt.run_id.clone();
    let gates = fault::register(&["tool_allow_send:before"]);
    let (handled, ()) = tokio::join!(
        fixture
            .runner
            .handle_tool_permission(message, &mut fixture.link, false),
        async {
            reached("tool_allow_send:before").await;
            fixture
                .backend
                .internal_for_run(&run_id, |state| {
                    let outcome = StoredValue::Inline(json!({}));
                    let mut event = crate::journal_commands::node_event(
                        &state.runs[&run_id],
                        "n",
                        EventKind::OperationOutcome,
                        json!({"outcome":outcome}),
                        false,
                    );
                    event.audit_seq = 2;
                    Ok((vec![event], ()))
                })
                .await
                .unwrap();
            gates["tool_allow_send:before"].send_replace(false);
        }
    );
    fault::clear();
    handled.unwrap();
    assert!(
        fixture.commands.try_recv().is_err(),
        "worker received an allow after the turn outcome"
    );
    fixture.backend.close().await.unwrap();
}

#[tokio::test]
async fn approved_tool_allow_is_suppressed_when_cancellation_commits_first() {
    let _scenario = SCENARIO.lock().await;
    let mut fixture = Fixture::start("auto", "ask").await;
    fixture.start_turn().await;
    fixture.approve_tool("call", ApprovalScope::Once).await;
    let run_id = fixture.runner.attempt.run_id.clone();
    let gates = fault::register(&["tool_allow_send:before"]);
    let (handled, ()) = tokio::join!(fixture.runner.drive_approval(&mut fixture.link), async {
        reached("tool_allow_send:before").await;
        fixture.backend.run_cancel(&run_id, None).await.unwrap();
        gates["tool_allow_send:before"].send_replace(false);
    });
    fault::clear();
    handled.unwrap();
    assert!(
        fixture.commands.try_recv().is_err(),
        "worker received an allow after cancellation"
    );
    fixture.backend.close().await.unwrap();
}

#[tokio::test]
async fn tool_request_asks_again_when_run_revoke_commits_first() {
    let _scenario = SCENARIO.lock().await;
    let mut fixture = Fixture::start("auto", "ask").await;
    fixture.start_turn().await;
    fixture.approve_tool("first", ApprovalScope::Run).await;
    fixture
        .runner
        .drive_approval(&mut fixture.link)
        .await
        .unwrap();
    assert!(matches!(
        fixture.command(),
        Message::ToolPermissionDecision { allowed: true, .. }
    ));
    let run = fixture.runner.snapshot().await.unwrap();
    let message = fixture.tool_request("second");
    let gates = fault::register(&["tool_auth_persist:before"]);
    let (handled, ()) = tokio::join!(
        fixture
            .runner
            .handle_tool_permission(message, &mut fixture.link, false),
        async {
            reached("tool_auth_persist:before").await;
            fixture
                .backend
                .run_approval_revoke(
                    &run.run_id,
                    "n",
                    &run.approvals[0].digest,
                    Principal::LocalAdmin,
                    "revoke",
                )
                .await
                .unwrap();
            gates["tool_auth_persist:before"].send_replace(false);
        }
    );
    fault::clear();
    handled.unwrap();
    let run = fixture.runner.snapshot().await.unwrap();
    let approval = run.nodes["n"].approval.as_ref().unwrap();
    assert_eq!(approval.status, ApprovalStatus::Pending);
    assert_eq!(
        approval.request.context.tool_call_id.as_deref(),
        Some("second")
    );
    assert!(fixture.commands.try_recv().is_err());
    fixture.backend.close().await.unwrap();
}

#[tokio::test]
async fn node_request_asks_again_when_run_revoke_commits_first() {
    let _scenario = SCENARIO.lock().await;
    let mut fixture = Fixture::start("ask", "auto").await;
    let request = json!({"method":"GET","url":URL});
    let message = fixture.node_request(&request).await;
    let Message::RequestOperation {
        request_fingerprint,
        ..
    } = &message
    else {
        unreachable!()
    };
    let run = fixture.runner.snapshot().await.unwrap();
    let actor = Principal::Node {
        run_id: run.run_id.clone(),
        node_id: "n".into(),
    };
    let specs = crate::policy::derive::derive_specs(&fixture.runner.node, &request).unwrap();
    let digest = run_approval_digest(
        &actor,
        &specs,
        request_fingerprint,
        &run.policy.as_ref().unwrap().digest,
    );
    // Stage a previously granted exact approval without running another external operation.
    fixture
        .backend
        .internal_for_run(&run.run_id, |state| {
            let mut event = flow_journal::Event::new(
                EventKind::RunApprovalUpserted,
                serde_json::to_value(RunApproval {
                    node_id: "n".into(),
                    digest: digest.clone(),
                    actor,
                    expires_at: None,
                    by: Principal::LocalAdmin,
                })?,
            );
            event.run_id = Some(run.run_id.clone());
            event.run_seq = state.runs[&run.run_id].last_run_seq + 1;
            Ok((vec![event], ()))
        })
        .await
        .unwrap();
    let gates = fault::register(&["auth_persist:before"]);
    let (handled, ()) = tokio::join!(fixture.runner.handle(message, &mut fixture.link), async {
        reached("auth_persist:before").await;
        fixture
            .backend
            .run_approval_revoke(&run.run_id, "n", &digest, Principal::LocalAdmin, "revoke")
            .await
            .unwrap();
        gates["auth_persist:before"].send_replace(false);
    });
    fault::clear();
    handled.unwrap();
    assert!(matches!(
        fixture.command(),
        Message::OperationPending { .. }
    ));
    let run = fixture.runner.snapshot().await.unwrap();
    assert!(run.nodes["n"].operation.is_none());
    fixture.backend.close().await.unwrap();
}

#[tokio::test]
async fn expired_tool_approval_commits_denial_and_releases_waiter() {
    let _scenario = SCENARIO.lock().await;
    let mut fixture = Fixture::start("auto", "ask").await;
    fixture.start_turn().await;
    let run = fixture.runner.snapshot().await.unwrap();
    let Message::ToolPermissionRequest {
        tool,
        tool_call_id,
        params,
        ..
    } = fixture.tool_request("expire")
    else {
        unreachable!()
    };
    let request = PermissionRequest {
        actor: Principal::Node {
            run_id: run.run_id.clone(),
            node_id: "n".into(),
        },
        operation_id: "expire".into(),
        specs: crate::policy::derive::tool_specs(&tool, &params, None).unwrap(),
        context: RequestContext {
            run_id: run.run_id.clone(),
            workflow_id: run.workflow_id.clone(),
            workflow_version: run.workflow_version,
            node_id: "n".into(),
            node_type: "http_call".into(),
            dispatch_id: fixture.runner.attempt.dispatch_id.clone(),
            attempt: 1,
            request_fingerprint: flow_dto::permission::canonical_digest(&params),
            tool_call_id: Some(tool_call_id),
        },
        policy_digest: run.policy.as_ref().unwrap().digest.clone(),
        request_hash: String::new(),
    }
    .sealed();
    let deadline = Utc::now().timestamp_millis() - 1;
    fixture
        .backend
        .internal_for_run(&run.run_id, |state| {
            let event = crate::journal_commands::node_event(
                &state.runs[&run.run_id],
                "n",
                EventKind::PermissionAsked,
                serde_json::to_value(PermissionAskedPayload {
                    gate: "tool".into(),
                    request: request.clone(),
                    asked_at: deadline - 1,
                    deadline,
                    operation_request: None,
                    matched: vec![],
                })?,
                true,
            );
            Ok((vec![event], ()))
        })
        .await
        .unwrap();
    fixture.runner.pending_approval = Some(PendingApproval {
        operation_id: request.operation_id,
        request_fingerprint: request.context.request_fingerprint,
        deadline,
    });
    fixture
        .runner
        .drive_approval(&mut fixture.link)
        .await
        .unwrap();
    fixture
        .runner
        .drive_approval(&mut fixture.link)
        .await
        .unwrap();
    let snapshot = fixture.runner.snapshot().await.unwrap();
    assert_eq!(
        snapshot.nodes["n"].approval.as_ref().unwrap().status,
        ApprovalStatus::Expired
    );
    assert!(
        matches!(fixture.command(), Message::ToolPermissionDecision { allowed: false, ref tool_call_id, .. } if tool_call_id == "expire")
    );
    fixture.backend.close().await.unwrap();
}

#[tokio::test]
async fn approved_tool_replay_requires_the_original_parameters() {
    let _scenario = SCENARIO.lock().await;
    let mut fixture = Fixture::start("auto", "ask").await;
    fixture.start_turn().await;
    fixture.approve_tool("approved", ApprovalScope::Once).await;
    fixture
        .runner
        .drive_approval(&mut fixture.link)
        .await
        .unwrap();
    let approved = fixture.command();
    let Message::ToolPermissionDecision {
        operation_id: approved_id,
        allowed: true,
        ..
    } = approved
    else {
        panic!("expected an allowed tool decision");
    };
    let replay = fixture.tool_request("approved");
    fixture
        .runner
        .handle_tool_permission(replay, &mut fixture.link, false)
        .await
        .unwrap();
    assert!(
        matches!(fixture.command(), Message::ToolPermissionDecision { operation_id, allowed: true, .. } if operation_id == approved_id)
    );

    let mut changed = fixture.tool_request("approved");
    let Message::ToolPermissionRequest { params, .. } = &mut changed else {
        unreachable!()
    };
    params["content"] = json!("different");
    assert!(fixture
        .runner
        .handle_tool_permission(changed, &mut fixture.link, false)
        .await
        .is_err());
    assert!(fixture.commands.try_recv().is_err());
    fixture.backend.close().await.unwrap();
}
