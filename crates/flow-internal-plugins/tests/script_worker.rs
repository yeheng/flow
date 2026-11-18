use std::time::Duration;

use flow_dispatcher::{execution_protocol::ExecutorInvocation, Dispatcher, IpcOptions, Job};
use flow_internal_plugins::script::{JsDeterminism, ScriptTask, SCRIPT_V1};
use serde_json::{json, Value};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn standalone_script_streams_large_values_and_preserves_prepared_context() {
    let dispatcher = Dispatcher::new_streaming(
        IpcOptions {
            executor: ExecutorInvocation::explicit(env!("CARGO_BIN_EXE_flow-script-worker")),
            x_max: 1,
            tag: None,
        },
        SCRIPT_V1,
    );
    let task = ScriptTask {
        code:
            "return {text: input.text, node: nodes.first, now: Date.now(), random: Math.random()};"
                .into(),
        input: json!({"text":"中".repeat(150_000)}),
        nodes: json!({"first": 42}),
        timeout_ms: None,
        determinism: Some(JsDeterminism {
            clock_ms: 12345,
            seed: 1,
        }),
    };
    let input = serde_json::to_vec(&task).unwrap();
    assert!(input.len() > flow_dispatcher::execution_protocol::TRANSFER_CHUNK_BYTES);
    let completion = dispatcher
        .execute(
            Job {
                id: "script".into(),
                handler: SCRIPT_V1.into(),
                input,
            },
            Instant::now() + Duration::from_secs(10),
            &CancellationToken::new(),
            |_| async { Ok(()) },
        )
        .await
        .unwrap();
    let output = completion.outcome.unwrap();
    assert!(output.len() > flow_dispatcher::execution_protocol::TRANSFER_CHUNK_BYTES);
    let value: Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(
        value,
        json!({"text": task.input["text"], "node": 42, "now": 12345, "random": 270369.0 / 4294967296.0})
    );

    let looping = ScriptTask {
        code: "while (true) {}".into(),
        timeout_ms: Some(10),
        ..task
    };
    let timed_out = dispatcher
        .execute(
            Job {
                id: "timeout".into(),
                handler: SCRIPT_V1.into(),
                input: serde_json::to_vec(&looping).unwrap(),
            },
            Instant::now() + Duration::from_secs(10),
            &CancellationToken::new(),
            |_| async { Ok(()) },
        )
        .await
        .unwrap();
    assert!(timed_out.outcome.is_err());
    assert_eq!(
        completion.worker_pid, timed_out.worker_pid,
        "bounded business failure keeps the worker usable"
    );
    dispatcher.shutdown().await;
}
