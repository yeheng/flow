//! Bounded value downloads using the same single-user token and projected run snapshot.
use axum::{
    body::{Body, Bytes},
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use flow_backend::journal::JournalBackend;
use flow_engine::journal_state::Run;
use flow_journal::{StoredValue, ValueRef};
use std::{io::Write, sync::Arc};

struct DownloadState {
    backend: Arc<JournalBackend>,
    token: String,
    slots: Arc<tokio::sync::Semaphore>,
}
pub fn router(
    backend: Arc<JournalBackend>,
    token: String,
) -> Result<Router, Box<dyn std::error::Error>> {
    if token.len() < 32 {
        return Err("download token must contain at least 32 bytes".into());
    }
    Ok(Router::new()
        .route("/runs/{run_id}/values/{output_id}", get(download))
        .with_state(Arc::new(DownloadState {
            backend,
            token,
            slots: Arc::new(tokio::sync::Semaphore::new(8)),
        })))
}
fn reference(run: &Run, id: &str) -> Option<ValueRef> {
    let mut values = vec![&run.input, &run.definition];
    values.extend(run.output.iter());
    for node in run.nodes.values() {
        values.extend(node.output.iter());
        if let Some(prepared) = &node.prepared {
            values.push(&prepared.input);
            values.push(&prepared.params);
            values.extend(prepared.predecessors.values());
        }
        if let Some(wait) = &node.wait {
            values.extend(wait.output.iter());
        }
        if let Some(op) = &node.operation {
            values.push(&op.request);
            values.extend(op.outcome.iter());
            if let Some(StoredValue::Inline(outcome)) = &op.outcome {
                if let Ok(raw) = serde_json::from_value::<ValueRef>(outcome["body_raw"].clone()) {
                    if raw.output_id == id {
                        return Some(raw);
                    }
                }
            }
        }
    }
    values.into_iter().find_map(|v| match v {
        StoredValue::Ref(r) if r.output_id == id => Some(r.clone()),
        _ => None,
    })
}
struct Chunks(tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>);
impl Write for Chunks {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        for bytes in bytes.chunks(flow_journal::CHUNK_BYTES) {
            self.0
                .blocking_send(Ok(Bytes::copy_from_slice(bytes)))
                .map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::BrokenPipe, "download disconnected")
                })?;
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
async fn download(
    State(state): State<Arc<DownloadState>>,
    Path((run_id, output_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let supplied = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .unwrap_or("");
    if supplied.len() != state.token.len()
        || supplied
            .bytes()
            .zip(state.token.bytes())
            .fold(0u8, |d, (a, b)| d | (a ^ b))
            != 0
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let permit = match state.slots.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => return StatusCode::TOO_MANY_REQUESTS.into_response(),
    };
    let (upper, row) = match state.backend.projection.get("run", &run_id).await {
        Ok(v) => v,
        Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };
    let run = match row.and_then(|v| serde_json::from_value::<Run>(v).ok()) {
        Some(v) => v,
        None => return StatusCode::NOT_FOUND.into_response(),
    };
    let value = match reference(&run, &output_id) {
        Some(v) if v.journal_id == state.backend.journal.id() => v,
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    let root = state.backend.journal.root().to_path_buf();
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tracing::info!(principal="local-token",%run_id,%output_id,action="value.download","JSONL value access");
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let mut chunks = Chunks(tx);
        if let Err(error) = flow_journal::value::read_value(&root, upper, &value, true, &mut chunks)
        {
            let _ = chunks
                .0
                .blocking_send(Err(std::io::Error::other(error.to_string())));
        }
    });
    let stream = futures::stream::unfold(rx, |mut rx| async {
        rx.recv().await.map(|item| (item, rx))
    });
    // No Content-Length: a corrupted or missing chunk propagates as a body-stream error.
    (
        [
            ("content-type", "application/octet-stream"),
            ("content-disposition", "attachment"),
            ("cache-control", "no-store"),
        ],
        Body::from_stream(stream),
    )
        .into_response()
}
