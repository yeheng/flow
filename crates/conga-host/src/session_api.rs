//! Shared session-store API: list / messages / rename / delete / search.
//!
//! The gateway's axum handlers and the desktop app's Tauri commands are
//! thin transports over these functions — one validation rule, one DTO
//! shape, one fail-loud policy. Transport-specific concerns (HTTP status
//! codes, active-connection checks, IPC errors) stay in the transports.

use std::path::Path;

use conga::{AgentMessage, EventStorage, SessionMeta};

use crate::session::SessionManager;

/// One listed session (id, message count, display name, mtime in ms).
#[derive(Debug, Clone, serde::Serialize)]
pub struct SessionListItem {
    pub id: String,
    pub msg_count: usize,
    pub name: Option<String>,
    /// Milliseconds since UNIX epoch; 0 when the file has no mtime.
    pub mtime_ms: u64,
}

/// A session-API failure, mapped by transports: `BadRequest` → HTTP 400,
/// `NotFound` → 404, `Internal` → 500 (fail loud, never silently adopted).
#[derive(Debug)]
pub enum SessionApiError {
    BadRequest(String),
    NotFound(String),
    Internal(String),
}

impl std::fmt::Display for SessionApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SessionApiError::BadRequest(m) => write!(f, "{m}"),
            SessionApiError::NotFound(m) => write!(f, "{m}"),
            SessionApiError::Internal(m) => write!(f, "{m}"),
        }
    }
}

/// List all sessions on disk, newest first. Does NOT depend on live
/// connections — reads the JSONL store directly, so it also serves sessions
/// created by the CLI or other devices.
pub async fn list_sessions(store_root: &Path) -> Result<Vec<SessionListItem>, SessionApiError> {
    let mgr = SessionManager::with_root(store_root.to_path_buf());
    let mut sessions = mgr
        .list()
        .await
        .map_err(|e| SessionApiError::Internal(e.to_string()))?;
    // Newest first.
    sessions.sort_by_key(|s| std::cmp::Reverse(s.mtime));
    Ok(sessions
        .into_iter()
        .map(|s| SessionListItem {
            id: s.id,
            msg_count: s.msg_count,
            name: s.name,
            mtime_ms: s
                .mtime
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        })
        .collect())
}

/// Backend-truth transcript for a session: `derive_messages` over the
/// on-disk event log, migrating a legacy `messages.jsonl` once. `Ok(None)`
/// means the session has no on-disk data (a local-only chat). A corrupt log
/// is `Internal` (fail loud, never silently adopt).
pub async fn session_messages(
    store_root: &Path,
    session_id: &str,
) -> Result<Option<Vec<AgentMessage>>, SessionApiError> {
    let storage = EventStorage::new(store_root.to_path_buf());
    if !storage.has_events(session_id) && !storage.messages_path(session_id).exists() {
        return Ok(None);
    }
    let mgr = SessionManager::with_root(store_root.to_path_buf());
    let events = mgr
        .open_or_migrate(session_id)
        .await
        .map_err(|e| SessionApiError::Internal(e.to_string()))?;
    Ok(Some(conga::derive_messages(&events)))
}

/// Session-wide prompt-cache accounting, folded from the same on-disk
/// event log (usage rows on Assistant events). `Ok(None)` = no on-disk
/// data (same contract as [`session_messages`]); read-only observability —
/// the hit rate is a fact about the log, recomputed on demand.
pub async fn session_cache_stats(
    store_root: &Path,
    session_id: &str,
) -> Result<Option<conga::CacheStats>, SessionApiError> {
    let storage = EventStorage::new(store_root.to_path_buf());
    if !storage.has_events(session_id) && !storage.messages_path(session_id).exists() {
        return Ok(None);
    }
    let mgr = SessionManager::with_root(store_root.to_path_buf());
    let events = mgr
        .open_or_migrate(session_id)
        .await
        .map_err(|e| SessionApiError::Internal(e.to_string()))?;
    Ok(Some(conga::cache_stats(&events)))
}

/// Persist the session's display name in its `meta.json` sidecar. Creates
/// the session directory if needed, so a chat can be named before its first
/// turn lands on disk. Bad id/name → `BadRequest`.
pub async fn rename_session(
    store_root: &Path,
    session_id: &str,
    name: &str,
) -> Result<(), SessionApiError> {
    if !conga::is_valid_session_id(session_id) {
        return Err(SessionApiError::BadRequest("invalid session id".into()));
    }
    let name = name.trim();
    if name.is_empty() || name.chars().count() > 200 {
        return Err(SessionApiError::BadRequest(
            "name must be 1..=200 chars".into(),
        ));
    }
    EventStorage::new(store_root.to_path_buf())
        .write_meta(
            session_id,
            &SessionMeta {
                name: Some(name.to_string()),
            },
        )
        .await
        .map_err(|e| SessionApiError::Internal(e.to_string()))
}

/// Delete a session's on-disk data wholesale (event log + meta sidecar +
/// per-tool state) and kill its process-global tool state (persistent
/// shell, extension PTYs via cleanup hooks). `Ok(false)` = never existed
/// (`NotFound` is the caller's choice of framing; this returns the fact).
/// Transports that track live connections refuse the delete themselves
/// BEFORE calling this.
pub async fn delete_session(store_root: &Path, session_id: &str) -> Result<bool, SessionApiError> {
    if !conga::is_valid_session_id(session_id) {
        return Err(SessionApiError::BadRequest("invalid session id".into()));
    }
    let removed = EventStorage::new(store_root.to_path_buf())
        .remove_session(session_id)
        .await
        .map_err(|e| SessionApiError::Internal(e.to_string()))?;
    if removed {
        // The session is gone; its shell/PTYs must not linger in the
        // host process. Only on actual removal - a failed delete leaves
        // the session (and its state) intact for a retry.
        crate::session_cleanup::cleanup_session_resources(session_id).await;
    }
    Ok(removed)
}

/// Full-text search across all session event logs (FTS5 sidecar index).
/// Async (sqlx) — callers just `.await`. Runs the incremental high-water
/// reindex check first, then the query — per-call reindex is an incremental
/// stat check, not a full rebuild. Empty/blank q → BadRequest.
#[cfg(feature = "session-index")]
pub async fn search_sessions(
    store_root: &Path,
    index_db: &Path,
    q: &str,
    limit: usize,
) -> Result<Vec<crate::session_index::SessionHit>, SessionApiError> {
    let q = q.trim();
    if q.is_empty() {
        return Err(SessionApiError::BadRequest("q must be non-empty".into()));
    }
    crate::session_index::reindex(store_root, index_db)
        .await
        .map_err(|e| SessionApiError::Internal(e.to_string()))?;
    crate::session_index::search(store_root, index_db, q, limit)
        .await
        .map_err(|e| SessionApiError::Internal(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use conga::SessionEvent;

    fn user_event(text: &str) -> SessionEvent {
        use conga::{AgentMessage, ContentBlock, UserMessage};
        SessionEvent::User(AgentMessage::User(UserMessage {
            content: vec![ContentBlock::text(text)],
            timestamp: 1,
        }))
    }

    #[tokio::test]
    async fn messages_none_for_unknown_session_some_after_write() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(session_messages(tmp.path(), "ghost")
            .await
            .unwrap()
            .is_none());
        EventStorage::new(tmp.path().to_path_buf())
            .append_event("sess-1", &user_event("hello"))
            .await
            .unwrap();
        let msgs = session_messages(tmp.path(), "sess-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msgs.len(), 1);
    }

    #[tokio::test]
    async fn rename_validates_then_persists() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(matches!(
            rename_session(tmp.path(), "../evil", "ok").await,
            Err(SessionApiError::BadRequest(_))
        ));
        assert!(matches!(
            rename_session(tmp.path(), "sess-1", "  ").await,
            Err(SessionApiError::BadRequest(_))
        ));
        rename_session(tmp.path(), "sess-1", " Release notes ")
            .await
            .unwrap();
        let items = list_sessions(tmp.path()).await.unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].name.as_deref(), Some("Release notes"));
    }

    #[tokio::test]
    async fn delete_reports_existence() {
        let tmp = tempfile::tempdir().unwrap();
        EventStorage::new(tmp.path().to_path_buf())
            .append_event("sess-1", &user_event("x"))
            .await
            .unwrap();
        assert!(delete_session(tmp.path(), "sess-1").await.unwrap());
        assert!(!delete_session(tmp.path(), "sess-1").await.unwrap());
        assert!(!EventStorage::new(tmp.path().to_path_buf()).has_events("sess-1"));
    }

    #[tokio::test]
    async fn delete_runs_cleanup_hooks_for_removed_session() {
        static SEEN: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
        crate::session_cleanup::register_hook(std::sync::Arc::new(|sid| {
            SEEN.lock().unwrap().push(sid.to_string())
        }));
        let tmp = tempfile::tempdir().unwrap();
        EventStorage::new(tmp.path().to_path_buf())
            .append_event("sess-hook", &user_event("x"))
            .await
            .unwrap();
        delete_session(tmp.path(), "sess-hook").await.unwrap();
        assert!(
            SEEN.lock().unwrap().contains(&"sess-hook".to_string()),
            "delete must run cleanup hooks for the removed session"
        );
    }
}
