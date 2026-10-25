//! Session-end cleanup for process-global tool state.
//!
//! Some tools keep per-session state in process-global registries (the
//! persistent `sh` in `tools/shell.rs`, PTYs in conga-ext's `terminal`
//! under the `terminal` feature). Those registries outlive individual
//! hosts/connections on purpose (a session resumes with its shell intact),
//! but they must not outlive the session itself: [`crate::session_api::delete_session`]
//! and the gateway's last-connection-close both funnel through
//! [`cleanup_session_resources`] here, which evicts the host-owned shell
//! and runs the hooks extension crates registered (e.g. terminal PTYs).

use std::sync::Arc;

use parking_lot::RwLock;

/// A cleanup hook: given a session id, kill/release whatever process-global
/// state that component keeps for it. Hooks must be quick (called inline
/// from async contexts) and idempotent.
pub type SessionCleanupHook = Arc<dyn Fn(&str) + Send + Sync>;

static HOOKS: RwLock<Vec<SessionCleanupHook>> = RwLock::new(Vec::new());

/// Register a hook invoked by [`run_hooks`] for every removed session.
/// Registration is one-way (process-global, like the state it cleans).
pub fn register_hook(hook: SessionCleanupHook) {
    HOOKS.write().push(hook);
}

/// Run all registered hooks for `session_id`.
pub fn run_hooks(session_id: &str) {
    let hooks = HOOKS.read().clone();
    for h in hooks {
        h(session_id);
    }
}

/// Kill a session's process-global tool state: the persistent shell (owned
/// by this crate) plus anything registered via [`register_hook`] (e.g.
/// conga-ext terminal PTYs). Safe to call for sessions that never had any
/// such state.
pub async fn cleanup_session_resources(session_id: &str) {
    crate::tools::shell::evict_session(session_id).await;
    run_hooks(session_id);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hooks_run_for_session_id() {
        static SEEN: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
        register_hook(Arc::new(|sid| SEEN.lock().unwrap().push(sid.to_string())));
        run_hooks("sess-x");
        assert!(SEEN.lock().unwrap().contains(&"sess-x".to_string()));
    }
}
