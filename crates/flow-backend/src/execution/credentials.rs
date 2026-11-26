//! Master-side credential lookup for a dispatch's frozen environment.

use std::future::Future;

use crate::secrets::{get_secret, ENVIRONMENT_CREDENTIALS};
use flow_config::plugin_manifest::Widget;
use flow_dispatcher::execution_protocol::message::PermitCredential;
use flow_domain::journal_state::Run;
use flow_dto::permission::OperationSpec;
use flow_journal::StoredValue;
use serde_json::Value;

use crate::journal::{JournalBackend, JournalError};

pub(super) async fn with_run_scope<T>(
    backend: &JournalBackend,
    run: &Run,
    drive: impl Future<Output = Result<T, JournalError>>,
) -> Result<T, JournalError> {
    let Some(binding) = run
        .execution
        .as_ref()
        .and_then(|run| run.environment.as_ref())
    else {
        return drive.await;
    };
    let store = backend
        .environments
        .get()
        .ok_or_else(|| flow_journal::Error::Invalid("environment store unavailable".into()))?;
    let credentials = store
        .credentials(binding)
        .map_err(flow_journal::Error::Invalid)?;
    ENVIRONMENT_CREDENTIALS.scope(credentials, drive).await
}

/// Resolve the named credentials after the authorization fact has committed.
pub(super) fn from_specs(specs: &[OperationSpec]) -> Vec<PermitCredential> {
    specs
        .iter()
        .filter_map(|spec| match spec {
            OperationSpec::Secret { secret_name } => credential(secret_name),
            _ => None,
        })
        .collect()
}

/// Recover the credential reference from a committed operation's inline request.
pub(super) fn from_resumed_request(request: &StoredValue) -> Vec<PermitCredential> {
    let StoredValue::Inline(request) = request else {
        return Vec::new();
    };
    request["credential"]["secret_ref"]
        .as_str()
        .and_then(credential)
        .into_iter()
        .collect()
}

/// Plugin nodes with manifest-defined needs resolve the declared secret fields.
pub(super) fn from_allowed_request(
    node_type: &str,
    specs: &[OperationSpec],
    params: &Value,
) -> Vec<PermitCredential> {
    if !specs.is_empty() {
        return from_specs(specs);
    }
    let Some(plugin) = crate::plugin_registry::discover_plugins()
        .into_iter()
        .find(|plugin| {
            plugin
                .manifest
                .ui
                .params_forms
                .iter()
                .any(|form| form.node_type == node_type)
        })
    else {
        return Vec::new();
    };
    plugin
        .manifest
        .ui
        .params_forms
        .iter()
        .filter(|form| form.node_type == node_type)
        .flat_map(|form| form.fields.iter())
        .filter(|field| matches!(field.widget, Widget::Secret))
        .filter_map(|field| params.get(&field.key).and_then(Value::as_str))
        .filter_map(credential)
        .collect()
}

fn credential(name: &str) -> Option<PermitCredential> {
    get_secret(name)
        .filter(|value| !value.is_empty())
        .map(|value| PermitCredential {
            name: name.to_owned(),
            value,
        })
}
