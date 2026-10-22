//! Shared RPC method catalogue. Subscription methods have their own registration.
pub const READ_METHODS: &[&str] = &[
    "journal.cluster.status",
    "journal.cluster.nodes",
    "run.observations.page",
    "command.status",
    "workflow.list",
    "run.list",
    "legacy.list",
    "workflow.get",
    "run.get",
    "run.compare",
    "legacy.get",
    "run.events.page",
    "run.audit.page",
    "workflow.validate",
    "workflow.get.view",
    "workflow.list.view",
    "workflow.versions",
    "run.get.view",
    "run.list.view",
    "run.stats",
    "run.timeline",
    "run.events.view",
    "schedule.list",
    "webhook.list",
    "template.list",
    "template.get",
    "config.get",
    "secrets.list",
    "nodetypes.list",
];

pub const WRITE_METHODS: &[&str] = &[
    "journal.cluster.remove",
    "workflow.import",
    "workflow.create",
    "workflow.update",
    "workflow.publish",
    "workflow.delete",
    "run.start",
    "run.regress",
    "run.cancel",
    "run.signal",
    "run.adjudicate",
    "schedule.change",
    "schedule.create",
    "schedule.update",
    "schedule.delete",
    "webhook.change",
    "webhook.create",
    "webhook.set_enabled",
    "webhook.delete",
    "template.create",
    "template.update",
    "template.delete",
    "secrets.set",
    "secrets.delete",
    "config.update",
];
pub fn is_write_method(method: &str) -> bool {
    WRITE_METHODS.contains(&method)
}

/// 方法是否为读面（写命令需要 request_id 幂等键与回执解包）。
pub fn is_read_method(method: &str) -> bool {
    READ_METHODS.contains(&method)
}
