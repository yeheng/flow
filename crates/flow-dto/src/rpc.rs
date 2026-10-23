//! One typed catalogue for registration, admission and exhaustive dispatch.
macro_rules! methods {
    (read { $($read:ident => $rname:literal,)* } write { $($write:ident => $wname:literal,)* }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum Method { $($read,)* $($write,)* }
        impl Method {
            pub const ALL: &'static [Self] = &[$(Self::$read,)* $(Self::$write,)*];
            pub const fn as_str(self) -> &'static str {
                match self { $(Self::$read => $rname,)* $(Self::$write => $wname,)* }
            }
            pub fn parse(name: &str) -> Option<Self> {
                match name { $($rname => Some(Self::$read),)* $($wname => Some(Self::$write),)* _ => None }
            }
            pub const fn is_write(self) -> bool {
                match self { $(Self::$read => false,)* $(Self::$write => true,)* }
            }
        }
        pub const READ_METHODS: &[&str] = &[$($rname,)*];
        pub const WRITE_METHODS: &[&str] = &[$($wname,)*];
    };
}
methods! {
    read {
        RunObservationsPage => "run.observations.page",
        CommandStatus => "command.status",
        WorkflowList => "workflow.list",
        RunList => "run.list",
        LegacyList => "legacy.list",
        WorkflowGet => "workflow.get",
        RunGet => "run.get",
        RunCompare => "run.compare",
        LegacyGet => "legacy.get",
        RunEventsPage => "run.events.page",
        RunAuditPage => "run.audit.page",
        WorkflowValidate => "workflow.validate",
        WorkflowGetView => "workflow.get.view",
        WorkflowListView => "workflow.list.view",
        WorkflowVersions => "workflow.versions",
        RunGetView => "run.get.view",
        RunListView => "run.list.view",
        RunStats => "run.stats",
        RunTimeline => "run.timeline",
        RunEventsView => "run.events.view",
        ScheduleList => "schedule.list",
        WebhookList => "webhook.list",
        TemplateList => "template.list",
        TemplateGet => "template.get",
        ConfigGet => "config.get",
        SecretsList => "secrets.list",
        NodetypesList => "nodetypes.list",
    }
    write {
        WorkflowImport => "workflow.import",
        WorkflowCreate => "workflow.create",
        WorkflowUpdate => "workflow.update",
        WorkflowPublish => "workflow.publish",
        WorkflowDelete => "workflow.delete",
        RunStart => "run.start",
        RunRegress => "run.regress",
        RunCancel => "run.cancel",
        RunSignal => "run.signal",
        RunAdjudicate => "run.adjudicate",
        ScheduleChange => "schedule.change",
        ScheduleCreate => "schedule.create",
        ScheduleUpdate => "schedule.update",
        ScheduleDelete => "schedule.delete",
        WebhookChange => "webhook.change",
        WebhookCreate => "webhook.create",
        WebhookSetEnabled => "webhook.set_enabled",
        WebhookDelete => "webhook.delete",
        TemplateCreate => "template.create",
        TemplateUpdate => "template.update",
        TemplateDelete => "template.delete",
        SecretsSet => "secrets.set",
        SecretsDelete => "secrets.delete",
        ConfigUpdate => "config.update",
    }
}
pub fn is_write_method(method: &str) -> bool {
    Method::parse(method).is_some_and(Method::is_write)
}
pub fn is_read_method(method: &str) -> bool {
    Method::parse(method).is_some_and(|method| !method.is_write())
}
