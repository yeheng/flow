//! 单一 RPC 方法目录：注册、准入与穷尽分发共用同一份清单。
//!
//! 这份目录存在的唯一目的是**避免三处手写方法名**。方法名一旦分散在
//! 「注册表」「准入校验」「分发 match」三处，任何新增或改名都会漏改一处，
//! 症状是「方法注册了但调不通」或者「调用了但没注册」。
//!
//! `methods!` 宏把每个方法写成 `变体 => "线上的名字"`，并同时派生：
//!
//! - `Method` 枚举与其 `as_str` / `parse` 互逆映射；
//! - `is_write`：**读写分类**是准入策略的依据（写方法需要令牌、需要串行化），
//!   所以分类必须与「这个方法存在吗」同源。
//! - `READ_METHODS` / `WRITE_METHODS` 两个常量切片，供准入与注册直接使用。
//!
//! 注意 `parse` 对未知方法返回 `None` 而不是报错：未注册的方法必须在
//! 准入层被当作「未知」处理，而不是 panic 或误判为读方法。
macro_rules! methods {
    (read { $($read:ident => $rname:literal,)* } write { $($write:ident => $wname:literal,)* }) => {
        /// 全部 RPC 方法的枚举。
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum Method { $($read,)* $($write,)* }
        impl Method {
            /// 全部方法的清单，注册时逐项枚举。
            pub const ALL: &'static [Self] = &[$(Self::$read,)* $(Self::$write,)*];
            /// 方法的线上名称。
            pub const fn as_str(self) -> &'static str {
                match self { $(Self::$read => $rname,)* $(Self::$write => $wname,)* }
            }
            /// 从线上名称反查方法；未知名称返回 None。
            pub fn parse(name: &str) -> Option<Self> {
                match name { $($rname => Some(Self::$read),)* $($wname => Some(Self::$write),)* _ => None }
            }
            /// 该方法是否修改权威状态。
            pub const fn is_write(self) -> bool {
                match self { $(Self::$read => false,)* $(Self::$write => true,)* }
            }
        }
        /// 全部只读方法的线上名称。
        pub const READ_METHODS: &[&str] = &[$($rname,)*];
        /// 全部写方法的线上名称。
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

/// 方法名是否属于写方法。
///
/// 未注册的方法返回 `false`：准入层对未知方法的处理是「直接拒绝」，
/// 这里必须与之一致，不能把未知方法误判成读方法而放行。
pub fn is_write_method(method: &str) -> bool {
    Method::parse(method).is_some_and(Method::is_write)
}

/// 方法名是否属于只读方法（已注册且非写方法）。
pub fn is_read_method(method: &str) -> bool {
    Method::parse(method).is_some_and(|method| !method.is_write())
}
