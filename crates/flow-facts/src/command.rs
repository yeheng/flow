//! Stable namespaces used in persisted command receipts.

const RUN_START_PREFIX: &str = "run.start:";
const SCHEDULE: &str = "schedule";

pub fn run_start_scope(source: &str, detail: &str, invocation: bool) -> String {
    let invocation = if invocation { "invocation:" } else { "" };
    format!("{RUN_START_PREFIX}{source}:{invocation}{detail}")
}

/// The schedule identifier encoded in a committed run-start receipt.
pub struct ScheduleScope<'a>(&'a str);

impl<'a> ScheduleScope<'a> {
    pub fn parse(scope: &'a str) -> Option<Self> {
        let (source, key) = scope.strip_prefix(RUN_START_PREFIX)?.split_once(':')?;
        (source == SCHEDULE).then_some(Self(key))
    }

    pub fn key(self) -> &'a str {
        self.0
    }
}
