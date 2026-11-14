//! Shared addressing for transient interactions. Durable commands retain journal semantics.
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    pub component: String,
    pub instance: String,
    pub scope: String,
}
impl Endpoint {
    pub fn validate(&self) -> Result<(), &'static str> {
        for value in [&self.component, &self.instance, &self.scope] {
            if value.is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
                return Err("invalid endpoint identity");
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    Command,
    Event,
    Query,
    Response,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InteractionMessage {
    pub id: String,
    pub kind: MessageKind,
    pub topic: String,
    pub correlation_id: Option<String>,
    pub payload: Value,
}
impl InteractionMessage {
    pub fn validate(&self) -> Result<(), &'static str> {
        for value in [
            Some(self.id.as_str()),
            Some(self.topic.as_str()),
            self.correlation_id.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if value.is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
                return Err("invalid message identity");
            }
        }
        if serde_json::to_vec(self)
            .map_err(|_| "invalid message")?
            .len()
            > 65536
        {
            return Err("message too large");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_identity_and_payload_budget() {
        let mut message = InteractionMessage {
            id: "1".into(),
            kind: MessageKind::Query,
            topic: "status".into(),
            correlation_id: None,
            payload: Value::Null,
        };
        assert!(message.validate().is_ok());
        message.topic = "\n".into();
        assert!(message.validate().is_err());
        message.topic = "status".into();
        message.payload = Value::String("x".repeat(65536));
        assert!(message.validate().is_err());
        assert!(Endpoint {
            component: "plugin".into(),
            instance: String::new(),
            scope: "workspace".into()
        }
        .validate()
        .is_err());
    }
}
