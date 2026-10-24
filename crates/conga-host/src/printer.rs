//! EventPrinter: 消费 AgentEvent，渲染到注入的 writer（可测/可复用）。
use std::io::Write;

use conga::{AgentEvent, ContentDelta};

pub struct EventPrinter<W: Write> {
    out: W,
}

impl<W: Write> EventPrinter<W> {
    pub fn new(out: W) -> Self {
        Self { out }
    }

    pub fn on_event(&mut self, ev: &AgentEvent) {
        match ev {
            AgentEvent::MessageUpdate { delta } => match delta {
                ContentDelta::TextDelta(t) => {
                    let _ = self.out.write_all(t.as_bytes());
                }
                ContentDelta::ThinkingDelta(_) => {}
                ContentDelta::ToolCallDelta { .. } => {}
            },
            AgentEvent::ToolExecutionStart {
                tool_name, args, ..
            } => {
                let _ = writeln!(self.out, "\n-> {tool_name} {}", args);
            }
            AgentEvent::ToolExecutionEnd {
                result, is_error, ..
            } => {
                let first = result
                    .content
                    .iter()
                    .find_map(|b| match b {
                        conga::ContentBlock::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .unwrap_or("");
                let head = first.lines().next().unwrap_or("");
                let tag = if *is_error { "ERR" } else { "ok" };
                let _ = writeln!(self.out, "   [{tag}] {head}");
            }
            AgentEvent::AfterProviderResponse { response, .. } => {
                if let Some(u) = &response.usage {
                    // Cache segment only when the provider reported cache
                    // tokens (read↔write); most OpenAI-compat providers
                    // report none — keep their line unchanged.
                    let cr = u.cache_read_tokens.unwrap_or(0);
                    let cw = u.cache_write_tokens.unwrap_or(0);
                    let cache = if cr > 0 || cw > 0 {
                        format!(", cache: {cr}↔{cw}")
                    } else {
                        String::new()
                    };
                    let _ = writeln!(
                        self.out,
                        "\n[in: {}, out: {}{}]",
                        u.input_tokens, u.output_tokens, cache
                    );
                }
            }
            AgentEvent::TurnEnd { .. } => {
                let _ = writeln!(self.out);
            }
            AgentEvent::Error { message } => {
                let _ = writeln!(self.out, "\n[error] {message}");
            }
            _ => {}
        }
        let _ = self.out.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use conga::AssistantMessage;

    #[test]
    fn streams_text_delta() {
        let mut buf: Vec<u8> = Vec::new();
        let mut p = EventPrinter::new(&mut buf);
        p.on_event(&AgentEvent::MessageUpdate {
            delta: ContentDelta::TextDelta("Hi".into()),
        });
        p.on_event(&AgentEvent::MessageUpdate {
            delta: ContentDelta::TextDelta(" there".into()),
        });
        assert_eq!(String::from_utf8(buf).unwrap(), "Hi there");
    }

    #[test]
    fn error_event_renders() {
        let mut buf: Vec<u8> = Vec::new();
        let mut p = EventPrinter::new(&mut buf);
        p.on_event(&AgentEvent::Error {
            message: "boom".into(),
        });
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("[error] boom"));
    }

    #[test]
    fn flush_after_every_event() {
        // Vec<u8> flush is a no-op, but the call path must run without panic
        // and leave the buffer intact (pipes rely on the flush to drain).
        let mut buf: Vec<u8> = Vec::new();
        let mut p = EventPrinter::new(&mut buf);
        p.on_event(&AgentEvent::MessageUpdate {
            delta: ContentDelta::TextDelta("x".into()),
        });
        assert_eq!(String::from_utf8(buf).unwrap(), "x");
    }

    #[test]
    fn prints_usage_after_response() {
        let mut buf: Vec<u8> = Vec::new();
        let mut p = EventPrinter::new(&mut buf);
        let model = "m".to_string();
        let mut msg = AssistantMessage::new(&model);
        msg.usage = Some(conga::types::message::Usage {
            input_tokens: 42,
            output_tokens: 7,
            cache_read_tokens: Some(100),
            cache_write_tokens: Some(50),
        });
        let _ = msg.stop_reason;
        p.on_event(&AgentEvent::AfterProviderResponse {
            model: "m".into(),
            response: msg,
        });
        let s = String::from_utf8(buf).unwrap();
        assert!(
            s.contains("[in: 42, out: 7, cache: 100↔50]"),
            "cache-reported usage line: {s:?}"
        );

        // No cache breakdown (None/0) -> the classic two-field line.
        let mut buf: Vec<u8> = Vec::new();
        let mut p = EventPrinter::new(&mut buf);
        let mut msg = AssistantMessage::new(&model);
        msg.usage = Some(conga::types::message::Usage {
            input_tokens: 42,
            output_tokens: 7,
            cache_read_tokens: None,
            cache_write_tokens: None,
        });
        let _ = msg.stop_reason;
        p.on_event(&AgentEvent::AfterProviderResponse {
            model: "m".into(),
            response: msg,
        });
        let s = String::from_utf8(buf).unwrap();
        assert!(
            s.contains("[in: 42, out: 7]") && !s.contains("cache"),
            "cache must be omitted when unreported: {s:?}"
        );
    }
}
