//! Parser SSE Claude menjadi [`Event`] native.
//!
//! Menangani KEDUA bentuk SSE yang di-emit endpoint web:
//! - Legacy: `{"type":"completion","completion":"<chunk>","stop_reason":...}`
//! - Modern gaya Anthropic: `message_start` -> `content_block_start` ->
//!   `content_block_delta` -> `content_block_stop` -> `message_delta` ->
//!   `message_stop`.
//!
//! Pemetaan delta modern:
//! - `text_delta` -> [`Event::Text`]
//! - `thinking_delta` / `thinking_summary_delta` /
//!   `thinking_cut_off_delta` -> [`Event::Reasoning`]
//! - `input_json_delta` -> diabaikan (tidak ada tool calling di v0.2)
//! - `signature_delta` -> diabaikan
//! - blok `tool_use`/`tool_result` web-search + chip sitasi ->
//!   [`Event::Annotation`]
//!
//! Parser ini murni (tanpa I/O): satu [`Value`] JSON per baris SSE
//! masuk, nol atau lebih [`Event`] keluar.

use crate::core::events::{Annotation, Event};
use crate::error::ProviderError;
use serde_json::Value;

/// Estimasi token dari jumlah karakter (backend web tidak
/// mengembalikan hitungan token asli; semua usage estimasi).
fn estimate_tokens(chars: u64) -> u64 {
    chars.div_ceil(4)
}

/// Parser stateful untuk satu stream completion Claude.
#[derive(Debug, Default)]
pub struct SseParser {
    text_chars: u64,
    reasoning_chars: u64,
    done: bool,
    stop_reason: Option<String>,
    /// Benar bila blok `tool_use` web-search terakhir belum
    /// diikuti `tool_result`-nya.
    search_tool_pending: bool,
}

impl SseParser {
    /// Parser baru (belum ada delta).
    pub fn new() -> Self {
        Self::default()
    }

    /// Benar bila stream sudah mencapai akhir
    /// (`message_stop` / legacy `stop_reason` non-null).
    pub fn is_done(&self) -> bool {
        self.done
    }

    /// `stop_reason` terakhir dari `message_delta` / legacy
    /// `completion` (bila ada).
    pub fn stop_reason(&self) -> Option<&str> {
        self.stop_reason.as_deref()
    }

    /// Event [`Event::Usage`] akhir: estimasi dari karakter yang
    /// ter-emit (`prompt_chars` = panjang prompt yang dikirim).
    pub fn usage_event(&self, prompt_chars: usize) -> Event {
        Event::Usage {
            prompt: estimate_tokens(prompt_chars as u64),
            completion: estimate_tokens(self.text_chars),
            reasoning: estimate_tokens(self.reasoning_chars),
            estimated: true,
        }
    }

    /// Proses satu nilai JSON SSE; kembalikan event yang dihasilkan.
    /// Error stream (`type: "error"`) menjadi `Err`.
    pub fn feed(&mut self, v: &Value) -> Result<Vec<Event>, ProviderError> {
        let mut events: Vec<Event> = Vec::new();
        let ev_type = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
        match ev_type {
            // --- Bentuk legacy ---
            "completion" => {
                if let Some(text) = v.get("completion").and_then(|t| t.as_str()) {
                    if !text.is_empty() {
                        self.text_chars += text.chars().count() as u64;
                        events.push(Event::Text(text.to_string()));
                    }
                }
                if let Some(sr) = v.get("stop_reason") {
                    if !sr.is_null() {
                        self.stop_reason = sr.as_str().map(str::to_string);
                        self.done = true;
                    }
                }
            }
            // --- Bentuk modern ---
            "message_start" => {}
            "content_block_start" => {
                if let Some(block) = v.get("content_block") {
                    events.extend(citations_from(block));
                    match block.get("type").and_then(|t| t.as_str()) {
                        Some("tool_use") => {
                            let name = block.get("name").and_then(|n| n.as_str()).unwrap_or("");
                            // Hanya blok search yang dikumpulkan; tool
                            // lain (artifacts, repl, ...) bukan anotasi.
                            // Nama di sini berasal dari stream itu
                            // sendiri — bukan nama ter-obfuscasi yang
                            // di-hardcode.
                            if name.to_lowercase().contains("search") {
                                self.search_tool_pending = true;
                                events.push(Event::Annotation(Annotation {
                                    kind: "tool_use".to_string(),
                                    text: format!("web search: {name}"),
                                    url: None,
                                }));
                            }
                        }
                        Some("tool_result") if self.search_tool_pending => {
                            self.search_tool_pending = false;
                            events.push(Event::Annotation(Annotation {
                                kind: "search_result".to_string(),
                                text: "web search result".to_string(),
                                url: None,
                            }));
                        }
                        _ => {}
                    }
                }
            }
            "content_block_delta" => {
                if let Some(delta) = v.get("delta") {
                    events.extend(citations_from(delta));
                    match delta.get("type").and_then(|t| t.as_str()) {
                        Some("text_delta") => {
                            if let Some(text) = delta.get("text").and_then(|t| t.as_str()) {
                                if !text.is_empty() {
                                    self.text_chars += text.chars().count() as u64;
                                    events.push(Event::Text(text.to_string()));
                                }
                            }
                        }
                        Some("thinking_delta") => {
                            if let Some(t) = delta.get("thinking").and_then(|t| t.as_str()) {
                                if !t.is_empty() {
                                    self.reasoning_chars += t.chars().count() as u64;
                                    events.push(Event::Reasoning(t.to_string()));
                                }
                            }
                        }
                        Some("thinking_summary_delta") => {
                            let summary = delta
                                .get("summary")
                                .and_then(|s| s.as_str())
                                .or_else(|| delta.get("thinking").and_then(|s| s.as_str()))
                                .unwrap_or("");
                            if !summary.is_empty() {
                                self.reasoning_chars += summary.chars().count() as u64;
                                events.push(Event::Reasoning(summary.to_string()));
                            }
                        }
                        Some("thinking_cut_off_delta") => {
                            // Cut-off tidak membawa teks; beri penanda
                            // singkat (atau ringkasan bila ada) agar
                            // pemotongan thinking tercatat, bukan hilang.
                            let summary = delta
                                .get("summary")
                                .and_then(|s| s.as_str())
                                .unwrap_or("[thinking cut off]");
                            self.reasoning_chars += summary.chars().count() as u64;
                            events.push(Event::Reasoning(summary.to_string()));
                        }
                        // v0.2 tidak punya tool calling: argumen JSON
                        // tool yang masuk dibuang dengan sengaja.
                        Some("input_json_delta") => {}
                        Some("signature_delta") => {}
                        _ => {}
                    }
                }
            }
            "content_block_stop" => {}
            "message_delta" => {
                if let Some(delta) = v.get("delta") {
                    if let Some(sr) = delta.get("stop_reason").and_then(|s| s.as_str()) {
                        self.stop_reason = Some(sr.to_string());
                    }
                    events.extend(citations_from(delta));
                }
            }
            "message_stop" => {
                self.done = true;
            }
            "error" => {
                let msg = v
                    .pointer("/error/message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("claude stream error");
                if msg.contains("thinking_messages_rate_limit_exceeded") {
                    return Err(ProviderError::RateLimited {
                        retry_after: None,
                        message: "Claude thinking-message rate limit exceeded \
                            (limit pesan thinking mingguan) — tunggu reset \
                            mingguan atau matikan thinking"
                            .to_string(),
                    });
                }
                return Err(ProviderError::upstream(502, msg));
            }
            // conversation_ready, ping, compaction_status,
            // cache_performance, tool_approval, mcp_auth_required:
            // tidak ada penanganan v0.2 — diabaikan.
            _ => {}
        }
        Ok(events)
    }
}

/// Ekstrak chip sitasi (`citations` array / `citation` tunggal)
/// dari satu objek JSON menjadi event [`Event::Annotation`].
fn citations_from(v: &Value) -> Vec<Event> {
    let mut out: Vec<Event> = Vec::new();
    if let Some(arr) = v.get("citations").and_then(|c| c.as_array()) {
        for c in arr {
            out.push(citation_event(c));
        }
    }
    if let Some(c) = v.get("citation").filter(|c| c.is_object()) {
        out.push(citation_event(c));
    }
    out
}

/// Satu objek sitasi -> [`Event::Annotation`].
fn citation_event(c: &Value) -> Event {
    let url = c.get("url").and_then(|u| u.as_str()).map(str::to_string);
    let text = c
        .get("text")
        .and_then(|t| t.as_str())
        .or_else(|| c.get("title").and_then(|t| t.as_str()))
        .unwrap_or("citation")
        .to_string();
    Event::Annotation(Annotation {
        kind: "citation".to_string(),
        text,
        url,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(line: &str) -> Value {
        serde_json::from_str(line).expect("fixture SSE harus JSON valid")
    }

    #[test]
    fn legacy_sse_shape_parsing() {
        let mut p = SseParser::new();
        let ev = p
            .feed(&parse(
                r#"{"type":"completion","completion":"Halo ","stop_reason":null}"#,
            ))
            .expect("feed gagal");
        assert_eq!(ev.len(), 1);
        assert!(matches!(ev[0], Event::Text(ref t) if t == "Halo "));
        assert!(!p.is_done());

        let ev = p
            .feed(&parse(
                r#"{"type":"completion","completion":"dunia","stop_reason":"end_turn"}"#,
            ))
            .expect("feed gagal");
        assert_eq!(ev.len(), 1);
        assert!(matches!(ev[0], Event::Text(ref t) if t == "dunia"));
        assert!(p.is_done());
        assert_eq!(p.stop_reason(), Some("end_turn"));
    }

    #[test]
    fn modern_content_block_sse_parsing() {
        let mut p = SseParser::new();
        // message_start: tidak menghasilkan event.
        let ev = p
            .feed(&parse(
                r#"{"type":"message_start","message":{"id":"msg_1"}}"#,
            ))
            .expect("feed gagal");
        assert!(ev.is_empty());

        // thinking_delta -> Reasoning, text_delta -> Text.
        let ev = p
            .feed(&parse(
                r#"{"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"hmm"}}"#,
            ))
            .expect("feed gagal");
        assert_eq!(ev.len(), 1);
        assert!(matches!(ev[0], Event::Reasoning(ref t) if t == "hmm"));

        let ev = p
            .feed(&parse(
                r#"{"type":"content_block_delta","delta":{"type":"text_delta","text":"Jawab"}}"#,
            ))
            .expect("feed gagal");
        assert_eq!(ev.len(), 1);
        assert!(matches!(ev[0], Event::Text(ref t) if t == "Jawab"));

        // input_json_delta diabaikan (tidak ada tool calling di v0.2).
        let ev = p
            .feed(&parse(
                r#"{"type":"content_block_delta","delta":{"type":"input_json_delta","partial_json":"{}"}}"#,
            ))
            .expect("feed gagal");
        assert!(ev.is_empty());

        // signature_delta juga diabaikan.
        let ev = p
            .feed(&parse(
                r#"{"type":"content_block_delta","delta":{"type":"signature_delta","signature":"abc"}}"#,
            ))
            .expect("feed gagal");
        assert!(ev.is_empty());

        // message_delta membawa stop_reason; message_stop menutup stream.
        let ev = p
            .feed(&parse(
                r#"{"type":"message_delta","delta":{"stop_reason":"max_tokens"}}"#,
            ))
            .expect("feed gagal");
        assert!(ev.is_empty());
        assert_eq!(p.stop_reason(), Some("max_tokens"));
        assert!(!p.is_done());
        let ev = p
            .feed(&parse(r#"{"type":"message_stop"}"#))
            .expect("feed gagal");
        assert!(ev.is_empty());
        assert!(p.is_done());
    }

    #[test]
    fn thinking_summary_and_cut_off_become_reasoning() {
        let mut p = SseParser::new();
        let ev = p
            .feed(&parse(
                r#"{"type":"content_block_delta","delta":{"type":"thinking_summary_delta","summary":"ringkasan"}}"#,
            ))
            .expect("feed gagal");
        assert_eq!(ev.len(), 1);
        assert!(matches!(ev[0], Event::Reasoning(ref t) if t == "ringkasan"));

        let ev = p
            .feed(&parse(
                r#"{"type":"content_block_delta","delta":{"type":"thinking_cut_off_delta","cut_off":true}}"#,
            ))
            .expect("feed gagal");
        assert_eq!(ev.len(), 1);
        assert!(matches!(ev[0], Event::Reasoning(ref t) if t == "[thinking cut off]"));
    }

    #[test]
    fn search_tool_blocks_and_citations_become_annotations() {
        let mut p = SseParser::new();
        let ev = p
            .feed(&parse(
                r#"{"type":"content_block_start","content_block":{"type":"tool_use","name":"web_search"}}"#,
            ))
            .expect("feed gagal");
        assert_eq!(ev.len(), 1);
        assert!(matches!(
            ev[0],
            Event::Annotation(ref a) if a.kind == "tool_use" && a.text.contains("web_search")
        ));
        // Tool non-search bukan anotasi.
        let ev = p
            .feed(&parse(
                r#"{"type":"content_block_start","content_block":{"type":"tool_use","name":"artifacts"}}"#,
            ))
            .expect("feed gagal");
        assert!(ev.is_empty());
        // tool_result setelah search -> anotasi search_result.
        let ev = p
            .feed(&parse(
                r#"{"type":"content_block_start","content_block":{"type":"tool_result"}}"#,
            ))
            .expect("feed gagal");
        assert_eq!(ev.len(), 1);
        assert!(matches!(
            ev[0],
            Event::Annotation(ref a) if a.kind == "search_result"
        ));
        // Chip sitasi di message_delta.
        let ev = p
            .feed(&parse(
                r#"{"type":"message_delta","delta":{"stop_reason":"end_turn","citations":[{"url":"https://example.com","text":"Sumber"}]}}"#,
            ))
            .expect("feed gagal");
        assert_eq!(ev.len(), 1);
        assert!(matches!(
            ev[0],
            Event::Annotation(ref a) if a.kind == "citation" && a.url.as_deref() == Some("https://example.com")
        ));
    }

    #[test]
    fn thinking_rate_limit_stream_error_is_rate_limited() {
        let mut p = SseParser::new();
        let err = p
            .feed(&parse(
                r#"{"type":"error","error":{"message":"thinking_messages_rate_limit_exceeded"}}"#,
            ))
            .expect_err("harus error");
        assert!(matches!(err, ProviderError::RateLimited { .. }));
        // Error stream biasa -> Upstream 502.
        let err = p
            .feed(&parse(r#"{"type":"error","error":{"message":"boom"}}"#))
            .expect_err("harus error");
        assert!(matches!(err, ProviderError::Upstream { status: 502, .. }));
    }

    #[test]
    fn usage_event_is_estimated_from_chars() {
        let mut p = SseParser::new();
        p.feed(&parse(
            r#"{"type":"content_block_delta","delta":{"type":"text_delta","text":"abcdefgh"}}"#,
        ))
        .expect("feed gagal");
        p.feed(&parse(
            r#"{"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"abcd"}}"#,
        ))
        .expect("feed gagal");
        match p.usage_event(40) {
            Event::Usage {
                prompt,
                completion,
                reasoning,
                estimated,
            } => {
                assert_eq!(prompt, 10); // 40 chars / 4
                assert_eq!(completion, 2); // 8 chars / 4
                assert_eq!(reasoning, 1); // 4 chars / 4
                assert!(estimated);
            }
            _ => panic!("harus Usage"),
        }
    }
}
