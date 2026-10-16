//! Parser stream NDJSON Grok (`POST /rest/app-chat/conversations/new`).
//!
//! Tiap baris adalah satu objek JSON. Bentuk frame (riset §4b/§4e):
//! - token teks di `result.response.token`, dengan flag `isThinking`
//!   (token thinking -> [`Event::Reasoning`], TIDAK dibuang) dan
//!   `messageTag: "final"`;
//! - stream berakhir saat `isFinished: true`;
//! - teks final penuh di `result.response.modelResponse.message`
//!   (kumulatif — parser hanya meng-emit selisihnya terhadap token
//!   yang sudah di-emit, agar tidak duplikasi);
//! - frame pertama bisa membawa
//!   `result.conversation {conversationId, title}`;
//! - anotasi (kutipan / sumber search) dikumpulkan dari field
//!   `annotations` / `citations` / `searchResults` / `sources`.
//!
//! Parser ini murni (tanpa I/O): `feed_line` menerima satu baris dan
//! mengembalikan event-eventnya, sehingga bisa dites dari fixture.

use crate::core::events::{Annotation, Event};
use crate::error::ProviderError;
use serde_json::Value;
use std::collections::HashSet;

/// Parser stateful untuk satu stream NDJSON Grok.
pub struct NdjsonParser {
    /// Teks jawaban yang sudah di-emit (untuk hitung selisih final).
    emitted_text: String,
    /// Teks reasoning yang sudah di-emit.
    emitted_reasoning: String,
    /// Anotasi yang sudah di-emit (dedup).
    seen_annotations: HashSet<(String, String, Option<String>)>,
    /// `conversationId` dari frame pertama, bila ada.
    conversation_id: Option<String>,
    /// Jumlah karakter prompt (untuk estimasi usage `chars / 4`).
    prompt_chars: usize,
    /// Benar setelah frame `isFinished: true` diproses.
    finished: bool,
    /// Benar setelah event [`Event::Usage`] di-emit.
    usage_emitted: bool,
}

impl NdjsonParser {
    /// Parser baru untuk satu stream (prompt sepanjang `prompt_chars`).
    pub fn new(prompt_chars: usize) -> Self {
        Self {
            emitted_text: String::new(),
            emitted_reasoning: String::new(),
            seen_annotations: HashSet::new(),
            conversation_id: None,
            prompt_chars,
            finished: false,
            usage_emitted: false,
        }
    }

    /// `conversationId` yang ditangkap dari frame pertama, bila ada.
    /// Dipakai untuk multi-turn (`POST conversations/{id}/responses`)
    /// begitu trait `ChatInput` membawa konteks percakapan.
    /// Belum dipakai jalur produksi (ChatInput belum membawa konteks
    /// percakapan) — accessor ini adalah hook untuk migrasi itu.
    #[allow(dead_code)]
    pub fn conversation_id(&self) -> Option<&str> {
        self.conversation_id.as_deref()
    }

    /// Benar bila frame `isFinished: true` sudah diproses.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Benar bila ada token teks jawaban yang di-emit.
    pub fn saw_text(&self) -> bool {
        !self.emitted_text.is_empty()
    }

    /// Benar bila ada token reasoning yang di-emit.
    pub fn saw_reasoning(&self) -> bool {
        !self.emitted_reasoning.is_empty()
    }

    /// Estimasi token dari jumlah karakter (`chars / 4`, dibulatkan
    /// ke atas). Backend web tidak mengembalikan hitungan asli.
    fn estimate_tokens(chars: usize) -> u64 {
        chars.div_ceil(4) as u64
    }

    /// Event penutup stream: [`Event::Usage`] (estimasi) lalu
    /// [`Event::Done`]. Hanya meng-emit sekali; panggilan berikutnya
    /// mengembalikan vec kosong.
    pub fn finish_events(&mut self) -> Vec<Event> {
        if self.usage_emitted {
            return Vec::new();
        }
        self.usage_emitted = true;
        self.finished = true;
        vec![
            Event::Usage {
                prompt: Self::estimate_tokens(self.prompt_chars),
                completion: Self::estimate_tokens(self.emitted_text.chars().count()),
                reasoning: Self::estimate_tokens(self.emitted_reasoning.chars().count()),
                estimated: true,
            },
            Event::Done,
        ]
    }

    /// Proses satu baris NDJSON menjadi event-eventnya.
    ///
    /// Baris kosong / bukan JSON / bukan objek diabaikan (bukan
    /// error): stream NDJSON Grok bisa menyisipkan baris non-JSON.
    /// Frame error (`error` di top-level atau di `response`)
    /// dikembalikan sebagai satu item `Err`.
    pub fn feed_line(&mut self, line: &str) -> Vec<Result<Event, ProviderError>> {
        let line = line.trim();
        if line.is_empty() || !line.starts_with('{') {
            return Vec::new();
        }
        let v: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => return Vec::new(),
        };
        if let Some(err) = v.get("error") {
            return vec![Err(ProviderError::upstream(502, &err.to_string()))];
        }
        // Frame pertama bisa membawa conversation (tanpa response).
        if let Some(conv_id) = v
            .pointer("/result/conversation/conversationId")
            .and_then(|c| c.as_str())
        {
            if self.conversation_id.is_none() {
                self.conversation_id = Some(conv_id.to_string());
            }
        }
        let Some(response) = v.pointer("/result/response") else {
            return Vec::new();
        };
        if let Some(err) = response.get("error") {
            return vec![Err(ProviderError::upstream(502, &err.to_string()))];
        }

        let mut out: Vec<Result<Event, ProviderError>> = Vec::new();

        // Anotasi (kutipan / sumber search) -> Event::Annotation.
        for ann in collect_annotations(response) {
            let key = (ann.kind.clone(), ann.text.clone(), ann.url.clone());
            if self.seen_annotations.insert(key) {
                out.push(Ok(Event::Annotation(ann)));
            }
        }

        // Token: thinking -> Reasoning; teks -> Text.
        let token = response.get("token").and_then(|t| t.as_str()).unwrap_or("");
        let is_thinking = response
            .get("isThinking")
            .and_then(|b| b.as_bool())
            .unwrap_or(false);
        let tag = response
            .get("messageTag")
            .and_then(|t| t.as_str())
            .unwrap_or("");
        let is_finished = response
            .get("isFinished")
            .and_then(|b| b.as_bool())
            .unwrap_or(false);
        if !token.is_empty() {
            if is_thinking {
                // Token thinking di-emit sebagai Reasoning — TIDAK
                // dibuang (perilaku lama memfilter `!is_thinking`).
                self.emitted_reasoning.push_str(token);
                out.push(Ok(Event::Reasoning(token.to_string())));
            } else if tag == "final" || tag.is_empty() || is_finished {
                // Paritas dengan filter lama untuk token teks: hanya
                // tag final / kosong / frame penutup yang di-emit.
                self.emitted_text.push_str(token);
                out.push(Ok(Event::Text(token.to_string())));
            }
        }

        // Teks final penuh (kumulatif): hanya emit selisihnya terhadap
        // token yang sudah di-emit, agar tidak duplikasi.
        if let Some(final_text) = response
            .pointer("/modelResponse/message")
            .and_then(|m| m.as_str())
        {
            if !final_text.is_empty() {
                let suffix = final_suffix(final_text, &self.emitted_text);
                if !suffix.is_empty() {
                    self.emitted_text.push_str(suffix);
                    out.push(Ok(Event::Text(suffix.to_string())));
                }
            }
        }

        if is_finished && !self.finished {
            out.extend(self.finish_events().into_iter().map(Ok));
        }
        out
    }
}

/// Selisih teks final kumulatif terhadap teks yang sudah di-emit.
///
/// Asumsi: teks final adalah superset yang diawali teks yang sudah
/// di-emit (token-token adalah prefix-nya). Bila asumsi itu gagal
/// (final tidak diawali teks ter-emit) dan sudah ada teks ter-emit,
/// kembalikan kosong (hindari duplikasi); bila belum ada teks
/// ter-emit sama sekali, kembalikan final utuh.
fn final_suffix<'a>(final_text: &'a str, emitted: &str) -> &'a str {
    if emitted.is_empty() {
        return final_text;
    }
    if let Some(suffix) = final_text.strip_prefix(emitted) {
        return suffix;
    }
    ""
}

/// Kumpulkan anotasi dari satu objek `response` (dan
/// `response.modelResponse`): field `annotations` / `citations` /
/// `searchResults` / `sources` (juga bentuk snake_case).
fn collect_annotations(response: &Value) -> Vec<Annotation> {
    let mut out = Vec::new();
    const KEYS: [&str; 5] = [
        "annotations",
        "citations",
        "searchResults",
        "search_results",
        "sources",
    ];
    for key in KEYS {
        if let Some(arr) = response.get(key).and_then(|a| a.as_array()) {
            for item in arr {
                if let Some(ann) = parse_annotation(item) {
                    out.push(ann);
                }
            }
        }
    }
    if let Some(model_response) = response.get("modelResponse") {
        for key in ["annotations", "citations"] {
            if let Some(arr) = model_response.get(key).and_then(|a| a.as_array()) {
                for item in arr {
                    if let Some(ann) = parse_annotation(item) {
                        out.push(ann);
                    }
                }
            }
        }
    }
    out
}

/// Parse satu item anotasi: string polos -> kutipan; objek -> ambil
/// `type`/`kind`, teks (`text`/`title`/`snippet`/`content`), URL
/// (`url`/`link`/`sourceUrl`/`href`). Mengembalikan `None` bila tidak
/// ada teks maupun URL.
fn parse_annotation(item: &Value) -> Option<Annotation> {
    match item {
        Value::String(s) => {
            if s.is_empty() {
                None
            } else {
                Some(Annotation {
                    kind: "citation".to_string(),
                    text: s.clone(),
                    url: None,
                })
            }
        }
        Value::Object(_) => {
            let kind = item
                .get("type")
                .or_else(|| item.get("kind"))
                .and_then(|k| k.as_str())
                .unwrap_or("citation")
                .to_string();
            let text = ["text", "title", "snippet", "content"]
                .iter()
                .find_map(|k| item.get(k).and_then(|t| t.as_str()))
                .unwrap_or("")
                .to_string();
            let url = ["url", "link", "sourceUrl", "href"]
                .iter()
                .find_map(|k| item.get(k).and_then(|u| u.as_str()))
                .map(str::to_string);
            if text.is_empty() && url.is_none() {
                None
            } else {
                Some(Annotation {
                    kind,
                    text: if text.is_empty() {
                        url.clone().unwrap_or_default()
                    } else {
                        text
                    },
                    url,
                })
            }
        }
        _ => None,
    }
}

/// Kumpulkan URL gambar dari satu frame/Value (untuk Imagine).
/// Dipakai jalur `generate_image`: URL `assets.grok.com` atau string
/// URL yang berakhiran ekstensi gambar umum.
pub fn collect_image_urls(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::String(s) => {
            if s.starts_with("https://")
                && (s.contains("assets.grok.com")
                    || s.ends_with(".png")
                    || s.ends_with(".jpg")
                    || s.ends_with(".jpeg")
                    || s.ends_with(".webp")
                    || s.ends_with(".gif"))
                && !out.contains(s)
            {
                out.push(s.clone());
            }
        }
        Value::Array(a) => {
            for x in a {
                collect_image_urls(x, out);
            }
        }
        Value::Object(m) => {
            for (_, x) in m {
                collect_image_urls(x, out);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn events_of(parser: &mut NdjsonParser, line: &str) -> Vec<Event> {
        parser
            .feed_line(line)
            .into_iter()
            .map(|r| r.expect("frame fixture tidak boleh error"))
            .collect()
    }

    #[test]
    fn text_token_becomes_text_event() {
        let mut p = NdjsonParser::new(0);
        let evs = events_of(
            &mut p,
            r#"{"result":{"response":{"token":"Halo","messageTag":"final"}}}"#,
        );
        assert_eq!(evs.len(), 1);
        assert!(matches!(evs[0], Event::Text(ref t) if t == "Halo"));
        assert!(p.saw_text());
        assert!(!p.saw_reasoning());
    }

    #[test]
    fn thinking_token_becomes_reasoning_not_discarded() {
        let mut p = NdjsonParser::new(0);
        let evs = events_of(
            &mut p,
            r#"{"result":{"response":{"token":"mikir...","isThinking":true}}}"#,
        );
        assert_eq!(evs.len(), 1);
        assert!(matches!(evs[0], Event::Reasoning(ref t) if t == "mikir..."));
        assert!(p.saw_reasoning());
    }

    #[test]
    fn is_finished_emits_usage_then_done() {
        let mut p = NdjsonParser::new(8);
        let evs = events_of(
            &mut p,
            r#"{"result":{"response":{"token":"ok","messageTag":"final","isFinished":true}}}"#,
        );
        assert_eq!(evs.len(), 3);
        assert!(matches!(evs[0], Event::Text(ref t) if t == "ok"));
        match &evs[1] {
            Event::Usage {
                prompt,
                completion,
                estimated,
                ..
            } => {
                assert_eq!(*prompt, 2); // 8 karakter / 4
                assert_eq!(*completion, 1); // "ok" -> 1
                assert!(*estimated);
            }
            _ => panic!("harus Usage"),
        }
        assert!(matches!(evs[2], Event::Done));
        assert!(p.is_finished());
        // finish_events kedua kalinya kosong (tidak duplikasi Usage/Done).
        assert!(p.finish_events().is_empty());
    }

    #[test]
    fn final_message_emits_only_suffix_beyond_tokens() {
        let mut p = NdjsonParser::new(0);
        events_of(
            &mut p,
            r#"{"result":{"response":{"token":"Hel","messageTag":"final"}}}"#,
        );
        events_of(
            &mut p,
            r#"{"result":{"response":{"token":"lo","messageTag":"final"}}}"#,
        );
        // Final kumulatif "Hello world" -> hanya " world" yang baru.
        let evs = events_of(
            &mut p,
            r#"{"result":{"response":{"modelResponse":{"message":"Hello world"},"isFinished":true}}}"#,
        );
        assert!(evs
            .iter()
            .any(|e| matches!(e, Event::Text(t) if t == " world")));
        // Final yang sama persis dengan token ter-emit -> tidak ada Text baru.
        let mut p2 = NdjsonParser::new(0);
        events_of(
            &mut p2,
            r#"{"result":{"response":{"token":"Hello","messageTag":"final"}}}"#,
        );
        let evs2 = events_of(
            &mut p2,
            r#"{"result":{"response":{"modelResponse":{"message":"Hello"}}}}"#,
        );
        assert!(evs2.is_empty());
    }

    #[test]
    fn first_frame_captures_conversation_id() {
        let mut p = NdjsonParser::new(0);
        let evs = events_of(
            &mut p,
            r#"{"result":{"conversation":{"conversationId":"conv-1","title":"Tanya"}}}"#,
        );
        assert!(evs.is_empty());
        assert_eq!(p.conversation_id(), Some("conv-1"));
    }

    #[test]
    fn annotations_become_annotation_events_deduped() {
        let mut p = NdjsonParser::new(0);
        let line = r#"{"result":{"response":{
            "token":"x","messageTag":"final",
            "citations":[{"title":"Sumber A","url":"https://example.com/a"}],
            "searchResults":[{"text":"Hasil B","url":"https://example.com/b"}]
        }}}"#;
        let evs = events_of(&mut p, line);
        let anns: Vec<&Annotation> = evs
            .iter()
            .filter_map(|e| match e {
                Event::Annotation(a) => Some(a),
                _ => None,
            })
            .collect();
        assert_eq!(anns.len(), 2);
        assert_eq!(anns[0].text, "Sumber A");
        assert_eq!(anns[0].url.as_deref(), Some("https://example.com/a"));
        // Frame yang sama lagi -> anotasi tidak di-emit ulang.
        let evs2 = events_of(&mut p, line);
        assert!(!evs2.iter().any(|e| matches!(e, Event::Annotation(_))));
    }

    #[test]
    fn error_frame_becomes_upstream_error() {
        let mut p = NdjsonParser::new(0);
        let results = p.feed_line(r#"{"error":{"code":7,"message":"rejected"}}"#);
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], Err(ProviderError::Upstream { .. })));
    }

    #[test]
    fn non_json_lines_ignored() {
        let mut p = NdjsonParser::new(0);
        assert!(p.feed_line("").is_empty());
        assert!(p.feed_line("bukan json").is_empty());
        assert!(p.feed_line("[1,2]").is_empty());
        assert!(p.feed_line("{invalid").is_empty());
    }

    #[test]
    fn collect_image_urls_finds_asset_urls() {
        let v: Value =
            serde_json::from_str(r#"{"a":"https://assets.grok.com/users/1/generated/x.png","b":"https://example.com/teks"}"#)
                .expect("fixture valid");
        let mut out = Vec::new();
        collect_image_urls(&v, &mut out);
        assert_eq!(out, vec!["https://assets.grok.com/users/1/generated/x.png"]);
    }
}
