//! Pembangun payload chat Grok (`POST /rest/app-chat/conversations/new`).
//!
//! Dipisah dari [`super::GrokProvider`] agar konstruksi payload bisa
//! dites tanpa jaringan/cookie. Semua bentuk field di sini berasal
//! dari riset (`docs/research-web2api-2026-10-16.md` §4): field yang
//! nilainya belum terverifikasi live ditandai eksplisit di komentar.

use crate::providers::{Message, Part, Role, Search, Thinking};
use serde_json::{json, Map, Value};

/// Satu file yang sudah di-upload ke Grok (siap direferensikan).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadedFile {
    /// Id dari `POST /rest/app-chat/upload-file-v2/direct`.
    pub file_metadata_id: String,
    /// Nama file yang dikirim saat upload.
    pub file_name: String,
    /// MIME file, mis. `"image/png"`.
    pub mime: String,
}

/// Bangun isi field `message` dari histori [`Message`] v2.
///
/// Grok (jalur REST) tidak menerima array messages: histori di-replay
/// secara stateless sebagai satu transkrip berlabel peran
/// (`System:` / `User:` / `Assistant:`) di field `message`
/// (`temporary: true`, tanpa history server-side). Ini pilihan sadar:
/// trait `ChatInput` v2 belum membawa `conversation_id` /
/// `parentResponseId`, jadi lane lanjutan
/// `POST /rest/app-chat/conversations/{id}/responses` belum bisa
/// dipakai — begitu trait membawa konteks percakapan, kirim hanya
/// pesan terbaru + id percakapan ke lane itu.
///
/// Bagian gambar tidak diratakan menjadi teks: di tempatnya tertulis
/// placeholder `[gambar terlampir]` dan file-nya direferensikan lewat
/// `fileAttachments` (urutan upload = urutan kemunculan part gambar).
pub fn build_message(messages: &[Message]) -> String {
    let mut lines: Vec<String> = Vec::new();
    for m in messages {
        let mut blocks: Vec<String> = Vec::new();
        for part in &m.parts {
            match part {
                Part::Text(t) => {
                    if !t.trim().is_empty() {
                        blocks.push(t.clone());
                    }
                }
                Part::ImageUrl(_) => {
                    blocks.push("[gambar terlampir]".to_string());
                }
            }
        }
        if blocks.is_empty() {
            continue;
        }
        let label = match m.role {
            Role::System => "System",
            Role::User => "User",
            Role::Assistant => "Assistant",
        };
        lines.push(format!("{label}: {}", blocks.join(" ")));
    }
    lines.join("\n\n")
}

/// Petakan [`Thinking`] ke nilai `reasoning_effort` Grok.
///
/// Nilai yang dikenal web Grok (riset §4c): `none` / `minimal` / `low`
/// / `medium` / `high` / `xhigh`. `Thinking::Auto` -> `None` (tidak
/// dikirim; ikut default web). Level numerik v2 dipetakan:
/// `0 -> minimal`, `1 -> low`, `2 -> medium`, `3 -> high`,
/// `>=4 -> xhigh`, `None -> medium`.
pub fn reasoning_effort_for(thinking: Thinking) -> Option<&'static str> {
    match thinking {
        Thinking::Off => Some("none"),
        Thinking::Auto => None,
        Thinking::On { level } => Some(match level {
            None => "medium",
            Some(0) => "minimal",
            Some(1) => "low",
            Some(2) => "medium",
            Some(3) => "high",
            Some(_) => "xhigh",
        }),
    }
}

/// Terapkan opsi thinking ke objek payload chat.
///
/// - [`Thinking::On`] -> `isReasoning: true` + `reasoning_effort`
///   sesuai [`reasoning_effort_for`]. Varian model `-thinking` TIDAK
///   dipakai untuk mengganti nama model: nama model sudah
///   di-resolve terhadap katalog live, dan menggantinya akan
///   memecahkan routing (`find_model`).
/// - [`Thinking::Off`] -> `isReasoning: false` + `reasoning_effort:
///   "none"` (menonaktifkan output thinking).
/// - [`Thinking::Auto`] -> kedua key tidak dikirim (default web).
pub fn apply_thinking(body: &mut Map<String, Value>, thinking: Thinking) {
    match thinking {
        Thinking::Auto => {}
        Thinking::Off => {
            body.insert("isReasoning".to_string(), json!(false));
            body.insert("reasoning_effort".to_string(), json!("none"));
        }
        Thinking::On { .. } => {
            body.insert("isReasoning".to_string(), json!(true));
            if let Some(effort) = reasoning_effort_for(thinking) {
                body.insert("reasoning_effort".to_string(), json!(effort));
            }
        }
    }
}

/// `toolOverrides` untuk web search: web search nyala, X search mati.
///
/// Varian X search (didokumentasikan, tidak dipakai untuk
/// [`Search::On`]): `{"webSearch": false, "xSearch": true}`.
pub fn tool_overrides_web_search() -> Value {
    json!({ "webSearch": true, "xSearch": false })
}

/// Terapkan opsi search ke objek payload chat.
///
/// - [`Search::On`] -> `disableSearch: false` + `toolOverrides`
///   web-search + key preset DeepSearch (spasi literal, BUKAN
///   underscore: `"deepsearch preset"`).
/// - [`Search::Off`] -> `disableSearch: true`, tanpa `toolOverrides`.
/// - [`Search::Auto`] -> `disableSearch: false` saja (perilaku lama:
///   Grok yang memutuskan), tanpa `toolOverrides`/preset.
///
/// BUKTI KONFLIK (riset §4c): capture browser 2026-08 tidak menemukan
/// toggle search web/X di opsi pembuatan sesi lane WebSocket yang
/// lebih baru, dan menyimpulkan search bukan flag payload di lane itu
/// — hanya `disableSearch: false`, dengan Grok yang memutuskan
/// sendiri. Jadi efektivitas `toolOverrides` di lane REST saat ini
/// BELUM terverifikasi live: kirim flag-nya, tapi jangan anggap search
/// terjamin menyala hanya karena flag terkirim. Nilai preset DeepSearch
/// (`true` di sini) juga belum terverifikasi — capture live dengan
/// toggle DeepSearch menyala masih dibutuhkan untuk memastikan nilai
/// yang benar (nama preset vs boolean).
pub fn apply_search(body: &mut Map<String, Value>, search: Search) {
    match search {
        Search::On => {
            body.insert("disableSearch".to_string(), json!(false));
            body.insert("toolOverrides".to_string(), tool_overrides_web_search());
            body.insert("deepsearch preset".to_string(), json!(true));
        }
        Search::Off => {
            body.insert("disableSearch".to_string(), json!(true));
        }
        Search::Auto => {
            body.insert("disableSearch".to_string(), json!(false));
        }
    }
}

/// Bangun array `fileAttachments` dari file yang sudah di-upload.
///
/// Bentuk tiap entri: `{"fileMetadataId", "fileName", "mimeType"}`.
/// Bentuk persis key-key ini belum terverifikasi live (endpoint upload
/// hanya mendokumentasikan `fileMetadataId` sebagai hasil); bila
/// capture live menunjukkan bentuk lain, sesuaikan di sini saja.
pub fn build_file_attachments(uploads: &[UploadedFile]) -> Value {
    Value::Array(
        uploads
            .iter()
            .map(|u| {
                json!({
                    "fileMetadataId": u.file_metadata_id,
                    "fileName": u.file_name,
                    "mimeType": u.mime,
                })
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::{ImageSource, Message, Part, Role};

    fn msg(role: Role, text: &str) -> Message {
        Message {
            role,
            parts: vec![Part::Text(text.to_string())],
        }
    }

    #[test]
    fn build_message_replays_history_as_labelled_transcript() {
        let messages = vec![
            msg(Role::System, "Kamu asisten."),
            msg(Role::User, "Halo"),
            msg(Role::Assistant, "Hai!"),
            msg(Role::User, "Lanjut"),
        ];
        assert_eq!(
            build_message(&messages),
            "System: Kamu asisten.\n\nUser: Halo\n\nAssistant: Hai!\n\nUser: Lanjut"
        );
        assert_eq!(build_message(&[]), "");
    }

    #[test]
    fn build_message_image_parts_become_attachment_placeholders() {
        let messages = vec![Message {
            role: Role::User,
            parts: vec![
                Part::Text("Lihat ini".to_string()),
                Part::ImageUrl(ImageSource::Url("https://example.com/a.png".to_string())),
            ],
        }];
        let built = build_message(&messages);
        assert!(built.contains("Lihat ini [gambar terlampir]"));
        // Payload base64 tidak pernah masuk pesan.
        assert!(!built.contains("aGVsbG8="));
    }

    #[test]
    fn reasoning_effort_mapping() {
        assert_eq!(reasoning_effort_for(Thinking::Off), Some("none"));
        assert_eq!(reasoning_effort_for(Thinking::Auto), None);
        assert_eq!(
            reasoning_effort_for(Thinking::On { level: None }),
            Some("medium")
        );
        assert_eq!(
            reasoning_effort_for(Thinking::On { level: Some(0) }),
            Some("minimal")
        );
        assert_eq!(
            reasoning_effort_for(Thinking::On { level: Some(1) }),
            Some("low")
        );
        assert_eq!(
            reasoning_effort_for(Thinking::On { level: Some(2) }),
            Some("medium")
        );
        assert_eq!(
            reasoning_effort_for(Thinking::On { level: Some(3) }),
            Some("high")
        );
        assert_eq!(
            reasoning_effort_for(Thinking::On { level: Some(4) }),
            Some("xhigh")
        );
        assert_eq!(
            reasoning_effort_for(Thinking::On { level: Some(9) }),
            Some("xhigh")
        );
    }

    #[test]
    fn apply_thinking_off_disables_reasoning() {
        let mut body = Map::new();
        apply_thinking(&mut body, Thinking::Off);
        assert_eq!(body["isReasoning"], json!(false));
        assert_eq!(body["reasoning_effort"], json!("none"));
    }

    #[test]
    fn apply_thinking_on_enables_reasoning_with_effort() {
        let mut body = Map::new();
        apply_thinking(&mut body, Thinking::On { level: Some(3) });
        assert_eq!(body["isReasoning"], json!(true));
        assert_eq!(body["reasoning_effort"], json!("high"));
    }

    #[test]
    fn apply_thinking_auto_sends_nothing() {
        let mut body = Map::new();
        apply_thinking(&mut body, Thinking::Auto);
        assert!(!body.contains_key("isReasoning"));
        assert!(!body.contains_key("reasoning_effort"));
    }

    #[test]
    fn apply_search_on_sends_tool_overrides_and_deepsearch_preset() {
        let mut body = Map::new();
        apply_search(&mut body, Search::On);
        assert_eq!(body["disableSearch"], json!(false));
        // toolOverrides: web search nyala, X search mati.
        assert_eq!(body["toolOverrides"]["webSearch"], json!(true));
        assert_eq!(body["toolOverrides"]["xSearch"], json!(false));
        // Key preset DeepSearch memakai spasi literal, bukan underscore.
        assert!(body.contains_key("deepsearch preset"));
        assert!(!body.contains_key("deepsearchPreset"));
        assert!(!body.contains_key("deepsearch_preset"));
    }

    #[test]
    fn apply_search_off_disables_search_without_overrides() {
        let mut body = Map::new();
        apply_search(&mut body, Search::Off);
        assert_eq!(body["disableSearch"], json!(true));
        assert!(!body.contains_key("toolOverrides"));
        assert!(!body.contains_key("deepsearch preset"));
    }

    #[test]
    fn apply_search_auto_keeps_legacy_disablesearch_only() {
        let mut body = Map::new();
        apply_search(&mut body, Search::Auto);
        assert_eq!(body["disableSearch"], json!(false));
        assert!(!body.contains_key("toolOverrides"));
    }

    #[test]
    fn build_file_attachments_shape() {
        let uploads = vec![UploadedFile {
            file_metadata_id: "fm-123".to_string(),
            file_name: "a.png".to_string(),
            mime: "image/png".to_string(),
        }];
        let v = build_file_attachments(&uploads);
        assert_eq!(v[0]["fileMetadataId"], json!("fm-123"));
        assert_eq!(v[0]["fileName"], json!("a.png"));
        assert_eq!(v[0]["mimeType"], json!("image/png"));
        assert_eq!(build_file_attachments(&[]), Value::Array(vec![]));
    }
}
