//! Pembangun payload murni (tanpa I/O) untuk protokol web Claude.
//!
//! Semua fungsi di sini adalah fungsi murni atas fixture sehingga
//! mudah dites tanpa jaringan/cookie. Bentuk wire mengikuti riset
//! `docs/research-web2api-2026-10-16.md` §3e.

use crate::core::catalog::resolve_model;
use crate::openai::ModelInfo;
use crate::providers::{Message, Part, Role, Search, Thinking};
use serde_json::{json, Value};

/// Alias model default web Claude (bukan model spesifik terkunci).
pub const WEB_DEFAULT_MODEL: &str = "claude";

/// UUID parent untuk pesan pertama dalam percakapan baru
/// (nilai nil-UUID yang dipakai frontend web).
pub const FIRST_PARENT_MESSAGE_UUID: &str = "00000000-0000-4000-8000-000000000000";

/// Bangun `prompt` completion dari histori chat.
///
/// Struktur peran dipertahankan seperti yang diharapkan API web:
/// sistem menjadi seksi `[System: ...]`, lalu tiap giliran diberi
/// prefix `Human:` / `Assistant:`. Bagian gambar TIDAK dimasukkan
/// ke prompt — gambar dikirim lewat `attachments`/`files` (vision),
/// bukan sebagai placeholder teks.
pub fn build_prompt(messages: &[Message]) -> String {
    let mut system = String::new();
    let mut turns: Vec<String> = Vec::new();
    for m in messages {
        let mut blocks: Vec<String> = Vec::new();
        for part in &m.parts {
            match part {
                Part::Text(t) => {
                    if !t.trim().is_empty() {
                        blocks.push(t.clone());
                    }
                }
                // Gambar travels via attachments/files, bukan prompt.
                Part::ImageUrl(_) => {}
            }
        }
        if blocks.is_empty() {
            continue;
        }
        let block = blocks.join(" ");
        match m.role {
            Role::System => {
                if !system.is_empty() {
                    system.push_str("\n\n");
                }
                system.push_str(&block);
            }
            Role::User => turns.push(format!("Human: {block}")),
            Role::Assistant => turns.push(format!("Assistant: {block}")),
        }
    }
    let turns = turns.join("\n\n");
    if system.is_empty() {
        turns
    } else if turns.is_empty() {
        format!("[System: {system}]")
    } else {
        format!("[System: {system}]\n\n{turns}")
    }
}

/// Nilai `paprika_mode` untuk body completion / create.
///
/// Web TIDAK punya param `thinking:{budget}` (itu API resmi) —
///
/// web memakai setting `paprika_mode: "extended" | null`.
/// `Thinking::Auto` mengikuti default web (null, tidak dipaksa).
pub fn paprika_mode_value(thinking: Thinking) -> Value {
    match thinking {
        Thinking::On { .. } => json!("extended"),
        _ => Value::Null,
    }
}

/// Payload settings percakapan untuk mode search.
///
/// `Search::On` -> `{settings: {enabled_web_search: true}}`
/// (setting sticky, dikirim via PUT settings setelah create).
/// Nama tool web-search ter-obfuscasi TIDAK pernah di-hardcode;
/// jalur settings ini satu-satunya mekanisme search di v0.2.
///
/// CATATAN: apakah `enabled_web_search` saja cukup untuk search
/// end-to-end BELUM TERVERIFIKASI — butuh capture live.
pub fn conversation_settings_payload(search: Search) -> Option<Value> {
    match search {
        Search::On => Some(json!({ "settings": { "enabled_web_search": true } })),
        _ => None,
    }
}

/// Body `POST .../chat_conversations` (create).
///
/// `model`: `None` bila model yang diminta adalah alias default
/// generik (`"claude"`) — biarkan web memakai default akun,
/// seperti perilaku v0.1 yang terverifikasi live.
pub fn build_create_body(conv_uuid: &str, model: Option<&str>, thinking: Thinking) -> Value {
    let mut body = json!({
        "uuid": conv_uuid,
        "name": "",
        "paprika_mode": paprika_mode_value(thinking),
    });
    if let Some(m) = model {
        body["model"] = json!(m);
    }
    body
}

/// Body `POST .../chat_conversations/{conv}/completion`.
///
/// `model`: `None` untuk alias default generik (lihat
/// [`build_create_body`]). `attachments`: lampiran dokumen
/// (konten terekstrak inline); `files`: referensi `file_uuid`
/// gambar. Bila `attachments` ada, `files` selalu dikirim juga
/// (boleh `[]` — alur doc-convert mewajibkan `files: []`).
/// Bila keduanya `None`, field tersebut dihilangkan agar jalur
/// teks-only identik dengan perilaku v0.1 yang terverifikasi.
pub fn build_completion_body(
    prompt: &str,
    model: Option<&str>,
    thinking: Thinking,
    attachments: Option<&[Value]>,
    files: Option<&[String]>,
) -> Value {
    let mut body = json!({
        "prompt": prompt,
        "parent_message_uuid": FIRST_PARENT_MESSAGE_UUID,
        "timezone": "Asia/Jakarta",
        "paprika_mode": paprika_mode_value(thinking),
    });
    if let Some(m) = model {
        body["model"] = json!(m);
    }
    if let Some(a) = attachments {
        body["attachments"] = json!(a);
        body["files"] = json!(files.unwrap_or(&[]));
    } else if let Some(f) = files {
        body["files"] = json!(f);
    }
    body
}

/// Resolusi model yang diminta terhadap katalog live.
///
/// Alias default generik (`"claude"`) atau request kosong memakai
/// default tanpa warning; selain itu didelegasikan ke
/// [`resolve_model`] (model tak dikenal -> default + warning,
/// katalog kosong -> default + warning).
pub fn resolve_requested_model(
    requested: &str,
    catalog: &[ModelInfo],
    web_default: &str,
) -> (String, Option<String>) {
    let req = requested.trim();
    if req.is_empty() || req == web_default {
        return (web_default.to_string(), None);
    }
    resolve_model(Some(req), catalog, web_default)
}

/// Parse katalog model dari respons `app_start`:
/// `claude_ai_available_models.models[].model_id`, didedup
/// sesuai urutan upstream. Respons tanpa field tersebut -> vec kosong.
pub fn parse_catalog_models(body: &Value) -> Vec<ModelInfo> {
    let mut out: Vec<ModelInfo> = Vec::new();
    let Some(models) = body
        .pointer("/claude_ai_available_models/models")
        .and_then(|m| m.as_array())
    else {
        return out;
    };
    for m in models {
        if let Some(id) = m.get("model_id").and_then(|i| i.as_str()) {
            if !id.is_empty() && !out.iter().any(|x| x.id == id) {
                out.push(ModelInfo::new(id, "anthropic"));
            }
        }
    }
    out
}

/// Normalisasi respons `convert_document` menjadi objek attachment
/// untuk body completion.
///
/// Respons convert **adalah** objek attachment; fungsi ini hanya
/// menormalisasi + memperbaiki `file_size`: HARUS integer, tidak
/// pernah null — bila respons memberi null/bukan-angka, fallback
/// ke panjang byte aktual file yang di-upload (kalau tidak, upstream
/// mengembalikan 400).
pub fn build_attachment(
    convert: &Value,
    fallback_name: &str,
    fallback_size: u64,
    fallback_type: &str,
) -> Value {
    let file_name = convert
        .get("file_name")
        .and_then(|v| v.as_str())
        .or_else(|| convert.get("sanitized_name").and_then(|v| v.as_str()))
        .unwrap_or(fallback_name);
    let file_type = convert
        .get("file_type")
        .and_then(|v| v.as_str())
        .unwrap_or(fallback_type);
    let file_size = convert
        .get("file_size")
        .and_then(|v| v.as_u64())
        .unwrap_or(fallback_size);
    let extracted = convert
        .get("extracted_content")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let mut att = json!({
        "file_name": file_name,
        "file_type": file_type,
        "file_size": file_size,
        "extracted_content": extracted,
        "origin": "UserUpload",
        "kind": "File",
    });
    if let Some(path) = convert.get("path").and_then(|v| v.as_str()) {
        att["path"] = json!(path);
    }
    att
}

/// Bangun body multipart/form-data untuk satu file (manual, tanpa
/// fitur `multipart` reqwest agar tidak mengubah `Cargo.toml`).
/// Nama field (`"file"`) adalah best-effort — lihat TODO(live-capture)
/// di pemanggil.
pub fn multipart_file_body(
    boundary: &str,
    field: &str,
    filename: &str,
    mime: &str,
    bytes: &[u8],
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"{field}\"; filename=\"{filename}\"\r\nContent-Type: {mime}\r\n\r\n"
        )
        .as_bytes(),
    );
    out.extend_from_slice(bytes);
    out.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    out
}

/// Nama file upload dari MIME (wiggle upload butuh filename).
pub fn filename_for_mime(mime: &str) -> &'static str {
    match mime {
        "image/png" => "upload.png",
        "image/jpeg" => "upload.jpg",
        "image/gif" => "upload.gif",
        "image/webp" => "upload.webp",
        "application/pdf" => "upload.pdf",
        "text/plain" => "upload.txt",
        "text/markdown" => "upload.md",
        _ => "upload.bin",
    }
}

/// Benar bila MIME tergolong dokumen (wajib `convert_document`).
/// Gambar (`image/*`) dan blob lain TIDAK dikonversi.
pub fn is_document_mime(mime: &str) -> bool {
    mime == "application/pdf"
        || mime.starts_with("text/")
        || mime == "application/msword"
        || mime.contains("officedocument")
        || mime == "application/json"
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
    fn build_prompt_preserves_role_structure() {
        let messages = vec![
            msg(Role::System, "Kamu asisten."),
            msg(Role::User, "Halo"),
            msg(Role::Assistant, "Hai!"),
            msg(Role::User, "Lanjut"),
        ];
        assert_eq!(
            build_prompt(&messages),
            "[System: Kamu asisten.]\n\nHuman: Halo\n\nAssistant: Hai!\n\nHuman: Lanjut"
        );
        assert_eq!(build_prompt(&[]), "");
        assert_eq!(build_prompt(&[msg(Role::System, "Saja")]), "[System: Saja]");
    }

    #[test]
    fn build_prompt_skips_images_they_travel_via_upload() {
        let messages = vec![Message {
            role: Role::User,
            parts: vec![
                Part::Text("Lihat ini".to_string()),
                Part::ImageUrl(ImageSource::Url("https://example.com/a.png".to_string())),
            ],
        }];
        let prompt = build_prompt(&messages);
        assert_eq!(prompt, "Human: Lihat ini");
        assert!(!prompt.contains("example.com"));
        // Pesan hanya-gambar tidak menghasilkan giliran teks.
        let image_only = vec![Message {
            role: Role::User,
            parts: vec![Part::ImageUrl(ImageSource::DataUri {
                mime: "image/png".to_string(),
                base64: "abcd".to_string(),
            })],
        }];
        assert_eq!(build_prompt(&image_only), "");
    }

    #[test]
    fn paprika_mode_present_when_on_null_when_off_or_auto() {
        assert_eq!(
            paprika_mode_value(Thinking::On { level: None }),
            json!("extended")
        );
        assert_eq!(paprika_mode_value(Thinking::Off), Value::Null);
        assert_eq!(paprika_mode_value(Thinking::Auto), Value::Null);

        let on = build_completion_body("hi", None, Thinking::On { level: None }, None, None);
        assert_eq!(on["paprika_mode"], json!("extended"));
        let off = build_completion_body("hi", None, Thinking::Off, None, None);
        assert_eq!(off["paprika_mode"], Value::Null);
        // Model generik -> field model dihilangkan (default web).
        assert!(off.get("model").is_none());
        let specific =
            build_completion_body("hi", Some("claude-sonnet-4-5"), Thinking::Off, None, None);
        assert_eq!(specific["model"], json!("claude-sonnet-4-5"));
        // Jalur teks-only: tanpa attachments/files.
        assert!(off.get("attachments").is_none());
        assert!(off.get("files").is_none());
    }

    #[test]
    fn completion_body_doc_flow_forces_files_empty_array() {
        let att = json!({"file_name": "a.txt"});
        let body = build_completion_body("hi", None, Thinking::Off, Some(&[att]), None);
        assert_eq!(body["files"], json!([]));
        // Alur gambar saja: files tanpa attachments.
        let body = build_completion_body(
            "hi",
            None,
            Thinking::Off,
            None,
            Some(&["uuid-1".to_string()]),
        );
        assert_eq!(body["files"], json!(["uuid-1"]));
        assert!(body.get("attachments").is_none());
    }

    #[test]
    fn settings_payload_includes_enabled_web_search_when_on() {
        let payload = conversation_settings_payload(Search::On).expect("harus ada payload");
        assert_eq!(payload["settings"]["enabled_web_search"], json!(true));
        assert!(conversation_settings_payload(Search::Off).is_none());
        assert!(conversation_settings_payload(Search::Auto).is_none());
    }

    #[test]
    fn resolve_requested_model_behavior() {
        let catalog = vec![
            ModelInfo::new("claude-sonnet-4-5", "anthropic"),
            ModelInfo::new("claude-opus-4-1", "anthropic"),
        ];
        // Alias generik / kosong: default tanpa warning.
        assert_eq!(
            resolve_requested_model("claude", &catalog, "claude"),
            ("claude".to_string(), None)
        );
        assert_eq!(
            resolve_requested_model("", &catalog, "claude"),
            ("claude".to_string(), None)
        );
        // Dikenal: dipakai tanpa warning.
        assert_eq!(
            resolve_requested_model("claude-sonnet-4-5", &catalog, "claude"),
            ("claude-sonnet-4-5".to_string(), None)
        );
        // Tak dikenal: default + warning.
        let (model, warning) = resolve_requested_model("nope", &catalog, "claude");
        assert_eq!(model, "claude");
        assert!(warning.is_some());
        // Katalog kosong: default + warning.
        let (model, warning) = resolve_requested_model("claude-sonnet-4-5", &[], "claude");
        assert_eq!(model, "claude");
        assert!(warning.is_some());
    }

    #[test]
    fn parse_catalog_models_dedups_in_order() {
        let body: Value = serde_json::from_str(
            r#"{"claude_ai_available_models":{"models":[
                {"model_id":"a"},{"model_id":"b"},{"model_id":"a"},{"model_id":""},
                {"other":"x"}
            ]}}"#,
        )
        .expect("fixture valid");
        let models = parse_catalog_models(&body);
        let ids: Vec<&str> = models.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, vec!["a", "b"]);
        assert!(parse_catalog_models(&json!({})).is_empty());
    }

    #[test]
    fn build_attachment_file_size_integer_fallback() {
        // file_size null -> fallback ke panjang byte aktual.
        let convert: Value = serde_json::from_str(
            r#"{"file_name":"doc.pdf","file_type":"application/pdf","file_size":null,"extracted_content":"isi"}"#,
        )
        .expect("fixture valid");
        let att = build_attachment(&convert, "upload.pdf", 1234, "application/pdf");
        assert_eq!(att["file_size"], json!(1234));
        assert!(
            att["file_size"].is_u64(),
            "file_size HARUS integer, bukan null"
        );
        assert_eq!(att["file_name"], json!("doc.pdf"));
        assert_eq!(att["extracted_content"], json!("isi"));
        assert_eq!(att["origin"], json!("UserUpload"));
        // file_size hilang + nama hilang -> fallback semua.
        let att = build_attachment(&json!({}), "upload.txt", 42, "text/plain");
        assert_eq!(att["file_size"], json!(42));
        assert_eq!(att["file_name"], json!("upload.txt"));
        assert_eq!(att["file_type"], json!("text/plain"));
        // file_size integer valid dipakai apa adanya.
        let att = build_attachment(&json!({"file_size": 777}), "upload.txt", 42, "text/plain");
        assert_eq!(att["file_size"], json!(777));
    }

    #[test]
    fn multipart_body_shape() {
        let body = multipart_file_body("B", "file", "upload.png", "image/png", b"XYZ");
        let text = String::from_utf8(body).expect("multipart harus UTF-8 untuk fixture ini");
        assert!(text.contains("--B\r\n"));
        assert!(text.contains("name=\"file\"; filename=\"upload.png\""));
        assert!(text.contains("Content-Type: image/png"));
        assert!(text.contains("XYZ"));
        assert!(text.ends_with("--B--\r\n"));
    }

    #[test]
    fn document_mime_classification() {
        assert!(is_document_mime("application/pdf"));
        assert!(is_document_mime("text/plain"));
        assert!(is_document_mime(
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        ));
        assert!(!is_document_mime("image/png"));
        assert!(!is_document_mime("image/jpeg"));
        assert_eq!(filename_for_mime("image/png"), "upload.png");
        assert_eq!(filename_for_mime("application/pdf"), "upload.pdf");
    }
}
