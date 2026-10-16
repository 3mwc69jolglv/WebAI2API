//! Provider Claude via antarmuka web claude.ai.
//!
//! Arsitektur v0.2 (Fase 2):
//! - Auth: mode full-cookie ([`CookieJar`]) sebagai jalur utama;
//!   `sessionKey` saja sebagai fallback terdegradasi. `401` ->
//!   [`ProviderError::AuthExpired`] (user harus re-paste cookie;
//!   tidak ada refresh background). Nilai cookie TIDAK PERNAH di-log.
//! - Katalog model live dari `edge-api/bootstrap/{org}/app_start`
//!   (cache [`ModelCatalog`], TTL 1 jam). `models()` menyajikan
//!   cache + alias default generik `"claude"`.
//! - Thinking via `paprika_mode: "extended" | null` (setting
//!   akun/percakapan, BUKAN param `thinking:{budget}` API resmi).
//! - Search via setting percakapan `enabled_web_search: true`.
//!   End-to-end search via settings saja BELUM TERVERIFIKASI —
//!   butuh capture live.
//! - Vision via wiggle upload (+ `convert_document` untuk dokumen).
//! - Lifecycle: create -> complete -> delete; satu retry
//!   pre-first-byte via `retry_completion` pada kegagalan retryable.
//!
//! CATATAN katalog: `models()` adalah method sinkron di trait
//! [`Provider`], jadi ia TIDAK BISA mengambil katalog dari upstream
//! (fetch butuh async). Yang ia kembalikan adalah isi cache live
//! (diisi/di-refresh oleh [`ClaudeProvider::chat`] sebelum
//! completion) + alias default generik. Sebelum fetch pertama yang
//! sukses, hanya alias yang terekspos — ini satu-satunya fallback
//! dan BUKAN daftar model statis: jalur fetch sendiri tidak punya
//! fallback statis (gagal -> cache dibiarkan kosong + warning).

mod payload;
mod sse;

use self::payload::{
    build_attachment, build_completion_body, build_create_body, build_prompt,
    conversation_settings_payload, filename_for_mime, is_document_mime, multipart_file_body,
    parse_catalog_models, resolve_requested_model, WEB_DEFAULT_MODEL,
};
use self::sse::SseParser;
use crate::core::auth::{persist_jar_best_effort, CookieJar};
use crate::core::catalog::ModelCatalog;
use crate::core::media::{download_image_source, ImageInput};
use crate::core::retry::{backoff_delay, is_retryable_status, Pacer};
use crate::error::{sanitize_snippet, ProviderError};
use crate::openai::ModelInfo;
use crate::providers::{
    http_client, ChatInput, Event, EventStream, ImageSource, Message, Part, Provider, Search,
    Thinking, CHROME_UA,
};
use async_stream::stream;
use futures::StreamExt;
use reqwest::header::{HeaderMap, HeaderValue};
use serde_json::Value;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Hasil wiggle upload satu file.
struct UploadedFile {
    /// UUID file di sisi Claude.
    file_uuid: String,
    /// Benar bila file tergolong dokumen (wajib `convert_document`).
    is_document: bool,
}

/// Ambil nilai `Retry-After` (detik) dari header respons, bila ada.
fn retry_after_secs(resp: &reqwest::Response) -> Option<u64> {
    resp.headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse().ok())
}

/// Hapus conversation sementara (best-effort, kegagalan hanya di-log).
async fn delete_conversation(client: &reqwest::Client, headers: &HeaderMap, org: &str, conv: &str) {
    let resp = client
        .delete(format!(
            "https://claude.ai/api/organizations/{org}/chat_conversations/{conv}"
        ))
        .headers(headers.clone())
        .send()
        .await;
    match resp {
        Ok(r) if r.status().is_success() => {}
        Ok(r) => tracing::debug!(
            status = r.status().as_u16(),
            "hapus conversation Claude gagal (best-effort)"
        ),
        Err(e) => tracing::debug!(error = %e, "hapus conversation Claude gagal (best-effort)"),
    }
}

pub struct ClaudeProvider {
    /// Cookie jar (di balik lock agar merge rotasi `Set-Cookie` bisa
    /// dilakukan dari method `&self`).
    jar: std::sync::RwLock<CookieJar>,
    /// Benar bila kredensial adalah cookie jar penuh (>1 cookie),
    /// bukan hanya `sessionKey` (jalur utama vs fallback terdegradasi).
    full_cookie: bool,
    client: reqwest::Client,
    org_id: RwLock<Option<String>>,
    catalog: std::sync::RwLock<ModelCatalog>,
    /// Pacing antar-request: lantai 1500ms untuk Claude
    /// (riset-backed: traffic mirip manusia; lihat desain §3.8).
    pacer: Pacer,
    /// File persistensi rotasi cookie (env
    /// `AIW2A_CLAUDE_COOKIES_FILE`, opsional; `None` = rotasi hanya
    /// in-memory).
    persist_path: Option<PathBuf>,
}

impl ClaudeProvider {
    pub fn new(cookies: String) -> Self {
        let trimmed = cookies.trim();
        // Input terlihat seperti header Cookie penuh bila mengandung
        // "sessionKey=" atau lebih dari satu pasangan (";").
        // Selain itu, token polos diperlakukan sebagai nilai sessionKey.
        let (jar, full_cookie) = if trimmed.contains("sessionKey=") || trimmed.contains(';') {
            let jar = CookieJar::parse(trimmed);
            let full = jar.len() > 1;
            (jar, full)
        } else if trimmed.is_empty() {
            (CookieJar::new(), false)
        } else {
            let mut jar = CookieJar::new();
            jar.set("sessionKey", trimmed);
            (jar, false)
        };
        Self {
            jar: std::sync::RwLock::new(jar),
            full_cookie,
            client: http_client(),
            org_id: RwLock::new(None),
            catalog: std::sync::RwLock::new(ModelCatalog::new(Duration::from_secs(3600))),
            pacer: Pacer::new(Duration::from_millis(1500)),
            persist_path: std::env::var("AIW2A_CLAUDE_COOKIES_FILE")
                .ok()
                .filter(|v| !v.trim().is_empty())
                .map(PathBuf::from),
        }
    }

    /// Baca satu nilai cookie dari jar (tanpa memegang lock lama).
    fn jar_get(&self, name: &str) -> Option<String> {
        // Lock std, kritis singkat; poison berarti panic sebelumnya —
        // pakai inner-nya agar tidak panic di sini.
        self.jar
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(name)
            .map(str::to_string)
    }

    /// Benar bila jar tidak berisi cookie apa pun.
    fn jar_is_empty(&self) -> bool {
        self.jar
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .is_empty()
    }

    /// Merge header `Set-Cookie` dari satu respons upstream ke jar
    /// (rotasi) lalu persist best-effort (tidak pernah menggagalkan
    /// request, tidak pernah me-log nilai cookie).
    fn merge_response_cookies(&self, headers: &HeaderMap) {
        let mut jar = self
            .jar
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        for val in headers.get_all("set-cookie") {
            if let Ok(s) = val.to_str() {
                jar.merge_set_cookie(s);
            }
        }
        persist_jar_best_effort(&jar, self.persist_path.as_deref());
    }

    /// Header browser untuk request ke claude.ai.
    /// Nilai cookie TIDAK PERNAH di-log; header yang tidak valid
    /// (karakter ilegal) menjadi error, bukan panic.
    fn headers(&self) -> Result<HeaderMap, ProviderError> {
        let mut h = HeaderMap::new();
        h.insert("accept", HeaderValue::from_static("application/json"));
        h.insert("content-type", HeaderValue::from_static("application/json"));
        h.insert("origin", HeaderValue::from_static("https://claude.ai"));
        h.insert("referer", HeaderValue::from_static("https://claude.ai/"));
        h.insert("user-agent", HeaderValue::from_static(CHROME_UA));
        let cookie = HeaderValue::from_str(
            &self
                .jar
                .read()
                .unwrap_or_else(|poison| poison.into_inner())
                .render(),
        )
        .map_err(|_| {
            ProviderError::InvalidInput(
                "cookie Claude mengandung karakter yang tidak valid untuk header HTTP".to_string(),
            )
        })?;
        h.insert("cookie", cookie);
        Ok(h)
    }

    /// Resolve UUID organisasi (di-cache). Cookie `lastActiveOrg`
    /// diutamakan bila cocok dengan salah satu org; bila tidak ada,
    /// dipakai org pertama — `lastActiveOrg` bisa tidak ada.
    async fn ensure_org(&self) -> Result<String, ProviderError> {
        {
            let g = self.org_id.read().await;
            if let Some(org) = g.clone() {
                return Ok(org);
            }
        }
        let resp = self
            .client
            .get("https://claude.ai/api/organizations")
            .headers(self.headers()?)
            .send()
            .await?;
        // Rotasi cookie dari respons organizations.
        self.merge_response_cookies(resp.headers());
        let status = resp.status().as_u16();
        if status == 401 {
            return Err(ProviderError::AuthExpired(
                "cookie Claude kedaluwarsa (401) — paste ulang cookie claude.ai; \
                 tidak ada refresh background"
                    .to_string(),
            ));
        }
        if status == 403 {
            return Err(ProviderError::Unauthorized(
                "claude.ai menolak (403) — kemungkinan diblokir Cloudflare / cookie tidak valid"
                    .to_string(),
            ));
        }
        if status != 200 {
            return Err(ProviderError::upstream(
                status,
                &format!("gagal mengambil organizations: HTTP {status}"),
            ));
        }
        let orgs: Value = resp
            .json()
            .await
            .map_err(|e| ProviderError::Parse(format!("respons organizations bukan JSON: {e}")))?;
        let arr = orgs
            .as_array()
            .ok_or_else(|| ProviderError::Parse("respons organizations bukan array".to_string()))?;
        let mut uuid: Option<String> = None;
        if let Some(last) = self.jar_get("lastActiveOrg") {
            if arr
                .iter()
                .any(|o| o.get("uuid").and_then(|u| u.as_str()) == Some(last.as_str()))
            {
                uuid = Some(last);
            }
        }
        let uuid = uuid
            .or_else(|| {
                arr.first()
                    .and_then(|o| o.get("uuid"))
                    .and_then(|u| u.as_str())
                    .map(str::to_string)
            })
            .ok_or_else(|| ProviderError::Parse("tidak menemukan organization uuid".to_string()))?;
        *self.org_id.write().await = Some(uuid.clone());
        Ok(uuid)
    }

    /// Refresh katalog model live bila cache basi (TTL 1 jam).
    /// Best-effort: kegagalan upstream hanya di-log (warning) dan
    /// cache dibiarkan apa adanya — TANPA fallback statis.
    /// Hanya 401 yang menjadi error (kredensial mati).
    async fn ensure_catalog(&self, org: &str) -> Result<(), ProviderError> {
        {
            let guard = self
                .catalog
                .read()
                .unwrap_or_else(|poison| poison.into_inner());
            if guard.is_fresh() {
                return Ok(());
            }
        }
        let resp = self
            .client
            .get(format!(
                "https://claude.ai/edge-api/bootstrap/{org}/app_start?include_system_prompts=false"
            ))
            .headers(self.headers()?)
            .send()
            .await?;
        // Rotasi cookie dari respons katalog.
        self.merge_response_cookies(resp.headers());
        let status = resp.status().as_u16();
        if status == 401 {
            return Err(ProviderError::AuthExpired(
                "cookie Claude kedaluwarsa (401) saat mengambil katalog model — \
                 paste ulang cookie claude.ai"
                    .to_string(),
            ));
        }
        if status != 200 {
            tracing::warn!(
                status,
                "gagal mengambil katalog model Claude — memakai cache lama / alias default"
            );
            return Ok(());
        }
        let body: Value = match resp.json().await {
            Ok(body) => body,
            Err(e) => {
                tracing::warn!(error = %e, "katalog model Claude bukan JSON — memakai cache lama");
                return Ok(());
            }
        };
        let models = parse_catalog_models(&body);
        let mut guard = self
            .catalog
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        guard.update(models);
        Ok(())
    }

    /// Buat conversation baru (+ aktifkan `enabled_web_search`
    /// via PUT settings bila `Search::On`).
    async fn create_conversation(
        &self,
        org: &str,
        conv_uuid: &str,
        model: Option<&str>,
        thinking: Thinking,
        search: Search,
    ) -> Result<(), ProviderError> {
        let body = build_create_body(conv_uuid, model, thinking);
        let resp = self
            .client
            .post(format!(
                "https://claude.ai/api/organizations/{org}/chat_conversations"
            ))
            .headers(self.headers()?)
            .json(&body)
            .send()
            .await?;
        // Rotasi cookie dari respons create conversation.
        self.merge_response_cookies(resp.headers());
        let status = resp.status().as_u16();
        if status == 401 {
            return Err(ProviderError::AuthExpired(
                "cookie Claude kedaluwarsa (401) saat membuat conversation — \
                 paste ulang cookie claude.ai"
                    .to_string(),
            ));
        }
        if status == 403 {
            return Err(ProviderError::Unauthorized(
                "claude.ai menolak (403) saat membuat conversation".to_string(),
            ));
        }
        if status == 429 {
            return Err(ProviderError::RateLimited {
                retry_after: retry_after_secs(&resp),
                message: "claude.ai rate limit (429) saat membuat conversation".to_string(),
            });
        }
        if status != 200 && status != 201 {
            let text = resp.text().await.unwrap_or_default();
            return Err(ProviderError::upstream(
                status,
                &format!("gagal membuat conversation: {text}"),
            ));
        }
        // Search: setting sticky via PUT. Kegagalan PUT hanya warning
        // (chat tetap jalan tanpa search) — end-to-end search via
        // settings saja belum terverifikasi, lihat doc di payload.rs.
        if let Some(settings) = conversation_settings_payload(search) {
            let put = self
                .client
                .put(format!(
                    "https://claude.ai/api/organizations/{org}/chat_conversations/{conv_uuid}?rendering_mode=raw"
                ))
                .headers(self.headers()?)
                .json(&settings)
                .send()
                .await;
            match put {
                Ok(r) => {
                    // Rotasi cookie dari respons PUT settings.
                    self.merge_response_cookies(r.headers());
                    if !r.status().is_success() {
                        tracing::warn!(
                            status = r.status().as_u16(),
                            "gagal mengaktifkan enabled_web_search — chat jalan tanpa search"
                        );
                    }
                }
                Err(e) => tracing::warn!(
                    error = %e,
                    "gagal mengaktifkan enabled_web_search — chat jalan tanpa search"
                ),
            }
        }
        Ok(())
    }

    /// Upload satu file via wiggle upload (multipart).
    ///
    /// TODO(live-capture): bentuk multipart persis BELUM terverifikasi
    /// dari traffic live. Yang belum pasti: nama field (`"file"` di
    /// sini adalah best-effort), apakah field tambahan diperlukan
    /// (`file_name`, `file_size`, `file_kind`, ...), dan apakah
    /// endpoint mengharapkan field lain selain file.
    async fn wiggle_upload(
        &self,
        org: &str,
        conv: &str,
        bytes: &[u8],
        mime: &str,
        filename: &str,
    ) -> Result<UploadedFile, ProviderError> {
        let boundary = format!("aiweb2api-{}", Uuid::new_v4().simple());
        let body = multipart_file_body(&boundary, "file", filename, mime, bytes);
        let resp = self
            .client
            .post(format!(
                "https://claude.ai/api/organizations/{org}/conversations/{conv}/wiggle/upload-file"
            ))
            .headers(self.headers()?)
            .header(
                "content-type",
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(body)
            .send()
            .await?;
        // Rotasi cookie dari respons wiggle upload.
        self.merge_response_cookies(resp.headers());
        let status = resp.status().as_u16();
        if status == 401 {
            return Err(ProviderError::AuthExpired(
                "cookie Claude kedaluwarsa (401) saat upload file — paste ulang cookie claude.ai"
                    .to_string(),
            ));
        }
        if status == 403 {
            return Err(ProviderError::Unauthorized(
                "claude.ai menolak (403) saat upload file".to_string(),
            ));
        }
        if status == 429 {
            return Err(ProviderError::RateLimited {
                retry_after: retry_after_secs(&resp),
                message: "claude.ai rate limit (429) saat upload file".to_string(),
            });
        }
        if status != 200 && status != 201 {
            let text = resp.text().await.unwrap_or_default();
            return Err(ProviderError::upstream(
                status,
                &format!("wiggle upload gagal: {text}"),
            ));
        }
        let v: Value = resp
            .json()
            .await
            .map_err(|e| ProviderError::Parse(format!("respons wiggle upload bukan JSON: {e}")))?;
        let file_uuid = v
            .get("file_uuid")
            .and_then(|u| u.as_str())
            .ok_or_else(|| {
                ProviderError::Parse("wiggle upload tidak mengembalikan file_uuid".to_string())
            })?
            .to_string();
        let file_kind = v.get("file_kind").and_then(|k| k.as_str()).unwrap_or("");
        let is_document =
            file_kind == "document" || (file_kind.is_empty() && is_document_mime(mime));
        Ok(UploadedFile {
            file_uuid,
            is_document,
        })
    }

    /// Konversi dokumen yang sudah di-upload menjadi attachment
    /// (konten terekstrak inline). HANYA untuk dokumen — gambar
    /// mengembalikan 415 dan TIDAK BOLEH dikirim ke sini.
    ///
    /// TODO(live-capture): bentuk multipart persis BELUM terverifikasi:
    /// nama field (`"file"` best-effort), apakah endpoint mengharapkan
    /// byte file ulang atau `file_uuid` hasil wiggle upload, dan
    /// field tambahan lain.
    async fn convert_document(
        &self,
        org: &str,
        bytes: &[u8],
        mime: &str,
        filename: &str,
    ) -> Result<Value, ProviderError> {
        let boundary = format!("aiweb2api-{}", Uuid::new_v4().simple());
        let body = multipart_file_body(&boundary, "file", filename, mime, bytes);
        let resp = self
            .client
            .post(format!(
                "https://claude.ai/api/organizations/{org}/convert_document"
            ))
            .headers(self.headers()?)
            .header(
                "content-type",
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(body)
            .send()
            .await?;
        // Rotasi cookie dari respons convert_document.
        self.merge_response_cookies(resp.headers());
        let status = resp.status().as_u16();
        if status == 415 {
            // 415 = file diperlakukan sebagai gambar; penelepon
            // fallback ke referensi `files` (bukan error fatal).
            return Err(ProviderError::Upstream {
                status: 415,
                body_snippet: "convert_document menolak (415) — file diperlakukan sebagai gambar"
                    .to_string(),
            });
        }
        if status == 401 {
            return Err(ProviderError::AuthExpired(
                "cookie Claude kedaluwarsa (401) saat convert document — \
                 paste ulang cookie claude.ai"
                    .to_string(),
            ));
        }
        if status == 403 {
            return Err(ProviderError::Unauthorized(
                "claude.ai menolak (403) saat convert document".to_string(),
            ));
        }
        if status == 429 {
            return Err(ProviderError::RateLimited {
                retry_after: retry_after_secs(&resp),
                message: "claude.ai rate limit (429) saat convert document".to_string(),
            });
        }
        if status != 200 && status != 201 {
            let text = resp.text().await.unwrap_or_default();
            return Err(ProviderError::upstream(
                status,
                &format!("convert_document gagal: {text}"),
            ));
        }
        let v: Value = resp.json().await.map_err(|e| {
            ProviderError::Parse(format!("respons convert_document bukan JSON: {e}"))
        })?;
        Ok(build_attachment(&v, filename, bytes.len() as u64, mime))
    }

    /// Upload semua gambar dalam histori: dokumen -> `attachments`,
    /// gambar -> `files` (`file_uuid`). Kegagalan upload adalah
    /// error (tidak pernah diam-diam di-drop).
    async fn prepare_media(
        &self,
        org: &str,
        conv: &str,
        messages: &[Message],
    ) -> Result<(Option<Vec<Value>>, Option<Vec<String>>), ProviderError> {
        let mut attachments: Vec<Value> = Vec::new();
        let mut files: Vec<String> = Vec::new();
        for m in messages {
            for part in &m.parts {
                let Part::ImageUrl(src) = part else {
                    continue;
                };
                let input = match src {
                    ImageSource::Url(url) => ImageInput::Url(url.clone()),
                    ImageSource::DataUri { mime, base64 } => ImageInput::DataUri {
                        mime: mime.clone(),
                        base64: base64.clone(),
                    },
                };
                let downloaded = download_image_source(&input).await?;
                let filename = filename_for_mime(&downloaded.mime);
                let uploaded = self
                    .wiggle_upload(org, conv, &downloaded.bytes, &downloaded.mime, filename)
                    .await?;
                if uploaded.is_document {
                    match self
                        .convert_document(org, &downloaded.bytes, &downloaded.mime, filename)
                        .await
                    {
                        Ok(att) => attachments.push(att),
                        Err(ProviderError::Upstream { status: 415, .. }) => {
                            // convert menolak: file ternyata gambar —
                            // pakai referensi file, bukan gagal.
                            files.push(uploaded.file_uuid);
                        }
                        Err(e) => return Err(e),
                    }
                } else {
                    files.push(uploaded.file_uuid);
                }
            }
        }
        let attachments = if attachments.is_empty() {
            None
        } else {
            Some(attachments)
        };
        let files = if files.is_empty() { None } else { Some(files) };
        Ok((attachments, files))
    }

    /// POST completion (percobaan pertama).
    async fn post_completion(
        &self,
        org: &str,
        conv: &str,
        body: &Value,
    ) -> Result<reqwest::Response, ProviderError> {
        let resp = self
            .client
            .post(format!(
                "https://claude.ai/api/organizations/{org}/chat_conversations/{conv}/completion"
            ))
            .headers(self.headers()?)
            .header("accept", "text/event-stream")
            .json(body)
            .send()
            .await?;
        // Rotasi cookie dari header respons completion (sebelum
        // body stream dibaca).
        self.merge_response_cookies(resp.headers());
        Ok(resp)
    }

    /// POST retry_completion (percobaan ulang pre-first-byte).
    async fn post_retry_completion(
        &self,
        org: &str,
        conv: &str,
        body: &Value,
    ) -> Result<reqwest::Response, ProviderError> {
        let resp = self
            .client
            .post(format!(
                "https://claude.ai/api/organizations/{org}/chat_conversations/{conv}/retry_completion"
            ))
            .headers(self.headers()?)
            .header("accept", "text/event-stream")
            .json(body)
            .send()
            .await?;
        // Rotasi cookie dari header respons retry_completion.
        self.merge_response_cookies(resp.headers());
        Ok(resp)
    }

    /// Kirim completion dengan satu retry pre-first-byte:
    /// - status retryable (529/500/502/503) -> backoff lalu retry
    ///   via `retry_completion` (429 TIDAK di-retry otomatis —
    ///   dikembalikan sebagai [`ProviderError::RateLimited`]);
    /// - error yang menyebut `paprika` saat thinking On -> retry
    ///   sekali dengan `paprika_mode` di-reset ke null (model tidak
    ///   mendukung mode extended);
    /// - `thinking_messages_rate_limit_exceeded` -> RateLimited.
    ///
    /// Maksimal 2 percobaan; tidak pernah me-replay stream yang
    /// sudah menerima byte.
    async fn post_completion_with_retry(
        &self,
        org: &str,
        conv: &str,
        body: &Value,
        thinking: Thinking,
    ) -> Result<reqwest::Response, ProviderError> {
        let mut body = body.clone();
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let resp = if attempt == 1 {
                self.post_completion(org, conv, &body).await?
            } else {
                self.post_retry_completion(org, conv, &body).await?
            };
            let status = resp.status().as_u16();
            if status == 200 {
                return Ok(resp);
            }
            let retry_after = retry_after_secs(&resp);
            let text = resp.text().await.unwrap_or_default();
            if status == 401 {
                return Err(ProviderError::AuthExpired(
                    "cookie Claude kedaluwarsa (401) saat completion — \
                     paste ulang cookie claude.ai"
                        .to_string(),
                ));
            }
            if status == 403 {
                return Err(ProviderError::Unauthorized(
                    "claude.ai menolak (403) saat completion — kemungkinan \
                     diblokir Cloudflare / cookie tidak valid"
                        .to_string(),
                ));
            }
            if text.contains("thinking_messages_rate_limit_exceeded") {
                return Err(ProviderError::RateLimited {
                    retry_after,
                    message: "Claude thinking-message rate limit exceeded \
                        (limit pesan thinking mingguan) — tunggu reset \
                        mingguan atau matikan thinking"
                        .to_string(),
                });
            }
            if status == 429 {
                return Err(ProviderError::RateLimited {
                    retry_after,
                    message: format!("claude.ai rate limit (429): {}", sanitize_snippet(&text)),
                });
            }
            if attempt == 1 && is_retryable_status(status) {
                tokio::time::sleep(backoff_delay(1)).await;
                continue;
            }
            if attempt == 1
                && matches!(thinking, Thinking::On { .. })
                && text.to_lowercase().contains("paprika")
            {
                tracing::warn!(
                    "model tampaknya tidak mendukung paprika_mode extended — \
                     retry dengan paprika_mode null"
                );
                body["paprika_mode"] = Value::Null;
                continue;
            }
            return Err(ProviderError::upstream(
                status,
                &format!("completion gagal: {text}"),
            ));
        }
    }
}

#[async_trait::async_trait]
impl Provider for ClaudeProvider {
    fn id(&self) -> &'static str {
        "claude"
    }

    fn web_default_model(&self) -> &'static str {
        // Alias model default akun (bukan model spesifik terkunci).
        WEB_DEFAULT_MODEL
    }

    fn models(&self) -> Vec<ModelInfo> {
        // Katalog live dari cache + alias default generik agar
        // routing dengan nama "claude" tetap jalan (lihat catatan
        // katalog di header modul). Tanpa fallback statis.
        let guard = self
            .catalog
            .read()
            .unwrap_or_else(|poison| poison.into_inner());
        let mut out: Vec<ModelInfo> = guard.models().to_vec();
        if !out.iter().any(|m| m.id == WEB_DEFAULT_MODEL) {
            out.push(ModelInfo::new(WEB_DEFAULT_MODEL, "anthropic"));
        }
        out
    }

    async fn warm_catalog(&self) -> Result<(), ProviderError> {
        // Tanpa jaringan bila belum dikonfigurasi.
        if self.jar_is_empty() {
            return Err(ProviderError::Unauthorized(
                "AIW2A_CLAUDE_COOKIES belum diisi".to_string(),
            ));
        }
        let org = self.ensure_org().await?;
        self.ensure_catalog(&org).await?;
        Ok(())
    }

    async fn chat(&self, input: &ChatInput) -> Result<EventStream, ProviderError> {
        if self.jar_is_empty() {
            return Err(ProviderError::Unauthorized(
                "AIW2A_CLAUDE_COOKIES belum diisi".to_string(),
            ));
        }
        // Pacing antar-request: lantai 1500ms untuk Claude
        // (riset-backed; lihat desain §3.8).
        self.pacer.wait().await;
        // Nilai cookie tidak pernah di-log — hanya mode auth-nya.
        tracing::debug!(full_cookie = self.full_cookie, "mode auth Claude");
        let org = self.ensure_org().await?;
        // Katalog live (best-effort; gagal -> warning + alias default).
        self.ensure_catalog(&org).await?;
        let (resolved_model, warning) = {
            let guard = self
                .catalog
                .read()
                .unwrap_or_else(|poison| poison.into_inner());
            resolve_requested_model(&input.model, guard.models(), WEB_DEFAULT_MODEL)
        };
        if let Some(w) = warning {
            tracing::warn!("{w}");
        }
        let model_param = if resolved_model == WEB_DEFAULT_MODEL {
            None
        } else {
            Some(resolved_model.as_str())
        };
        let conv_uuid = Uuid::new_v4().to_string();

        // 1. Buat conversation baru (+ settings search bila On).
        self.create_conversation(&org, &conv_uuid, model_param, input.thinking, input.search)
            .await?;
        let headers = self.headers()?;

        // 2. Vision: upload gambar/dokumen (gagal -> error + cleanup).
        let (attachments, files) = match self.prepare_media(&org, &conv_uuid, &input.messages).await
        {
            Ok(m) => m,
            Err(e) => {
                delete_conversation(&self.client, &headers, &org, &conv_uuid).await;
                return Err(e);
            }
        };

        // 3. Kirim completion (SSE), dengan satu retry pre-first-byte.
        let prompt = build_prompt(&input.messages);
        let prompt_chars = prompt.chars().count();
        let body = build_completion_body(
            &prompt,
            model_param,
            input.thinking,
            attachments.as_deref(),
            files.as_deref(),
        );
        let resp = match self
            .post_completion_with_retry(&org, &conv_uuid, &body, input.thinking)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                delete_conversation(&self.client, &headers, &org, &conv_uuid).await;
                return Err(e);
            }
        };

        let mut byte_stream = resp.bytes_stream();
        // Clone untuk cleanup: hapus conversation setelah respons
        // selesai, biar tidak nyampah di histori akun.
        let cleanup_client = self.client.clone();
        let cleanup_headers = headers.clone();
        let cleanup_org = org.clone();
        let cleanup_conv = conv_uuid.clone();
        let s = stream! {
            let mut buf = String::new();
            let mut parser = SseParser::new();
            let mut got_any = false;
            let mut stream_err: Option<ProviderError> = None;
            while let Some(chunk) = byte_stream.next().await {
                let bytes = match chunk {
                    Ok(b) => b,
                    Err(e) => {
                        stream_err = Some(ProviderError::Network(e.to_string()));
                        break;
                    }
                };
                buf.push_str(&String::from_utf8_lossy(&bytes));
                let mut stop = false;
                while let Some(pos) = buf.find('\n') {
                    let line: String = buf.drain(..=pos).collect();
                    let line = line.trim();
                    let Some(data) = line.strip_prefix("data:") else {
                        continue;
                    };
                    let data = data.trim();
                    if data.is_empty() || data == "[DONE]" {
                        continue;
                    }
                    let v: Value = match serde_json::from_str(data) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    match parser.feed(&v) {
                        Ok(events) => {
                            for ev in events {
                                got_any = true;
                                yield Ok(ev);
                            }
                        }
                        Err(e) => {
                            stream_err = Some(e);
                            stop = true;
                            break;
                        }
                    }
                    if parser.is_done() {
                        stop = true;
                        break;
                    }
                }
                if stop {
                    break;
                }
            }
            if let Some(e) = stream_err {
                yield Err(e);
            } else if !got_any {
                yield Err(ProviderError::Parse(
                    "respons Claude kosong/tidak dikenali — protokol web mungkin berubah"
                        .to_string(),
                ));
            } else {
                tracing::debug!(
                    stop_reason = ?parser.stop_reason(),
                    "stream Claude selesai"
                );
                yield Ok(parser.usage_event(prompt_chars));
                yield Ok(Event::Done);
            }
            // Bersih-bersih: hapus conversation sementara (best-effort).
            delete_conversation(&cleanup_client, &cleanup_headers, &cleanup_org, &cleanup_conv)
                .await;
        };
        Ok(Box::pin(s))
    }
}
