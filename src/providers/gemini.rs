//! Provider Gemini via antarmuka web gemini.google.com.
//!
//! Protokol (riset `docs/research-web2api-2026-10-16.md` §1):
//! `POST .../StreamGenerate` dengan body `f.req` nested-array +
//! `at` (XSRF `SNlM0e`) + `bl`; respons `wrb.fr` length-prefixed.
//! Katalog model live via batchexecute RPC `otAQ7b` (lihat
//! [`GeminiProvider::refresh_models`]).
//!
//! Fitur Fase 2:
//! - Event native: thought bertag -> [`Event::Reasoning`] SEBELUM
//!   teks body -> [`Event::Text`]; usage estimasi (`chars / 4`,
//!   `estimated: true`).
//! - Vision: gambar input diunduh via `core::media`, lalu upload
//!   resumable ke `content-push.googleapis.com` -> referensi blob
//!   WIZ yang disertakan di payload. Upload gagal -> error Upstream
//!   yang jelas; gambar TIDAK PERNAH di-drop diam-diam.
//! - Thinking: varian katalog `"<model>-thinking"` (toggle UI yang
//!   ortogonal terhadap model dasar). `@think=N` TIDAK PERNAH
//!   diimplementasikan — `inner[17]` adalah index turn, bukan
//!   kedalaman thinking.
//! - Search: native/warisan backend web, tanpa flag yang
//!   didokumentasikan. Toggle search deterministik adalah
//!   UNVERIFIED dan butuh capture live dengan toggle UI menyala —
//!   tidak ada flag yang dikarang.
//! - Auth: cookie di-parse sekali via `core::auth::CookieJar`.
//!   Degradasi diam-diam (mis. `SNlM0e` hilang / body respons
//!   kosong) -> [`ProviderError::AuthExpired`], bukan jawaban
//!   model-salah yang diam-diam.
//!
//! KETERBATASAN YANG DIKETAHUI (butuh capture live, Fase 5):
//! - Slot lampiran gambar di payload `StreamGenerate` (`inner[61]`)
//!   dan bentuk respons upload blob WIZ belum terverifikasi dari
//!   capture resmi — ditulis best-effort dari riset repo web2api.
//! - Slot thought eksak belum terverifikasi (lihat `parse.rs`).
//! - Upload belum mengirim `SAPISIDHASH` (butuh SHA-1; tidak ada
//!   dependensi SHA-1) — bila Google mewajibkannya, upload gagal
//!   dengan error Upstream yang jelas.

mod parse;

use crate::core::auth::{persist_jar_best_effort, CookieJar};
use crate::core::catalog::{resolve_model, ModelCatalog};
use crate::core::events::{Event, EventStream};
use crate::core::media::{download_image_source, DownloadedImage, ImageInput};
use crate::core::retry::Pacer;
use crate::error::ProviderError;
use crate::openai::ModelInfo;
use crate::providers::{
    http_client, ChatInput, GeneratedImage, ImageSource, Message, Part, Provider, Role, Search,
    Thinking,
};
use async_stream::stream;
use futures::StreamExt;
use parse::{
    estimate_tokens, extract_image_urls, parse_catalog_payload, parse_wrb_frames,
    segments_from_inner,
};
use rand::Rng;
use reqwest::header::HeaderMap;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;

const GENERATE_URL: &str = "https://gemini.google.com/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate";
const BATCHEXECUTE_URL: &str = "https://gemini.google.com/_/BatchedExecute";
const INIT_URL: &str = "https://gemini.google.com/app";
/// Endpoint upload resumable gambar (Scotty / content-push).
const CONTENT_PUSH_URL: &str = "https://content-push.googleapis.com/upload/";
/// TTL cache katalog model live (1 jam, sesuai desain §3.6).
const CATALOG_TTL: Duration = Duration::from_secs(3600);

/// Token bootstrap hasil scrape halaman Gemini.
#[derive(Default)]
struct GeminiTokens {
    snlm0e: Option<String>, // -> form field `at`
    bl: Option<String>,     // cfb2h -> query param `bl`
    sid: Option<String>,    // FdrFJe -> query param `f.sid`
}

/// State kumulatif satu frame stream (untuk delta thought/teks).
#[derive(Default)]
struct FrameState {
    last_thought: String,
    last_text: String,
}

/// Provider Gemini via antarmuka web gemini.google.com.
///
/// Autentikasi: cookie Google (minimal `__Secure-1PSID`).
pub struct GeminiProvider {
    /// Cookie jar (di balik lock agar merge rotasi `Set-Cookie` bisa
    /// dilakukan dari method `&self`).
    cookies: std::sync::RwLock<CookieJar>,
    client: reqwest::Client,
    tokens: RwLock<GeminiTokens>,
    reqid: Mutex<u64>,
    catalog: std::sync::RwLock<ModelCatalog>,
    /// Slug/id model -> hash model untuk header
    /// `x-goog-ext-525001261-jspb` (dari katalog live; TIDAK PERNAH
    /// di-hardcode — hash dari capture lama bisa basi sewaktu-waktu).
    model_hashes: std::sync::RwLock<HashMap<String, String>>,
    /// Pacing antar-request (0ms untuk Gemini — mekanisme tersedia,
    /// interval minimum 0; hanya Claude yang memakai lantai 1500ms).
    pacer: Pacer,
    /// File persistensi rotasi cookie (env
    /// `AIW2A_GEMINI_COOKIES_FILE`, opsional; `None` = rotasi hanya
    /// in-memory).
    persist_path: Option<PathBuf>,
}

impl GeminiProvider {
    /// Buat provider dari string header `Cookie` (di-parse sekali
    /// menjadi [`CookieJar`]; nilai cookie TIDAK PERNAH di-log).
    pub fn new(cookies: String) -> Self {
        Self {
            cookies: std::sync::RwLock::new(CookieJar::parse(cookies.trim())),
            client: http_client(),
            tokens: RwLock::new(GeminiTokens::default()),
            reqid: Mutex::new(rand::thread_rng().gen_range(10000..99999)),
            catalog: std::sync::RwLock::new(ModelCatalog::new(CATALOG_TTL)),
            model_hashes: std::sync::RwLock::new(HashMap::new()),
            pacer: Pacer::new(Duration::from_millis(0)),
            persist_path: std::env::var("AIW2A_GEMINI_COOKIES_FILE")
                .ok()
                .filter(|v| !v.trim().is_empty())
                .map(PathBuf::from),
        }
    }

    /// Nilai header `Cookie` dari jar (nama terurut, deterministik).
    fn cookie_header(&self) -> String {
        // Lock std, kritis singkat; poison berarti panic sebelumnya —
        // pakai inner-nya agar tidak panic di sini.
        self.cookies
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .render()
    }

    /// Benar bila jar tidak berisi cookie apa pun.
    fn cookies_empty(&self) -> bool {
        self.cookies
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .is_empty()
    }

    /// Baca satu nilai cookie dari jar.
    fn cookies_get(&self, name: &str) -> Option<String> {
        self.cookies
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(name)
            .map(str::to_string)
    }

    /// Merge header `Set-Cookie` dari satu respons upstream ke jar
    /// (rotasi) lalu persist best-effort (tidak pernah menggagalkan
    /// request, tidak pernah me-log nilai cookie).
    fn merge_response_cookies(&self, headers: &HeaderMap) {
        let mut jar = self
            .cookies
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        for val in headers.get_all("set-cookie") {
            if let Ok(s) = val.to_str() {
                jar.merge_set_cookie(s);
            }
        }
        persist_jar_best_effort(&jar, self.persist_path.as_deref());
    }

    /// Ambil SNlM0e/cfb2h/FdrFJe dari halaman /app (di-cache).
    ///
    /// `SNlM0e` yang hilang berarti sesi tidak lagi terautentikasi
    /// (degradasi diam-diam) -> [`ProviderError::AuthExpired`].
    async fn ensure_tokens(&self) -> Result<(), ProviderError> {
        {
            let t = self.tokens.read().await;
            if t.snlm0e.is_some() {
                return Ok(());
            }
        }
        let resp = self
            .client
            .get(INIT_URL)
            .header("cookie", self.cookie_header())
            .header("accept", "text/html")
            .send()
            .await?;
        // Rotasi cookie dari respons halaman /app (mis. `__Secure-1PSIDTS` baru).
        self.merge_response_cookies(resp.headers());
        let status = resp.status().as_u16();
        if status == 401 || status == 403 {
            return Err(ProviderError::AuthExpired(format!(
                "gemini.google.com/app menolak sesi (HTTP {status}) — cookie kedaluwarsa/tidak valid"
            )));
        }
        if status != 200 {
            return Err(ProviderError::Upstream {
                status,
                body_snippet: format!("gagal memuat gemini.google.com/app: HTTP {status}"),
            });
        }
        let html = resp.text().await?;
        let snlm0e = extract_json_string(&html, "SNlM0e");
        let cfb2h = extract_json_string(&html, "cfb2h");
        let fdrfje = extract_json_string(&html, "FdrFJe");

        let Some(snlm0e) = snlm0e else {
            return Err(ProviderError::AuthExpired(
                "tidak menemukan token SNlM0e — sesi tidak lagi terautentikasi \
                 (cookie Google kedaluwarsa/tidak valid, atau akun belum login \
                 di gemini.google.com)"
                    .to_string(),
            ));
        };
        let mut t = self.tokens.write().await;
        t.snlm0e = Some(snlm0e);
        t.bl = cfb2h;
        t.sid = fdrfje;
        Ok(())
    }

    fn next_reqid(&self) -> u64 {
        // Mutex std tidak bisa gagal kecuali poisoned (bug panic
        // sebelumnya); pakai inner-nya agar tidak panic di sini.
        let mut r = self.reqid.lock().unwrap_or_else(|e| e.into_inner());
        let v = *r;
        *r += 100000;
        v
    }

    /// Bangun header model `x-goog-ext-525001261-jspb`.
    ///
    /// Index 4 = hash model dari katalog live (`model_hashes`);
    /// `null` = default akun (model `"gemini"` atau slug yang hash-nya
    /// belum diketahui dari katalog — TIDAK PERNAH hash hardcode,
    /// dan TIDAK PERNAH mengunci ke model spesifik). Index 7 = flag
    /// temporary chat.
    fn model_header(&self, model: &str) -> String {
        let mut hash: Option<String> = None;
        if model != "gemini" && !model.is_empty() {
            if let Ok(map) = self.model_hashes.read() {
                hash = map.get(model).cloned();
            }
            if hash.is_none() {
                tracing::debug!(
                    model = model,
                    "hash model tidak diketahui dari katalog live — memakai default akun (null)"
                );
            }
        }
        let h = hash
            .map(|s| format!("\"{s}\""))
            .unwrap_or_else(|| "null".to_string());
        format!(
            "[1,null,null,null,{h},null,null,true,[4,5,6,8],null,null,null,null,null,null,null]"
        )
    }

    /// Refresh katalog model live via batchexecute RPC `otAQ7b`.
    ///
    /// Sukses: cache + peta hash diperbarui, daftar model
    /// dikembalikan. Gagal upstream: cache dibiarkan apa adanya
    /// (bisa kosong) dan error dikembalikan — penelepon me-log
    /// warning; TIDAK ADA daftar fallback statis.
    pub async fn refresh_models(&self) -> Result<Vec<ModelInfo>, ProviderError> {
        if self.cookies_empty() {
            return Err(ProviderError::Unauthorized(
                "AIW2A_GEMINI_COOKIES belum diisi".to_string(),
            ));
        }
        self.ensure_tokens().await?;
        let (snlm0e, bl, sid) = {
            let t = self.tokens.read().await;
            (
                t.snlm0e.clone().unwrap_or_default(),
                t.bl.clone(),
                t.sid.clone(),
            )
        };
        // Envelope batchexecute: f.req = [[[rpcid, params-json, null, "generic"]]].
        let f_req = serde_json::to_string(&json!([[["otAQ7b", "[]", null, "generic"]]]))
            .map_err(|e| ProviderError::Parse(e.to_string()))?;
        let form_body =
            serde_urlencoded::to_string([("at", snlm0e.as_str()), ("f.req", f_req.as_str())])
                .map_err(|e| ProviderError::Parse(e.to_string()))?;
        let mut url = format!("{BATCHEXECUTE_URL}?hl=en&rt=c");
        if let Some(bl) = bl {
            url.push_str(&format!("&bl={}", urlencoding(&bl)));
        }
        if let Some(sid) = sid {
            url.push_str(&format!("&f.sid={}", urlencoding(&sid)));
        }
        let resp = self
            .client
            .post(&url)
            .header("cookie", self.cookie_header())
            .header(
                "content-type",
                "application/x-www-form-urlencoded;charset=utf-8",
            )
            .header("origin", "https://gemini.google.com")
            .header("referer", "https://gemini.google.com/")
            .header("x-same-domain", "1")
            .body(form_body)
            .send()
            .await?;
        // Rotasi cookie dari respons batchexecute.
        self.merge_response_cookies(resp.headers());
        let status = resp.status().as_u16();
        if status == 401 || status == 403 {
            return Err(ProviderError::AuthExpired(format!(
                "batchexecute otAQ7b menolak sesi (HTTP {status}) — cookie kedaluwarsa/tidak valid"
            )));
        }
        if status != 200 {
            return Err(ProviderError::upstream(
                status,
                &format!("gemini batchexecute otAQ7b -> HTTP {status}"),
            ));
        }
        let body = resp.text().await?;
        let entries = parse_catalog_payload(&body);
        if entries.is_empty() {
            return Err(ProviderError::Parse(
                "katalog otAQ7b kosong/tidak dikenali — format payload mungkin berubah".to_string(),
            ));
        }
        let models: Vec<ModelInfo> = entries
            .iter()
            .map(|e| ModelInfo::new(&e.id, "google"))
            .collect();
        if let Ok(mut hashes) = self.model_hashes.write() {
            hashes.clear();
            for e in &entries {
                if let Some(hex) = &e.hex {
                    hashes.insert(e.id.clone(), hex.clone());
                }
            }
        }
        if let Ok(mut catalog) = self.catalog.write() {
            catalog.update(models.clone());
        }
        Ok(models)
    }

    /// Refresh katalog bila basi; kegagalan hanya di-log (chat tetap
    /// jalan dengan katalog lama/kosong -> default web + warning).
    async fn maybe_refresh_catalog(&self) {
        let fresh = match self.catalog.read() {
            Ok(g) => g.is_fresh(),
            Err(_) => false,
        };
        if fresh {
            return;
        }
        if let Err(e) = self.refresh_models().await {
            tracing::warn!("gagal refresh katalog model Gemini: {e}");
        }
    }

    /// Resolusi model untuk satu request chat.
    ///
    /// Aturan (`core::catalog::resolve_model`): diminta ada di katalog
    /// -> dipakai; tidak ada / katalog kosong -> default web
    /// `"gemini"` + warning. `Thinking::On` mencoba varian katalog
    /// `"<model>-thinking"` (toggle UI ortogonal terhadap model
    /// dasar); bila varian tidak ada di katalog, model dasar dipakai
    /// + warning. TIDAK PERNAH `@think=N` / `inner[17]`.
    async fn resolve_chat_model(&self, requested: &str, thinking: Thinking) -> String {
        self.maybe_refresh_catalog().await;
        let snapshot: Vec<ModelInfo> = match self.catalog.read() {
            Ok(g) => g.models().to_vec(),
            Err(_) => Vec::new(),
        };
        let (mut resolved, warning) = resolve_model(Some(requested), &snapshot, "gemini");
        if let Some(w) = warning {
            tracing::warn!("{w}");
        }
        if matches!(thinking, Thinking::On { .. }) {
            let variant = format!("{resolved}-thinking");
            if snapshot.iter().any(|m| m.id == variant) {
                resolved = variant;
            } else {
                tracing::warn!(
                    "thinking diminta tapi varian '{variant}' tidak ada di katalog live — \
                     memakai model dasar '{resolved}'"
                );
            }
        }
        resolved
    }

    /// POST StreamGenerate mentah (dipakai chat streaming & image gen).
    async fn post_stream_generate(
        &self,
        prompt: &str,
        model: &str,
        attachments: &[String],
    ) -> Result<reqwest::Response, ProviderError> {
        self.ensure_tokens().await?;

        let (snlm0e, bl, sid) = {
            let t = self.tokens.read().await;
            (
                t.snlm0e.clone().unwrap_or_default(),
                t.bl.clone(),
                t.sid.clone(),
            )
        };

        let inner = Self::build_inner_req(prompt, attachments);
        let inner_str =
            serde_json::to_string(&inner).map_err(|e| ProviderError::Parse(e.to_string()))?;
        let f_req = serde_json::to_string(&json!([null, inner_str]))
            .map_err(|e| ProviderError::Parse(e.to_string()))?;
        let form_body =
            serde_urlencoded::to_string([("at", snlm0e.as_str()), ("f.req", f_req.as_str())])
                .map_err(|e| ProviderError::Parse(e.to_string()))?;

        let mut url = format!("{GENERATE_URL}?hl=en&_reqid={}&rt=c", self.next_reqid());
        if let Some(bl) = bl {
            url.push_str(&format!("&bl={}", urlencoding(&bl)));
        }
        if let Some(sid) = sid {
            url.push_str(&format!("&f.sid={}", urlencoding(&sid)));
        }

        let uuid_val = Uuid::new_v4().to_string().to_uppercase();
        let resp = self
            .client
            .post(&url)
            .header("cookie", self.cookie_header())
            .header(
                "content-type",
                "application/x-www-form-urlencoded;charset=utf-8",
            )
            .header("origin", "https://gemini.google.com")
            .header("referer", "https://gemini.google.com/")
            .header("x-same-domain", "1")
            .header("x-goog-ext-525005358-jspb", format!(r#"["{uuid_val}",1]"#))
            .header("x-goog-ext-525001261-jspb", self.model_header(model))
            .header("x-goog-ext-73010989-jspb", "[0]")
            .header("x-goog-ext-73010990-jspb", "[0,0,0]")
            .body(form_body)
            .send()
            .await?;

        // Rotasi cookie dari respons StreamGenerate (sebelum body
        // stream dibaca).
        self.merge_response_cookies(resp.headers());
        let status = resp.status().as_u16();
        if status == 401 || status == 403 {
            // Token bootstrap mungkin basi -> paksa refresh sekali.
            if let Ok(mut t) = self.tokens.try_write() {
                t.snlm0e = None;
            }
            return Err(ProviderError::AuthExpired(format!(
                "gemini StreamGenerate menolak sesi (HTTP {status}) — cookie kedaluwarsa/tidak valid"
            )));
        }
        if status != 200 {
            // Token bootstrap mungkin basi -> paksa refresh sekali.
            if status == 400 {
                if let Ok(mut t) = self.tokens.try_write() {
                    t.snlm0e = None;
                }
            }
            return Err(ProviderError::Upstream {
                status,
                body_snippet: format!("gemini StreamGenerate -> HTTP {status}"),
            });
        }
        Ok(resp)
    }

    /// Bangun inner_req_list (array 81 slot) sesuai protokol StreamGenerate.
    ///
    /// `attachments` adalah referensi blob WIZ dari upload gambar
    /// (vision). Slot `inner[61]` dipakai untuk lampiran — slot ini
    /// UNVERIFIED dan butuh capture live (Fase 5); tanpa lampiran
    /// nilainya `[]` seperti v1.
    ///
    /// CATATAN: `inner[17]` adalah index turn dalam percakapan,
    /// BUKAN kedalaman thinking — jangan pernah mengisinya dari
    /// `Thinking` (suffix `@think=N` terbukti salah, riset §1b).
    fn build_inner_req(prompt: &str, attachments: &[String]) -> Value {
        let mut inner: Vec<Value> = vec![Value::Null; 81];
        inner[0] = json!([prompt, 0, null, null, null, null, 0]);
        inner[1] = json!(["en"]);
        inner[2] = json!(["", "", "", null, null, null, null, null, null, ""]);
        inner[4] = json!(Uuid::new_v4().simple().to_string());
        inner[6] = json!([1]);
        inner[7] = json!(1); // streaming flag
        inner[10] = json!(1);
        inner[11] = json!(0);
        inner[17] = json!([[0]]); // index turn (BUKAN thinking depth)
        inner[18] = json!(0);
        inner[27] = json!(1);
        inner[30] = json!([4]);
        inner[41] = json!(1);
        // Temporary chat: boolean true (bukan 1), dicerminkan juga di header
        // x-goog-ext-525001261-jspb index 7.
        inner[45] = json!(true);
        inner[53] = json!(0);
        inner[55] = json!([[1]]);
        inner[59] = json!(Uuid::new_v4().to_string().to_uppercase());
        inner[61] = json!(attachments);
        inner[68] = json!(1);
        inner[79] = json!(1);
        inner[80] = json!(1);
        Value::Array(inner)
    }

    /// Upload satu gambar (vision) ke `content-push.googleapis.com`
    /// via upload resumable dua langkah, kembalikan referensi blob
    /// WIZ-nya.
    ///
    /// Cookie wajib (upload anonim ditolak saat referensi dipakai:
    /// `BardErrorInfo 1100`, riset §1b). Gagal di langkah apa pun
    /// -> error Upstream yang jelas; gambar TIDAK PERNAH di-drop
    /// diam-diam. Gambar di-upload fresh per request (TTL
    /// server-side ~24 jam, riset §1d).
    async fn upload_image(&self, image: &DownloadedImage) -> Result<String, ProviderError> {
        if self.cookies_get("__Secure-1PSID").is_none() {
            return Err(ProviderError::Unauthorized(
                "vision Gemini butuh cookie __Secure-1PSID — upload gambar \
                 tanpa sesi login ditolak backend (BardErrorInfo 1100)"
                    .to_string(),
            ));
        }
        let filename = format!("image.{}", mime_extension(&image.mime));
        let meta = json!({ "fileName": filename, "mimeType": image.mime });
        let meta_str =
            serde_json::to_string(&meta).map_err(|e| ProviderError::Parse(e.to_string()))?;
        let start = self
            .client
            .post(CONTENT_PUSH_URL)
            .header("cookie", self.cookie_header())
            .header("origin", "https://gemini.google.com")
            .header("referer", "https://gemini.google.com/")
            .header("content-type", "application/json; charset=UTF-8")
            .header("x-goog-upload-protocol", "resumable")
            .header("x-goog-upload-command", "start")
            .header(
                "x-goog-upload-header-content-length",
                image.bytes.len().to_string(),
            )
            .header("x-goog-upload-header-content-type", &image.mime)
            .body(meta_str)
            .send()
            .await?;
        // Rotasi cookie dari respons mulai-upload (content-push).
        self.merge_response_cookies(start.headers());
        let status = start.status().as_u16();
        if !matches!(status, 200 | 201) {
            let body = start.text().await.unwrap_or_default();
            return Err(ProviderError::upstream(
                status,
                &format!("mulai upload gambar Gemini gagal: HTTP {status} — {body}"),
            ));
        }
        let session_url = start
            .headers()
            .get("x-goog-upload-url")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let Some(session_url) = session_url else {
            return Err(ProviderError::Parse(
                "mulai upload gambar berhasil tapi respons tidak membawa \
                 header x-goog-upload-url — protokol upload mungkin berubah"
                    .to_string(),
            ));
        };
        let upload = self
            .client
            .put(&session_url)
            .header("cookie", self.cookie_header())
            .header("x-goog-upload-command", "upload, finalize")
            .header("x-goog-upload-offset", "0")
            .header("content-type", &image.mime)
            .body(image.bytes.clone())
            .send()
            .await?;
        // Rotasi cookie dari respons upload (content-push).
        self.merge_response_cookies(upload.headers());
        let status = upload.status().as_u16();
        if !matches!(status, 200 | 201) {
            let body = upload.text().await.unwrap_or_default();
            return Err(ProviderError::upstream(
                status,
                &format!("upload gambar Gemini gagal: HTTP {status} — {body}"),
            ));
        }
        let body = upload.text().await?;
        let value: Value = serde_json::from_str(&body)
            .map_err(|e| ProviderError::Parse(format!("respons upload gambar bukan JSON: {e}")))?;
        match find_blob_reference(&value) {
            Some(blob) => Ok(blob),
            None => Err(ProviderError::Parse(
                "upload gambar berhasil tapi respons tidak membawa referensi \
                 blob WIZ — format respons upload mungkin berubah"
                    .to_string(),
            )),
        }
    }
}

/// Ubah satu inner frame menjadi event-event delta.
///
/// Thought (bila ada) di-emit sebagai [`Event::Reasoning`] SEBELUM
/// teks body sebagai [`Event::Text`]. Teks StreamGenerate bersifat
/// kumulatif: hanya sufiks baru yang di-emit (bila frame saat ini
/// berawalan teks sebelumnya); bila tidak berawalan, seluruh teks
/// frame di-emit (fallback bila backend me-reset kumulasi).
fn events_for_frame(inner: &Value, state: &mut FrameState) -> Vec<Event> {
    let (thought, text) = segments_from_inner(inner);
    let mut events = Vec::new();
    if let Some(thought) = thought {
        let delta = cumulative_delta(&state.last_thought, &thought);
        state.last_thought = thought;
        if !delta.is_empty() {
            events.push(Event::Reasoning(delta));
        }
    }
    if let Some(text) = text {
        let delta = cumulative_delta(&state.last_text, &text);
        state.last_text = text;
        if !delta.is_empty() {
            events.push(Event::Text(delta));
        }
    }
    events
}

/// Delta kumulatif: sufiks `current` setelah `previous`, atau
/// seluruh `current` bila tidak berawalan `previous`, atau kosong
/// bila sama.
fn cumulative_delta(previous: &str, current: &str) -> String {
    if current == previous {
        String::new()
    } else if let Some(suffix) = current.strip_prefix(previous) {
        suffix.to_string()
    } else {
        current.to_string()
    }
}

/// Ratakan histori chat v2 menjadi satu prompt teks.
///
/// Teks tiap pesan digabung urut (bagian teks dalam pesan digabung
/// dengan spasi, pesan digabung dengan baris kosong); pesan sistem
/// menjadi prefix `[System: ...]`. Bagian gambar TIDAK masuk prompt
/// — gambar ditangani jalur vision (upload-then-reference).
/// Riwayat dilipat ke satu turn (mode `multi_turn` native dengan
/// `conversation_id` adalah peningkatan Fase 4+, bukan v0.2).
fn messages_to_prompt(messages: &[Message]) -> String {
    let mut system = String::new();
    let mut parts: Vec<String> = Vec::new();
    for m in messages {
        let mut blocks: Vec<String> = Vec::new();
        for part in &m.parts {
            if let Part::Text(t) = part {
                if !t.trim().is_empty() {
                    blocks.push(t.clone());
                }
            }
        }
        if blocks.is_empty() {
            continue;
        }
        let block = blocks.join(" ");
        if m.role == Role::System {
            if !system.is_empty() {
                system.push_str("\n\n");
            }
            system.push_str(&block);
        } else {
            parts.push(block);
        }
    }
    let mut prompt = parts.join("\n\n");
    if !system.is_empty() {
        prompt = format!("[System: {system}]\n\n{prompt}");
    }
    prompt
}

/// Cari referensi blob WIZ di respons upload (rekursif).
///
/// Prioritas: kunci yang mengandung `"blob"` -> `"resource"` /
/// `"resourceName"` -> `"id"`. Bentuk respons upload belum
/// terverifikasi dari capture resmi; kunci-kunci ini adalah
/// best-effort dari riset repo web2api.
fn find_blob_reference(v: &Value) -> Option<String> {
    match v {
        Value::Object(map) => {
            for key in ["blob", "resource", "resourceName", "id"] {
                let wanted = if key == "blob" {
                    map.iter().find(|(k, _)| k.to_lowercase().contains("blob"))
                } else {
                    map.get_key_value(key)
                };
                if let Some((_, Value::String(s))) = wanted {
                    if !s.is_empty() {
                        return Some(s.clone());
                    }
                }
            }
            map.values().find_map(find_blob_reference)
        }
        Value::Array(arr) => arr.iter().find_map(find_blob_reference),
        _ => None,
    }
}

/// Ekstensi file dari MIME gambar (untuk metadata upload).
fn mime_extension(mime: &str) -> &'static str {
    match mime {
        "image/png" => "png",
        "image/jpeg" => "jpg",
        "image/webp" => "webp",
        "image/gif" => "gif",
        "image/bmp" => "bmp",
        "image/heic" => "heic",
        "image/svg+xml" => "svg",
        _ => "bin",
    }
}

/// Ambil nilai string JSON `"key":"..."` dari `haystack` (tanpa
/// dependensi regex; cukup untuk token bootstrap `SNlM0e`/`bl`/`f.sid`
/// yang tidak mengandung quote tak ter-escape).
fn extract_json_string(haystack: &str, key: &str) -> Option<String> {
    let needle = format!("\"{key}\":\"");
    let start = haystack.find(&needle)? + needle.len();
    let rest = &haystack[start..];
    let mut out = String::new();
    let mut chars = rest.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                if let Some(next) = chars.next() {
                    out.push(next);
                }
            }
            '"' => return Some(out),
            _ => out.push(c),
        }
    }
    None
}

#[async_trait::async_trait]
impl Provider for GeminiProvider {
    fn id(&self) -> &'static str {
        "gemini"
    }

    fn web_default_model(&self) -> &'static str {
        // Alias model default akun (bukan model spesifik terkunci).
        "gemini"
    }

    fn models(&self) -> Vec<ModelInfo> {
        if let Ok(catalog) = self.catalog.read() {
            if !catalog.models().is_empty() {
                return catalog.models().to_vec();
            }
        }
        // Katalog belum pernah berhasil diambil: satu-satunya entri
        // adalah slug default web — BUKAN daftar model statis. Ini
        // menjaga routing (`Registry::find_model("gemini")`) dan
        // kompatibilitas konsumen lama; begitu katalog live diambil,
        // daftar live yang dipakai. Pengambilan yang gagal tidak
        // pernah mengarang daftar model.
        vec![ModelInfo::new("gemini", "google")]
    }

    async fn warm_catalog(&self) -> Result<(), ProviderError> {
        // `refresh_models` mengembalikan `Err` tanpa jaringan bila
        // cookie kosong (provider belum dikonfigurasi).
        self.refresh_models().await.map(|_| ())
    }

    async fn chat(&self, input: &ChatInput) -> Result<EventStream, ProviderError> {
        if self.cookies_empty() {
            return Err(ProviderError::Unauthorized(
                "AIW2A_GEMINI_COOKIES belum diisi".to_string(),
            ));
        }
        // Pacing antar-request (0ms untuk Gemini — mekanisme tersedia).
        self.pacer.wait().await;
        // Search: native/warisan backend web — tidak ada flag
        // deterministik yang terverifikasi (lihat doc modul). On/Auto
        // = kirim seperti biasa; Off tidak bisa dijamin, hanya di-log.
        if matches!(input.search, Search::Off) {
            tracing::warn!(
                "Search::Off tidak bisa dijamin di Gemini — tidak ada toggle search \
                 terverifikasi di protokol StreamGenerate; request dikirim seperti biasa"
            );
        }
        if matches!(input.thinking, Thinking::Off) {
            tracing::debug!(
                "Thinking::Off di Gemini: tidak ada flag thinking-off terverifikasi; \
                 memakai default model"
            );
        }

        // Prompt teks dari messages (gambar ditangani jalur vision).
        let prompt = messages_to_prompt(&input.messages);
        let prompt_chars = prompt.chars().count();

        // Vision: unduh + upload SEMUA gambar dulu. Gagal di gambar
        // mana pun -> error jelas; gambar tidak pernah di-drop diam-diam.
        let mut attachments: Vec<String> = Vec::new();
        for message in &input.messages {
            for part in &message.parts {
                let Part::ImageUrl(src) = part else {
                    continue;
                };
                let image_input = match src {
                    ImageSource::Url(url) => ImageInput::Url(url.clone()),
                    ImageSource::DataUri { mime, base64 } => ImageInput::DataUri {
                        mime: mime.clone(),
                        base64: base64.clone(),
                    },
                };
                let downloaded = download_image_source(&image_input).await?;
                let blob = self.upload_image(&downloaded).await?;
                attachments.push(blob);
            }
        }

        let model = self.resolve_chat_model(&input.model, input.thinking).await;
        let resp = self
            .post_stream_generate(&prompt, &model, &attachments)
            .await?;

        let mut byte_stream = resp.bytes_stream();
        let s = stream! {
            let mut buf = String::new();
            let mut state = FrameState::default();
            let mut text_chars = 0usize;
            let mut thought_chars = 0usize;
            let mut total_bytes = 0usize;
            let mut got_any = false;
            let mut failed = false;
            while let Some(chunk) = byte_stream.next().await {
                let bytes = match chunk {
                    Ok(b) => b,
                    Err(e) => {
                        failed = true;
                        yield Err(ProviderError::Network(e.to_string()));
                        break;
                    }
                };
                total_bytes += bytes.len();
                buf.push_str(&String::from_utf8_lossy(&bytes));
                while let Some(pos) = buf.find('\n') {
                    let line: String = buf.drain(..=pos).collect();
                    for inner in parse_wrb_frames(&line) {
                        for event in events_for_frame(&inner, &mut state) {
                            match &event {
                                Event::Text(t) => text_chars += t.chars().count(),
                                Event::Reasoning(t) => thought_chars += t.chars().count(),
                                _ => {}
                            }
                            got_any = true;
                            yield Ok(event);
                        }
                    }
                }
            }
            if failed {
                // Error jaringan sudah di-yield; jangan tambah error kedua.
            } else if total_bytes == 0 {
                // Gejala gagal auth Gemini: body respons kosong,
                // bukan error (riset §1d). Bukan Parse — sesi mati.
                yield Err(ProviderError::AuthExpired(
                    "respons Gemini kosong (0 byte) — sesi mungkin kedaluwarsa atau \
                     ter-degradasi ke anonim; cookie perlu di-refresh"
                        .to_string(),
                ));
            } else if !got_any {
                yield Err(ProviderError::Parse(
                    "respons Gemini tidak dikenali — protokol web mungkin berubah".to_string(),
                ));
            } else {
                // Usage selalu estimasi (chars/4); backend web tidak
                // mengembalikan hitungan token asli.
                yield Ok(Event::Usage {
                    prompt: estimate_tokens(prompt_chars),
                    completion: estimate_tokens(text_chars),
                    reasoning: estimate_tokens(thought_chars),
                    estimated: true,
                });
                yield Ok(Event::Done);
            }
        };
        Ok(Box::pin(s))
    }

    async fn generate_image(&self, prompt: &str) -> Result<GeneratedImage, ProviderError> {
        if self.cookies_empty() {
            return Err(ProviderError::Unauthorized(
                "AIW2A_GEMINI_COOKIES belum diisi".to_string(),
            ));
        }
        // Rute image di web Gemini otomatis dari prompt; frasa eksplisit membantu.
        let img_prompt = format!("generate an image of {prompt}");
        let resp = self
            .post_stream_generate(&img_prompt, "gemini", &[])
            .await?;
        let body = resp
            .text()
            .await
            .map_err(|e| ProviderError::Network(e.to_string()))?;

        let urls = extract_image_urls(&body);
        let Some(url) = urls.into_iter().next() else {
            return Err(ProviderError::Parse(
                "tidak menemukan URL gambar di respons Gemini — mungkin kuota habis atau prompt ditolak"
                    .to_string(),
            ));
        };

        // URL googleusercontent biasanya hanya bisa diakses dari konteks browser.
        // Coba unduh langsung; kalau 403, butuh impersonasi TLS Chrome (lihat README).
        let dl = self
            .client
            .get(&url)
            .header("cookie", self.cookie_header())
            .header("referer", "https://gemini.google.com/")
            .send()
            .await?;
        let status = dl.status().as_u16();
        if status != 200 {
            return Err(ProviderError::Upstream {
                status,
                body_snippet: format!(
                    "unduh gambar Gemini gagal (HTTP {status}) — URL-nya terikat browser; \
                     butuh impersonasi TLS Chrome di jaringan non-sandbox"
                ),
            });
        }
        let mime = dl
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("image/png")
            .split(';')
            .next()
            .unwrap_or("image/png")
            .to_string();
        let bytes = dl
            .bytes()
            .await
            .map_err(|e| ProviderError::Network(e.to_string()))?
            .to_vec();
        if bytes.len() > 15 * 1024 * 1024 {
            return Err(ProviderError::Parse(
                "gambar terlalu besar (>15MB)".to_string(),
            ));
        }
        Ok(GeneratedImage {
            bytes,
            mime,
            source_url: Some(url),
        })
    }
}

fn urlencoding(s: &str) -> String {
    serde_urlencoded::to_string([("", s)])
        .unwrap_or_default()
        .trim_start_matches('=')
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::media::download_image_source;
    use crate::providers::{Message, Part, Role};

    fn user_message(parts: Vec<Part>) -> Message {
        Message {
            role: Role::User,
            parts,
        }
    }

    #[test]
    fn messages_to_prompt_uses_text_in_order_and_skips_images() {
        let messages = vec![
            Message {
                role: Role::System,
                parts: vec![Part::Text("Kamu asisten.".to_string())],
            },
            user_message(vec![
                Part::Text("Lihat".to_string()),
                Part::ImageUrl(ImageSource::Url("https://example.com/a.png".to_string())),
                Part::Text("ini".to_string()),
            ]),
            Message {
                role: Role::Assistant,
                parts: vec![Part::Text("Siap.".to_string())],
            },
        ];
        let prompt = messages_to_prompt(&messages);
        assert_eq!(prompt, "[System: Kamu asisten.]\n\nLihat ini\n\nSiap.");
        // Gambar tidak masuk prompt teks (ditangani jalur vision).
        assert!(!prompt.contains("example.com"));
        assert_eq!(messages_to_prompt(&[]), "");
    }

    #[test]
    fn resolve_model_behavior_in_catalog_not_in_catalog_empty() {
        // In-catalog -> dipakai, tanpa warning.
        let catalog = vec![
            ModelInfo::new("gemini", "google"),
            ModelInfo::new("gemini-3.6-flash", "google"),
        ];
        let (model, warning) = resolve_model(Some("gemini-3.6-flash"), &catalog, "gemini");
        assert_eq!(model, "gemini-3.6-flash");
        assert_eq!(warning, None);
        // Not-in-catalog -> default + warning.
        let (model, warning) = resolve_model(Some("tidak-ada"), &catalog, "gemini");
        assert_eq!(model, "gemini");
        assert!(warning.is_some());
        assert!(warning.unwrap().contains("tidak-ada"));
        // Empty catalog -> default + warning (tanpa fallback statis).
        let (model, warning) = resolve_model(Some("gemini-3.6-flash"), &[], "gemini");
        assert_eq!(model, "gemini");
        assert!(warning.is_some());
        // Request kosong -> default, tanpa warning.
        let (model, warning) = resolve_model(None, &catalog, "gemini");
        assert_eq!(model, "gemini");
        assert_eq!(warning, None);
    }

    #[test]
    fn cookie_jar_roundtrip_via_core() {
        let raw = "__Secure-1PSID=abc123; SAPISID=xyz; __Secure-1PSIDTS=ts1";
        let jar = CookieJar::parse(raw);
        assert_eq!(jar.get("__Secure-1PSID"), Some("abc123"));
        assert_eq!(jar.get("SAPISID"), Some("xyz"));
        // Render -> parse lagi harus identik (roundtrip).
        let rendered = jar.render();
        assert_eq!(CookieJar::parse(&rendered), jar);
        // Provider memakai jar yang sama (parse sekali di konstruksi).
        let provider = GeminiProvider::new(raw.to_string());
        assert_eq!(provider.cookie_header(), rendered);
        assert!(!provider.cookies_empty());
        assert!(GeminiProvider::new("".to_string()).cookies_empty());
    }

    #[tokio::test]
    async fn image_source_download_from_data_uri_fixture() {
        let input = ImageInput::DataUri {
            mime: "image/png".to_string(),
            base64: "aGVsbG8=".to_string(), // "hello"
        };
        let out = download_image_source(&input)
            .await
            .expect("decode data URI fixture harus berhasil");
        assert_eq!(out.bytes, b"hello");
        assert_eq!(out.mime, "image/png");
    }

    #[test]
    fn model_header_uses_catalog_hex_or_null() {
        let provider = GeminiProvider::new("fixture".to_string());
        // Tanpa katalog: default akun (null), bukan hash hardcode.
        let header = provider.model_header("gemini");
        assert!(header.contains("null"), "default harus null: {header}");
        assert!(
            header.contains("true,[4,5,6,8]"),
            "flag temp chat dipertahankan: {header}"
        );
        let header = provider.model_header("gemini-3.6-flash");
        assert!(header.contains("null"), "slug tanpa hash -> null: {header}");
        // Dengan hash dari katalog live: hash dipakai.
        if let Ok(mut map) = provider.model_hashes.write() {
            map.insert(
                "gemini-3.6-flash".to_string(),
                "56fdd199312815e2".to_string(),
            );
        }
        let header = provider.model_header("gemini-3.6-flash");
        assert!(
            header.contains("\"56fdd199312815e2\""),
            "hash katalog harus dipakai: {header}"
        );
    }

    #[test]
    fn cumulative_delta_prefix_and_reset() {
        assert_eq!(cumulative_delta("Hello", "Hello world"), " world");
        assert_eq!(cumulative_delta("Hello", "Hello"), "");
        // Backend me-reset kumulasi -> kirim utuh, bukan sufiks salah.
        assert_eq!(cumulative_delta("Hello", "Bonjour"), "Bonjour");
        assert_eq!(cumulative_delta("", "Hi"), "Hi");
    }

    #[test]
    fn events_for_frame_reasoning_before_text() {
        let inner = json!([
            null,
            null,
            null,
            null,
            [[null, ["Final answer"], ["thoughts", "Thinking..."]]],
        ]);
        let mut state = FrameState::default();
        let events = events_for_frame(&inner, &mut state);
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], Event::Reasoning(_)));
        assert!(matches!(events[1], Event::Text(_)));
        // Frame kedua dengan teks kumulatif hanya meng-emit delta.
        let inner2 = json!([
            null,
            null,
            null,
            null,
            [[null, ["Final answer!"], ["thoughts", "Thinking..."]]],
        ]);
        let events2 = events_for_frame(&inner2, &mut state);
        assert_eq!(events2.len(), 1);
        assert!(matches!(&events2[0], Event::Text(t) if t == "!"));
    }

    #[test]
    fn extract_json_string_finds_tokens() {
        let html = r#"x "SNlM0e":"token123" y "cfb2h":"build9" z"#;
        assert_eq!(
            extract_json_string(html, "SNlM0e").as_deref(),
            Some("token123")
        );
        assert_eq!(
            extract_json_string(html, "cfb2h").as_deref(),
            Some("build9")
        );
        assert_eq!(extract_json_string(html, "tidak-ada"), None);
        // Escape quote di dalam nilai ditangani.
        let html = r#""SNlM0e":"a\"b""#;
        assert_eq!(extract_json_string(html, "SNlM0e").as_deref(), Some("a\"b"));
    }

    #[test]
    fn build_inner_req_attachments_and_turn_index() {
        let inner = GeminiProvider::build_inner_req("hi", &["blob-1".to_string()]);
        assert_eq!(inner[0][0].as_str(), Some("hi"));
        assert_eq!(inner[61], json!(["blob-1"]));
        // inner[17] adalah index turn, bukan thinking depth.
        assert_eq!(inner[17], json!([[0]]));
        let empty = GeminiProvider::build_inner_req("hi", &[]);
        assert_eq!(empty[61], json!([]));
    }

    #[test]
    fn find_blob_reference_prefers_blob_key() {
        let v = json!({"blobId": "wiz-123", "id": "other"});
        assert_eq!(find_blob_reference(&v).as_deref(), Some("wiz-123"));
        let v = json!({"nested": {"resource": "res-9"}});
        assert_eq!(find_blob_reference(&v).as_deref(), Some("res-9"));
        assert_eq!(find_blob_reference(&json!({"a": 1})), None);
    }

    #[test]
    fn mime_extension_mapping() {
        assert_eq!(mime_extension("image/png"), "png");
        assert_eq!(mime_extension("image/jpeg"), "jpg");
        assert_eq!(mime_extension("image/webp"), "webp");
        assert_eq!(mime_extension("application/pdf"), "bin");
    }
}
