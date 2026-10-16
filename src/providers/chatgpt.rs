//! Provider ChatGPT via antarmuka web chatgpt.com.
//!
//! # AUDIT (2026-10-16, migrasi Fase 2 langkah 3)
//!
//! Yang **sudah ada** sebelum migrasi ini dan dipertahankan:
//! - `access_token` JWT via `GET /api/auth/session` (cache ~45 menit).
//! - Handshake sentinel prepare/finalize (3 endpoint fallback, cache 30 menit).
//! - Solver proof-of-work (namun **spekulatif** — lihat [`solve_pow`]).
//! - `POST /backend-api/f/conversation` + parsing SSE snapshot lama
//!   (`message.content.parts` kumulatif).
//! - Image generation via turn percakapan + pencarian URL estuary.
//!
//! Yang **belum ada** dan ditambahkan migrasi ini:
//! - `conduit_token` / header `X-Conduit-Token` (`POST /f/conversation/prepare`).
//! - Resume stream via `/f/conversation/resume` dengan budget handoff (maks 64).
//! - SSE JSON-patch v1 (`delta_encoding "v1"`, add/replace/append/remove/bulk-patch,
//!   typed events) + ledger emisi per-turn (exactly-once, berurutan).
//! - Katalog model live (`GET /backend-api/models`, cache TTL 1 jam).
//! - Vision: upload 3 langkah file-service -> `image_asset_pointer`.
//! - Thinking: slug `-thinking` + `thinking_effort` (validasi katalog).
//! - Search: instruksi sistem + `citations`/`search_result_groups` -> `Event::Annotation`.
//! - Envelope request lengkap, 401 refresh-sekali-retry, `_puid` +
//!   `ChatGPT-Account-ID`, error challenge eksplisit (Turnstile/Arkose/`cf-mitigated`).
//!
//! # Gap yang masih butuh live capture (jangan tebak)
//!
//! - Varian PoW SHA3-512 + fingerprint array: stub [`solve_pow_sha3_512_fingerprint`]
//!   (TODO live-capture). Varian SHA-256 (capture octo-lex) yang dipakai.
//! - Diff body toggle search UI web (tidak ada flag `web_search: true`
//!   di protokol yang didokumentasikan).
//! - Method/parameter `/f/conversation/resume` (diimplementasikan sebagai
//!   POST `{resume_conversation_token}` — belum terverifikasi).
//! - Apakah `PUT` blob file-service lolos browserless di jaringan target
//!   (risiko 403 Cloudflare error 1010).
//! - Konversi prompt oversized (>96 KiB) ke lampiran JSONL
//!   (content_type/use_case lampiran JSONL belum terdokumentasi).

use crate::core::auth::{persist_jar_best_effort, CookieJar};
use crate::core::catalog::{resolve_model, ModelCatalog};
use crate::core::events::Annotation;
use crate::core::media::{download_image_source, ImageInput};
use crate::core::retry::Pacer;
use crate::error::ProviderError;
use crate::openai::ModelInfo;
use crate::providers::{
    http_client, ChatInput, Event, EventStream, GeneratedImage, ImageSource, Part, Provider, Role,
    Search, Thinking, CHROME_UA,
};
use async_stream::stream;
use futures::StreamExt;
use reqwest::header::HeaderMap;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use uuid::Uuid;

const SESSION_URL: &str = "https://chatgpt.com/api/auth/session";
const MODELS_URL: &str = "https://chatgpt.com/backend-api/models";
const CONVERSATION_URL: &str = "https://chatgpt.com/backend-api/f/conversation";
const CONDUIT_PREPARE_URL: &str = "https://chatgpt.com/backend-api/f/conversation/prepare";
const RESUME_URL: &str = "https://chatgpt.com/backend-api/f/conversation/resume";
const FILES_URL: &str = "https://chatgpt.com/backend-api/files";
const SENTINEL_PREPARE_URLS: [&str; 3] = [
    "https://chatgpt.com/backend-api/sentinel/chat-requirements/prepare",
    "https://chatgpt.com/backend-api/sentinel/chat-requirements",
    "https://chatgpt.com/backend-api/requirements",
];
const SENTINEL_FINALIZE_URL: &str =
    "https://chatgpt.com/backend-api/sentinel/chat-requirements/finalize";

/// Alias model default web ChatGPT (routing lokal). Dikirim ke upstream
/// sebagai `"auto"` (lihat [`upstream_model_slug`]) — TIDAK PERNAH
/// model spesifik terkunci.
const WEB_DEFAULT: &str = "chatgpt-auto";
/// Slug upstream untuk model default web.
const UPSTREAM_AUTO: &str = "auto";
/// TTL cache katalog model live.
const CATALOG_TTL_SECS: u64 = 3600;
/// Maksimum lampiran gambar per turn (gptweb2api).
const MAX_ATTACHMENTS: usize = 10;
/// Budget maksimum segmen yang di-resume per turn (`GPTWEB_STREAM_MAX_HANDOFFS`).
const MAX_HANDOFFS: usize = 64;
/// Estimasi wire di atas ini memicu peringatan oversized (konversi JSONL
/// belum terverifikasi — lihat TODO di [`ChatGptProvider::chat`]).
const OVERSIZED_WIRE_BYTES: usize = 96 * 1024;
/// Instruksi sistem untuk [`Search::On`].
///
/// TIDAK ADA flag `web_search: true` di protokol yang didokumentasikan:
/// search diminta lewat instruksi sistem, dan hasilnya dikumpulkan dari
/// `citations` / `content_references` / `search_result_groups` di stream.
/// Toggle search yang deterministik butuh live capture diff toggle UI web
/// (TODO live-capture).
const SEARCH_SYSTEM_INSTRUCTION: &str = "Use web search when the answer needs current or external information, and include citations for any facts obtained from search results.";

/// Metadata katalog per model (dari `GET /backend-api/models`).
#[derive(Debug, Clone)]
struct ModelMeta {
    /// Slug model, mis. `"gpt-5-6"`.
    slug: String,
    /// Daftar `thinking_effort` yang diizinkan model ini (bisa kosong
    /// bila upstream tidak mempublikasikannya).
    thinking_efforts: Vec<String>,
}

/// Satu lampiran gambar yang sudah di-upload ke file-service.
#[derive(Debug, Clone)]
struct AttachmentRef {
    /// ID file (`file_...`).
    id: String,
    /// Nama file.
    name: String,
    /// Ukuran byte.
    size: u64,
    /// MIME, mis. `"image/png"`.
    mime: String,
}

/// Satu pesan yang siap dikirim ke wire (gambar sudah jadi attachment ref).
#[derive(Debug, Clone)]
struct WireMessage {
    /// Peran pengirim.
    role: Role,
    /// Teks pesan (gabungan semua part teks).
    text: String,
    /// Lampiran gambar (hanya pesan user).
    attachments: Vec<AttachmentRef>,
}

/// State bersama provider (di balik `Arc` agar bisa dipindah ke dalam
/// `stream!` untuk resume handoff tanpa meminjam `&self`).
struct Inner {
    /// String header `Cookie` yang dikirim ke upstream — selalu
    /// di-render ulang dari `jar` setelah merge `Set-Cookie`
    /// (rotasi), agar cookie yang dirotasi upstream ikut terkirim
    /// di request berikutnya.
    cookies: std::sync::RwLock<String>,
    /// Cookie yang sama, ter-parse (untuk lookup `_puid`, session-token,
    /// `ChatGPT-Account-ID`, `__Secure-oai-is`, dan merge rotasi).
    jar: std::sync::RwLock<CookieJar>,
    /// Nilai cookie `__Secure-oai-is` (blob `ois1...`) -> header `x-oai-is`.
    oai_is: String,
    /// Nilai header `ChatGPT-Account-ID`, bila ada di cookie jar.
    account_id: Option<String>,
    client: reqwest::Client,
    token: RwLock<Option<(String, Instant)>>,
    sentinel: RwLock<Option<(String, Instant)>>,
    conduit: RwLock<Option<(String, Instant)>>,
    device_id: String,
    session_id: String,
    catalog: RwLock<ModelCatalog>,
    meta: RwLock<Vec<ModelMeta>>,
    /// Pacing antar-request (0ms untuk ChatGPT — mekanisme tersedia,
    /// interval minimum 0; hanya Claude yang memakai lantai 1500ms).
    pacer: Pacer,
    /// File persistensi rotasi cookie (env
    /// `AIW2A_CHATGPT_COOKIES_FILE`, opsional; `None` = rotasi hanya
    /// in-memory).
    persist_path: Option<PathBuf>,
}

/// Provider ChatGPT via antarmuka web chatgpt.com.
///
/// Autentikasi: FULL cookie header browser -> ditukar menjadi access token
/// lewat `/api/auth/session` (di-cache ~45 menit). Cookie `_puid` ikut
/// terkirim lewat header `Cookie`; `ChatGPT-Account-ID` dikirim sebagai
/// header bila ada di cookie jar.
pub struct ChatGptProvider {
    inner: Arc<Inner>,
}

impl ChatGptProvider {
    /// Provider baru dari string header `Cookie` browser.
    pub fn new(cookies: String) -> Self {
        Self {
            inner: Inner::new(cookies),
        }
    }
}

impl Inner {
    fn new(cookies: String) -> Arc<Self> {
        let raw = cookies.trim().to_string();
        let jar = CookieJar::parse(&raw);
        let oai_is = jar
            .get("__Secure-oai-is")
            .or_else(|| jar.get("oai-is"))
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                // Fallback: kalau string-nya HANYA blob ois1 (tanpa nama cookie),
                // anggap itu nilai oai-is-nya saja.
                if raw.starts_with("ois1.") && !raw.contains('=') && !raw.contains(';') {
                    raw.clone()
                } else {
                    String::new()
                }
            });
        let account_id = jar
            .get("ChatGPT-Account-ID")
            .or_else(|| jar.get("chatgpt-account-id"))
            .map(|s| s.to_string());
        let persist_path = std::env::var("AIW2A_CHATGPT_COOKIES_FILE")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .map(PathBuf::from);
        Arc::new(Self {
            cookies: std::sync::RwLock::new(raw),
            jar: std::sync::RwLock::new(jar),
            oai_is,
            account_id,
            client: http_client(),
            token: RwLock::new(None),
            sentinel: RwLock::new(None),
            conduit: RwLock::new(None),
            device_id: Uuid::new_v4().to_string(),
            session_id: Uuid::new_v4().to_string(),
            catalog: RwLock::new(ModelCatalog::new(Duration::from_secs(CATALOG_TTL_SECS))),
            meta: RwLock::new(Vec::new()),
            pacer: Pacer::new(Duration::from_millis(0)),
            persist_path,
        })
    }

    /// Benar bila string cookie kosong (provider belum dikonfigurasi).
    fn cookies_empty(&self) -> bool {
        self.cookies
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .is_empty()
    }

    /// Nilai header `Cookie` saat ini (di-render dari jar yang bisa
    /// berotasi — lihat [`Inner::merge_response_cookies`]).
    fn cookie_header(&self) -> String {
        // Lock std, kritis singkat; poison berarti panic sebelumnya —
        // pakai inner-nya agar tidak panic di sini.
        self.cookies
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    /// Merge header `Set-Cookie` dari satu respons upstream ke jar
    /// (rotasi), sinkronkan string `Cookie` dari jar, lalu persist
    /// best-effort (tidak pernah menggagalkan request, tidak pernah
    /// me-log nilai cookie).
    ///
    /// CATATAN: render ulang dari jar tidak mempertahankan pasangan
    /// tanpa `=` (bukan pasangan cookie valid) — pasangan seperti itu
    /// tidak pernah sampai ke sini karena jalur chat mensyaratkan
    /// cookie sesi yang valid sebelum request apa pun dikirim.
    fn merge_response_cookies(&self, headers: &HeaderMap) {
        let rendered = {
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
            jar.render()
        };
        if let Ok(mut cookies) = self.cookies.write() {
            *cookies = rendered;
        }
    }

    fn has_session_cookie(&self) -> bool {
        self.jar
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .get("__Secure-next-auth.session-token")
            .is_some()
    }

    /// Header bersama semua request terautentikasi ke `chatgpt.com`.
    ///
    /// Sengaja TIDAK mengirim nilai `oai-client-*` (build/version):
    /// nilai hasil capture tidak boleh di-hardcode (lihat desain §3.5).
    fn base_headers(&self, req: reqwest::RequestBuilder, token: &str) -> reqwest::RequestBuilder {
        req.header("authorization", format!("Bearer {token}"))
            .header("cookie", self.cookie_header())
            .header("user-agent", CHROME_UA)
            .header("oai-device-id", &self.device_id)
            .header("oai-session-id", &self.session_id)
            .header("oai-language", "en-US")
            .header("sec-fetch-site", "same-origin")
            .header("sec-fetch-mode", "cors")
            .header("sec-fetch-dest", "empty")
    }

    /// Mint `access_token` JWT via `GET /api/auth/session` dari cookie
    /// `__Secure-next-auth.session-token` (di-cache ~45 menit).
    ///
    /// TODO: pertukaran OAuth `refresh_token` (Codex `auth.json`) belum
    /// diimplementasikan — konstruktor hanya menerima cookie browser.
    async fn access_token(&self) -> Result<String, ProviderError> {
        {
            let g = self.token.read().await;
            if let Some((tok, at)) = g.clone() {
                if at.elapsed() < Duration::from_secs(45 * 60) {
                    return Ok(tok);
                }
            }
        }
        let resp = self
            .client
            .get(SESSION_URL)
            .header("cookie", self.cookie_header())
            .header("accept", "application/json")
            .header("user-agent", CHROME_UA)
            .send()
            .await?;
        // Rotasi cookie dari endpoint session (mis. session-token baru).
        self.merge_response_cookies(resp.headers());
        let status = resp.status().as_u16();
        if status == 401 || status == 403 {
            return Err(ProviderError::Unauthorized(
                "chatgpt.com menolak cookie (401/403) — login kedaluwarsa atau diblokir verifikasi"
                    .to_string(),
            ));
        }
        if status != 200 {
            return Err(ProviderError::Upstream {
                status,
                body_snippet: format!("gagal mengambil session: HTTP {status}"),
            });
        }
        let v: Value = resp.json().await.map_err(|e| {
            ProviderError::Parse(format!(
                "session bukan JSON (mungkin halaman verifikasi): {e}"
            ))
        })?;
        // Sesi kedaluwarsa/tidak lengkap: HTTP 200 dengan body {}.
        let token = v
            .get("accessToken")
            .and_then(|t| t.as_str())
            .filter(|t| !t.is_empty())
            .ok_or_else(|| {
                ProviderError::Unauthorized(
                    "tidak ada accessToken di session — cookie tidak valid / akun ter-logout. \
                     Pastikan AIW2A_CHATGPT_COOKIES berisi FULL cookie header dari tab Network browser."
                        .to_string(),
                )
            })?
            .to_string();
        *self.token.write().await = Some((token.clone(), Instant::now()));
        Ok(token)
    }

    /// Alur sentinel: prepare -> PoW (bila diminta) -> finalize.
    /// Mengembalikan (requirements_token, proof_token).
    async fn sentinel_tokens(
        &self,
        bearer: &str,
    ) -> Result<(Option<String>, Option<String>), ProviderError> {
        {
            let g = self.sentinel.read().await;
            if let Some((tok, at)) = g.clone() {
                if at.elapsed() < Duration::from_secs(30 * 60) {
                    let parts: Vec<&str> = tok.splitn(2, '|').collect();
                    return Ok((
                        parts.first().map(|s| s.to_string()),
                        parts.get(1).map(|s| s.to_string()),
                    ));
                }
            }
        }
        let mut prepared: Option<Value> = None;
        for url in SENTINEL_PREPARE_URLS {
            let mut req = self.client.post(url);
            req = self.base_headers(req, bearer);
            req = req
                .header("content-type", "application/json")
                .header("accept", "application/json")
                .header("origin", "https://chatgpt.com")
                .header("referer", "https://chatgpt.com/");
            let resp = match req.json(&json!({})).send().await {
                Ok(r) => r,
                Err(e) => return Err(ProviderError::Network(e.to_string())),
            };
            // Rotasi cookie dari respons sentinel prepare.
            self.merge_response_cookies(resp.headers());
            if resp.status().as_u16() == 404 {
                continue;
            }
            if !resp.status().is_success() {
                let s = resp.status().as_u16();
                let t = resp.text().await.unwrap_or_default();
                if let Some(err) = challenge_error_from_text(&t) {
                    return Err(err);
                }
                return Err(ProviderError::upstream(
                    s,
                    &format!("sentinel prepare gagal: {s} {t}"),
                ));
            }
            prepared = resp.json().await.ok();
            break;
        }
        let prep = match prepared {
            Some(v) => v,
            // Endpoint tidak ada / tidak butuh verifikasi.
            None => return Ok((None, None)),
        };
        if let Some(detail) = prep.get("detail").and_then(|d| d.as_str()) {
            if let Some(err) = challenge_error_from_text(detail) {
                return Err(err);
            }
            return Err(ProviderError::upstream(
                502,
                &format!("sentinel prepare error: {detail}"),
            ));
        }
        // Challenge Turnstile/Arkose/`so` yang eksplisit wajib ditolak
        // dengan error jelas — TIDAK ada solver lokal (lihat
        // `challenge_error_for`); fallback browser-relay adalah Fase 4+.
        for key in ["turnstile", "arkose", "so"] {
            if let Some(ch) = prep.get(key) {
                if ch.get("required").and_then(|r| r.as_bool()) == Some(true) {
                    return Err(challenge_error_for(key));
                }
            }
        }

        let prepare_token = prep
            .get("prepare_token")
            .or_else(|| prep.get("prepareToken"))
            .and_then(|t| t.as_str())
            .unwrap_or("")
            .to_string();

        // Proof-of-work bila diminta (seed hadir = wajib, kecuali
        // `required: false` eksplisit).
        let mut pow_solution: Option<Value> = None;
        if let Some(pow) = prep
            .get("proofofwork")
            .or_else(|| prep.get("proofOfWork"))
            .or_else(|| prep.get("proof_of_work"))
        {
            let required = pow
                .get("required")
                .and_then(|r| r.as_bool())
                .unwrap_or(true);
            if required {
                let seed = pow.get("seed").and_then(|s| s.as_str()).ok_or_else(|| {
                    ProviderError::Parse(
                        "tantangan proof-of-work tanpa seed — format tidak dikenal".to_string(),
                    )
                })?;
                let difficulty = pow.get("difficulty").cloned().unwrap_or(json!("00000"));
                let proof = solve_pow(seed, &difficulty).ok_or_else(|| {
                    ProviderError::Parse(
                        "gagal menyelesaikan proof-of-work ChatGPT — buka chatgpt.com di browser \
                         dan selesaikan verifikasinya, lalu coba lagi"
                            .to_string(),
                    )
                })?;
                pow_solution = Some(proof);
            }
        }

        // Finalize.
        let mut fin_body = json!({ "prepare_token": prepare_token });
        if let Some(sol) = pow_solution {
            fin_body["proofofwork"] = sol;
        }
        // Teruskan HANYA token yang sudah terselesaikan — jangan pernah
        // mengirim deskriptor challenge (mis. `{required, dx}`) sebagai token.
        if let Some(tok) = solved_token(prep.get("turnstile")) {
            fin_body["turnstile"] = json!(tok);
        }
        if let Some(tok) = solved_token(prep.get("so")) {
            fin_body["so"] = json!(tok);
        }
        let mut req = self.client.post(SENTINEL_FINALIZE_URL);
        req = self.base_headers(req, bearer);
        req = req
            .header("content-type", "application/json")
            .header("accept", "application/json")
            .header("origin", "https://chatgpt.com")
            .header("referer", "https://chatgpt.com/");
        let resp = req
            .json(&fin_body)
            .send()
            .await
            .map_err(|e| ProviderError::Network(e.to_string()))?;
        // Rotasi cookie dari respons sentinel finalize.
        self.merge_response_cookies(resp.headers());
        if !resp.status().is_success() {
            let s = resp.status().as_u16();
            let t = resp.text().await.unwrap_or_default();
            if let Some(err) = challenge_error_from_text(&t) {
                return Err(err);
            }
            return Err(ProviderError::upstream(
                s,
                &format!("sentinel finalize gagal: {s} {t}"),
            ));
        }
        // Token bisa datang di body JSON dan/atau header respons.
        let header_req = resp
            .headers()
            .get("openai-sentinel-chat-requirements-token")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let header_proof = resp
            .headers()
            .get("openai-sentinel-proof-token")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let v: Value = resp.json().await.unwrap_or(json!({}));
        let req_token = header_req.or_else(|| {
            [
                "openai-sentinel-chat-requirements-token",
                "requirements_token",
                "token",
            ]
            .iter()
            .filter_map(|k| v.get(*k).and_then(|t| t.as_str()))
            .next()
            .map(|s| s.to_string())
        });
        let proof_token = header_proof.or_else(|| {
            ["openai-sentinel-proof-token", "proof_token"]
                .iter()
                .filter_map(|k| v.get(*k).and_then(|t| t.as_str()))
                .next()
                .map(|s| s.to_string())
        });
        let cache_val = format!(
            "{}|{}",
            req_token.clone().unwrap_or_default(),
            proof_token.clone().unwrap_or_default()
        );
        *self.sentinel.write().await = Some((cache_val, Instant::now()));
        Ok((req_token, proof_token))
    }

    /// `POST /backend-api/f/conversation/prepare` -> `conduit_token`
    /// (dikirim sebagai header `X-Conduit-Token` di conversation).
    ///
    /// Best-effort: gagal prepare TIDAK fatal (riset §2) — kembalikan
    /// `None` dan lanjutkan tanpa conduit token (dengan warning log).
    async fn conduit_token(
        &self,
        bearer: &str,
        sentinel: &(Option<String>, Option<String>),
    ) -> Option<String> {
        {
            let g = self.conduit.read().await;
            if let Some((tok, at)) = g.clone() {
                if at.elapsed() < Duration::from_secs(5 * 60) && !tok.is_empty() {
                    return Some(tok);
                }
            }
        }
        let mut req = self.client.post(CONDUIT_PREPARE_URL);
        req = self.base_headers(req, bearer);
        req = req
            .header("content-type", "application/json")
            .header("accept", "application/json")
            .header("origin", "https://chatgpt.com")
            .header("referer", "https://chatgpt.com/");
        if let Some(t) = &sentinel.0 {
            if !t.is_empty() {
                req = req.header("openai-sentinel-chat-requirements-token", t);
            }
        }
        if let Some(t) = &sentinel.1 {
            if !t.is_empty() {
                req = req.header("openai-sentinel-proof-token", t);
            }
        }
        let resp = match req.json(&json!({})).send().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("chatgpt: conduit prepare gagal (network): {e}");
                return None;
            }
        };
        // Rotasi cookie dari respons conduit prepare.
        self.merge_response_cookies(resp.headers());
        if resp.status().as_u16() == 404 {
            return None;
        }
        if !resp.status().is_success() {
            tracing::warn!(
                "chatgpt: conduit prepare HTTP {} (lanjut tanpa conduit_token)",
                resp.status()
            );
            return None;
        }
        let v: Value = resp.json().await.unwrap_or(json!({}));
        let tok = ["conduit_token", "conduitToken", "token"]
            .iter()
            .filter_map(|k| v.get(*k).and_then(|t| t.as_str()))
            .next()
            .map(|s| s.to_string());
        if let Some(t) = &tok {
            if !t.is_empty() {
                *self.conduit.write().await = Some((t.clone(), Instant::now()));
            }
        }
        tok
    }

    /// Bundle auth satu turn: bearer + sentinel + conduit.
    ///
    /// Error challenge (Turnstile/Arkose/`cf-mitigated`) bersifat fatal
    /// dan diteruskan; kegagalan sentinel lain best-effort (lanjut tanpa
    /// token sentinel, seperti perilaku lama).
    async fn auth_bundle(
        &self,
    ) -> Result<(String, (Option<String>, Option<String>), Option<String>), ProviderError> {
        let token = self.access_token().await?;
        let sentinel = match self.sentinel_tokens(&token).await {
            Ok(s) => s,
            Err(e) => {
                if is_challenge_error(&e) {
                    return Err(e);
                }
                tracing::warn!("chatgpt: sentinel gagal ({e}) — lanjut tanpa token sentinel");
                (None, None)
            }
        };
        let conduit = self.conduit_token(&token, &sentinel).await;
        Ok((token, sentinel, conduit))
    }

    /// Terapkan header conversation (sentinel, conduit, akun, oai-is)
    /// ke request yang sudah punya header dasar + content-type/accept.
    fn apply_conversation_headers(
        &self,
        req: reqwest::RequestBuilder,
        sentinel: &(Option<String>, Option<String>),
        conduit: Option<&str>,
    ) -> reqwest::RequestBuilder {
        let mut req = req;
        if let Some(t) = &sentinel.0 {
            if !t.is_empty() {
                req = req.header("openai-sentinel-chat-requirements-token", t);
            }
        }
        if let Some(t) = &sentinel.1 {
            if !t.is_empty() {
                req = req.header("openai-sentinel-proof-token", t);
            }
        }
        if let Some(t) = conduit {
            if !t.is_empty() {
                req = req.header("X-Conduit-Token", t);
            }
        }
        if let Some(acc) = &self.account_id {
            req = req.header("ChatGPT-Account-ID", acc);
        }
        if !self.oai_is.is_empty() {
            req = req.header("x-oai-is", &self.oai_is);
        }
        req
    }

    /// Petakan status respons conversation/resume ke `Ok(resp)` (200)
    /// atau error yang jelas. Mengonsumsi `resp` (body dibaca untuk error).
    async fn map_conversation_response(
        &self,
        resp: reqwest::Response,
    ) -> Result<reqwest::Response, ProviderError> {
        let status = resp.status().as_u16();
        if status == 200 {
            return Ok(resp);
        }
        let retry_after = resp
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok());
        let text = resp.text().await.unwrap_or_default();
        if let Some(err) = challenge_error_from_text(&text) {
            return Err(err);
        }
        match status {
            401 => Err(ProviderError::Unauthorized(
                "chatgpt.com menolak request (401) — token kedaluwarsa atau sesi tidak valid"
                    .to_string(),
            )),
            403 => {
                if text.to_lowercase().contains("unusual activity") {
                    Err(ProviderError::upstream(
                        403,
                        "chatgpt.com menolak request (403 Unusual activity) — kemungkinan \
                         masalah sentinel / TLS fingerprint / proxy; bila berlanjut, butuh \
                         browser-relay (Phase 4+)",
                    ))
                } else {
                    Err(ProviderError::upstream(
                        403,
                        &format!("conversation gagal: 403 {text}"),
                    ))
                }
            }
            429 => Err(ProviderError::RateLimited {
                retry_after,
                message: "chatgpt rate limit (HTTP 429)".to_string(),
            }),
            _ => Err(ProviderError::upstream(
                status,
                &format!("conversation gagal: {status} {text}"),
            )),
        }
    }

    /// Satu percobaan `POST /backend-api/f/conversation` (tanpa retry 401).
    async fn send_conversation_once(
        &self,
        body: &Value,
    ) -> Result<reqwest::Response, ProviderError> {
        let (token, sentinel, conduit) = self.auth_bundle().await?;
        let mut req = self.client.post(CONVERSATION_URL);
        req = self.base_headers(req, &token);
        req = req
            .header("content-type", "application/json")
            .header("accept", "text/event-stream")
            .header("origin", "https://chatgpt.com")
            .header("referer", "https://chatgpt.com/");
        req = self.apply_conversation_headers(req, &sentinel, conduit.as_deref());
        let resp = req.json(body).send().await?;
        // Rotasi cookie dari respons conversation (sebelum body stream dibaca).
        self.merge_response_cookies(resp.headers());
        self.map_conversation_response(resp).await
    }

    /// Kirim satu turn conversation, kembalikan respons SSE mentah.
    ///
    /// 401 -> refresh token sekali lalu retry (aman: turn belum diterima
    /// upstream, jadi bukan replay generasi yang sudah disubmit).
    async fn post_conversation(&self, body: &Value) -> Result<reqwest::Response, ProviderError> {
        match self.send_conversation_once(body).await {
            Ok(r) => Ok(r),
            Err(ProviderError::Unauthorized(_)) => {
                *self.token.write().await = None;
                *self.sentinel.write().await = None;
                self.send_conversation_once(body).await
            }
            Err(e) => Err(e),
        }
    }

    /// Satu percobaan resume stream (tanpa retry 401).
    async fn send_resume_once(
        &self,
        resume_token: &str,
    ) -> Result<reqwest::Response, ProviderError> {
        let (token, sentinel, conduit) = self.auth_bundle().await?;
        let mut req = self.client.post(RESUME_URL);
        req = self.base_headers(req, &token);
        req = req
            .header("content-type", "application/json")
            .header("accept", "text/event-stream")
            .header("origin", "https://chatgpt.com")
            .header("referer", "https://chatgpt.com/");
        req = self.apply_conversation_headers(req, &sentinel, conduit.as_deref());
        let resp = req
            .json(&json!({ "resume_conversation_token": resume_token }))
            .send()
            .await?;
        // Rotasi cookie dari respons resume.
        self.merge_response_cookies(resp.headers());
        self.map_conversation_response(resp).await
    }

    /// Lanjutkan stream panjang/terputus via `/f/conversation/resume`.
    ///
    /// TODO(live-capture): method/parameter endpoint ini belum terverifikasi
    /// dari capture live — diimplementasikan sebagai POST
    /// `{resume_conversation_token}`. Bila upstream menolaknya, handoff
    /// berakhir dengan error upstream yang jelas (bukan diam-diam).
    async fn resume_stream(&self, resume_token: &str) -> Result<reqwest::Response, ProviderError> {
        match self.send_resume_once(resume_token).await {
            Ok(r) => Ok(r),
            Err(ProviderError::Unauthorized(_)) => {
                *self.token.write().await = None;
                *self.sentinel.write().await = None;
                self.send_resume_once(resume_token).await
            }
            Err(e) => Err(e),
        }
    }

    /// Unduh satu URL estuary/content dengan Bearer token sesi ini.
    async fn download_estuary(&self, url: &str) -> Result<GeneratedImage, ProviderError> {
        let token = self.access_token().await?;
        let dl = self
            .client
            .get(url)
            .header("authorization", format!("Bearer {token}"))
            .header("cookie", self.cookie_header())
            .header("user-agent", CHROME_UA)
            .send()
            .await?;
        // Rotasi cookie dari respons unduhan estuary.
        self.merge_response_cookies(dl.headers());
        let status = dl.status().as_u16();
        if status != 200 {
            return Err(ProviderError::Upstream {
                status,
                body_snippet: format!("unduh hasil gambar ChatGPT gagal: HTTP {status}"),
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
            source_url: Some(url.to_string()),
        })
    }
}

/// Error jelas untuk challenge anti-bot yang TIDAK bisa diselesaikan lokal.
///
/// Turnstile (Cloudflare) dan challenge `so` (session-observer) butuh
/// konteks browser asli; tidak ada solver lokal yang andal di repo-repo
/// yang diriset. Fallback browser-relay adalah Fase 4+ (di luar cakupan).
fn challenge_error_for(kind: &str) -> ProviderError {
    let detail = match kind {
        "turnstile" => {
            "ChatGPT meminta Turnstile token (turnstile token is required) — challenge \
             Cloudflare yang tidak bisa diselesaikan lokal; butuh browser-relay \
             fallback (Phase 4+, di luar cakupan)"
        }
        "arkose" => {
            "ChatGPT meminta Arkose token (arkose token is required) — challenge \
             yang tidak bisa diselesaikan lokal; butuh browser-relay fallback \
             (Phase 4+, di luar cakupan)"
        }
        "so" => {
            "ChatGPT meminta challenge session-observer (`so`) — challenge \
             yang tidak bisa diselesaikan lokal; butuh browser-relay fallback \
             (Phase 4+, di luar cakupan)"
        }
        other => {
            return ProviderError::Upstream {
                status: 403,
                body_snippet: format!(
                    "ChatGPT meminta challenge '{other}' yang tidak bisa diselesaikan lokal; \
                     butuh browser-relay fallback (Phase 4+, di luar cakupan)"
                ),
            };
        }
    };
    ProviderError::Upstream {
        status: 403,
        body_snippet: detail.to_string(),
    }
}

/// Deteksi penanda challenge di teks error upstream (case-insensitive).
fn challenge_error_from_text(text: &str) -> Option<ProviderError> {
    let lower = text.to_lowercase();
    if lower.contains("turnstile token is required") || lower.contains("turnstile token failed") {
        Some(challenge_error_for("turnstile"))
    } else if lower.contains("arkose token is required") {
        Some(challenge_error_for("arkose"))
    } else if lower.contains("cf-mitigated: challenge") || lower.contains("cf-mitigated") {
        Some(challenge_error_for("cf-mitigated"))
    } else {
        None
    }
}

/// Benar bila error ini adalah error challenge (untuk membedakannya dari
/// kegagalan sentinel best-effort yang boleh dilanjutkan tanpa token).
fn is_challenge_error(err: &ProviderError) -> bool {
    matches!(
        err,
        ProviderError::Upstream { body_snippet, .. }
            if body_snippet.contains("browser-relay")
    )
}

/// Ambil token yang sudah terselesaikan dari nilai prepare.
///
/// Mengembalikan `Some(token)` hanya bila nilainya string non-kosong atau
/// objek dengan field `token` string non-kosong. Deskriptor challenge
/// (mis. `{required: true, dx: ...}`) menghasilkan `None` — deskriptor
/// bukan token dan tidak boleh dikirim ke finalize.
fn solved_token(v: Option<&Value>) -> Option<String> {
    let v = v?;
    if let Some(s) = v.as_str() {
        if !s.trim().is_empty() {
            return Some(s.to_string());
        }
    }
    if let Some(t) = v.get("token").and_then(|t| t.as_str()) {
        if !t.trim().is_empty() {
            return Some(t.to_string());
        }
    }
    None
}

/// Cara menginterpretasikan nilai `difficulty` PoW.
///
/// Encoding difficulty TIDAK sepenuhnya terdokumentasi (perbedaan
/// capture antar-repo) — TODO(live-capture): konfirmasi encoding yang
/// dipakai endpoint saat ini.
enum PowThreshold {
    /// 3 byte pertama digest (big-endian) harus `< limit`.
    /// Bentuk ini sesuai capture octo-lex: `parseInt(difficulty, 16)`.
    FirstBytesLessThan(u32),
    /// Digest hex harus diawali `count` karakter `0`
    /// (bentuk `"00000"` — semua nol).
    LeadingZeroHexChars(usize),
    /// Digest harus punya minimal `bits` bit nol di depan
    /// (bentuk difficulty numerik).
    LeadingZeroBits(u32),
}

/// Parse nilai `difficulty` PoW ke [`PowThreshold`].
fn parse_pow_threshold(difficulty: &Value) -> Option<PowThreshold> {
    match difficulty {
        Value::String(s) => {
            let s = s.trim();
            if s.is_empty() {
                return None;
            }
            let hex = s.strip_prefix("0x").unwrap_or(s);
            if !hex.is_empty() && hex.chars().all(|c| c == '0') {
                return Some(PowThreshold::LeadingZeroHexChars(hex.len()));
            }
            if hex.len() >= 6 && hex.chars().take(6).all(|c| c.is_ascii_hexdigit()) {
                let threshold = u32::from_str_radix(&hex[..6], 16).ok()?;
                return Some(PowThreshold::FirstBytesLessThan(threshold));
            }
            None
        }
        Value::Number(n) => n.as_u64().map(|b| PowThreshold::LeadingZeroBits(b as u32)),
        _ => None,
    }
}

/// Jumlah bit nol di depan digest.
fn leading_zero_bits(digest: &[u8]) -> u32 {
    let mut n = 0u32;
    for b in digest {
        if *b == 0 {
            n += 8;
        } else {
            n += b.leading_zeros();
            break;
        }
    }
    n
}

/// Cek apakah `counter` memenuhi PoW SHA-256 untuk `seed` + `difficulty`.
fn pow_sha256_satisfies(seed: &str, threshold: &PowThreshold, counter: u64) -> bool {
    // TODO(live-capture): encoding counter (desimal, tanpa pemisah) sesuai
    // ringkasan capture octo-lex (`SHA-256(seed + counter)`); verifikasi
    // live bila finalize menolak jawaban.
    let input = format!("{seed}{counter}");
    let digest = Sha256::digest(input.as_bytes());
    match threshold {
        PowThreshold::FirstBytesLessThan(limit) => {
            let first = u32::from_be_bytes([digest[0], digest[1], digest[2], 0]) >> 8;
            first < *limit
        }
        PowThreshold::LeadingZeroHexChars(count) => {
            let hex = format!("{digest:x}");
            hex.chars().take(*count).all(|c| c == '0')
        }
        PowThreshold::LeadingZeroBits(bits) => leading_zero_bits(&digest) >= *bits,
    }
}

/// Selesaikan PoW varian SHA-256 (`SHA-256(seed + counter)`).
///
/// Varian capture octo-lex: terselesaikan dalam ~31 iterasi untuk
/// difficulty tipikal. Batas 1.000.000 iterasi adalah pengaman, bukan
/// target (PoW yang sehat selesai jauh sebelum itu).
fn solve_pow_sha256_counter(seed: &str, difficulty: &Value) -> Option<String> {
    let threshold = parse_pow_threshold(difficulty)?;
    for counter in 0..1_000_000u64 {
        if pow_sha256_satisfies(seed, &threshold, counter) {
            return Some(counter.to_string());
        }
    }
    None
}

/// Selesaikan PoW varian SHA3-512 + array fingerprint browser (gptweb2api).
///
/// TODO(live-capture): format pastinya belum terkonfirmasi — deskripsi
/// riset menyebut array fingerprint 18-field, JSON-encoded + base64,
/// di-hash dengan seed via SHA3-512, prefix digest hex <= difficulty,
/// plus bootstrap proof (`gAAAAAC...`). Sampai capture live mengonfirmasi
/// bentuk fingerprint array + jawaban yang diharapkan, solver ini
/// sengaja mengembalikan `None` daripada menebak format yang salah.
fn solve_pow_sha3_512_fingerprint(_seed: &str, _difficulty: &Value) -> Option<String> {
    None
}

/// Solver PoW yang bisa ditukar (swappable): coba SHA-256 dulu
/// (capture octo-lex), lalu varian SHA3-512.
///
/// Mengembalikan objek `proofofwork` untuk finalize:
/// `{seed, difficulty, answer}`.
fn solve_pow(seed: &str, difficulty: &Value) -> Option<Value> {
    if let Some(answer) = solve_pow_sha256_counter(seed, difficulty) {
        return Some(json!({ "seed": seed, "difficulty": difficulty, "answer": answer }));
    }
    if let Some(answer) = solve_pow_sha3_512_fingerprint(seed, difficulty) {
        return Some(json!({ "seed": seed, "difficulty": difficulty, "answer": answer }));
    }
    None
}

impl Inner {
    /// Ambil katalog model live dari `GET /backend-api/models`
    /// (401 -> refresh token sekali lalu retry).
    async fn fetch_catalog(&self) -> Result<Vec<(ModelInfo, ModelMeta)>, ProviderError> {
        for attempt in 0..2 {
            let token = self.access_token().await?;
            let mut req = self.client.get(MODELS_URL);
            req = self.base_headers(req, &token);
            req = req
                .header("accept", "application/json")
                .header("origin", "https://chatgpt.com")
                .header("referer", "https://chatgpt.com/");
            let resp = req.send().await?;
            // Rotasi cookie dari respons katalog model.
            self.merge_response_cookies(resp.headers());
            let status = resp.status().as_u16();
            if status == 401 && attempt == 0 {
                *self.token.write().await = None;
                *self.sentinel.write().await = None;
                continue;
            }
            if status != 200 {
                let text = resp.text().await.unwrap_or_default();
                if let Some(err) = challenge_error_from_text(&text) {
                    return Err(err);
                }
                return Err(ProviderError::upstream(
                    status,
                    &format!("GET /backend-api/models gagal: {status} {text}"),
                ));
            }
            let v: Value = resp
                .json()
                .await
                .map_err(|e| ProviderError::Parse(format!("models bukan JSON: {e}")))?;
            return Ok(parse_models_response(&v));
        }
        Err(ProviderError::Unauthorized(
            "GET /backend-api/models gagal: 401 setelah refresh token".to_string(),
        ))
    }

    /// Refresh katalog bila basi (best-effort: gagal upstream hanya
    /// dicatat sebagai warning, cache lama tetap dipakai).
    async fn refresh_catalog(&self, force: bool) {
        if !force {
            if let Ok(g) = self.catalog.try_read() {
                if g.is_fresh() {
                    return;
                }
            }
        }
        match self.fetch_catalog().await {
            Ok(list) => {
                let (infos, metas): (Vec<ModelInfo>, Vec<ModelMeta>) = list.into_iter().unzip();
                self.catalog.write().await.update(infos);
                *self.meta.write().await = metas;
            }
            Err(e) => {
                tracing::warn!(
                    "chatgpt: gagal refresh katalog model ({e}) — memakai cache lama / default web"
                );
            }
        }
    }

    /// Snapshot katalog (kosong bila belum pernah berhasil diambil).
    fn catalog_snapshot(&self) -> Vec<ModelInfo> {
        match self.catalog.try_read() {
            Ok(g) => g.models().to_vec(),
            Err(_) => Vec::new(),
        }
    }

    /// Snapshot metadata katalog.
    fn meta_snapshot(&self) -> Vec<ModelMeta> {
        match self.meta.try_read() {
            Ok(g) => g.clone(),
            Err(_) => Vec::new(),
        }
    }

    /// Upload satu file ke file-service (3 langkah):
    /// `POST /backend-api/files` -> `PUT upload_url` (byte mentah,
    /// `x-ms-blob-type: BlockBlob`, TANPA header auth — URL bertanda
    /// tangan) -> `POST /backend-api/files/{id}/uploaded`.
    async fn upload_file(
        &self,
        file_name: &str,
        bytes: &[u8],
        mime: &str,
    ) -> Result<AttachmentRef, ProviderError> {
        let token = self.access_token().await?;
        let mut req = self.client.post(FILES_URL);
        req = self.base_headers(req, &token);
        req = req
            .header("content-type", "application/json")
            .header("accept", "application/json")
            .header("origin", "https://chatgpt.com")
            .header("referer", "https://chatgpt.com/");
        let payload = build_file_upload_payload(file_name, bytes.len());
        let resp = req.json(&payload).send().await?;
        // Rotasi cookie dari respons file-service.
        self.merge_response_cookies(resp.headers());
        let status = resp.status().as_u16();
        if status != 200 && status != 201 {
            let text = resp.text().await.unwrap_or_default();
            if let Some(err) = challenge_error_from_text(&text) {
                return Err(err);
            }
            return Err(ProviderError::upstream(
                status,
                &format!("POST /backend-api/files gagal: {status} {text}"),
            ));
        }
        let v: Value = resp
            .json()
            .await
            .map_err(|e| ProviderError::Parse(format!("files bukan JSON: {e}")))?;
        let file_id = v
            .get("file_id")
            .or_else(|| v.get("fileId"))
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .to_string();
        let upload_url = v
            .get("upload_url")
            .or_else(|| v.get("uploadUrl"))
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .to_string();
        if file_id.is_empty() || upload_url.is_empty() {
            return Err(ProviderError::Parse(
                "POST /backend-api/files tidak mengembalikan file_id/upload_url".to_string(),
            ));
        }

        // Langkah 2: PUT byte mentah ke URL bertanda tangan.
        let put = self
            .client
            .put(&upload_url)
            .header("x-ms-blob-type", "BlockBlob")
            .header("content-type", mime)
            .body(bytes.to_vec())
            .send()
            .await?;
        let put_status = put.status().as_u16();
        if put_status == 403 {
            return Err(ProviderError::upstream(
                403,
                "PUT upload blob ditolak (403) — kemungkinan Cloudflare error 1010 \
                 untuk signature TLS non-browser; upload gambar butuh browser-relay \
                 (Phase 4+) atau jaringan lain",
            ));
        }
        if !(200..300).contains(&put_status) {
            return Err(ProviderError::upstream(
                put_status,
                &format!("PUT upload blob gagal: HTTP {put_status}"),
            ));
        }

        // Langkah 3: tandai upload selesai.
        let mut req3 = self.client.post(format!("{FILES_URL}/{file_id}/uploaded"));
        req3 = self.base_headers(req3, &token);
        req3 = req3
            .header("content-type", "application/json")
            .header("accept", "application/json")
            .header("origin", "https://chatgpt.com")
            .header("referer", "https://chatgpt.com/");
        let resp3 = req3.json(&json!({})).send().await?;
        // Rotasi cookie dari respons uploaded.
        self.merge_response_cookies(resp3.headers());
        let status3 = resp3.status().as_u16();
        if !(200..300).contains(&status3) {
            let text = resp3.text().await.unwrap_or_default();
            return Err(ProviderError::upstream(
                status3,
                &format!("POST /backend-api/files/{{id}}/uploaded gagal: {status3} {text}"),
            ));
        }

        Ok(AttachmentRef {
            id: file_id,
            name: file_name.to_string(),
            size: bytes.len() as u64,
            mime: mime.to_string(),
        })
    }

    /// Bangun pesan wire dari [`ChatInput`]: gambar diunduh/didekode lalu
    /// di-upload ke file-service (maks [`MAX_ATTACHMENTS`] per turn).
    ///
    /// Gambar di pesan non-user diabaikan (vision hanya untuk pesan user);
    /// teks pesan sistem diteruskan apa adanya (dijadikan `system_hints`
    /// oleh [`build_conversation_body`]).
    async fn build_wire_messages(
        &self,
        input: &ChatInput,
    ) -> Result<Vec<WireMessage>, ProviderError> {
        // Cek cap SEBELUM upload apa pun (tanpa jaringan bila melebihi cap).
        let image_count = input
            .messages
            .iter()
            .flat_map(|m| &m.parts)
            .filter(|p| matches!(p, Part::ImageUrl(_)))
            .count();
        if image_count > MAX_ATTACHMENTS {
            return Err(ProviderError::InvalidInput(format!(
                "terlalu banyak lampiran gambar: {image_count} (maks {MAX_ATTACHMENTS} per turn)"
            )));
        }
        let mut out = Vec::new();
        let mut image_index = 0usize;
        for m in &input.messages {
            let mut text = String::new();
            let mut attachments = Vec::new();
            for part in &m.parts {
                match part {
                    Part::Text(t) => text.push_str(t),
                    Part::ImageUrl(src) => {
                        if m.role != Role::User {
                            tracing::warn!(
                                "chatgpt: gambar di pesan non-user diabaikan (vision hanya untuk pesan user)"
                            );
                            continue;
                        }
                        let (downloaded, url) = match src {
                            ImageSource::Url(url) => (
                                download_image_source(&ImageInput::Url(url.clone())).await?,
                                Some(url.as_str()),
                            ),
                            ImageSource::DataUri { mime, base64 } => (
                                download_image_source(&ImageInput::DataUri {
                                    mime: mime.clone(),
                                    base64: base64.clone(),
                                })
                                .await?,
                                None,
                            ),
                        };
                        let name = image_file_name(url, &downloaded.mime, image_index);
                        image_index += 1;
                        let uploaded = self
                            .upload_file(&name, &downloaded.bytes, &downloaded.mime)
                            .await?;
                        attachments.push(uploaded);
                    }
                }
            }
            out.push(WireMessage {
                role: m.role,
                text,
                attachments,
            });
        }
        Ok(out)
    }
}

/// Parse respons `GET /backend-api/models` menjadi
/// `(ModelInfo, ModelMeta)` per entri.
///
/// Bentuk yang diterima: `{models: [{slug, ...}]}` atau
/// `{data: [{id, ...}]}`. Entri tanpa slug/id/model dilewati.
fn parse_models_response(body: &Value) -> Vec<(ModelInfo, ModelMeta)> {
    let Some(arr) = body
        .get("models")
        .or_else(|| body.get("data"))
        .and_then(|a| a.as_array())
    else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in arr {
        let Some(slug) = entry
            .get("slug")
            .or_else(|| entry.get("id"))
            .or_else(|| entry.get("model"))
            .and_then(|s| s.as_str())
            .filter(|s| !s.is_empty())
        else {
            continue;
        };
        let mut efforts: Vec<String> = Vec::new();
        for key in [
            "thinking_efforts",
            "thinking_effort",
            "allowed_thinking_efforts",
        ] {
            if let Some(list) = entry.get(key).and_then(|l| l.as_array()) {
                for e in list {
                    if let Some(s) = e.as_str() {
                        efforts.push(s.to_string());
                    }
                }
            }
        }
        efforts.sort();
        efforts.dedup();
        out.push((
            ModelInfo::new(slug, "openai"),
            ModelMeta {
                slug: slug.to_string(),
                thinking_efforts: efforts,
            },
        ));
    }
    out
}

/// Body `POST /backend-api/files` untuk satu file.
fn build_file_upload_payload(file_name: &str, file_size: usize) -> Value {
    json!({
        "file_name": file_name,
        "file_size": file_size,
        "use_case": "multimodal",
    })
}

/// Turunkan nama file gambar dari URL (disanitasi) atau default
/// `image-<index>.<ext>` untuk data URI / URL tanpa nama file.
fn image_file_name(url: Option<&str>, mime: &str, index: usize) -> String {
    let ext = match mime {
        "image/png" => "png",
        "image/jpeg" => "jpg",
        "image/webp" => "webp",
        "image/gif" => "gif",
        _ => "bin",
    };
    if let Some(url) = url {
        if let Some(last) = url.split('/').next_back() {
            let base = last.split(['?', '#']).next().unwrap_or("");
            let clean: String = base
                .chars()
                .filter(|c| c.is_ascii_alphanumeric() || *c == '.' || *c == '-' || *c == '_')
                .collect();
            if !clean.is_empty() && clean.len() <= 128 {
                return clean;
            }
        }
    }
    format!("image-{index}.{ext}")
}

/// Resolusi model yang diminta: alias web-default (`"chatgpt-auto"` /
/// kosong) dipakai apa adanya; sisanya via
/// [`core::catalog::resolve_model`](resolve_model) (katalog kosong ->
/// web-default + warning, TANPA daftar fallback statis).
fn resolve_requested_model(requested: &str, catalog: &[ModelInfo]) -> (String, Option<String>) {
    let req = requested.trim();
    if req.is_empty() || req == WEB_DEFAULT {
        return (WEB_DEFAULT.to_string(), None);
    }
    resolve_model(Some(req), catalog, WEB_DEFAULT)
}

/// Petakan slug hasil resolusi ke slug upstream: alias lokal
/// `"chatgpt-auto"` dikirim sebagai `"auto"`.
fn upstream_model_slug(resolved: &str) -> &str {
    if resolved == WEB_DEFAULT {
        UPSTREAM_AUTO
    } else {
        resolved
    }
}

/// Resolusi thinking: (slug model efektif, `thinking_effort`).
///
/// - [`Thinking::Auto`] -> slug apa adanya, tanpa field effort.
/// - [`Thinking::Off`] -> effort `"none"` BILA katalog menyatakan
///   model mendukungnya; bila tidak diketahui/tidak didukung, effort
///   tidak dikirim (jangan kirim effort yang tidak didukung).
/// - [`Thinking::On`] -> pakai varian slug `-thinking` bila ada di
///   katalog (mis. `gpt-5-6` -> `gpt-5-6-thinking`); effort dari level
///   (`0=minimal, 1=low, 2=medium, 3=high, 4=xhigh, 5+=max`), divalidasi
///   terhadap katalog (fallback `medium`, lalu effort pertama yang
///   diizinkan). Bila katalog kosong, effort dikirim apa adanya
///   (best-effort; upstream yang memvalidasi).
///
/// `thinking_effort` yang di-echo upstream adalah CERMIN request, bukan
/// bukti effort diterapkan — kode ini TIDAK PERNAH membaca echo tersebut.
fn resolve_thinking(
    requested: &str,
    thinking: Thinking,
    meta: &[ModelMeta],
) -> (String, Option<String>) {
    let find = |slug: &str| meta.iter().find(|m| m.slug == slug);
    match thinking {
        Thinking::Auto => (requested.to_string(), None),
        Thinking::Off => {
            let effort = find(requested).and_then(|m| {
                m.thinking_efforts
                    .iter()
                    .any(|e| e == "none")
                    .then_some("none".to_string())
            });
            (requested.to_string(), effort)
        }
        Thinking::On { level } => {
            let variant = format!("{requested}-thinking");
            let (effective, efforts) = if let Some(vm) = find(&variant) {
                (variant, vm.thinking_efforts.clone())
            } else {
                (
                    requested.to_string(),
                    find(requested)
                        .map(|m| m.thinking_efforts.clone())
                        .unwrap_or_default(),
                )
            };
            let effort = level.map(|l| {
                let chosen = map_thinking_level(l);
                if efforts.is_empty() || efforts.iter().any(|e| e == chosen) {
                    chosen.to_string()
                } else if efforts.iter().any(|e| e == "medium") {
                    "medium".to_string()
                } else {
                    efforts
                        .first()
                        .cloned()
                        .unwrap_or_else(|| chosen.to_string())
                }
            });
            (effective, effort)
        }
    }
}

/// Petakan level thinking numerik ke nama effort upstream.
fn map_thinking_level(level: u8) -> &'static str {
    match level {
        0 => "minimal",
        1 => "low",
        2 => "medium",
        3 => "high",
        4 => "xhigh",
        _ => "max",
    }
}

/// Bangun body `POST /backend-api/f/conversation` lengkap per riset §2.
///
/// - Pesan sistem -> `system_hints` (plus instruksi search bila
///   [`Search::On`]).
/// - Pesan user/assistant -> `messages` (gambar sebagai
///   `image_asset_pointer` + `metadata.attachments`).
/// - `conversation_id` SENGAJA tidak dikirim: turn stateless adalah
///   percakapan baru (`parent_message_id: "client-created-root"`).
///   Melanjutkan percakapan tersimpan butuh `conversation_id`, yang
///   tidak dibawa [`ChatInput`] (follow-up, bukan v0.2).
/// - `paragen_cot_summary_display_override: "allow"` selalu dikirim.
fn build_conversation_body(
    wire: &[WireMessage],
    model: &str,
    thinking_effort: Option<&str>,
    search: Search,
    timezone_offset_min: i32,
    timezone: Option<&str>,
) -> Value {
    let mut system_hints: Vec<String> = Vec::new();
    let mut messages: Vec<Value> = Vec::new();
    for m in wire {
        if m.role == Role::System {
            if !m.text.trim().is_empty() {
                system_hints.push(m.text.clone());
            }
            continue;
        }
        let role = match m.role {
            Role::User => "user",
            Role::Assistant => "assistant",
            Role::System => "system",
        };
        let mut parts: Vec<Value> = Vec::new();
        for a in &m.attachments {
            parts.push(json!({
                "content_type": "image_asset_pointer",
                "asset_pointer": format!("file-service://{}", a.id),
                "size_bytes": a.size,
            }));
        }
        if !m.text.is_empty() {
            parts.push(json!(m.text.clone()));
        }
        if parts.is_empty() {
            continue;
        }
        let content = if m.attachments.is_empty() {
            json!({ "content_type": "text", "parts": parts })
        } else {
            json!({ "content_type": "multimodal_text", "parts": parts })
        };
        let mut metadata = json!({});
        if !m.attachments.is_empty() {
            metadata["attachments"] = json!(m
                .attachments
                .iter()
                .map(|a| json!({
                    "id": a.id,
                    "name": a.name,
                    "size": a.size,
                    "mime_type": a.mime,
                }))
                .collect::<Vec<_>>());
        }
        messages.push(json!({
            "id": Uuid::new_v4().to_string(),
            "author": { "role": role },
            "create_time": chrono::Utc::now().timestamp(),
            "content": content,
            "metadata": metadata,
        }));
    }
    if search == Search::On {
        system_hints.push(SEARCH_SYSTEM_INSTRUCTION.to_string());
    }
    let mut body = json!({
        "action": "next",
        "messages": messages,
        "model": model,
        "parent_message_id": "client-created-root",
        "timezone_offset_min": timezone_offset_min,
        "conversation_mode": { "kind": "primary_assistant" },
        "history_and_training_disabled": true,
        "supports_buffering": true,
        "supported_encodings": ["v1"],
        "system_hints": system_hints,
        "enable_message_followups": true,
        "client_contextual_info": { "app_name": "chatgpt.com" },
        "paragen_cot_summary_display_override": "allow",
        "force_parallel_switch": "auto",
    });
    if let Some(effort) = thinking_effort {
        if let Some(obj) = body.as_object_mut() {
            obj.insert("thinking_effort".to_string(), json!(effort));
        }
    }
    if let Some(tz) = timezone {
        if let Some(obj) = body.as_object_mut() {
            obj.insert("timezone".to_string(), json!(tz));
        }
    }
    body
}

/// Estimasi ukuran wire (byte JSON terserialisasi) untuk deteksi
/// prompt oversized. Gagal serialisasi (praktis tidak mungkin untuk
/// `Value`) diperlakukan sebagai oversized.
fn estimate_wire_bytes(body: &Value) -> usize {
    serde_json::to_string(body)
        .map(|s| s.len())
        .unwrap_or(usize::MAX)
}

/// Offset zona waktu lokal (menit) + nama zona dari env `TZ` (bila ada).
fn local_timezone() -> (i32, Option<String>) {
    let offset_min = chrono::Local::now().offset().local_minus_utc() / 60;
    let tz = std::env::var("TZ").ok().filter(|v| !v.trim().is_empty());
    (offset_min, tz)
}

/// Satu hasil parsing satu event SSE (internal — bukan [`Event`]).
enum ParsedItem {
    /// Event ternormalisasi untuk konsumen.
    Event(Event),
    /// Server menyerahkan segmen berikutnya ke endpoint resume.
    Handoff {
        /// Token dari event `resume_conversation_token`.
        resume_token: String,
    },
    /// Error upstream di dalam stream.
    Error(ProviderError),
    /// Akhir stream (`[DONE]`, `message_stream_complete`, atau handoff
    /// tanpa token).
    Done,
}

/// State emisi per message id (ledger exactly-once).
#[derive(Default)]
struct EmittedState {
    /// Teks yang sudah di-emit untuk message ini.
    text: String,
    /// Reasoning yang sudah di-emit untuk message ini.
    reasoning: String,
    /// Kunci anotasi yang sudah di-emit (deduplikasi sitasi).
    annotation_keys: Vec<String>,
}

/// Parser SSE JSON-patch v1 ChatGPT.
///
/// Mempertahankan message tree dari frame patch, lalu mengekstrak delta
/// dari snapshot terbaru dikurangi ledger per-message id, sehingga
/// segmen yang di-resume/replay di-emit TEPAT SEKALI, berurutan:
/// - snapshot diawali prefix yang sudah di-emit -> hanya suffix baru;
/// - snapshot terkandung dalam yang sudah di-emit -> tidak ada emisi;
/// - overlap kosakata di batas segmen (hingga 1024 karakter) -> hanya
///   bagian setelah overlap;
/// - selain itu -> seluruh snapshot (konten baru).
struct V1Parser {
    /// Message tree hasil penerapan patch (root: `{message: ...}`).
    tree: Value,
    /// Ledger emisi per message id.
    ledger: HashMap<String, EmittedState>,
    /// Token resume terakhir dari event `resume_conversation_token`.
    resume_token: Option<String>,
}

impl V1Parser {
    fn new() -> Self {
        Self {
            tree: Value::Null,
            ledger: HashMap::new(),
            resume_token: None,
        }
    }

    /// Tangani satu event SSE (`event:` + `data:` yang sudah dipasangkan
    /// oleh penelepon; `data` boleh multi-baris, digabung `\n`).
    fn handle_sse(&mut self, event: &str, data: &str) -> Vec<ParsedItem> {
        let data = data.trim();
        if data == "[DONE]" {
            return vec![ParsedItem::Done];
        }
        match event {
            "delta_encoding" => {
                // Event pertama: `data: "v1"` menandai encoding patch v1.
                // Tidak ada state yang dibutuhkan selain pengakuan ini;
                // frame patch/legacy ditangani sama (lihat handle_data_frame).
                Vec::new()
            }
            "resume_conversation_token" => {
                if let Some(tok) = parse_resume_token(data) {
                    self.resume_token = Some(tok);
                }
                Vec::new()
            }
            "stream_handoff" => {
                let tok = parse_resume_token(data).or_else(|| self.resume_token.clone());
                match tok {
                    Some(t) => vec![ParsedItem::Handoff { resume_token: t }],
                    // Handoff tanpa token = tidak ada segmen lanjutan.
                    None => vec![ParsedItem::Done],
                }
            }
            // Stream message ini selesai -> akhir turn.
            "message_stream_complete" => vec![ParsedItem::Done],
            "input_message" | "server_ste_metadata" | "conversation_detail_metadata" => Vec::new(),
            "error" => vec![ParsedItem::Error(ProviderError::upstream(502, data))],
            _ => self.handle_data_frame(data),
        }
    }

    /// Tangani satu payload `data:` tanpa nama event (frame patch,
    /// snapshot legacy, atau string `"v1"`).
    fn handle_data_frame(&mut self, data: &str) -> Vec<ParsedItem> {
        let Ok(v) = serde_json::from_str::<Value>(data) else {
            return Vec::new();
        };
        match &v {
            Value::String(s) => {
                let _ = s;
                Vec::new()
            }
            Value::Object(map) => {
                if map.contains_key("o") {
                    return self.handle_patch_frame(&v);
                }
                if map.contains_key("message") {
                    // Snapshot legacy (tanpa p/o): ganti tree utuh.
                    self.tree = v.clone();
                    return self.extract();
                }
                if map.contains_key("detail") || map.contains_key("error") {
                    return vec![ParsedItem::Error(ProviderError::upstream(502, data))];
                }
                Vec::new()
            }
            _ => Vec::new(),
        }
    }

    /// Terapkan satu frame patch (tunggal atau `bulk-patch`), lalu ekstrak.
    fn handle_patch_frame(&mut self, frame: &Value) -> Vec<ParsedItem> {
        let op = frame.get("o").and_then(|o| o.as_str()).unwrap_or("");
        if op == "bulk-patch" {
            if let Some(subs) = frame.get("v").and_then(|v| v.as_array()) {
                for sub in subs {
                    let p = sub.get("p").and_then(|p| p.as_str()).unwrap_or("");
                    let o = sub.get("o").and_then(|o| o.as_str()).unwrap_or("");
                    let val = sub.get("v").cloned().unwrap_or(Value::Null);
                    apply_json_patch(&mut self.tree, p, o, &val);
                }
            }
        } else {
            let p = frame.get("p").and_then(|p| p.as_str()).unwrap_or("");
            let val = frame.get("v").cloned().unwrap_or(Value::Null);
            apply_json_patch(&mut self.tree, p, op, &val);
        }
        self.extract()
    }

    /// Ekstrak delta dari snapshot message di tree, dikurangi ledger.
    fn extract(&mut self) -> Vec<ParsedItem> {
        let msg = match self.tree.get("message") {
            Some(m) => m.clone(),
            None => {
                if self.tree.get("content").is_some() {
                    self.tree.clone()
                } else {
                    return Vec::new();
                }
            }
        };
        // Echo pesan user tidak pernah di-emit.
        if let Some(role) = msg
            .get("author")
            .and_then(|a| a.get("role"))
            .and_then(|r| r.as_str())
        {
            if role != "assistant" {
                return Vec::new();
            }
        }
        let id = msg
            .get("id")
            .or_else(|| msg.get("message_id"))
            .and_then(|i| i.as_str())
            .unwrap_or("default")
            .to_string();
        let (text, reasoning) = extract_message_text(&msg);
        let candidates = extract_annotations(&msg);
        let mut items = Vec::new();
        {
            let entry = self.ledger.entry(id).or_default();
            let new_text = dedup_suffix(&entry.text, &text);
            if !new_text.is_empty() {
                entry.text.push_str(&new_text);
                items.push(ParsedItem::Event(Event::Text(new_text)));
            }
            let new_reasoning = dedup_suffix(&entry.reasoning, &reasoning);
            if !new_reasoning.is_empty() {
                entry.reasoning.push_str(&new_reasoning);
                items.push(ParsedItem::Event(Event::Reasoning(new_reasoning)));
            }
            for (kind, text, url) in candidates {
                let key = format!("{kind}\0{text}\0{}", url.as_deref().unwrap_or(""));
                if !entry.annotation_keys.contains(&key) {
                    entry.annotation_keys.push(key);
                    items.push(ParsedItem::Event(Event::Annotation(Annotation {
                        kind,
                        text,
                        url,
                    })));
                }
            }
        }
        items
    }
}

/// Parse token resume dari payload event (string JSON, objek dengan
/// field token, atau token mentah).
fn parse_resume_token(data: &str) -> Option<String> {
    let data = data.trim();
    if data.is_empty() {
        return None;
    }
    if let Ok(v) = serde_json::from_str::<Value>(data) {
        if let Some(s) = v.as_str() {
            if !s.is_empty() {
                return Some(s.to_string());
            }
        }
        for key in ["resume_token", "resume_conversation_token", "token"] {
            if let Some(s) = v.get(key).and_then(|t| t.as_str()) {
                if !s.is_empty() {
                    return Some(s.to_string());
                }
            }
        }
        return None;
    }
    if !data.contains(|c: char| c.is_whitespace() || c == '{' || c == '"') {
        return Some(data.to_string());
    }
    None
}

/// Ekstrak (teks, reasoning) dari satu objek message.
///
/// - `thoughts` / `reasoning_recap` -> reasoning.
/// - `text` -> teks.
/// - `image_asset_pointer` -> dilewati (gambar di jalur chat teks tidak
///   diunduh; image generation punya jalurnya sendiri).
/// - Tanpa `content_type` -> teks.
fn extract_message_text(msg: &Value) -> (String, String) {
    let mut text = String::new();
    let mut reasoning = String::new();
    let Some(content) = msg.get("content") else {
        if let Some(t) = msg.get("text").and_then(|t| t.as_str()) {
            text.push_str(t);
        }
        return (text, reasoning);
    };
    match content {
        Value::String(s) => text.push_str(s),
        Value::Object(_) => {
            let ctype = content
                .get("content_type")
                .and_then(|c| c.as_str())
                .unwrap_or("text");
            if let Some(parts) = content.get("parts").and_then(|p| p.as_array()) {
                for part in parts {
                    collect_part(part, ctype, &mut text, &mut reasoning);
                }
            } else if let Some(t) = content.get("text").and_then(|t| t.as_str()) {
                if ctype == "thoughts" || ctype == "reasoning_recap" {
                    reasoning.push_str(t);
                } else {
                    text.push_str(t);
                }
            }
        }
        Value::Array(parts) => {
            for part in parts {
                collect_part(part, "text", &mut text, &mut reasoning);
            }
        }
        _ => {}
    }
    (text, reasoning)
}

/// Kumpulkan satu part konten ke akumulator teks/reasoning.
fn collect_part(part: &Value, default_ctype: &str, text: &mut String, reasoning: &mut String) {
    match part {
        Value::String(s) => {
            if default_ctype == "thoughts" || default_ctype == "reasoning_recap" {
                reasoning.push_str(s);
            } else {
                text.push_str(s);
            }
        }
        Value::Object(_) => {
            let ptype = part
                .get("content_type")
                .and_then(|c| c.as_str())
                .unwrap_or(default_ctype);
            if ptype == "image_asset_pointer" {
                return;
            }
            if let Some(t) = part.get("text").and_then(|t| t.as_str()) {
                if ptype == "thoughts" || ptype == "reasoning_recap" {
                    reasoning.push_str(t);
                } else {
                    text.push_str(t);
                }
            } else if let Some(parts) = part.get("parts").and_then(|p| p.as_array()) {
                for p in parts {
                    collect_part(p, ptype, text, reasoning);
                }
            }
        }
        _ => {}
    }
}

/// Ekstrak kandidat anotasi (sitasi/search) dari message.
///
/// Sumber: `metadata` dan level message langsung (keduanya diperiksa;
/// deduplikasi via kunci di ledger). Bentuk yang diterima: `citations`,
/// `content_references`, `search_result_groups` (dengan/tanpa `results`).
fn extract_annotations(msg: &Value) -> Vec<(String, String, Option<String>)> {
    let mut out = Vec::new();
    for source in [msg.get("metadata"), Some(msg)] {
        let Some(src) = source else { continue };
        if let Some(arr) = src.get("citations").and_then(|a| a.as_array()) {
            for item in arr {
                if let Some((t, u)) = annotation_text_url(item) {
                    out.push(("citation".to_string(), t, u));
                }
            }
        }
        if let Some(arr) = src.get("content_references").and_then(|a| a.as_array()) {
            for item in arr {
                if let Some((t, u)) = annotation_text_url(item) {
                    out.push(("content_reference".to_string(), t, u));
                }
            }
        }
        if let Some(arr) = src.get("search_result_groups").and_then(|a| a.as_array()) {
            for group in arr {
                if let Some(results) = group.get("results").and_then(|r| r.as_array()) {
                    for item in results {
                        if let Some((t, u)) = annotation_text_url(item) {
                            out.push(("search_result".to_string(), t, u));
                        }
                    }
                } else if let Some((t, u)) = annotation_text_url(group) {
                    out.push(("search_result".to_string(), t, u));
                }
            }
        }
    }
    out
}

/// Ambil (teks, url) dari satu item sitasi/search (berbagai nama field).
fn annotation_text_url(item: &Value) -> Option<(String, Option<String>)> {
    let text = item
        .get("text")
        .or_else(|| item.get("title"))
        .or_else(|| item.get("snippet"))
        .or_else(|| item.get("name"))
        .and_then(|t| t.as_str())?;
    if text.is_empty() {
        return None;
    }
    let url = item
        .get("url")
        .or_else(|| item.get("link"))
        .or_else(|| item.get("href"))
        .and_then(|u| u.as_str())
        .map(|s| s.to_string());
    Some((text.to_string(), url))
}

/// Bagian snapshot yang belum di-emit (lihat dok [`V1Parser`]).
fn dedup_suffix(emitted: &str, snapshot: &str) -> String {
    if snapshot.is_empty() {
        return String::new();
    }
    if let Some(suffix) = snapshot.strip_prefix(emitted) {
        return suffix.to_string();
    }
    if emitted.contains(snapshot) {
        return String::new();
    }
    // Overlap kosakata di batas segmen: cari overlap terpanjang
    // (dibatasi 1024 karakter) antara akhir `emitted` dan awal `snapshot`.
    let e_chars: Vec<char> = emitted.chars().collect();
    let s_chars: Vec<char> = snapshot.chars().collect();
    let window = 1024.min(e_chars.len()).min(s_chars.len());
    for k in (1..=window).rev() {
        if e_chars[e_chars.len() - k..] == s_chars[..k] {
            return s_chars[k..].iter().collect();
        }
    }
    snapshot.to_string()
}

/// Terapkan satu operasi JSON-patch mini (add/replace/append/remove)
/// ke `tree`. Path memakai sintaks JSON Pointer (`""` = root,
/// `"/message/content/parts/0"`, `"-"` = akhir array). Path yang tidak
/// bisa dinavigasi diabaikan (frame rusak tidak boleh mematikan stream).
fn apply_json_patch(tree: &mut Value, path: &str, op: &str, value: &Value) {
    if path.is_empty() {
        match op {
            "add" | "replace" => *tree = value.clone(),
            "append" => append_value(tree, value),
            "remove" => *tree = Value::Null,
            _ => {}
        }
        return;
    }
    let segments: Vec<String> = path
        .split('/')
        .skip(1)
        .map(|s| s.replace("~1", "/").replace("~0", "~"))
        .collect();
    if segments.is_empty() {
        return;
    }
    match op {
        "add" | "replace" => {
            if let Some(target) = navigate_mut(tree, &segments) {
                *target = value.clone();
            }
        }
        "append" => {
            if let Some(target) = navigate_mut(tree, &segments) {
                append_value(target, value);
            }
        }
        "remove" => {
            let (parent_segs, last) = segments.split_at(segments.len() - 1);
            let last = &last[0];
            if parent_segs.is_empty() {
                remove_child(tree, last);
            } else if let Some(parent) = navigate_mut(tree, parent_segs) {
                remove_child(parent, last);
            }
        }
        _ => {}
    }
}

/// Semantik append: string += string, array += elemen/array, `null` = ganti.
fn append_value(target: &mut Value, value: &Value) {
    match (&mut *target, value) {
        (Value::String(s), Value::String(v)) => s.push_str(v),
        (Value::Array(a), Value::Array(vs)) => a.extend(vs.iter().cloned()),
        (Value::Array(a), v) => a.push(v.clone()),
        (Value::Null, v) => *target = v.clone(),
        _ => {}
    }
}

/// Navigasi ke node di `segments`, membuat node perantara (`Null`)
/// bila belum ada. `"-"` berarti append satu slot `Null` di array.
fn navigate_mut<'a>(mut node: &'a mut Value, segments: &[String]) -> Option<&'a mut Value> {
    for seg in segments {
        match node {
            Value::Object(map) => {
                node = map.entry(seg.clone()).or_insert(Value::Null);
            }
            Value::Array(arr) => {
                if seg == "-" {
                    arr.push(Value::Null);
                    let len = arr.len();
                    node = &mut arr[len - 1];
                } else {
                    let Ok(idx) = seg.parse::<usize>() else {
                        return None;
                    };
                    if idx > arr.len() {
                        return None;
                    }
                    if idx == arr.len() {
                        arr.push(Value::Null);
                    }
                    node = &mut arr[idx];
                }
            }
            _ => return None,
        }
    }
    Some(node)
}

/// Hapus anak `key` dari objek/array (indeks di luar jangkauan diabaikan).
fn remove_child(node: &mut Value, key: &str) {
    match node {
        Value::Object(map) => {
            map.remove(key);
        }
        Value::Array(arr) => {
            if let Ok(idx) = key.parse::<usize>() {
                if idx < arr.len() {
                    arr.remove(idx);
                }
            }
        }
        _ => {}
    }
}

#[async_trait::async_trait]
impl Provider for ChatGptProvider {
    fn id(&self) -> &'static str {
        "chatgpt"
    }

    fn web_default_model(&self) -> &'static str {
        WEB_DEFAULT
    }

    fn models(&self) -> Vec<ModelInfo> {
        // Alias web-default SELALU ada (untuk routing server) + katalog
        // live dari cache. Bagian katalog kosong bila belum pernah
        // berhasil diambil (upstream failure -> tanpa daftar statis).
        let mut out = vec![ModelInfo::new(WEB_DEFAULT, "openai")];
        if let Ok(g) = self.inner.catalog.try_read() {
            for m in g.models() {
                if m.id != WEB_DEFAULT && !out.iter().any(|x| x.id == m.id) {
                    out.push(m.clone());
                }
            }
        }
        out
    }

    async fn warm_catalog(&self) -> Result<(), ProviderError> {
        // Tanpa jaringan bila belum dikonfigurasi / tanpa cookie sesi.
        if self.inner.cookies_empty() || !self.inner.has_session_cookie() {
            return Err(ProviderError::Unauthorized(
                "AIW2A_CHATGPT_COOKIES belum diisi / tanpa cookie sesi".to_string(),
            ));
        }
        let list = self.inner.fetch_catalog().await?;
        let (infos, metas): (Vec<ModelInfo>, Vec<ModelMeta>) = list.into_iter().unzip();
        self.inner.catalog.write().await.update(infos);
        *self.inner.meta.write().await = metas;
        Ok(())
    }

    async fn chat(&self, input: &ChatInput) -> Result<EventStream, ProviderError> {
        let inner = Arc::clone(&self.inner);
        if inner.cookies_empty() {
            return Err(ProviderError::Unauthorized(
                "AIW2A_CHATGPT_COOKIES belum diisi".to_string(),
            ));
        }
        // Fail-fast: blob ois1 saja tidak cukup untuk session exchange.
        if !inner.has_session_cookie() {
            return Err(ProviderError::Unauthorized(
                "AIW2A_CHATGPT_COOKIES harus berisi FULL cookie header dari browser \
                 (wajib ada __Secure-next-auth.session-token). Ambil via DevTools > Network > \
                 klik request ke chatgpt.com > copy nilai header Cookie. \
                 Nilai 'ois1...' saja tidak cukup — itu hanya cookie __Secure-oai-is."
                    .to_string(),
            ));
        }
        // Pacing antar-request (0ms untuk ChatGPT — mekanisme tersedia).
        inner.pacer.wait().await;
        // Katalog live: refresh bila basi (best-effort, gagal = warning).
        inner.refresh_catalog(false).await;
        let catalog = inner.catalog_snapshot();
        let (resolved, warning) = resolve_requested_model(&input.model, &catalog);
        if let Some(w) = warning {
            tracing::warn!("chatgpt: {w}");
        }
        let meta = inner.meta_snapshot();
        let (effective_model, thinking_effort) = resolve_thinking(&resolved, input.thinking, &meta);
        let upstream_model = upstream_model_slug(&effective_model);
        let wire = inner.build_wire_messages(input).await?;
        if wire
            .iter()
            .filter(|m| m.role != Role::System)
            .all(|m| m.text.trim().is_empty() && m.attachments.is_empty())
        {
            return Err(ProviderError::InvalidInput(
                "prompt kosong — kirim setidaknya satu pesan teks/gambar".to_string(),
            ));
        }
        let (offset_min, timezone) = local_timezone();
        let body = build_conversation_body(
            &wire,
            upstream_model,
            thinking_effort.as_deref(),
            input.search,
            offset_min,
            timezone.as_deref(),
        );
        if estimate_wire_bytes(&body) > OVERSIZED_WIRE_BYTES {
            // TODO(live-capture): prompt oversized (>96 KiB) seharusnya
            // diserialisasi lossless ke lampiran file JSONL (pola
            // gptweb2api); content_type/use_case lampiran JSONL belum
            // terdokumentasi, jadi prompt dikirim apa adanya — lossless,
            // tanpa pemotongan.
            tracing::warn!(
                "chatgpt: prompt melebihi 96 KiB estimasi wire — dikirim apa adanya (konversi JSONL belum terverifikasi)"
            );
        }
        let first = inner.post_conversation(&body).await?;
        let prompt_chars: usize = wire.iter().map(|m| m.text.chars().count()).sum();

        let s = stream! {
            let mut current: Option<reqwest::Response> = Some(first);
            let mut parser = V1Parser::new();
            let mut text_chars: usize = 0;
            let mut reasoning_chars: usize = 0;
            let mut got_any = false;
            let mut finished = false;
            let mut handoffs: usize = 0;
            while !finished {
                let Some(resp) = current.take() else { break };
                let mut byte_stream = resp.bytes_stream();
                let mut buf = String::new();
                let mut pending_event = String::new();
                let mut pending_data: Vec<String> = Vec::new();
                let mut handoff_token: Option<String> = None;
                let mut saw_error: Option<ProviderError> = None;
                let mut stop_inner = false;
                while let Some(chunk) = byte_stream.next().await {
                    let bytes = match chunk {
                        Ok(b) => b,
                        Err(e) => {
                            yield Err(ProviderError::Network(e.to_string()));
                            finished = true;
                            break;
                        }
                    };
                    buf.push_str(&String::from_utf8_lossy(&bytes));
                    while let Some(pos) = buf.find('\n') {
                        let line: String = buf.drain(..=pos).collect();
                        let line = line.trim();
                        if line.is_empty() {
                            let items =
                                parser.handle_sse(&pending_event, &pending_data.join("\n"));
                            pending_event.clear();
                            pending_data.clear();
                            for item in items {
                                match item {
                                    ParsedItem::Event(ev) => {
                                        match &ev {
                                            Event::Text(t) => {
                                                text_chars += t.chars().count();
                                                got_any = true;
                                            }
                                            Event::Reasoning(t) => {
                                                reasoning_chars += t.chars().count();
                                                got_any = true;
                                            }
                                            Event::Annotation(_) => {
                                                got_any = true;
                                            }
                                            _ => {}
                                        }
                                        yield Ok(ev);
                                    }
                                    ParsedItem::Handoff { resume_token } => {
                                        handoff_token = Some(resume_token);
                                        stop_inner = true;
                                    }
                                    ParsedItem::Error(e) => {
                                        saw_error = Some(e);
                                        stop_inner = true;
                                    }
                                    // Done (eksplisit) maupun EOF tanpa handoff
                                    // sama-sama mengakhiri turn (lihat cabang akhir).
                                    ParsedItem::Done => {
                                        stop_inner = true;
                                    }
                                }
                            }
                        } else if let Some(ev) = line.strip_prefix("event:") {
                            pending_event = ev.trim().to_string();
                        } else if let Some(d) = line.strip_prefix("data:") {
                            pending_data.push(d.trim_start().to_string());
                        }
                    }
                    if stop_inner {
                        break;
                    }
                }
                if finished {
                    continue;
                }
                // EOF: flush event SSE terakhir tanpa baris kosong.
                if !pending_data.is_empty() || !pending_event.is_empty() {
                    let items = parser.handle_sse(&pending_event, &pending_data.join("\n"));
                    for item in items {
                        match item {
                            ParsedItem::Event(ev) => {
                                match &ev {
                                    Event::Text(t) => {
                                        text_chars += t.chars().count();
                                        got_any = true;
                                    }
                                    Event::Reasoning(t) => {
                                        reasoning_chars += t.chars().count();
                                        got_any = true;
                                    }
                                    Event::Annotation(_) => {
                                        got_any = true;
                                    }
                                    _ => {}
                                }
                                yield Ok(ev);
                            }
                            ParsedItem::Handoff { resume_token } => {
                                handoff_token = Some(resume_token);
                            }
                            ParsedItem::Error(e) => {
                                saw_error = Some(e);
                            }
                            ParsedItem::Done => {}
                        }
                    }
                }
                if let Some(e) = saw_error {
                    yield Err(e);
                    finished = true;
                } else if let Some(tok) = handoff_token {
                    handoffs += 1;
                    if handoffs > MAX_HANDOFFS {
                        yield Err(ProviderError::Parse(format!(
                            "terlalu banyak handoff resume (>{MAX_HANDOFFS}) — stream terpotong"
                        )));
                        finished = true;
                    } else {
                        match inner.resume_stream(&tok).await {
                            Ok(r) => {
                                current = Some(r);
                            }
                            Err(e) => {
                                yield Err(e);
                                finished = true;
                            }
                        }
                    }
                } else {
                    // Done eksplisit atau EOF tanpa handoff: turn selesai.
                    if !got_any {
                        yield Err(ProviderError::Parse(
                            "respons ChatGPT kosong/tidak dikenali — endpoint web mungkin berubah \
                             atau request diblokir verifikasi".to_string(),
                        ));
                    } else {
                        // Usage SELALU estimasi (chars/4) — tidak ada
                        // provider web yang mengembalikan token count asli.
                        yield Ok(Event::Usage {
                            prompt: (prompt_chars / 4) as u64,
                            completion: (text_chars / 4) as u64,
                            reasoning: (reasoning_chars / 4) as u64,
                            estimated: true,
                        });
                        yield Ok(Event::Done);
                    }
                    finished = true;
                }
            }
        };
        Ok(Box::pin(s))
    }

    async fn generate_image(&self, prompt: &str) -> Result<GeneratedImage, ProviderError> {
        let inner = Arc::clone(&self.inner);
        if inner.cookies_empty() {
            return Err(ProviderError::Unauthorized(
                "AIW2A_CHATGPT_COOKIES belum diisi".to_string(),
            ));
        }
        if !inner.has_session_cookie() {
            return Err(ProviderError::Unauthorized(
                "AIW2A_CHATGPT_COOKIES harus berisi FULL cookie header dari browser \
                 (wajib ada __Secure-next-auth.session-token)."
                    .to_string(),
            ));
        }
        // Image gen di web ChatGPT = tool invocation dalam satu turn
        // conversation biasa (persistent turn). Model: web default
        // ("auto" upstream) — TIDAK PERNAH dikunci ke satu model spesifik.
        let turn = format!("Generate an image: {prompt}");
        let wire = vec![WireMessage {
            role: Role::User,
            text: turn,
            attachments: Vec::new(),
        }];
        let (offset_min, timezone) = local_timezone();
        let body = build_conversation_body(
            &wire,
            UPSTREAM_AUTO,
            None,
            Search::Auto,
            offset_min,
            timezone.as_deref(),
        );
        let resp = inner.post_conversation(&body).await?;
        let text = resp
            .text()
            .await
            .map_err(|e| ProviderError::Network(e.to_string()))?;

        // Gambar tiba sebagai file reference estuary/content di stream SSE.
        // Cari URL-nya dengan pencarian string biasa (tanpa regex).
        for (marker, prefix) in [
            ("https://chatgpt.com/backend-api/estuary/content?", ""),
            ("/backend-api/estuary/content?", "https://chatgpt.com"),
        ] {
            if let Some(start) = text.find(marker) {
                let rest = &text[start..];
                let end = rest
                    .find(|c: char| c == '"' || c == '\\' || c.is_whitespace())
                    .unwrap_or(rest.len());
                let url = format!("{prefix}{}", &rest[..end]);
                return inner.download_estuary(&url).await;
            }
        }
        Err(ProviderError::Parse(
            "tidak menemukan URL gambar di respons ChatGPT - mungkin limit image free-tier habis atau prompt ditolak".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::Message;

    fn frame_json(p: &str, o: &str, v: Value) -> String {
        serde_json::to_string(&json!({ "p": p, "o": o, "v": v })).expect("frame fixture valid")
    }

    fn snapshot_frame(id: &str, text: &str) -> String {
        frame_json(
            "",
            "add",
            json!({ "message": {
                "id": id,
                "author": { "role": "assistant" },
                "content": { "content_type": "text", "parts": [text] },
            }}),
        )
    }

    fn texts(items: &[ParsedItem]) -> Vec<String> {
        items
            .iter()
            .filter_map(|it| match it {
                ParsedItem::Event(Event::Text(t)) => Some(t.clone()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn pow_sha256_counter_solves_and_verifies() {
        let seed = "fixture-seed-123";
        let difficulty = json!("ffffff");
        let answer = solve_pow_sha256_counter(seed, &difficulty)
            .expect("PoW harus terselesaikan untuk difficulty longgar");
        let threshold = parse_pow_threshold(&difficulty).expect("threshold harus ter-parse");
        let counter: u64 = answer.parse().expect("jawaban PoW adalah counter numerik");
        assert!(pow_sha256_satisfies(seed, &threshold, counter));
        // Difficulty ketat (6 nol hex) tidak dipenuhi counter 0.
        let strict =
            parse_pow_threshold(&json!("000000")).expect("threshold ketat harus ter-parse");
        assert!(!pow_sha256_satisfies(seed, &strict, 0));
        // Objek finalize membawa seed/difficulty/answer.
        let proof = solve_pow(seed, &difficulty).expect("solve_pow harus berhasil");
        assert_eq!(proof["seed"], seed);
        assert_eq!(proof["difficulty"], difficulty);
        assert_eq!(proof["answer"], answer);
    }

    #[test]
    fn pow_threshold_parsing() {
        assert!(matches!(
            parse_pow_threshold(&json!("ffffff")),
            Some(PowThreshold::FirstBytesLessThan(0xff_ffff))
        ));
        assert!(matches!(
            parse_pow_threshold(&json!("00000")),
            Some(PowThreshold::LeadingZeroHexChars(5))
        ));
        assert!(matches!(
            parse_pow_threshold(&json!(20)),
            Some(PowThreshold::LeadingZeroBits(20))
        ));
        assert!(parse_pow_threshold(&json!("")).is_none());
        assert!(parse_pow_threshold(&json!("zzzz")).is_none());
        assert!(parse_pow_threshold(&json!(null)).is_none());
    }

    #[test]
    fn pow_sha3_variant_returns_none_pending_live_capture() {
        assert!(solve_pow_sha3_512_fingerprint("seed", &json!("ffffff")).is_none());
    }

    #[test]
    fn challenge_error_detection() {
        assert!(challenge_error_from_text("turnstile token is required").is_some());
        assert!(challenge_error_from_text("TURNSTILE TOKEN FAILED").is_some());
        assert!(challenge_error_from_text("arkose token is required").is_some());
        assert!(challenge_error_from_text("cf-mitigated: challenge").is_some());
        assert!(challenge_error_from_text("some other error").is_none());
        assert!(is_challenge_error(&challenge_error_for("turnstile")));
        assert!(!is_challenge_error(&ProviderError::Parse("x".to_string())));
        // Deskriptor challenge bukan token.
        assert!(solved_token(Some(&json!({ "required": true, "dx": "abc" }))).is_none());
        assert_eq!(
            solved_token(Some(&json!("tok-1"))).as_deref(),
            Some("tok-1")
        );
        assert_eq!(
            solved_token(Some(&json!({ "token": "tok-2" }))).as_deref(),
            Some("tok-2")
        );
    }

    #[test]
    fn v1_patch_add_append_replace_bulk() {
        let mut p = V1Parser::new();
        assert!(p.handle_sse("delta_encoding", "\"v1\"").is_empty());
        let items = p.handle_sse("", &snapshot_frame("m1", "Hello "));
        assert_eq!(texts(&items), vec!["Hello "]);
        // Append ke parts[0].
        let items = p.handle_sse(
            "",
            &frame_json("/message/content/parts/0", "append", json!("world")),
        );
        assert_eq!(texts(&items), vec!["world"]);
        // Replace snapshot penuh -> hanya suffix baru.
        let items = p.handle_sse(
            "",
            &frame_json("/message/content/parts/0", "replace", json!("Hello world!")),
        );
        assert_eq!(texts(&items), vec!["!"]);
        // Bulk-patch: dua append -> satu delta gabungan (ledger).
        let items = p.handle_sse(
            "",
            &frame_json(
                "",
                "bulk-patch",
                json!([
                    { "p": "/message/content/parts/0", "o": "append", "v": " A" },
                    { "p": "/message/content/parts/0", "o": "append", "v": " B" },
                ]),
            ),
        );
        assert_eq!(texts(&items), vec![" A B"]);
    }

    #[test]
    fn v1_replay_detection_emits_exactly_once() {
        let mut p = V1Parser::new();
        let items = p.handle_sse("", &snapshot_frame("m1", "Hello world"));
        assert_eq!(texts(&items), vec!["Hello world"]);
        // Replay snapshot identik -> tidak ada emisi.
        let items = p.handle_sse("", &snapshot_frame("m1", "Hello world"));
        assert!(items.is_empty());
        // Replay prefix + ekstensi -> hanya ekstensi.
        let items = p.handle_sse("", &snapshot_frame("m1", "Hello world!"));
        assert_eq!(texts(&items), vec!["!"]);
        // Segmen baru (id baru) -> teks penuh.
        let items = p.handle_sse("", &snapshot_frame("m2", "Second segment"));
        assert_eq!(texts(&items), vec!["Second segment"]);
        // Replay segmen lama setelah segmen baru -> tidak ada emisi.
        let items = p.handle_sse("", &snapshot_frame("m1", "Hello world"));
        assert!(items.is_empty());
    }

    #[test]
    fn v1_overlap_dedup_handles_segment_boundary() {
        // Snapshot kedua mengulang ekor snapshot pertama (overlap kosakata).
        let mut p = V1Parser::new();
        let items = p.handle_sse("", &snapshot_frame("m1", "alpha beta gamma"));
        assert_eq!(texts(&items), vec!["alpha beta gamma"]);
        // Snapshot baru: "beta gamma delta" — overlap "beta gamma".
        let items = p.handle_sse("", &snapshot_frame("m1", "beta gamma delta"));
        assert_eq!(texts(&items), vec![" delta"]);
    }

    #[test]
    fn v1_typed_events() {
        let mut p = V1Parser::new();
        // thoughts -> Reasoning.
        let frame = frame_json(
            "",
            "add",
            json!({ "message": {
                "id": "m1",
                "author": { "role": "assistant" },
                "content": { "content_type": "thoughts", "parts": ["thinking..."] },
            }}),
        );
        let items = p.handle_sse("", &frame);
        assert_eq!(items.len(), 1);
        assert!(matches!(
            &items[0],
            ParsedItem::Event(Event::Reasoning(t)) if t == "thinking..."
        ));
        // citations -> Annotation (plus teks baru dari message yang sama).
        let frame = frame_json(
            "",
            "add",
            json!({ "message": {
                "id": "m1",
                "author": { "role": "assistant" },
                "content": { "content_type": "text", "parts": ["Answer."] },
                "metadata": { "citations": [{ "text": "Source title", "url": "https://example.com/s" }] },
            }}),
        );
        let items = p.handle_sse("", &frame);
        assert_eq!(texts(&items), vec!["Answer."]);
        let anns: Vec<&Annotation> = items
            .iter()
            .filter_map(|it| match it {
                ParsedItem::Event(Event::Annotation(a)) => Some(a),
                _ => None,
            })
            .collect();
        assert_eq!(anns.len(), 1);
        assert_eq!(anns[0].kind, "citation");
        assert_eq!(anns[0].text, "Source title");
        assert_eq!(anns[0].url.as_deref(), Some("https://example.com/s"));
        // Sitasi yang sama di-replay -> tidak di-emit lagi.
        let items = p.handle_sse("", &frame);
        assert!(items.is_empty());
        // search_result_groups -> Annotation search_result.
        let frame = frame_json(
            "",
            "add",
            json!({ "message": {
                "id": "m1",
                "author": { "role": "assistant" },
                "content": { "content_type": "text", "parts": ["Answer. More."] },
                "metadata": { "search_result_groups": [{ "results": [{ "title": "Result", "url": "https://example.com/r" }] }] },
            }}),
        );
        let items = p.handle_sse("", &frame);
        assert_eq!(texts(&items), vec![" More."]);
        assert!(items.iter().any(|it| matches!(
            it,
            ParsedItem::Event(Event::Annotation(a)) if a.kind == "search_result"
        )));
        // message_stream_complete -> Done; [DONE] -> Done.
        assert!(matches!(
            p.handle_sse("message_stream_complete", "{}").as_slice(),
            [ParsedItem::Done]
        ));
        assert!(matches!(
            p.handle_sse("", "[DONE]").as_slice(),
            [ParsedItem::Done]
        ));
        // Event non-konten diabaikan.
        assert!(p.handle_sse("input_message", "{}").is_empty());
        assert!(p.handle_sse("server_ste_metadata", "{}").is_empty());
        assert!(p
            .handle_sse("conversation_detail_metadata", "{}")
            .is_empty());
        // Error event -> Error.
        assert!(matches!(
            p.handle_sse("error", "boom").as_slice(),
            [ParsedItem::Error(_)]
        ));
    }

    #[test]
    fn v1_user_echo_ignored_and_legacy_snapshot_parses() {
        let mut p = V1Parser::new();
        // Echo user tidak di-emit.
        let frame = frame_json(
            "",
            "add",
            json!({ "message": {
                "id": "u1",
                "author": { "role": "user" },
                "content": { "content_type": "text", "parts": ["echo?"] },
            }}),
        );
        assert!(p.handle_sse("", &frame).is_empty());
        // Snapshot legacy (tanpa p/o) masih diparse.
        let legacy = serde_json::to_string(&json!({ "message": {
            "id": "m1",
            "author": { "role": "assistant" },
            "content": { "content_type": "text", "parts": ["Legacy hello"] },
        } }))
        .expect("legacy fixture valid");
        let items = p.handle_sse("", &legacy);
        assert_eq!(texts(&items), vec!["Legacy hello"]);
        // Remove tidak crash dan tidak meng-emit.
        let items = p.handle_sse(
            "",
            &frame_json("/message/content/parts/0", "remove", Value::Null),
        );
        assert!(items.is_empty());
    }

    #[test]
    fn v1_resume_token_and_handoff() {
        let mut p = V1Parser::new();
        assert!(p
            .handle_sse("resume_conversation_token", "\"tok-abc\"")
            .is_empty());
        let items = p.handle_sse("stream_handoff", "{}");
        assert!(matches!(
            items.as_slice(),
            [ParsedItem::Handoff { resume_token }] if resume_token == "tok-abc"
        ));
        // Handoff tanpa token -> Done (tidak ada segmen lanjutan).
        let mut p2 = V1Parser::new();
        assert!(matches!(
            p2.handle_sse("stream_handoff", "{}").as_slice(),
            [ParsedItem::Done]
        ));
        // Token dari payload handoff sendiri.
        let mut p3 = V1Parser::new();
        let items = p3.handle_sse("stream_handoff", "{\"resume_token\":\"tok-x\"}");
        assert!(matches!(
            items.as_slice(),
            [ParsedItem::Handoff { resume_token }] if resume_token == "tok-x"
        ));
    }

    #[test]
    fn envelope_construction() {
        let wire = vec![
            WireMessage {
                role: Role::System,
                text: "Be helpful.".to_string(),
                attachments: Vec::new(),
            },
            WireMessage {
                role: Role::User,
                text: "Hi".to_string(),
                attachments: Vec::new(),
            },
            WireMessage {
                role: Role::Assistant,
                text: "Hello!".to_string(),
                attachments: Vec::new(),
            },
        ];
        let body =
            build_conversation_body(&wire, "auto", None, Search::Auto, 420, Some("Asia/Jakarta"));
        assert_eq!(body["action"], "next");
        assert_eq!(body["model"], "auto");
        assert_eq!(body["supported_encodings"], json!(["v1"]));
        assert_eq!(body["conversation_mode"]["kind"], "primary_assistant");
        assert_eq!(body["paragen_cot_summary_display_override"], "allow");
        assert_eq!(body["force_parallel_switch"], "auto");
        assert_eq!(body["timezone_offset_min"], 420);
        assert_eq!(body["timezone"], "Asia/Jakarta");
        assert_eq!(body["system_hints"], json!(["Be helpful."]));
        // Pesan sistem dilipat ke system_hints, bukan messages.
        assert_eq!(
            body["messages"].as_array().expect("messages array").len(),
            2
        );
        assert_eq!(body["messages"][0]["author"]["role"], "user");
        assert_eq!(body["messages"][0]["content"]["content_type"], "text");
        assert!(body.get("conversation_id").is_none());
        assert!(body.get("thinking_effort").is_none());
        // Effort hadir bila diminta.
        let body2 = build_conversation_body(&wire, "auto", Some("high"), Search::Auto, 0, None);
        assert_eq!(body2["thinking_effort"], "high");
        // Search::On menambahkan instruksi search ke system_hints.
        let body3 = build_conversation_body(&wire, "auto", None, Search::On, 0, None);
        let hints = body3["system_hints"].as_array().expect("hints array");
        assert!(hints
            .iter()
            .any(|h| h.as_str().unwrap_or("").contains("web search")));
    }

    #[test]
    fn envelope_multimodal_attachment() {
        let wire = vec![WireMessage {
            role: Role::User,
            text: "What is this?".to_string(),
            attachments: vec![AttachmentRef {
                id: "file_123".to_string(),
                name: "a.png".to_string(),
                size: 42,
                mime: "image/png".to_string(),
            }],
        }];
        let body = build_conversation_body(&wire, "auto", None, Search::Auto, 0, None);
        let content = &body["messages"][0]["content"];
        assert_eq!(content["content_type"], "multimodal_text");
        assert_eq!(content["parts"][0]["content_type"], "image_asset_pointer");
        assert_eq!(
            content["parts"][0]["asset_pointer"],
            "file-service://file_123"
        );
        assert_eq!(content["parts"][1], "What is this?");
        assert_eq!(
            body["messages"][0]["metadata"]["attachments"][0]["id"],
            "file_123"
        );
    }

    #[test]
    fn resolve_requested_model_behavior() {
        let catalog = vec![
            ModelInfo::new("gpt-5", "openai"),
            ModelInfo::new("auto", "openai"),
        ];
        assert_eq!(
            resolve_requested_model("gpt-5", &catalog),
            ("gpt-5".to_string(), None)
        );
        assert_eq!(
            resolve_requested_model("chatgpt-auto", &catalog),
            ("chatgpt-auto".to_string(), None)
        );
        assert_eq!(
            resolve_requested_model("", &catalog).0,
            "chatgpt-auto".to_string()
        );
        let (m, w) = resolve_requested_model("nope", &catalog);
        assert_eq!(m, "chatgpt-auto");
        assert!(w.is_some(), "model tak dikenal harus warning");
        let (m, w) = resolve_requested_model("gpt-5", &[]);
        assert_eq!(m, "chatgpt-auto");
        assert!(w.is_some(), "katalog kosong harus warning");
        assert_eq!(upstream_model_slug("chatgpt-auto"), "auto");
        assert_eq!(upstream_model_slug("gpt-5"), "gpt-5");
    }

    #[test]
    fn thinking_resolution() {
        let meta = vec![
            ModelMeta {
                slug: "gpt-5-6".to_string(),
                thinking_efforts: vec![
                    "none".to_string(),
                    "low".to_string(),
                    "medium".to_string(),
                    "high".to_string(),
                ],
            },
            ModelMeta {
                slug: "gpt-5-6-thinking".to_string(),
                thinking_efforts: vec![
                    "low".to_string(),
                    "medium".to_string(),
                    "high".to_string(),
                    "max".to_string(),
                ],
            },
        ];
        assert_eq!(
            resolve_thinking("gpt-5-6", Thinking::Auto, &meta),
            ("gpt-5-6".to_string(), None)
        );
        assert_eq!(
            resolve_thinking("gpt-5-6", Thinking::Off, &meta),
            ("gpt-5-6".to_string(), Some("none".to_string()))
        );
        assert_eq!(
            resolve_thinking("gpt-5-6", Thinking::On { level: Some(2) }, &meta),
            ("gpt-5-6-thinking".to_string(), Some("medium".to_string()))
        );
        assert_eq!(
            resolve_thinking("gpt-5-6", Thinking::On { level: None }, &meta),
            ("gpt-5-6-thinking".to_string(), None)
        );
        // Level di atas max dipetakan ke "max".
        assert_eq!(
            resolve_thinking("gpt-5-6", Thinking::On { level: Some(9) }, &meta),
            ("gpt-5-6-thinking".to_string(), Some("max".to_string()))
        );
        // Model tak dikenal: effort dikirim apa adanya, tanpa varian.
        assert_eq!(
            resolve_thinking("misteri", Thinking::On { level: Some(1) }, &meta),
            ("misteri".to_string(), Some("low".to_string()))
        );
        assert_eq!(
            resolve_thinking("misteri", Thinking::Off, &meta),
            ("misteri".to_string(), None)
        );
    }

    #[test]
    fn parse_models_response_parses_fixture() {
        let v = json!({ "models": [
            { "slug": "gpt-5", "thinking_efforts": ["low", "high"] },
            { "id": "auto" },
            { "slug": "" },
            { "nope": true },
        ]});
        let list = parse_models_response(&v);
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].0.id, "gpt-5");
        assert_eq!(
            list[0].1.thinking_efforts,
            vec!["high".to_string(), "low".to_string()]
        );
        assert_eq!(list[1].0.id, "auto");
        assert!(parse_models_response(&json!({})).is_empty());
        assert!(parse_models_response(&json!({ "data": [{ "model": "m1" }] })).len() == 1);
    }

    #[test]
    fn file_upload_payload_and_image_names() {
        let p = build_file_upload_payload("foto.png", 1234);
        assert_eq!(p["file_name"], "foto.png");
        assert_eq!(p["file_size"], 1234);
        assert_eq!(p["use_case"], "multimodal");
        assert_eq!(
            image_file_name(Some("https://example.com/cat.png?x=1"), "image/png", 0),
            "cat.png"
        );
        assert_eq!(image_file_name(None, "image/jpeg", 3), "image-3.jpg");
        assert_eq!(
            image_file_name(Some("https://example.com/"), "image/png", 0),
            "image-0.png"
        );
    }

    #[test]
    fn models_include_web_default_alias_without_network() {
        let p = ChatGptProvider::new("__Secure-next-auth.session-token=fixture".to_string());
        assert!(p.models().iter().any(|m| m.id == "chatgpt-auto"));
        assert_eq!(p.web_default_model(), "chatgpt-auto");
    }

    #[test]
    fn oversized_wire_estimate() {
        let big = "x".repeat(200_000);
        let wire = vec![WireMessage {
            role: Role::User,
            text: big,
            attachments: Vec::new(),
        }];
        let body = build_conversation_body(&wire, "auto", None, Search::Auto, 0, None);
        assert!(estimate_wire_bytes(&body) > OVERSIZED_WIRE_BYTES);
    }

    #[tokio::test]
    async fn attachment_cap_enforced_before_upload() {
        let p = ChatGptProvider::new("fixture".to_string());
        let mut parts = Vec::new();
        for _ in 0..11 {
            parts.push(Part::ImageUrl(ImageSource::DataUri {
                mime: "image/png".to_string(),
                base64: "aGVsbG8=".to_string(),
            }));
        }
        let input = ChatInput {
            messages: vec![Message {
                role: Role::User,
                parts,
            }],
            model: "auto".to_string(),
            thinking: Thinking::Auto,
            search: Search::Auto,
            stream: false,
        };
        // Cap dicek SEBELUM upload apa pun: tidak ada jaringan di sini.
        let err = p
            .inner
            .build_wire_messages(&input)
            .await
            .expect_err("11 lampiran harus ditolak");
        assert!(matches!(err, ProviderError::InvalidInput(_)));
    }
}
