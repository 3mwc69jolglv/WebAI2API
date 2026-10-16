//! Provider Grok via antarmuka web grok.com.
//!
//! Autentikasi: cookie `sso` (dan `sso-rw`) dari browser yang login,
//! dikelola lewat [`CookieJar`] (rotasi via `Set-Cookie` upstream).
//! Alur: pemanasan `GET grok.com/` (kumpulkan `grok_device_id`,
//! `__cf_bm`, `cf_clearance`) lalu `POST
//! /rest/app-chat/conversations/new` dengan header `x-statsig-id`
//! (signature anti-bot; di-refresh otomatis bila 403 code 7 "page is
//! out of date"). Respons NDJSON -> event via [`ndjson::NdjsonParser`].
//!
//! Submodul: [`payload`] (konstruksi body chat) dan [`ndjson`]
//! (parser stream). File ini memegang transport/auth/katalog/upload.

mod ndjson;
mod payload;

use crate::core::auth::{persist_jar_best_effort, CookieJar};
use crate::core::catalog::{resolve_model, ModelCatalog};
use crate::core::events::EventStream;
use crate::core::media::{download_image_source, ImageInput, MAX_IMAGE_DOWNLOAD_BYTES};
use crate::core::retry::{retry_after_delay, Pacer};
use crate::error::ProviderError;
use crate::openai::ModelInfo;
use crate::providers::{
    http_client, ChatInput, GeneratedImage, ImageSource, Part, Provider, Thinking, CHROME_UA,
};
use async_stream::stream;
use base64::Engine as _;
use futures::StreamExt;
use ndjson::NdjsonParser;
use payload::UploadedFile;
use regex::Regex;
use reqwest::header::{HeaderMap, HeaderValue};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use uuid::Uuid;

/// Endpoint percakapan baru Grok (lane REST).
const CONVERSATIONS_NEW_URL: &str = "https://grok.com/rest/app-chat/conversations/new";
/// Endpoint katalog mode Grok.
const MODES_URL: &str = "https://grok.com/rest/modes";
/// Endpoint upload file Grok (saat ini).
const UPLOAD_URL: &str = "https://grok.com/rest/app-chat/upload-file-v2/direct";
/// Halaman utama Grok (pemanasan cookie).
const GROK_HOME_URL: &str = "https://grok.com/";
/// Halaman index Grok (scrape meta untuk signer eksternal).
const GROK_INDEX_URL: &str = "https://grok.com/index";
/// TTL cache katalog model (1 jam).
const CATALOG_TTL: Duration = Duration::from_secs(3600);

/// Provider Grok via antarmuka web grok.com.
pub struct GrokProvider {
    /// Nilai cookie `sso` (dipakai cek kosong; nilai penuh di `jar`).
    sso: String,
    client: reqwest::Client,
    /// Cookie jar: `sso`/`sso-rw` + hasil pemanasan/rotasi.
    jar: RwLock<CookieJar>,
    /// Benar bila pemanasan sudah pernah berhasil.
    warmed: RwLock<bool>,
    /// Cache (x-statsig-id, waktu diambil). Kedaluwarsa 1 jam.
    statsig: RwLock<Option<(String, Instant)>>,
    /// Katalog model: live dari `/rest/modes` bila segar, ditambah
    /// alias default web `"grok-auto"` (lihat [`GrokProvider::models`]).
    /// TIDAK ADA daftar tier fallback: katalog kosong berarti hanya
    /// alias yang terekspos sampai fetch live pertama berhasil.
    catalog: std::sync::RwLock<ModelCatalog>,
    /// Pacing antar-request (0ms untuk Grok — mekanisme tersedia,
    /// interval minimum 0; hanya Claude yang memakai lantai 1500ms).
    pacer: Pacer,
    /// File persistensi rotasi cookie (env `AIW2A_GROK_COOKIES_FILE`,
    /// opsional; `None` = rotasi hanya in-memory).
    persist_path: Option<PathBuf>,
}

impl GrokProvider {
    pub fn new(sso: String) -> Self {
        let sso = sso.trim().to_string();
        let mut jar = CookieJar::new();
        jar.set("sso", &sso);
        jar.set("sso-rw", &sso);
        let catalog = ModelCatalog::new(CATALOG_TTL);
        let persist_path = std::env::var("AIW2A_GROK_COOKIES_FILE")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .map(PathBuf::from);
        Self {
            sso,
            client: http_client(),
            jar: RwLock::new(jar),
            warmed: RwLock::new(false),
            statsig: RwLock::new(None),
            catalog: std::sync::RwLock::new(catalog),
            pacer: Pacer::new(Duration::from_millis(0)),
            persist_path,
        }
    }

    /// Header statis tanpa cookie (untuk pemanasan & scrape meta).
    fn static_headers() -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            "accept",
            HeaderValue::from_static("text/html,application/xhtml+xml"),
        );
        h.insert(
            "accept-language",
            HeaderValue::from_static("en-US,en;q=0.9"),
        );
        h.insert("user-agent", HeaderValue::from_static(CHROME_UA));
        h
    }

    /// Header dasar request API Grok (dengan cookie dari jar).
    fn base_headers(&self, cookie: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("accept", HeaderValue::from_static("*/*"));
        h.insert(
            "accept-language",
            HeaderValue::from_static("en-US,en;q=0.9"),
        );
        h.insert("content-type", HeaderValue::from_static("application/json"));
        h.insert("origin", HeaderValue::from_static("https://grok.com"));
        h.insert("referer", HeaderValue::from_static("https://grok.com/"));
        h.insert("user-agent", HeaderValue::from_static(CHROME_UA));
        if let Ok(v) = HeaderValue::from_str(cookie) {
            h.insert("cookie", v);
        }
        h.insert("cache-control", HeaderValue::from_static("no-cache"));
        h.insert("pragma", HeaderValue::from_static("no-cache"));
        h.insert("priority", HeaderValue::from_static("u=1, i"));
        h.insert("sec-fetch-dest", HeaderValue::from_static("empty"));
        h.insert("sec-fetch-mode", HeaderValue::from_static("cors"));
        h.insert("sec-fetch-site", HeaderValue::from_static("same-origin"));
        h
    }

    /// Nilai header `Cookie` saat ini dari jar.
    async fn cookie_header(&self) -> String {
        self.jar.read().await.render()
    }

    /// Merge header `Set-Cookie` dari satu respons ke jar (rotasi)
    /// lalu persist best-effort (tidak pernah menggagalkan request,
    /// tidak pernah me-log nilai cookie).
    /// `Set-Cookie` kosong untuk `sso`/`sso-rw` diabaikan agar tidak
    /// menimpa SSO yang valid dengan nilai kosong.
    async fn merge_response_cookies(&self, headers: &HeaderMap) {
        let mut jar = self.jar.write().await;
        for val in headers.get_all("set-cookie") {
            let Ok(s) = val.to_str() else {
                continue;
            };
            if let Some((name, value)) = s.split(';').next().and_then(|p| p.split_once('=')) {
                if (name.trim() == "sso" || name.trim() == "sso-rw") && value.trim().is_empty() {
                    continue;
                }
            }
            jar.merge_set_cookie(s);
        }
        persist_jar_best_effort(&jar, self.persist_path.as_deref());
    }

    /// Reset sesi transport setelah 403 non-stale: buang signature
    /// statsig + cookie pemanasan (`grok_device_id`, `__cf_bm`,
    /// `cf_clearance`) agar request berikutnya pemanasan ulang.
    async fn reset_transport_session(&self) {
        *self.statsig.write().await = None;
        {
            let mut jar = self.jar.write().await;
            for name in ["grok_device_id", "__cf_bm", "cf_clearance"] {
                jar.remove(name);
            }
        }
        *self.warmed.write().await = false;
    }

    /// Pemanasan: `GET grok.com/` lalu simpan cookie tambahan (sekali
    /// saja sampai reset). Gagal pemanasan tidak fatal: request
    /// berikutnya mencoba lagi.
    async fn warm(&self) {
        if *self.warmed.read().await {
            return;
        }
        let resp = match self
            .client
            .get(GROK_HOME_URL)
            .headers(Self::static_headers())
            .send()
            .await
        {
            Ok(r) => r,
            Err(_) => return,
        };
        self.merge_response_cookies(resp.headers()).await;
        *self.warmed.write().await = true;
    }

    /// Header lengkap satu request API: pemanasan + cookie + statsig
    /// (best-effort) + id request unik.
    async fn headers_for_request(&self) -> HeaderMap {
        self.warm().await;
        let cookie = self.cookie_header().await;
        let mut headers = self.base_headers(&cookie);
        // x-statsig-id: best-effort — kalau gagal, coba tanpa signature.
        if let Ok(Some(sig)) = self.statsig_id().await {
            if let Ok(v) = HeaderValue::from_str(&sig) {
                headers.insert("x-statsig-id", v);
            }
        }
        if let Ok(v) = HeaderValue::from_str(&Uuid::new_v4().to_string()) {
            headers.insert("x-xai-request-id", v);
        }
        headers
    }

    /// Ambil (atau pakai cache) signature x-statsig-id.
    ///
    /// Mode default: generate lokal memakai format fallback fetch interceptor
    /// browser grok.com — `btoa("x1:" + error)` saat evaluasi Statsig gagal
    /// (grok2api PR #567, divalidasi 200 dengan 3 SSO token). Tidak ada lagi
    /// ketergantungan ke signer komunitas pihak ketiga (SPOF + data keluar).
    /// Env `AIW2A_GROK_STATSIG_SIGNER`: isi dengan URL signer eksternal
    /// (mis. self-hosted) untuk memakai alur lama; kosongkan eksplisit
    /// (`""`) untuk menonaktifkan signing. Cache 1 jam.
    async fn statsig_id(&self) -> Result<Option<String>, ProviderError> {
        {
            let g = self.statsig.read().await;
            if let Some((sig, at)) = g.clone() {
                if at.elapsed() < Duration::from_secs(3600) {
                    return Ok(Some(sig));
                }
            }
        }
        let signer_override = std::env::var("AIW2A_GROK_STATSIG_SIGNER").ok();
        let sig = match signer_override.as_deref().map(str::trim) {
            // Eksplisit dikosongkan = signing dimatikan.
            Some("") => return Ok(None),
            // URL diisi = signer eksternal (self-hosted) — alur lama.
            Some(url) => self.sign_via_remote(url).await?,
            // Tidak diset = generate lokal (default).
            None => local_statsig_id(),
        };
        *self.statsig.write().await = Some((sig.clone(), Instant::now()));
        Ok(Some(sig))
    }

    /// Minta signature ke signer eksternal (alur lama: scrape meta
    /// grok-site-verification lalu POST ke signer). Dipakai hanya bila
    /// `AIW2A_GROK_STATSIG_SIGNER` diisi URL.
    async fn sign_via_remote(&self, signer_url: &str) -> Result<String, ProviderError> {
        let cookie = self.cookie_header().await;
        let html = self
            .client
            .get(GROK_INDEX_URL)
            .header("cookie", &cookie)
            .headers(Self::static_headers())
            .send()
            .await
            .map_err(|e| ProviderError::Network(e.to_string()))?
            .text()
            .await
            .map_err(|e| ProviderError::Network(e.to_string()))?;
        let meta = extract_site_verification(&html).ok_or_else(|| {
            ProviderError::Parse(
                "tidak menemukan meta grok-site-verification — halaman grok.com mungkin berubah"
                    .to_string(),
            )
        })?;
        let resp = self
            .client
            .post(signer_url)
            .json(&json!({
                "method": "POST",
                "path": "/rest/app-chat/conversations/new",
                "environment": { "metaContent": meta },
            }))
            .send()
            .await
            .map_err(|e| {
                ProviderError::Network(format!("gagal menghubungi statsig signer: {e}"))
            })?;
        if !resp.status().is_success() {
            return Err(ProviderError::Upstream {
                status: resp.status().as_u16(),
                body_snippet: "statsig signer menolak request".to_string(),
            });
        }
        let v: Value = resp
            .json()
            .await
            .map_err(|e| ProviderError::Parse(format!("respons signer bukan JSON: {e}")))?;
        let sig = v
            .get("x-statsig-id")
            .and_then(|s| s.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                ProviderError::Parse("signer tidak mengembalikan x-statsig-id".to_string())
            })?;
        // Validasi: harus decode base64 menjadi tepat 70 byte (dari grok2api).
        let valid = base64::engine::general_purpose::STANDARD
            .decode(sig)
            .map(|b| b.len() == 70)
            .unwrap_or(false);
        if !valid {
            return Err(ProviderError::Parse(
                "x-statsig-id dari signer tidak valid".to_string(),
            ));
        }
        Ok(sig.to_string())
    }

    /// POST `conversations/new` sekali jalan (tanpa handling 403).
    async fn post_new_once(&self, body: &Value) -> Result<reqwest::Response, ProviderError> {
        let headers = self.headers_for_request().await;
        let resp = self
            .client
            .post(CONVERSATIONS_NEW_URL)
            .headers(headers)
            .json(body)
            .send()
            .await
            .map_err(|e| ProviderError::Network(e.to_string()))?;
        self.merge_response_cookies(resp.headers()).await;
        Ok(resp)
    }

    /// POST dengan penanganan 403 code 7: refresh statsig lalu replay
    /// sekali. Replay ini aman: 403 berarti request DITOLAK sebelum
    /// disubmit — tidak ada generasi yang ter-submit ganda (aturan
    /// "jangan pernah me-replay generasi yang sudah disubmit").
    /// 403 lain -> reset sesi transport + error Upstream, tanpa retry.
    async fn post_conversation(&self, body: &Value) -> Result<reqwest::Response, ProviderError> {
        let resp = self.post_new_once(body).await?;
        if resp.status().as_u16() == 403 {
            let text = resp.text().await.unwrap_or_default();
            if is_statsig_refreshable(&text) {
                // Buang signature basi, minta yang baru, ulangi sekali.
                *self.statsig.write().await = None;
                let retry = self.post_new_once(body).await?;
                let rs = retry.status().as_u16();
                if rs != 200 {
                    let retry_after = retry_after_of(&retry);
                    let t2 = retry.text().await.unwrap_or_default();
                    self.reset_transport_session().await;
                    return Err(map_http_error(rs, &t2, retry_after.as_deref()));
                }
                return Ok(retry);
            }
            self.reset_transport_session().await;
            return Err(ProviderError::upstream(403, &text));
        }
        Ok(resp)
    }

    /// Ambil katalog mode live dari `GET /rest/modes`.
    ///
    /// Gagal upstream (jaringan / status != 200 / bentuk tak dikenali)
    /// -> vec kosong + warning di log (TIDAK ada fabrikasi model).
    async fn fetch_modes(&self) -> Vec<ModelInfo> {
        let headers = self.headers_for_request().await;
        let resp = match self.client.get(MODES_URL).headers(headers).send().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("grok GET /rest/modes gagal jaringan: {e}");
                return Vec::new();
            }
        };
        self.merge_response_cookies(resp.headers()).await;
        let status = resp.status().as_u16();
        if status != 200 {
            tracing::warn!("grok GET /rest/modes -> HTTP {status} — katalog live tidak tersedia");
            return Vec::new();
        }
        let text = match resp.text().await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!("grok GET /rest/modes: gagal baca body: {e}");
                return Vec::new();
            }
        };
        let v: Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(_) => {
                tracing::warn!("grok GET /rest/modes: respons bukan JSON — bentuk mungkin berubah");
                return Vec::new();
            }
        };
        let modes = parse_modes_response(&v);
        if modes.is_empty() {
            tracing::warn!("grok GET /rest/modes: tidak ada mode yang dikenali dari respons");
        }
        modes
    }

    /// Refresh katalog bila basi (TTL 1 jam). Mode live dari
    /// `/rest/modes` adalah otoritatif bila berhasil; gagal refresh
    /// -> katalog lama dipertahankan + warning (chat tidak gagal
    /// hanya karena katalog). TIDAK ADA daftar fallback: sebelum
    /// fetch live pertama yang sukses, hanya alias `"grok-auto"`
    /// yang terekspos (lihat [`GrokProvider::models`]).
    async fn refresh_catalog(&self) {
        let fresh = match self.catalog.read() {
            Ok(g) => g.is_fresh(),
            Err(poison) => poison.into_inner().is_fresh(),
        };
        if fresh {
            return;
        }
        let live = self.fetch_modes().await;
        if live.is_empty() {
            tracing::warn!(
                "grok: katalog live gagal diambil — memakai katalog lama / alias default web"
            );
            return;
        }
        match self.catalog.write() {
            Ok(mut g) => g.update(live),
            Err(poison) => poison.into_inner().update(live),
        }
    }

    /// Upload satu gambar (URL / data URI) ke Grok, kembalikan
    /// referensinya. Gagal upload -> error Upstream yang jelas (chat
    /// tidak dilanjutkan tanpa lampiran — diam-diam menghilangkan
    /// gambar akan mengubah makna request).
    ///
    /// Gambar dikirim sebagai byte multipart — TIDAK PERNAH base64
    /// mentah tanpa wrapper Data-URI (Grok secara eksplisit gagal
    /// untuk base64 mentah). Data URI di-decode dulu lewat
    /// `core::media`, URL diunduh lewat sana juga.
    async fn upload_image(&self, src: &ImageSource) -> Result<UploadedFile, ProviderError> {
        let input = match src {
            ImageSource::Url(url) => ImageInput::Url(url.clone()),
            ImageSource::DataUri { mime, base64 } => ImageInput::DataUri {
                mime: mime.clone(),
                base64: base64.clone(),
            },
        };
        let downloaded = download_image_source(&input).await?;
        let file_name = file_name_for(src, &downloaded.mime);
        let boundary = format!("----aiweb2api-{}", Uuid::new_v4().simple());
        let body = build_multipart_body(&boundary, &file_name, &downloaded.mime, &downloaded.bytes);
        let mut headers = self.headers_for_request().await;
        // Timpa content-type JSON dari headers_for_request -> multipart.
        if let Ok(v) = HeaderValue::from_str(&format!("multipart/form-data; boundary={boundary}")) {
            headers.insert("content-type", v);
        }
        let resp = self
            .client
            .post(UPLOAD_URL)
            .headers(headers)
            .body(body)
            .send()
            .await
            .map_err(|e| ProviderError::Network(e.to_string()))?;
        self.merge_response_cookies(resp.headers()).await;
        let status = resp.status().as_u16();
        let retry_after = retry_after_of(&resp);
        let text = resp.text().await.unwrap_or_default();
        if status != 200 {
            return Err(map_http_error(status, &text, retry_after.as_deref()));
        }
        let v: Value = serde_json::from_str(&text)
            .map_err(|e| ProviderError::Parse(format!("respons upload Grok bukan JSON: {e}")))?;
        let Some(id) = find_file_metadata_id(&v) else {
            return Err(ProviderError::Parse(
                "upload Grok tidak mengembalikan fileMetadataId — bentuk respons mungkin berubah"
                    .to_string(),
            ));
        };
        Ok(UploadedFile {
            file_metadata_id: id,
            file_name,
            mime: downloaded.mime,
        })
    }
}

/// Nilai header `Retry-After` dari respons, bila ada.
fn retry_after_of(resp: &reqwest::Response) -> Option<String> {
    resp.headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

/// Petakan status HTTP error Grok ke [`ProviderError`]:
/// 429 -> [`ProviderError::RateLimited`] (dengan cooldown via
/// `Retry-After` bila ada), 401 -> [`ProviderError::AuthExpired`],
/// sisanya -> Upstream (status + snippet tersanitasi).
fn map_http_error(status: u16, body: &str, retry_after: Option<&str>) -> ProviderError {
    match status {
        401 => ProviderError::AuthExpired(
            "grok.com menolak SSO (401) — cookie kedaluwarsa atau tidak valid".to_string(),
        ),
        429 => ProviderError::RateLimited {
            retry_after: retry_after_delay(retry_after).map(|d| d.as_secs()),
            message: "grok.com rate limit (429) — coba lagi nanti".to_string(),
        },
        _ => ProviderError::upstream(status, body),
    }
}

/// Apakah body 403 ini "stale page" yang bisa pulih via refresh statsig?
/// Bukan blokir akun permanen (blocked-user) — itu terminal.
fn is_statsig_refreshable(text: &str) -> bool {
    let t = text.to_lowercase();
    if t.contains("blocked-user") || t.contains("user is blocked") {
        return false;
    }
    t.contains("\"code\":7")
        || t.contains("\"code\":\"7\"")
        || t.contains("out of date")
        || t.contains("reload to continue")
        || t.contains("anti-bot")
}

/// Ambil isi `<meta name="grok-site-verification" content="...">`.
///
/// CATATAN: nama meta di halaman asli memakai U+2015 HORIZONTAL BAR
/// ("grok-site―verification"), bukan hyphen ASCII biasa — regex di bawah
/// menerima keduanya.
///
/// Dipakai hanya oleh alur signer eksternal (`sign_via_remote`); mode
/// default (generate lokal) tidak membutuhkan meta ini.
fn extract_site_verification(html: &str) -> Option<String> {
    // Cari tag meta yang mengandung "grok-site" + "verification" lalu ambil content-nya.
    let re = Regex::new(r#"<meta[^>]*grok-site.verification[^>]*>"#).ok()?;
    let tag = re.find(html)?.as_str();
    let re_content = Regex::new(r#"content=["']([^"']+)["']"#).ok()?;
    re_content
        .captures(tag)
        .and_then(|c| c.get(1).map(|m| m.as_str().to_string()))
}

/// Generate `x-statsig-id` secara lokal mengikuti format fallback fetch
/// interceptor browser grok.com: `btoa("x1:" + error)` saat evaluasi
/// Statsig SDK gagal (grok2api PR #567 — divalidasi 200 memakai 3 SSO token).
///
/// Pesan error divariasi setiap generate agar tidak menjadi fingerprint statis.
fn local_statsig_id() -> String {
    use rand::Rng as _;
    let mut rng = rand::thread_rng();
    let msg = if rng.gen_bool(0.5) {
        let rand_str: String = (0..5)
            .map(|_| {
                const POOL: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
                POOL[rng.gen_range(0..POOL.len())] as char
            })
            .collect();
        format!("x1:TypeError: Cannot read properties of null (reading 'children['{rand_str}']')")
    } else {
        let rand_str: String = (0..10)
            .map(|_| rng.gen_range(b'a'..=b'z') as char)
            .collect();
        format!("x1:TypeError: Cannot read properties of undefined (reading '{rand_str}')")
    };
    base64::engine::general_purpose::STANDARD.encode(msg.as_bytes())
}

/// Parse respons `GET /rest/modes` menjadi daftar model.
///
/// Bentuk ditoleransi (belum terverifikasi live — endpoint ini belum
/// pernah dipanggil dari kode ini): array string, array objek
/// (`id` / `modeId` / `mode` / `name` / `slug`), atau objek dengan
/// array di `modes` / `data` / `results` (atau objek `modes` yang
/// nilainya objek-objek mode). Entri kosong / duplikat dibuang.
fn parse_modes_response(v: &Value) -> Vec<ModelInfo> {
    let mut out: Vec<ModelInfo> = Vec::new();
    let push_item = |item: &Value, out: &mut Vec<ModelInfo>| {
        let id = match item {
            Value::String(s) => Some(s.as_str()),
            Value::Object(_) => ["id", "modeId", "mode", "name", "slug"]
                .iter()
                .find_map(|k| item.get(k).and_then(|x| x.as_str())),
            _ => None,
        };
        if let Some(id) = id {
            let id = id.trim();
            if !id.is_empty() && !out.iter().any(|m| m.id == id) {
                out.push(ModelInfo::new(id, "xai"));
            }
        }
    };
    match v {
        Value::Array(items) => {
            for item in items {
                push_item(item, &mut out);
            }
        }
        Value::Object(_) => {
            for key in ["modes", "data", "results"] {
                match v.get(key) {
                    Some(Value::Array(items)) => {
                        for item in items {
                            push_item(item, &mut out);
                        }
                    }
                    // `modes` sebagai map: kunci = id mode.
                    Some(Value::Object(map)) if key == "modes" => {
                        for (id, _) in map {
                            push_item(&Value::String(id.clone()), &mut out);
                        }
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
    out
}

/// Cari `fileMetadataId` (atau `file_metadata_id`) secara rekursif
/// di respons upload.
fn find_file_metadata_id(v: &Value) -> Option<String> {
    match v {
        Value::Object(m) => {
            for key in ["fileMetadataId", "file_metadata_id"] {
                if let Some(s) = m.get(key).and_then(|x| x.as_str()) {
                    return Some(s.to_string());
                }
            }
            for (_, x) in m {
                if let Some(found) = find_file_metadata_id(x) {
                    return Some(found);
                }
            }
            None
        }
        Value::Array(a) => {
            for x in a {
                if let Some(found) = find_file_metadata_id(x) {
                    return Some(found);
                }
            }
            None
        }
        _ => None,
    }
}

/// Ekstensi file dari MIME gambar.
fn mime_to_ext(mime: &str) -> &'static str {
    match mime {
        "image/png" => "png",
        "image/jpeg" => "jpg",
        "image/webp" => "webp",
        "image/gif" => "gif",
        _ => "bin",
    }
}

/// Nama file untuk upload: basename URL bila ada, kalau tidak
/// `image.<ext>`. Disanitasi untuk header multipart (tanpa `"`,
/// CR, LF).
fn file_name_for(src: &ImageSource, mime: &str) -> String {
    let ext = mime_to_ext(mime);
    let base = match src {
        ImageSource::Url(url) => url
            .split('?')
            .next()
            .unwrap_or("")
            .rsplit('/')
            .next()
            .unwrap_or(""),
        ImageSource::DataUri { .. } => "",
    };
    let mut name = if base.is_empty() {
        format!("image.{ext}")
    } else if base.contains('.') {
        base.to_string()
    } else {
        format!("{base}.{ext}")
    };
    name.retain(|c| c != '"' && c != '\r' && c != '\n');
    if name.is_empty() {
        name = format!("image.{ext}");
    }
    name
}

/// Bangun body multipart/form-data satu file (`name="file"`).
/// Dibangun manual agar tidak perlu fitur `multipart` reqwest.
fn build_multipart_body(boundary: &str, file_name: &str, mime: &str, bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{file_name}\"\r\nContent-Type: {mime}\r\n\r\n"
        )
        .as_bytes(),
    );
    out.extend_from_slice(bytes);
    out.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    out
}

#[async_trait::async_trait]
impl Provider for GrokProvider {
    fn id(&self) -> &'static str {
        "grok"
    }

    fn web_default_model(&self) -> &'static str {
        "grok-auto"
    }

    fn models(&self) -> Vec<ModelInfo> {
        let mut out = match self.catalog.read() {
            Ok(g) => g.models().to_vec(),
            Err(poison) => poison.into_inner().models().to_vec(),
        };
        // Alias default web SELALU ada agar routing
        // `Registry::find_model("grok-auto")` tetap jalan sebelum
        // fetch live pertama berhasil. Ini satu-satunya entri
        // non-live — BUKAN daftar model statis.
        if !out.iter().any(|m| m.id == "grok-auto") {
            out.push(ModelInfo::new("grok-auto", "xai"));
        }
        out
    }

    async fn warm_catalog(&self) -> Result<(), ProviderError> {
        if self.sso.is_empty() {
            // Tanpa jaringan: provider belum dikonfigurasi.
            return Err(ProviderError::Unauthorized(
                "AIW2A_GROK_SSO belum diisi".to_string(),
            ));
        }
        let live = self.fetch_modes().await;
        if live.is_empty() {
            return Err(ProviderError::upstream(
                502,
                "grok GET /rest/modes gagal / tidak dikenali — katalog live tidak tersedia",
            ));
        }
        match self.catalog.write() {
            Ok(mut g) => g.update(live),
            Err(poison) => poison.into_inner().update(live),
        }
        Ok(())
    }

    async fn chat(&self, input: &ChatInput) -> Result<EventStream, ProviderError> {
        if self.sso.is_empty() {
            return Err(ProviderError::Unauthorized(
                "AIW2A_GROK_SSO belum diisi".to_string(),
            ));
        }
        // Pacing antar-request (0ms untuk Grok — mekanisme tersedia).
        self.pacer.wait().await;

        // Katalog: refresh bila basi (best-effort), lalu resolve model.
        // Model tak dikenal -> web default + warning (tidak gagal).
        self.refresh_catalog().await;
        let catalog_snapshot = self.models();
        let (model, warning) = resolve_model(Some(&input.model), &catalog_snapshot, "grok-auto");
        if let Some(w) = warning {
            tracing::warn!("{w}");
        }

        // Histori -> satu transkrip `message` (stateless replay;
        // lihat payload::build_message). Gambar di-upload dulu.
        let message = payload::build_message(&input.messages);
        let mut uploads: Vec<UploadedFile> = Vec::new();
        for m in &input.messages {
            for part in &m.parts {
                if let Part::ImageUrl(src) = part {
                    uploads.push(self.upload_image(src).await?);
                }
            }
        }

        let mut body = json!({
            "temporary": true,
            "modelName": model,
            "message": message,
            "fileAttachments": payload::build_file_attachments(&uploads),
            "imageAttachments": [],
            "enableImageGeneration": false,
            "returnImageBytes": false,
            "returnRawGrokInXaiRequest": false,
            "enableImageStreaming": false,
            "imageGenerationCount": 2,
            "forceOnce": false,
            "isPreset": false,
            "sendFinalMetadata": true,
            "customInstructions": "",
        });
        if let Some(obj) = body.as_object_mut() {
            payload::apply_thinking(obj, input.thinking);
            payload::apply_search(obj, input.search);
            // Edit Imagine: gambar yang di-upload direferensikan lewat
            // mediaGenInput.imageToImage.inputAssets (riset §4a PR #940).
            // Hanya bila modelnya Imagine + ada gambar; chat teks biasa
            // memakai fileAttachments saja.
            if model.contains("imagine") && !uploads.is_empty() {
                let asset_ids: Vec<Value> =
                    uploads.iter().map(|u| json!(u.file_metadata_id)).collect();
                obj.insert("kind".to_string(), json!("CONVERSATION_KIND_IMAGINE"));
                obj.insert(
                    "mediaGenInput".to_string(),
                    json!({ "imageToImage": { "prompt": message, "inputAssets": asset_ids } }),
                );
            }
        }

        let resp = self.post_conversation(&body).await?;
        let status = resp.status().as_u16();
        if status != 200 {
            let retry_after = retry_after_of(&resp);
            let text = resp.text().await.unwrap_or_default();
            return Err(map_http_error(status, &text, retry_after.as_deref()));
        }

        let thinking_on = matches!(input.thinking, Thinking::On { .. });
        let prompt_chars = message.chars().count();
        let mut byte_stream = resp.bytes_stream();
        let s = stream! {
            let mut parser = NdjsonParser::new(prompt_chars);
            let mut buf = String::new();
            let mut failed = false;
            'outer: while let Some(chunk) = byte_stream.next().await {
                let bytes = match chunk {
                    Ok(b) => b,
                    Err(e) => {
                        yield Err(ProviderError::Network(e.to_string()));
                        failed = true;
                        break 'outer;
                    }
                };
                buf.push_str(&String::from_utf8_lossy(&bytes));
                while let Some(pos) = buf.find('\n') {
                    let line: String = buf.drain(..=pos).collect();
                    for item in parser.feed_line(&line) {
                        match item {
                            Ok(ev) => yield Ok(ev),
                            Err(e) => {
                                yield Err(e);
                                failed = true;
                                break 'outer;
                            }
                        }
                    }
                    if parser.is_finished() {
                        break 'outer;
                    }
                }
            }
            if !failed {
                // Stream "missing-thinking": jawaban terlihat tanpa
                // reasoning padahal thinking diminta -> kualitas
                // terdegradasi, BUKAN error fatal (riset §4c/§4e).
                if thinking_on && parser.saw_text() && !parser.saw_reasoning() {
                    tracing::warn!(
                        "aliran Grok terdegradasi (missing-thinking): jawaban terlihat tanpa reasoning \
                         padahal thinking diminta — stream diterima apa adanya"
                    );
                }
                if !parser.is_finished() {
                    for ev in parser.finish_events() {
                        yield Ok(ev);
                    }
                }
            }
        };
        Ok(Box::pin(s))
    }

    async fn generate_image(&self, prompt: &str) -> Result<GeneratedImage, ProviderError> {
        if self.sso.is_empty() {
            return Err(ProviderError::Unauthorized(
                "AIW2A_GROK_SSO belum diisi".to_string(),
            ));
        }
        // Imagine text-to-image via lane REST `conversations/new`
        // dengan `mediaGenInput` (riset §4a PR #991 untuk video,
        // #940 untuk edit — bentuk text-to-image dianalogikan dari
        // keduanya dan BELUM terverifikasi live; gating tier Imagine
        // — Basic vs berbayar — juga belum terverifikasi).
        // Jalur legacy `/rest/media/post/create` + `parentPostId`
        // DIHAPUS upstream dan TIDAK dipakai sebagai fallback.
        // Routing server (`/v1/images/generations`) meneruskan model
        // `grok-*` ke provider ini.
        let body = json!({
            "temporary": true,
            "modelName": "grok-imagine-image",
            "message": prompt,
            "fileAttachments": [],
            "imageAttachments": [],
            "disableSearch": true,
            "enableImageGeneration": true,
            "returnImageBytes": false,
            "returnRawGrokInXaiRequest": false,
            "enableImageStreaming": false,
            "imageGenerationCount": 1,
            "forceOnce": false,
            "isPreset": false,
            "sendFinalMetadata": true,
            "customInstructions": "",
            "kind": "CONVERSATION_KIND_IMAGINE",
            "mediaGenInput": { "textToImage": { "prompt": prompt } },
        });
        // Satu submit; replay hanya bila 403-code-7 (request ditolak
        // pra-submit) di post_conversation — TIDAK PERNAH me-replay
        // generasi yang sudah disubmit.
        let resp = self.post_conversation(&body).await?;
        let status = resp.status().as_u16();
        if status != 200 {
            let retry_after = retry_after_of(&resp);
            let text = resp.text().await.unwrap_or_default();
            return Err(map_http_error(status, &text, retry_after.as_deref()));
        }
        let text = resp.text().await.unwrap_or_default();
        let mut urls: Vec<String> = Vec::new();
        for line in text.lines() {
            if let Ok(v) = serde_json::from_str::<Value>(line.trim()) {
                ndjson::collect_image_urls(&v, &mut urls);
            }
        }
        let Some(url) = urls.into_iter().next() else {
            return Err(ProviderError::Parse(
                "tidak menemukan URL gambar di respons Imagine — mungkin kuota habis, \
                 prompt ditolak moderasi, atau bentuk respons berubah"
                    .to_string(),
            ));
        };
        // Unduh server-side (URL assets.grok.com terikat sesi).
        let mut dl_headers = HeaderMap::new();
        dl_headers.insert("user-agent", HeaderValue::from_static(CHROME_UA));
        dl_headers.insert("referer", HeaderValue::from_static("https://grok.com/"));
        if let Ok(v) = HeaderValue::from_str(&self.cookie_header().await) {
            dl_headers.insert("cookie", v);
        }
        let dl = self
            .client
            .get(&url)
            .headers(dl_headers)
            .send()
            .await
            .map_err(|e| ProviderError::Network(e.to_string()))?;
        let dl_status = dl.status().as_u16();
        if dl_status != 200 {
            return Err(ProviderError::Upstream {
                status: dl_status,
                body_snippet: format!("unduh gambar Imagine gagal (HTTP {dl_status})"),
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
        if bytes.len() > MAX_IMAGE_DOWNLOAD_BYTES {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::catalog::resolve_model;

    #[test]
    fn local_statsig_id_decodes_to_x1_prefix() {
        for _ in 0..5 {
            let sig = local_statsig_id();
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(&sig)
                .expect("x-statsig-id lokal harus base64 valid");
            let text = String::from_utf8(decoded).expect("payload harus UTF-8");
            assert!(
                text.starts_with("x1:"),
                "harus berprefix x1:, dapat: {text}"
            );
        }
    }

    #[test]
    fn statsig_refreshable_detection() {
        // 403 code 7 stale-page -> refresh.
        assert!(is_statsig_refreshable(
            r#"{"code":7,"message":"Request rejected by anti-bot rules."}"#
        ));
        assert!(is_statsig_refreshable(
            "page is out of date, reload to continue"
        ));
        assert!(is_statsig_refreshable("anti-bot check failed"));
        // Blokir akun permanen -> terminal, jangan refresh.
        assert!(!is_statsig_refreshable(
            r#"{"code":7,"message":"blocked-user"}"#
        ));
        assert!(!is_statsig_refreshable("user is blocked"));
        assert!(!is_statsig_refreshable(
            r#"{"code":403,"message":"forbidden"}"#
        ));
    }

    #[test]
    fn models_returns_web_default_alias_only_before_live_fetch() {
        let grok = GrokProvider::new("fixture".to_string());
        // Sebelum fetch live pertama: HANYA alias default web —
        // tidak ada daftar tier fallback yang di-hardcode.
        let models = grok.models();
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].id, "grok-auto");
        assert_eq!(grok.web_default_model(), "grok-auto");
    }

    #[tokio::test]
    async fn warm_catalog_without_sso_errors_without_network() {
        let grok = GrokProvider::new(String::new());
        assert!(grok.warm_catalog().await.is_err());
    }

    #[test]
    fn resolve_model_against_grok_catalog() {
        let grok = GrokProvider::new("fixture".to_string());
        let catalog = grok.models();
        // Alias default dikenal -> dipakai tanpa warning.
        let (model, warning) = resolve_model(Some("grok-auto"), &catalog, "grok-auto");
        assert_eq!(model, "grok-auto");
        assert!(warning.is_none());
        // Tak dikenal -> web default + warning.
        let (model, warning) = resolve_model(Some("grok-tidak-ada"), &catalog, "grok-auto");
        assert_eq!(model, "grok-auto");
        assert!(warning.is_some());
        // Katalog kosong -> web default + warning.
        let (model, warning) = resolve_model(Some("grok-4"), &[], "grok-auto");
        assert_eq!(model, "grok-auto");
        assert!(warning.is_some());
    }

    #[test]
    fn parse_modes_response_shapes() {
        // Array string.
        let v: Value = serde_json::from_str(r#"["auto","fast"]"#).expect("fixture");
        let modes = parse_modes_response(&v);
        assert_eq!(modes.len(), 2);
        assert_eq!(modes[0].id, "auto");
        // Objek {modes: [{id}, {modeId}]} + dedup.
        let v: Value =
            serde_json::from_str(r#"{"modes":[{"id":"auto"},{"modeId":"fast"},{"id":"auto"}]}"#)
                .expect("fixture");
        let modes = parse_modes_response(&v);
        assert_eq!(modes.len(), 2);
        assert_eq!(modes[1].id, "fast");
        // Bentuk tak dikenali -> kosong (penelepon memakai fallback).
        let v: Value = serde_json::from_str(r#"{"aneh":true}"#).expect("fixture");
        assert!(parse_modes_response(&v).is_empty());
        let v: Value = serde_json::from_str(r#"42"#).expect("fixture");
        assert!(parse_modes_response(&v).is_empty());
    }

    #[test]
    fn find_file_metadata_id_recursive() {
        let v: Value =
            serde_json::from_str(r#"{"result":{"fileMetadataId":"fm-9"}}"#).expect("fixture");
        assert_eq!(find_file_metadata_id(&v).as_deref(), Some("fm-9"));
        let v: Value = serde_json::from_str(r#"{"file_metadata_id":"fm-snake"}"#).expect("fixture");
        assert_eq!(find_file_metadata_id(&v).as_deref(), Some("fm-snake"));
        let v: Value = serde_json::from_str(r#"{"a":1}"#).expect("fixture");
        assert_eq!(find_file_metadata_id(&v), None);
    }

    #[test]
    fn multipart_body_shape() {
        let body = build_multipart_body("B", "a.png", "image/png", b"BYTES");
        let text = String::from_utf8_lossy(&body);
        assert!(text.starts_with("--B\r\n"));
        assert!(text.contains("filename=\"a.png\""));
        assert!(text.contains("Content-Type: image/png"));
        assert!(text.contains("BYTES"));
        assert!(text.ends_with("\r\n--B--\r\n"));
    }

    #[test]
    fn file_name_for_url_datauri_and_sanitization() {
        assert_eq!(
            file_name_for(
                &ImageSource::Url("https://example.com/foto.jpg?x=1".to_string()),
                "image/jpeg"
            ),
            "foto.jpg"
        );
        assert_eq!(
            file_name_for(
                &ImageSource::Url("https://example.com/noext".to_string()),
                "image/png"
            ),
            "noext.png"
        );
        assert_eq!(
            file_name_for(
                &ImageSource::DataUri {
                    mime: "image/webp".to_string(),
                    base64: "abcd".to_string(),
                },
                "image/webp"
            ),
            "image.webp"
        );
        // Karakter berbahaya untuk header dihapus.
        let name = file_name_for(
            &ImageSource::Url("https://example.com/a\"b.png".to_string()),
            "image/png",
        );
        assert!(!name.contains('"'));
    }

    #[test]
    fn map_http_error_statuses() {
        assert!(matches!(
            map_http_error(401, "x", None),
            ProviderError::AuthExpired(_)
        ));
        match map_http_error(429, "x", Some("120")) {
            ProviderError::RateLimited {
                retry_after,
                message,
            } => {
                assert_eq!(retry_after, Some(120));
                assert!(message.contains("429"));
            }
            _ => panic!("harus RateLimited"),
        }
        match map_http_error(500, "boom", None) {
            ProviderError::Upstream {
                status,
                body_snippet,
            } => {
                assert_eq!(status, 500);
                assert_eq!(body_snippet, "boom");
            }
            _ => panic!("harus Upstream"),
        }
    }
}
