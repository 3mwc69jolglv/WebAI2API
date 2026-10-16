//! HTTP client bersama + titik ekstensi transport.
//!
//! Client adalah reqwest polos dengan UA Chrome (lihat
//! [`http_client`]).
//!
//! ## Evaluasi `rquest` (Fase 4, 2026-10-16) — KEPUTUSAN: tetap reqwest
//!
//! Percobaan migrasi ke `rquest` dengan impersonasi TLS Chrome
//! **gagal pada tahap resolusi dependensi**, sebelum kode apa pun
//! sempat dikompilasi:
//! - `cargo build` pada crate probe dengan `rquest = "0.2"` / `"0.1"`
//!   / `"=0.2.2"` selalu gagal: `no matching package named 'rquest'
//!   found` di index crates.io.
//! - `cargo search rquest` mengembalikan `rquest = "0.0.0"` (stub,
//!   bukan versi rilis) dan `cargo info rquest` gagal: `could not
//!   find 'rquest' in registry`.
//!
//! Artinya, dari sandbox ini, crate `rquest` tidak dapat diambil dari
//! crates.io — entah index yang dilayani sandbox ini terfilter/basi,
//! atau crate tersebut tidak lagi tersedia di registry tersebut.
//! Tanpa crate yang bisa diunduh, tidak ada API yang bisa
//! diverifikasi lewat kompilasi, dan menebak API-nya dari ingatan
//! lalu mengarang kode migrasi akan melanggar prinsip "jangan
//! hardcode hasil tebakan".
//!
//! Yang dibutuhkan untuk mencoba lagi di lingkungan dengan akses
//! crates.io penuh: `rquest` yang bisa di-resolve + dikompilasi,
//! lalu ganti seluruh jalur `reqwest::` di provider (tipe
//! `Client`/`Response`/`Request`/`Error`, `HeaderMap`, builder)
//! dengan padanan `rquest::` — API-nya mirip reqwest, tapi
//! kesepadanan tiap method (`bytes_stream`, `json`, dsb.) harus
//! diverifikasi lewat kompilasi, bukan asumsi. Sampai saat itu,
//! reqwest + UA Chrome tetap dipakai, dan [`Transport`] di bawah
//! tetap menjadi titik ekstensi untuk fallback browser-relay.

use crate::error::ProviderError;
use std::time::Duration;

/// User-Agent Chrome yang dipakai semua request ke backend web.
pub const CHROME_UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

/// reqwest client bersama: timeout wajar, UA browser.
///
/// CATATAN: untuk Grok/ChatGPT, proteksi bot mereka juga memeriksa TLS
/// fingerprint. Impersonasi TLS Chrome (mis. via `rquest`) belum bisa
/// dipakai — lihat catatan evaluasi `rquest` di header modul ini.
/// Bila provider melempar 403 Cloudflare di jaringan non-sandbox,
/// fallback-nya adalah browser-relay (via [`Transport`]), bukan
/// menebak-nebak konfigurasi TLS.
pub fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .user_agent(CHROME_UA)
        .timeout(Duration::from_secs(180))
        .build()
        // Membangun client dengan konfigurasi statis di atas tidak bisa
        // gagal dalam praktik (tidak ada resolver/TLS custom yang bisa
        // invalid); expect ini menandai kegagalan sebagai bug fatal.
        .expect("gagal membuat HTTP client")
}

/// Abstraksi pengiriman request HTTP.
///
/// Implementasi default adalah [`reqwest::Client`] di bawah. Fallback
/// browser-relay (Fase 4+) mengimplementasikan trait ini dengan
/// mengeksekusi request lewat browser terkontrol (CDP/Playwright),
/// sehingga provider tidak peduli lewat jalur mana request dikirim.
//
// Phase 5: titik ekstensi browser-relay — belum ada implementasi
// selain `reqwest::Client`, dan belum ada provider yang memakai
// trait ini di jalur produksi.
#[allow(dead_code)]
#[async_trait::async_trait]
pub trait Transport: Send + Sync {
    /// Kirim `request` dan kembalikan respons mentahnya.
    async fn execute(&self, request: reqwest::Request) -> Result<reqwest::Response, ProviderError>;
}

// Phase 5: lihat catatan pada [`Transport`].
#[allow(dead_code)]
#[async_trait::async_trait]
impl Transport for reqwest::Client {
    async fn execute(&self, request: reqwest::Request) -> Result<reqwest::Response, ProviderError> {
        // Memanggil method inherent Client::execute, bukan trait ini
        // (method inherent lebih diprioritaskan, jadi tidak rekursif).
        self.execute(request).await.map_err(ProviderError::from)
    }
}

/// Transport default: reqwest client bersama ([`http_client`]).
//
// Phase 5: lihat catatan pada [`Transport`].
#[allow(dead_code)]
pub fn default_transport() -> reqwest::Client {
    http_client()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Transport tiruan untuk menguji trait tanpa jaringan.
    struct FailingTransport;

    #[async_trait::async_trait]
    impl Transport for FailingTransport {
        async fn execute(
            &self,
            _request: reqwest::Request,
        ) -> Result<reqwest::Response, ProviderError> {
            Err(ProviderError::Network(
                "fixture: transport gagal".to_string(),
            ))
        }
    }

    #[tokio::test]
    async fn transport_trait_dispatches_to_implementation() {
        let transport = FailingTransport;
        let req = reqwest::Request::new(
            reqwest::Method::GET,
            "https://example.invalid/"
                .parse()
                .expect("URL fixture valid"),
        );
        let err = transport.execute(req).await.expect_err("harus gagal");
        assert!(matches!(err, ProviderError::Network(_)));
    }

    #[test]
    fn default_transport_builds() {
        // Cukup membangun client; tidak ada request jaringan.
        let _client = default_transport();
    }
}
