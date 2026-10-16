//! Helper media generik: ambil sumber gambar (URL / data URI).
//!
//! Dipakai nanti oleh provider untuk vision (upload-then-reference):
//! provider mengunduh/mendekode sumber gambar lewat sini, lalu
//! meng-upload-nya ke endpoint upload provider-nya sendiri. Tidak ada
//! endpoint provider di sini.

use crate::core::http::http_client;
use crate::error::ProviderError;
use base64::Engine as _;

/// Batas ukuran gambar yang diunduh/didekode (15 MB, sama seperti
/// batas gambar hasil generate di provider).
pub const MAX_IMAGE_DOWNLOAD_BYTES: usize = 15 * 1024 * 1024;

/// Sumber gambar input dari client: URL eksternal atau data URI.
#[derive(Debug, Clone)]
pub enum ImageInput {
    /// URL http(s) yang harus diunduh.
    Url(String),
    /// Data URI: MIME + payload base64 (tanpa prefix `data:`).
    DataUri {
        /// MIME gambar, mis. `"image/png"`.
        mime: String,
        /// Payload base64 (tanpa prefix `data:<mime>;base64,`).
        base64: String,
    },
}

/// Gambar yang sudah diunduh/didekode: byte mentah + MIME-nya.
#[derive(Debug, Clone)]
pub struct DownloadedImage {
    /// Byte gambar mentah.
    pub bytes: Vec<u8>,
    /// MIME gambar, mis. `"image/png"`.
    pub mime: String,
}

/// Ambil sumber gambar: unduh URL, atau decode data URI.
///
/// Hasil selalu dibatasi [`MAX_IMAGE_DOWNLOAD_BYTES`]; sumber yang
/// lebih besar ditolak agar tidak membanjiri memori.
pub async fn download_image_source(input: &ImageInput) -> Result<DownloadedImage, ProviderError> {
    match input {
        ImageInput::Url(url) => {
            let resp = http_client().get(url).send().await?;
            let status = resp.status().as_u16();
            if status != 200 {
                return Err(ProviderError::Upstream {
                    status,
                    body_snippet: format!("unduh gambar gagal: HTTP {status}"),
                });
            }
            let mime = resp
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("application/octet-stream")
                .split(';')
                .next()
                .unwrap_or("application/octet-stream")
                .to_string();
            let bytes = resp
                .bytes()
                .await
                .map_err(|e| ProviderError::Network(e.to_string()))?
                .to_vec();
            check_size(bytes.len())?;
            Ok(DownloadedImage { bytes, mime })
        }
        ImageInput::DataUri { mime, base64 } => {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(base64)
                .map_err(|e| ProviderError::Parse(format!("data URI base64 tidak valid: {e}")))?;
            check_size(bytes.len())?;
            Ok(DownloadedImage {
                bytes,
                mime: mime.clone(),
            })
        }
    }
}

/// Tolak payload gambar yang melebihi batas ukuran.
fn check_size(len: usize) -> Result<(), ProviderError> {
    if len > MAX_IMAGE_DOWNLOAD_BYTES {
        return Err(ProviderError::Parse(format!(
            "gambar terlalu besar (>{}MB)",
            MAX_IMAGE_DOWNLOAD_BYTES / (1024 * 1024)
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn data_uri_decodes() {
        let input = ImageInput::DataUri {
            mime: "image/png".to_string(),
            base64: base64::engine::general_purpose::STANDARD.encode(b"fake-png-bytes"),
        };
        let out = download_image_source(&input)
            .await
            .expect("decode data URI harus berhasil");
        assert_eq!(out.bytes, b"fake-png-bytes");
        assert_eq!(out.mime, "image/png");
    }

    #[tokio::test]
    async fn data_uri_rejects_bad_base64() {
        let input = ImageInput::DataUri {
            mime: "image/png".to_string(),
            base64: "!!!bukan-base64!!!".to_string(),
        };
        let err = download_image_source(&input)
            .await
            .expect_err("base64 tidak valid harus gagal");
        assert!(matches!(err, ProviderError::Parse(_)));
    }

    #[test]
    fn image_input_url_variant_constructs() {
        // Konstruksi saja (tanpa jaringan): jalur URL dites live di Fase 5.
        let input = ImageInput::Url("https://example.com/a.png".to_string());
        assert!(matches!(input, ImageInput::Url(url) if url.ends_with("a.png")));
    }
}
