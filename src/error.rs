use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

/// Error yang dikembalikan provider web (ChatGPT/Claude/Gemini/Grok).
#[derive(Debug)]
pub enum ProviderError {
    /// Kredensial (cookie/token) hilang, kedaluwarsa, atau ditolak.
    Unauthorized(String),
    /// Rate limit upstream. `retry_after` dalam detik bila upstream
    /// memberikannya (header `Retry-After`); `None` bila tidak.
    RateLimited {
        retry_after: Option<u64>,
        message: String,
    },
    /// Kredensial kedaluwarsa dan tidak bisa di-refresh otomatis
    /// (mis. Claude: perlu re-paste cookie). Beda dari `Unauthorized`
    /// yang umum: ini eksplisit berarti "kredensial mati".
    AuthExpired(String),
    /// Provider mengembalikan HTTP error. `body_snippet` adalah
    /// cuplikan body maksimal ~500 karakter yang SUDAH dibersihkan
    /// dari nilai kredensial (lihat [`sanitize_snippet`]).
    Upstream { status: u16, body_snippet: String },
    /// Parameter request yang tidak bisa dipenuhi backend web
    /// (mis. sampling param seperti `temperature`).
    ///
    /// Dicadangkan untuk provider yang menolak param di lapisannya
    /// sendiri; server saat ini mengembalikan
    /// `ApiError::UnsupportedParameter` sebelum memanggil provider.
    #[allow(dead_code)]
    UnsupportedParameter(String),
    /// Input client tidak valid.
    InvalidInput(String),
    /// Respons provider tidak bisa diparse.
    Parse(String),
    /// Gagal jaringan / timeout.
    Network(String),
}

impl ProviderError {
    /// Bangun error `Upstream` dari body mentah upstream, dengan
    /// cuplikan yang sudah dibersihkan via [`sanitize_snippet`].
    /// Penelepon sebaiknya memakai konstruktor ini, bukan membangun
    /// varian `Upstream` langsung dari body mentah.
    pub fn upstream(status: u16, body: &str) -> Self {
        ProviderError::Upstream {
            status,
            body_snippet: sanitize_snippet(body),
        }
    }
}

/// Bersihkan body upstream menjadi cuplikan aman untuk error/log:
/// baris yang mengandung nama header/nilai kredensial
/// (`authorization`, `cookie`, `set-cookie`, `access_token`,
/// `sessionkey`, `bearer`, `api_key`) diganti redaksi, lalu hasil
/// dipotong maksimal 500 karakter. Best-effort: penelepon tetap
/// tidak boleh menaruh nilai kredensial mentah ke dalam body.
pub fn sanitize_snippet(body: &str) -> String {
    const SENSITIVE: [&str; 7] = [
        "authorization",
        "cookie",
        "access_token",
        "sessionkey",
        "bearer",
        "api_key",
        "apikey",
    ];
    let mut out = String::new();
    for line in body.lines() {
        let lower = line.to_lowercase();
        if SENSITIVE.iter().any(|k| lower.contains(k)) {
            out.push_str("[redacted credential-like content]\n");
        } else {
            out.push_str(line);
            out.push('\n');
        }
    }
    let mut snippet: String = out.chars().take(500).collect();
    // Hindari cuplikan terpotong di tengah kata kunci sensitif yang
    // lolos filter baris (mis. kata terbelah di batas 500 karakter):
    // bila 500 karakter pertama mengandung fragmen kunci, redaksi.
    let lower = snippet.to_lowercase();
    if SENSITIVE.iter().any(|k| lower.contains(k)) {
        snippet = "[redacted credential-like content]".to_string();
    }
    snippet.trim_end().to_string()
}

impl std::fmt::Display for ProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProviderError::Unauthorized(m) => write!(f, "unauthorized: {m}"),
            ProviderError::RateLimited {
                retry_after,
                message,
            } => match retry_after {
                Some(secs) => write!(f, "rate limited (retry after {secs}s): {message}"),
                None => write!(f, "rate limited: {message}"),
            },
            ProviderError::AuthExpired(m) => write!(f, "auth expired: {m}"),
            ProviderError::Upstream {
                status,
                body_snippet,
            } => {
                write!(f, "upstream {status}: {body_snippet}")
            }
            ProviderError::UnsupportedParameter(m) => {
                write!(f, "unsupported parameter: {m}")
            }
            ProviderError::InvalidInput(m) => write!(f, "invalid input: {m}"),
            ProviderError::Parse(m) => write!(f, "parse error: {m}"),
            ProviderError::Network(m) => write!(f, "network error: {m}"),
        }
    }
}

impl std::error::Error for ProviderError {}

impl From<reqwest::Error> for ProviderError {
    fn from(e: reqwest::Error) -> Self {
        ProviderError::Network(e.to_string())
    }
}

/// Error API publik, diserialisasi dalam format error ala OpenAI.
#[derive(Debug)]
pub enum ApiError {
    BadRequest(String),
    Unauthorized(String),
    /// Sumber daya tidak ada / tidak didukung provider (HTTP 404),
    /// mis. generate gambar pada provider yang tidak
    /// mengimplementasikannya (Claude).
    NotFound(String),
    /// Parameter yang tidak bisa dipenuhi backend web -> 400 dengan
    /// code `unsupported_parameter`.
    UnsupportedParameter(String),
    Provider(ProviderError),
    Internal(String),
}

impl From<ProviderError> for ApiError {
    fn from(e: ProviderError) -> Self {
        ApiError::Provider(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message, code, retry_after): (StatusCode, String, &'static str, Option<u64>) =
            match &self {
                ApiError::BadRequest(m) => {
                    (StatusCode::BAD_REQUEST, m.clone(), "bad_request", None)
                }
                ApiError::Unauthorized(m) => {
                    (StatusCode::UNAUTHORIZED, m.clone(), "unauthorized", None)
                }
                ApiError::NotFound(m) => (StatusCode::NOT_FOUND, m.clone(), "not_found", None),
                ApiError::UnsupportedParameter(m) => (
                    StatusCode::BAD_REQUEST,
                    format!("parameter '{m}' tidak didukung oleh backend web"),
                    "unsupported_parameter",
                    None,
                ),
                ApiError::Internal(m) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    m.clone(),
                    "internal_error",
                    None,
                ),
                ApiError::Provider(e) => match e {
                    ProviderError::Unauthorized(m) => (
                        StatusCode::UNAUTHORIZED,
                        format!("provider auth gagal: {m}"),
                        "provider_unauthorized",
                        None,
                    ),
                    ProviderError::RateLimited {
                        retry_after,
                        message,
                    } => (
                        StatusCode::TOO_MANY_REQUESTS,
                        format!("provider rate limit: {message}"),
                        "rate_limited",
                        *retry_after,
                    ),
                    ProviderError::AuthExpired(m) => (
                        StatusCode::UNAUTHORIZED,
                        format!("kredensial provider kedaluwarsa: {m}"),
                        "auth_expired",
                        None,
                    ),
                    ProviderError::Upstream {
                        status,
                        body_snippet,
                    } => {
                        let s = StatusCode::from_u16(*status).unwrap_or(StatusCode::BAD_GATEWAY);
                        (
                            s,
                            format!("provider error: {body_snippet}"),
                            "provider_upstream",
                            None,
                        )
                    }
                    ProviderError::UnsupportedParameter(m) => (
                        StatusCode::BAD_REQUEST,
                        format!("parameter '{m}' tidak didukung oleh backend web"),
                        "unsupported_parameter",
                        None,
                    ),
                    ProviderError::InvalidInput(m) => {
                        (StatusCode::BAD_REQUEST, m.clone(), "invalid_input", None)
                    }
                    ProviderError::Parse(m) => (
                        StatusCode::BAD_GATEWAY,
                        format!("gagal membaca respons provider: {m}"),
                        "provider_parse",
                        None,
                    ),
                    ProviderError::Network(m) => (
                        StatusCode::BAD_GATEWAY,
                        format!("gagal menghubungi provider: {m}"),
                        "provider_network",
                        None,
                    ),
                },
            };
        let body = Json(json!({
            "error": {
                "message": message,
                "type": code,
                "code": code,
            }
        }));
        let mut response = (status, body).into_response();
        if let Some(secs) = retry_after {
            if let Ok(value) = axum::http::HeaderValue::from_str(&secs.to_string()) {
                response.headers_mut().insert("retry-after", value);
            }
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_snippet_redacts_credential_lines() {
        let body = "error: boom\nCookie: sessionKey=secret123\nSet-Cookie: a=b\nok line";
        let snippet = sanitize_snippet(body);
        assert!(!snippet.contains("secret123"));
        assert!(!snippet.contains("sessionKey"));
        assert!(snippet.contains("[redacted credential-like content]"));
        assert!(snippet.contains("ok line"));
    }

    #[test]
    fn sanitize_snippet_truncates_to_500_chars() {
        let body = "x".repeat(2000);
        let snippet = sanitize_snippet(&body);
        assert!(snippet.chars().count() <= 500);
    }

    #[test]
    fn upstream_constructor_sanitizes() {
        let err = ProviderError::upstream(500, "Authorization: Bearer abc\nboom");
        match err {
            ProviderError::Upstream {
                status,
                body_snippet,
            } => {
                assert_eq!(status, 500);
                assert!(!body_snippet.contains("abc"));
                assert!(body_snippet.contains("boom"));
            }
            _ => panic!("harus Upstream"),
        }
    }

    #[test]
    fn api_error_status_codes() {
        let resp = ApiError::UnsupportedParameter("temperature".to_string()).into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let resp = ApiError::Provider(ProviderError::RateLimited {
            retry_after: Some(30),
            message: "slow down".to_string(),
        })
        .into_response();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(resp.headers().get("retry-after").unwrap(), "30");
        let resp = ApiError::Provider(ProviderError::AuthExpired("x".to_string())).into_response();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        // Varian parameter/input provider dipetakan ke 400.
        let resp = ApiError::Provider(ProviderError::UnsupportedParameter(
            "temperature".to_string(),
        ))
        .into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let resp =
            ApiError::Provider(ProviderError::InvalidInput("buruk".to_string())).into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
}
