mod core;
mod error;
mod openai;
mod providers;
mod server;

use providers::{
    chatgpt::ChatGptProvider, claude::ClaudeProvider, gemini::GeminiProvider, grok::GrokProvider,
    Registry,
};
use server::AppState;
use std::env;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

fn env_nonempty(name: &str) -> Option<String> {
    env::var(name).ok().filter(|v| !v.trim().is_empty())
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive("aiweb2api=info".parse().unwrap()),
        )
        .init();

    let mut registry = Registry::new();

    if let Some(c) = env_nonempty("AIW2A_CHATGPT_COOKIES") {
        registry.add(Arc::new(ChatGptProvider::new(c)));
    }
    if let Some(c) = env_nonempty("AIW2A_CLAUDE_COOKIES") {
        registry.add(Arc::new(ClaudeProvider::new(c)));
    }
    if let Some(c) = env_nonempty("AIW2A_GEMINI_COOKIES") {
        registry.add(Arc::new(GeminiProvider::new(c)));
    }
    if let Some(sso) = env_nonempty("AIW2A_GROK_SSO") {
        registry.add(Arc::new(GrokProvider::new(sso)));
    }

    if registry.is_empty() {
        tracing::warn!(
            "tidak ada provider yang dikonfigurasi — isi cookie di .env (lihat .env.example). \
             Server tetap jalan tapi /v1/models kosong."
        );
    } else {
        // Panaskan katalog model live semua provider secara konkuren.
        // Best-effort: gagal upstream hanya dicatat sebagai warning,
        // TIDAK PERNAH menggagalkan startup.
        registry.warm_all().await;
        let models: Vec<String> = registry.all_models().iter().map(|m| m.id.clone()).collect();
        tracing::info!("model tersedia: {}", models.join(", "));
    }

    let port: u16 = env::var("AIW2A_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);
    let api_key = env_nonempty("AIW2A_API_KEY");
    if api_key.is_some() {
        tracing::info!("autentikasi API aktif (Bearer token)");
    } else {
        tracing::warn!("AIW2A_API_KEY kosong — API terbuka tanpa autentikasi");
    }

    let state = AppState { registry, api_key };
    let app = server::router(state);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .expect("gagal bind port");
    tracing::info!(
        "AiWeb2API v{} berjalan di http://0.0.0.0:{port}",
        env!("CARGO_PKG_VERSION")
    );

    axum::serve(listener, app).await.expect("server error");
}
