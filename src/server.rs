use crate::core::events::{Annotation, Event as ProviderEvent};
use crate::error::ApiError;
use crate::openai::{
    annotations_to_citations, event_to_chunk, event_usage_to_openai, AssistantMessage, ChatChunk,
    ChatCompletion, ChatCompletionRequest, ChatMessage, ChunkChoice, ChunkDelta, CompletionChoice,
    ImageGenRequest, MessageContent, ModelList, Usage,
};
use crate::providers::{ChatInput, ImageSource, Message, Part, Registry, Role, Search, Thinking};
use async_stream::stream;
use axum::{
    extract::State,
    http::HeaderMap,
    response::{
        sse::{Event as SseEvent, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use base64::Engine as _;
use futures::StreamExt;
use serde_json::json;
use std::convert::Infallible;
use uuid::Uuid;

#[derive(Clone)]
pub struct AppState {
    pub registry: Registry,
    pub api_key: Option<String>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/health", get(health))
        .route("/v1/models", get(list_models))
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/images/generations", post(images_generations))
        .with_state(state)
}

async fn index() -> impl IntoResponse {
    Json(json!({
        "name": "AiWeb2API",
        "version": env!("CARGO_PKG_VERSION"),
        "description": "API kompatibel-OpenAI di atas akun web ChatGPT, Claude, Gemini, dan Grok.",
        "endpoints": ["/health", "/v1/models", "/v1/chat/completions"],
    }))
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    Json(json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "models": state.registry.all_models().iter().map(|m| &m.id).collect::<Vec<_>>(),
    }))
}

async fn list_models(State(state): State<AppState>) -> impl IntoResponse {
    Json(ModelList {
        object: "list".to_string(),
        data: state.registry.all_models(),
    })
}

fn check_auth(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    let Some(key) = &state.api_key else {
        return Ok(());
    };
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if auth == format!("Bearer {key}") {
        Ok(())
    } else {
        Err(ApiError::Unauthorized(
            "API key tidak valid / tidak disertakan".to_string(),
        ))
    }
}

fn new_id() -> String {
    format!("chatcmpl-{}", Uuid::new_v4().simple())
}

/// Parameter sampling pertama yang dikirim client secara eksplisit,
/// bila ada. Backend web tidak bisa memenuhi parameter sampling apa
/// pun, jadi request yang mengisinya ditolak (400
/// `unsupported_parameter`) alih-alih diam-diam diabaikan.
fn unsupported_sampling_param(req: &ChatCompletionRequest) -> Option<&'static str> {
    if req.temperature.is_some() {
        return Some("temperature");
    }
    if req.top_p.is_some() {
        return Some("top_p");
    }
    if req.max_tokens.is_some() {
        return Some("max_tokens");
    }
    if req.frequency_penalty.is_some() {
        return Some("frequency_penalty");
    }
    if req.presence_penalty.is_some() {
        return Some("presence_penalty");
    }
    if req.logit_bias.is_some() {
        return Some("logit_bias");
    }
    if req.seed.is_some() {
        return Some("seed");
    }
    None
}

/// Parse URL gambar ala OpenAI menjadi [`ImageSource`]: `data:` URI
/// -> `DataUri` (MIME + base64), sisanya -> `Url`.
fn parse_image_source(url: &str) -> ImageSource {
    if let Some(rest) = url.strip_prefix("data:") {
        if let Some((meta, base64)) = rest.split_once(',') {
            let mime = meta.split(';').next().unwrap_or_default();
            return ImageSource::DataUri {
                mime: if mime.is_empty() {
                    "application/octet-stream".to_string()
                } else {
                    mime.to_string()
                },
                base64: base64.to_string(),
            };
        }
    }
    ImageSource::Url(url.to_string())
}

/// Konversi pesan OpenAI menjadi histori [`Message`] v2 (teks + gambar).
fn convert_messages(messages: &[ChatMessage]) -> Vec<Message> {
    messages
        .iter()
        .map(|m| {
            let role = match m.role.as_str() {
                "system" => Role::System,
                "assistant" => Role::Assistant,
                _ => Role::User,
            };
            let mut parts: Vec<Part> = Vec::new();
            match &m.content {
                MessageContent::Text(s) => {
                    parts.push(Part::Text(s.clone()));
                }
                MessageContent::Parts(content_parts) => {
                    for p in content_parts {
                        match p.kind.as_str() {
                            "image_url" | "input_image" => {
                                if let Some(img) = &p.image_url {
                                    parts.push(Part::ImageUrl(parse_image_source(&img.url)));
                                }
                            }
                            _ => {
                                parts.push(Part::Text(p.text.clone()));
                            }
                        }
                    }
                }
                MessageContent::None => {}
            }
            Message { role, parts }
        })
        .collect()
}

async fn chat_completions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ChatCompletionRequest>,
) -> Result<Response, ApiError> {
    check_auth(&state, &headers)?;

    if let Some(param) = unsupported_sampling_param(&req) {
        return Err(ApiError::UnsupportedParameter(param.to_string()));
    }
    if req.messages.is_empty() {
        return Err(ApiError::BadRequest("messages kosong".to_string()));
    }
    let provider = state.registry.find_model(&req.model).ok_or_else(|| {
        let avail: Vec<String> = state
            .registry
            .all_models()
            .iter()
            .map(|m| m.id.clone())
            .collect();
        ApiError::BadRequest(format!(
            "model '{}' tidak dikenal atau provider-nya belum dikonfigurasi. Tersedia: {}",
            req.model,
            if avail.is_empty() {
                "(belum ada — isi cookie di .env)".to_string()
            } else {
                avail.join(", ")
            }
        ))
    })?;

    let chat_messages = convert_messages(&req.messages);
    let has_text = chat_messages
        .iter()
        .flat_map(|m| &m.parts)
        .any(|p| matches!(p, Part::Text(t) if !t.trim().is_empty()));
    if !has_text {
        return Err(ApiError::BadRequest("prompt kosong".to_string()));
    }

    let input = ChatInput {
        messages: chat_messages,
        model: req.model.clone(),
        thinking: Thinking::Auto,
        search: Search::Auto,
        stream: req.stream,
    };
    tracing::debug!(
        model = input.model.as_str(),
        thinking = ?input.thinking,
        search = ?input.search,
        stream = input.stream,
        "ChatInput v2 dibangun (thinking/search masih Auto default di Fase 1)"
    );
    let event_stream = provider.chat(&input).await.map_err(ApiError::from)?;

    let id = new_id();
    let created = chrono::Utc::now().timestamp();
    let model = req.model.clone();

    if req.stream {
        let s = stream! {
            // chunk pembuka: role
            let open = ChatChunk {
                id: id.clone(),
                object: "chat.completion.chunk".to_string(),
                created,
                model: model.clone(),
                choices: vec![ChunkChoice {
                    index: 0,
                    delta: ChunkDelta { role: Some("assistant".to_string()), content: None, reasoning_content: None },
                    finish_reason: None,
                }],
                usage: None,
            };
            yield Ok::<_, Infallible>(SseEvent::default().data(serde_json::to_string(&open).unwrap()));

            let mut es = event_stream;
            let mut failed = false;
            let mut last_usage: Option<Usage> = None;
            while let Some(item) = es.next().await {
                match item {
                    Ok(ProviderEvent::Annotation(_)) => {
                        // Streaming belum meng-emit citations (tidak ada
                        // representasi chunk standar); anotasi dipakai di
                        // jalur non-streaming sebagai field `citations`.
                    }
                    Ok(ProviderEvent::Usage { prompt, completion, reasoning, estimated }) => {
                        if estimated {
                            tracing::debug!("usage dari provider web adalah estimasi, bukan hitungan token asli");
                        }
                        last_usage = Some(event_usage_to_openai(prompt, completion, reasoning));
                    }
                    Ok(ProviderEvent::Image { .. }) => {
                        // Gambar tidak punya representasi dalam SSE chat teks.
                    }
                    Ok(ProviderEvent::Done) => break,
                    Ok(ev) => {
                        if let Some(chunk) = event_to_chunk(&ev, &id, created, &model) {
                            yield Ok(SseEvent::default().data(serde_json::to_string(&chunk).unwrap()));
                        }
                    }
                    Err(e) => {
                        failed = true;
                        let payload = json!({ "error": e.to_string() });
                        yield Ok(SseEvent::default().event("error").data(payload.to_string()));
                        break;
                    }
                }
            }

            if !failed {
                let done = ChatChunk {
                    id: id.clone(),
                    object: "chat.completion.chunk".to_string(),
                    created,
                    model: model.clone(),
                    choices: vec![ChunkChoice {
                        index: 0,
                        delta: ChunkDelta::default(),
                        finish_reason: Some("stop".to_string()),
                    }],
                    usage: last_usage,
                };
                yield Ok(SseEvent::default().data(serde_json::to_string(&done).unwrap()));
            }
            yield Ok(SseEvent::default().data("[DONE]"));
        };
        Ok(Sse::new(s).keep_alive(KeepAlive::default()).into_response())
    } else {
        let mut full = String::new();
        let mut reasoning = String::new();
        let mut citations: Vec<Annotation> = Vec::new();
        let mut last_usage: Option<Usage> = None;
        let mut es = event_stream;
        while let Some(item) = es.next().await {
            match item {
                Ok(ProviderEvent::Text(t)) => full.push_str(&t),
                Ok(ProviderEvent::Reasoning(r)) => reasoning.push_str(&r),
                Ok(ProviderEvent::Annotation(a)) => citations.push(a),
                Ok(ProviderEvent::Usage {
                    prompt,
                    completion,
                    reasoning: reasoning_tokens,
                    estimated,
                }) => {
                    if estimated {
                        tracing::debug!(
                            "usage dari provider web adalah estimasi, bukan hitungan token asli"
                        );
                    }
                    last_usage = Some(event_usage_to_openai(prompt, completion, reasoning_tokens));
                }
                Ok(ProviderEvent::Image { .. }) | Ok(ProviderEvent::Done) => {}
                Err(e) => return Err(ApiError::from(e)),
            }
        }
        if full.trim().is_empty() {
            return Err(ApiError::Internal(
                "provider mengembalikan teks kosong".to_string(),
            ));
        }
        let completion = ChatCompletion {
            id,
            object: "chat.completion".to_string(),
            created,
            model,
            choices: vec![CompletionChoice {
                index: 0,
                message: AssistantMessage {
                    role: "assistant".to_string(),
                    content: full,
                    reasoning_content: if reasoning.is_empty() {
                        None
                    } else {
                        Some(reasoning)
                    },
                    citations: annotations_to_citations(&citations),
                },
                finish_reason: "stop".to_string(),
            }],
            usage: last_usage.unwrap_or_default(),
        };
        Ok(Json(completion).into_response())
    }
}

/// Routing model -> id provider untuk `/v1/images/generations`.
///
/// - `gpt-image-*` / `chatgpt-image` -> ChatGPT (tool image dalam conversation)
/// - `gemini-image-*` / `nano-banana` -> Gemini (Nano Banana)
/// - `grok-*` -> Grok (Imagine via lane REST; gating tier belum
///   terverifikasi live — lihat `docs/validasi-live.md`)
/// - `claude-*` -> 404: Claude tidak mengimplementasikan generate
///   gambar raster (hanya SVG/diagram via kode)
/// - lainnya -> 400: model gambar tidak dikenal
fn image_provider_id(model: &str) -> Result<&'static str, ApiError> {
    if model.starts_with("gpt-image") || model == "chatgpt-image" {
        Ok("chatgpt")
    } else if model.starts_with("gemini-image") || model == "nano-banana" {
        Ok("gemini")
    } else if model.starts_with("grok") {
        Ok("grok")
    } else if model.starts_with("claude") {
        Err(ApiError::NotFound(
            "Claude tidak mendukung generate gambar raster (hanya SVG/diagram via kode)"
                .to_string(),
        ))
    } else {
        Err(ApiError::BadRequest(format!(
            "model gambar tidak dikenal: '{model}'. Pakai gpt-image-1, chatgpt-image, \
             gemini-image-1, nano-banana, atau model grok-* (Imagine)"
        )))
    }
}

/// POST /v1/images/generations — ala OpenAI, di atas akun web provider.
async fn images_generations(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ImageGenRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    check_auth(&state, &headers)?;

    if req.n.unwrap_or(1) > 1 {
        return Err(ApiError::BadRequest(
            "n > 1 tidak didukung — web provider hanya menghasilkan 1 gambar per turn".to_string(),
        ));
    }
    if req.prompt.trim().is_empty() {
        return Err(ApiError::BadRequest("prompt kosong".to_string()));
    }

    let provider_id = image_provider_id(req.model.as_str())?;

    let Some(provider) = state.registry.find_provider(provider_id) else {
        return Err(ApiError::BadRequest(format!(
            "provider {provider_id} tidak aktif — isi cookie-nya di .env lalu restart server"
        )));
    };

    let img = provider.generate_image(&req.prompt).await?;
    tracing::debug!(
        mime = img.mime.as_str(),
        bytes = img.bytes.len(),
        "gambar diterima dari provider"
    );
    let data = match req.response_format.as_deref().unwrap_or("b64_json") {
        "url" => json!({ "url": img.source_url.clone().unwrap_or_default() }),
        _ => json!({ "b64_json": base64::engine::general_purpose::STANDARD.encode(&img.bytes) }),
    };
    Ok(Json(json!({
        "created": chrono::Utc::now().timestamp(),
        "data": [data],
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req_with(json: &str) -> ChatCompletionRequest {
        serde_json::from_str(json).expect("request fixture valid")
    }

    #[test]
    fn sampling_params_detected() {
        assert_eq!(
            unsupported_sampling_param(&req_with(
                r#"{"model":"m","messages":[],"temperature":0.5}"#
            )),
            Some("temperature")
        );
        assert_eq!(
            unsupported_sampling_param(&req_with(r#"{"model":"m","messages":[],"top_p":0.9}"#)),
            Some("top_p")
        );
        assert_eq!(
            unsupported_sampling_param(&req_with(
                r#"{"model":"m","messages":[],"max_tokens":100}"#
            )),
            Some("max_tokens")
        );
        assert_eq!(
            unsupported_sampling_param(&req_with(
                r#"{"model":"m","messages":[],"frequency_penalty":0.1}"#
            )),
            Some("frequency_penalty")
        );
        assert_eq!(
            unsupported_sampling_param(&req_with(
                r#"{"model":"m","messages":[],"presence_penalty":0.1}"#
            )),
            Some("presence_penalty")
        );
        assert_eq!(
            unsupported_sampling_param(&req_with(
                r#"{"model":"m","messages":[],"logit_bias":{"1":5}}"#
            )),
            Some("logit_bias")
        );
        assert_eq!(
            unsupported_sampling_param(&req_with(r#"{"model":"m","messages":[],"seed":7}"#)),
            Some("seed")
        );
        // Tanpa parameter sampling -> tidak ditolak.
        assert_eq!(
            unsupported_sampling_param(&req_with(r#"{"model":"m","messages":[]}"#)),
            None
        );
    }

    #[test]
    fn convert_messages_text_and_image() {
        let req = req_with(
            r#"{"model":"m","messages":[
                {"role":"system","content":"Kamu asisten."},
                {"role":"user","content":[
                    {"type":"text","text":"lihat"},
                    {"type":"image_url","image_url":{"url":"data:image/png;base64,abcd"}}
                ]},
                {"role":"user","content":"halo"}
            ]}"#,
        );
        let messages = convert_messages(&req.messages);
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].role, Role::System);
        assert_eq!(messages[1].parts.len(), 2);
        match &messages[1].parts[1] {
            Part::ImageUrl(ImageSource::DataUri { mime, base64 }) => {
                assert_eq!(mime, "image/png");
                assert_eq!(base64, "abcd");
            }
            _ => panic!("harus DataUri"),
        }
        match &messages[2].parts[0] {
            Part::Text(t) => assert_eq!(t, "halo"),
            _ => panic!("harus Text"),
        }
    }

    #[test]
    fn image_provider_id_routing() {
        // ChatGPT + Gemini: seperti sebelumnya.
        assert_eq!(image_provider_id("gpt-image-1").ok(), Some("chatgpt"));
        assert_eq!(image_provider_id("chatgpt-image").ok(), Some("chatgpt"));
        assert_eq!(image_provider_id("gemini-image-1").ok(), Some("gemini"));
        assert_eq!(image_provider_id("nano-banana").ok(), Some("gemini"));
        // Grok: routing DIBUKA (Imagine sudah diimplementasikan).
        assert_eq!(image_provider_id("grok-imagine-image").ok(), Some("grok"));
        assert_eq!(image_provider_id("grok-4").ok(), Some("grok"));
        // Claude: 404 (tidak diimplementasikan), bukan 400.
        match image_provider_id("claude-sonnet-4-5") {
            Err(ApiError::NotFound(msg)) => {
                assert!(msg.contains("tidak mendukung generate gambar raster"));
            }
            other => panic!("Claude harus 404 NotFound, dapat: {other:?}"),
        }
        // Model tak dikenal: 400.
        assert!(matches!(
            image_provider_id("model-aneh"),
            Err(ApiError::BadRequest(_))
        ));
    }

    #[test]
    fn parse_image_source_url_vs_data_uri() {
        assert!(matches!(
            parse_image_source("https://example.com/a.png"),
            ImageSource::Url(_)
        ));
        match parse_image_source("data:image/jpeg;base64,xyz") {
            ImageSource::DataUri { mime, base64 } => {
                assert_eq!(mime, "image/jpeg");
                assert_eq!(base64, "xyz");
            }
            _ => panic!("harus DataUri"),
        }
        // `data:` tanpa koma -> fallback ke Url.
        assert!(matches!(
            parse_image_source("data:broken"),
            ImageSource::Url(_)
        ));
    }
}
