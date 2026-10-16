use crate::core::events::{Annotation, Event};
use serde::{Deserialize, Serialize};

/// Pesan chat ala OpenAI. `content` bisa string polos atau array of parts.
#[derive(Debug, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    #[serde(default)]
    pub content: MessageContent,
}

#[derive(Debug, Deserialize, Default)]
#[serde(untagged)]
pub enum MessageContent {
    Text(String),
    Parts(Vec<ContentPart>),
    #[default]
    None,
}

#[derive(Debug, Deserialize)]
pub struct ContentPart {
    #[serde(rename = "type", default)]
    pub kind: String,
    #[serde(default)]
    pub text: String,
    /// Untuk part gambar (`type: "image_url"` / `"input_image"`).
    #[serde(default)]
    pub image_url: Option<ImageUrlPart>,
}

/// Payload `image_url` dalam part gambar ala OpenAI.
#[derive(Debug, Deserialize)]
pub struct ImageUrlPart {
    #[serde(default)]
    pub url: String,
    // `detail` bagian dari skema OpenAI; sengaja belum dipakai:
    // resolusi/detail gambar ditangani per provider saat upload
    // (tidak ada pemakaian `detail` di v0.2). Field dipertahankan
    // agar request OpenAI yang menyertakannya tetap ter-parse.
    #[allow(dead_code)]
    #[serde(default)]
    pub detail: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ChatCompletionRequest {
    pub model: String,
    #[serde(default)]
    pub messages: Vec<ChatMessage>,
    #[serde(default)]
    pub stream: bool,
    // --- Parameter sampling. Backend web TIDAK BISA memenuhi ini;
    // server menolak request yang mengisinya secara eksplisit
    // (400 `unsupported_parameter`) alih-alih diam-diam mengabaikan.
    // Field-field ini ada di sini agar server bisa mendeteksi
    // "client mengirim nilai" (Some) vs "client tidak mengirim" (None).
    /// Suhu sampling. Web backend tidak bisa memenuhinya -> ditolak.
    #[serde(default)]
    pub temperature: Option<f32>,
    /// Nucleus sampling. Web backend tidak bisa memenuhinya -> ditolak.
    #[serde(default)]
    pub top_p: Option<f32>,
    /// Batas token output. Web backend tidak bisa memenuhinya -> ditolak.
    #[serde(default)]
    pub max_tokens: Option<u32>,
    /// Penalti frekuensi. Web backend tidak bisa memenuhinya -> ditolak.
    #[serde(default)]
    pub frequency_penalty: Option<f32>,
    /// Penalti kehadiran. Web backend tidak bisa memenuhinya -> ditolak.
    #[serde(default)]
    pub presence_penalty: Option<f32>,
    /// Bias logit. Web backend tidak bisa memenuhinya -> ditolak.
    #[serde(default)]
    pub logit_bias: Option<serde_json::Map<String, serde_json::Value>>,
    /// Seed determinisme. Web backend tidak bisa memenuhinya -> ditolak.
    #[serde(default)]
    pub seed: Option<i64>,
}

/// Request POST /v1/images/generations.
#[derive(Deserialize)]
pub struct ImageGenRequest {
    pub model: String,
    pub prompt: String,
    #[serde(default)]
    pub n: Option<u32>,
    #[serde(default)]
    pub response_format: Option<String>, // "b64_json" (default) | "url"
    // `size` bagian dari skema OpenAI; web provider selalu 1 gambar
    // per turn sehingga sengaja diabaikan (tidak dibaca). Field
    // dipertahankan agar request yang menyertakannya tetap ter-parse.
    #[allow(dead_code)]
    #[serde(default)]
    pub size: Option<String>, // diabaikan: web provider selalu 1 gambar per turn
}

#[derive(Debug, Serialize)]
pub struct ModelList {
    pub object: String,
    pub data: Vec<ModelInfo>,
}

#[derive(Debug, Serialize, Clone)]
pub struct ModelInfo {
    pub id: String,
    pub object: String,
    pub created: i64,
    pub owned_by: String,
}

impl ModelInfo {
    pub fn new(id: &str, owned_by: &str) -> Self {
        Self {
            id: id.to_string(),
            object: "model".to_string(),
            created: 1_700_000_000,
            owned_by: owned_by.to_string(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ChatCompletion {
    pub id: String,
    pub object: String,
    pub created: i64,
    pub model: String,
    pub choices: Vec<CompletionChoice>,
    pub usage: Usage,
}

#[derive(Debug, Serialize)]
pub struct CompletionChoice {
    pub index: u32,
    pub message: AssistantMessage,
    pub finish_reason: String,
}

#[derive(Debug, Serialize)]
pub struct AssistantMessage {
    pub role: String,
    pub content: String,
    /// Thinking yang terakumulasi (bila provider meng-emit Reasoning).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
    /// Sitasi/sumber yang terakumulasi dari event Annotation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub citations: Option<Vec<Citation>>,
}

/// Satu sitasi/sumber yang menyertai jawaban.
#[derive(Debug, Serialize, Clone)]
pub struct Citation {
    /// Jenis anotasi, mis. `"citation"` atau `"search_result"`.
    pub kind: String,
    /// Teks kutipan/sumber.
    pub text: String,
    /// URL sumber, bila ada.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

#[derive(Debug, Serialize, Default, Clone)]
pub struct Usage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

/// Konversi hitungan token event menjadi [`Usage`] OpenAI.
///
/// Token reasoning dihitung ke dalam `completion_tokens` (seperti
/// konvensi OpenAI: reasoning termasuk completion) dan ke `total`.
/// Penelepon bertanggung jawab mencatat bahwa angka ini estimasi.
pub fn event_usage_to_openai(prompt: u64, completion: u64, reasoning: u64) -> Usage {
    let prompt = u32::try_from(prompt).unwrap_or(u32::MAX);
    let completion = u32::try_from(completion).unwrap_or(u32::MAX);
    let reasoning = u32::try_from(reasoning).unwrap_or(u32::MAX);
    Usage {
        prompt_tokens: prompt,
        completion_tokens: completion.saturating_add(reasoning),
        total_tokens: prompt.saturating_add(completion).saturating_add(reasoning),
    }
}

#[derive(Debug, Serialize)]
pub struct ChatChunk {
    pub id: String,
    pub object: String,
    pub created: i64,
    pub model: String,
    pub choices: Vec<ChunkChoice>,
    /// Usage, bila event Usage sudah diterima (hanya pada chunk akhir
    /// atau chunk khusus usage; chunk delta biasa tidak membawanya).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
}

#[derive(Debug, Serialize)]
pub struct ChunkChoice {
    pub index: u32,
    pub delta: ChunkDelta,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
}

#[derive(Debug, Serialize, Default)]
pub struct ChunkDelta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    /// Delta thinking/reasoning (field OpenAI `reasoning_content`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
}

/// Terjemahkan satu [`Event`] provider menjadi chunk SSE OpenAI.
///
/// - `Text` -> delta `content`.
/// - `Reasoning` -> delta `reasoning_content`.
/// - `Usage` -> chunk dengan objek `usage`. Flag `estimated` TIDAK
///   dikirim di wire (format OpenAI tidak punya flag itu); bila
///   `estimated`, dicatat di log.
/// - `Done` -> chunk dengan `finish_reason: "stop"`. Penelepon tetap
///   mengirim `[DONE]` sesudahnya (itu penanda akhir SSE, bukan chunk).
/// - `Annotation` -> `None`: anotasi tidak punya representasi chunk;
///   penelepon mengakumulasikannya dan menaruhnya sebagai field
///   `citations` pada pesan akhir (lihat [`annotations_to_citations`]).
/// - `Image` -> `None`: gambar tidak punya representasi dalam SSE
///   chat teks; diabaikan di jalur chat.
pub fn event_to_chunk(event: &Event, id: &str, created: i64, model: &str) -> Option<ChatChunk> {
    let base = |delta: ChunkDelta, finish_reason: Option<String>, usage: Option<Usage>| ChatChunk {
        id: id.to_string(),
        object: "chat.completion.chunk".to_string(),
        created,
        model: model.to_string(),
        choices: vec![ChunkChoice {
            index: 0,
            delta,
            finish_reason,
        }],
        usage,
    };
    match event {
        Event::Text(text) => Some(base(
            ChunkDelta {
                role: None,
                content: Some(text.clone()),
                reasoning_content: None,
            },
            None,
            None,
        )),
        Event::Reasoning(text) => Some(base(
            ChunkDelta {
                role: None,
                content: None,
                reasoning_content: Some(text.clone()),
            },
            None,
            None,
        )),
        Event::Usage {
            prompt,
            completion,
            reasoning,
            estimated,
        } => {
            if *estimated {
                // Flag estimasi hanya di log, bukan di wire.
                tracing::debug!(
                    "usage dari provider web adalah estimasi, bukan hitungan token asli"
                );
            }
            Some(base(
                ChunkDelta::default(),
                None,
                Some(event_usage_to_openai(*prompt, *completion, *reasoning)),
            ))
        }
        Event::Done => Some(base(ChunkDelta::default(), Some("stop".to_string()), None)),
        Event::Annotation(_) | Event::Image { .. } => None,
    }
}

/// Kumpulkan anotasi menjadi field `citations` pesan akhir.
/// Mengembalikan `None` bila tidak ada anotasi (field dihilangkan).
pub fn annotations_to_citations(annotations: &[Annotation]) -> Option<Vec<Citation>> {
    if annotations.is_empty() {
        return None;
    }
    Some(
        annotations
            .iter()
            .map(|a| Citation {
                kind: a.kind.clone(),
                text: a.text.clone(),
                url: a.url.clone(),
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_to_chunk_text_and_reasoning() {
        let text = event_to_chunk(&Event::Text("halo".to_string()), "id1", 0, "m")
            .expect("Text harus jadi chunk");
        assert_eq!(text.choices[0].delta.content.as_deref(), Some("halo"));
        assert_eq!(text.choices[0].delta.reasoning_content, None);
        assert_eq!(text.choices[0].finish_reason, None);

        let reasoning = event_to_chunk(&Event::Reasoning("mikir".to_string()), "id1", 0, "m")
            .expect("Reasoning harus jadi chunk");
        assert_eq!(
            reasoning.choices[0].delta.reasoning_content.as_deref(),
            Some("mikir")
        );
        assert_eq!(reasoning.choices[0].delta.content, None);
    }

    #[test]
    fn event_to_chunk_done_and_usage() {
        let done = event_to_chunk(&Event::Done, "id1", 0, "m").expect("Done harus jadi chunk");
        assert_eq!(done.choices[0].finish_reason.as_deref(), Some("stop"));

        let usage = event_to_chunk(
            &Event::Usage {
                prompt: 10,
                completion: 5,
                reasoning: 3,
                estimated: true,
            },
            "id1",
            0,
            "m",
        )
        .expect("Usage harus jadi chunk");
        let usage = usage.usage.expect("chunk Usage harus bawa usage");
        assert_eq!(usage.prompt_tokens, 10);
        // Reasoning dihitung ke completion (konvensi OpenAI).
        assert_eq!(usage.completion_tokens, 8);
        assert_eq!(usage.total_tokens, 18);

        // Annotation & Image tidak punya representasi chunk.
        assert!(event_to_chunk(
            &Event::Annotation(Annotation {
                kind: "citation".to_string(),
                text: "sumber".to_string(),
                url: None,
            }),
            "id1",
            0,
            "m",
        )
        .is_none());
        assert!(event_to_chunk(
            &Event::Image {
                bytes: vec![],
                mime: "image/png".to_string(),
            },
            "id1",
            0,
            "m",
        )
        .is_none());
    }

    #[test]
    fn annotations_to_citations_empty_is_none() {
        assert!(annotations_to_citations(&[]).is_none());
        let citations = annotations_to_citations(&[Annotation {
            kind: "citation".to_string(),
            text: "sumber".to_string(),
            url: Some("https://example.com".to_string()),
        }])
        .expect("harus ada citations");
        assert_eq!(citations.len(), 1);
        assert_eq!(citations[0].text, "sumber");
    }

    #[test]
    fn request_deserializes_sampling_params_as_some() {
        let req: ChatCompletionRequest =
            serde_json::from_str(r#"{"model":"m","messages":[],"temperature":0.7,"seed":42}"#)
                .expect("parse request");
        assert_eq!(req.temperature, Some(0.7));
        assert_eq!(req.seed, Some(42));
        assert_eq!(req.top_p, None);

        let req: ChatCompletionRequest =
            serde_json::from_str(r#"{"model":"m","messages":[]}"#).expect("parse request");
        assert_eq!(req.temperature, None);
        assert_eq!(req.seed, None);
    }

    #[test]
    fn request_deserializes_image_parts() {
        let req: ChatCompletionRequest = serde_json::from_str(
            r#"{"model":"m","messages":[{"role":"user","content":[
                {"type":"text","text":"lihat"},
                {"type":"image_url","image_url":{"url":"data:image/png;base64,abcd"}}
            ]}]}"#,
        )
        .expect("parse request");
        match &req.messages[0].content {
            MessageContent::Parts(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(
                    parts[1].image_url.as_ref().expect("part gambar").url,
                    "data:image/png;base64,abcd"
                );
            }
            _ => panic!("harus Parts"),
        }
    }

    #[test]
    fn event_image_fields_readable() {
        let event = Event::Image {
            bytes: vec![1, 2],
            mime: "image/png".to_string(),
        };
        match event {
            Event::Image { bytes, mime } => {
                assert_eq!(bytes, vec![1, 2]);
                assert_eq!(mime, "image/png");
            }
            _ => panic!("harus Image"),
        }
    }
}
