use crate::core::events::{Event, EventStream};
use crate::error::ProviderError;
use crate::openai::ModelInfo;
use futures::StreamExt;
use std::pin::Pin;
use std::sync::Arc;

pub mod chatgpt;
pub mod claude;
pub mod gemini;
pub mod grok;

/// Re-export client bersama + UA dari core (satu sumber kebenaran).
pub use crate::core::http::{http_client, CHROME_UA};

/// Aliran potongan teks (delta) dari provider — bentuk lama, hanya
/// dipakai [`text_only_stream`] untuk konsumen yang hanya butuh teks.
pub type TextStream = Pin<Box<dyn futures::Stream<Item = Result<String, ProviderError>> + Send>>;

/// Peran satu pesan dalam histori chat.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Instruksi sistem.
    System,
    /// Pesan pengguna.
    User,
    /// Pesan asisten (riwayat).
    Assistant,
}

/// Satu konten dalam pesan: teks atau gambar.
#[derive(Debug, Clone)]
pub enum Part {
    /// Potongan teks.
    Text(String),
    /// Gambar (URL atau data URI).
    ImageUrl(ImageSource),
}

/// Sumber gambar dalam pesan (vision input).
#[derive(Debug, Clone)]
pub enum ImageSource {
    /// URL http(s) gambar.
    Url(String),
    /// Data URI: MIME + payload base64 (tanpa prefix `data:`).
    DataUri {
        /// MIME gambar, mis. `"image/png"`.
        mime: String,
        /// Payload base64 (tanpa prefix `data:<mime>;base64,`).
        base64: String,
    },
}

/// Satu pesan dalam histori chat.
#[derive(Debug, Clone)]
pub struct Message {
    /// Peran pengirim pesan.
    pub role: Role,
    /// Potongan konten pesan (teks/gambar).
    pub parts: Vec<Part>,
}

/// Mode thinking/reasoning sebelum menjawab.
///
/// Varian `Off`/`On` adalah bagian kontrak v2 [`ChatInput`]:
/// dicocokkan (matched) di logika provider, tapi di jalur produksi
/// saat ini hanya `Auto` yang dikonstruksi (server belum mengekspos
/// opsi thinking ke client). Varian dipertahankan untuk kontrak v2,
/// bukan dihapus.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Thinking {
    /// Thinking dimatikan.
    #[allow(dead_code)]
    Off,
    /// Ikuti default web provider (default).
    #[default]
    Auto,
    /// Thinking diaktifkan, dengan level opsional (mapping per provider).
    #[allow(dead_code)]
    On {
        /// Level thinking (mapping per provider, mis. budget/effort).
        level: Option<u8>,
    },
}

/// Mode internet search.
///
/// Varian `Off`/`On` adalah bagian kontrak v2 [`ChatInput`]
/// (lihat catatan pada [`Thinking`]); server saat ini selalu `Auto`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Search {
    /// Search dimatikan.
    #[allow(dead_code)]
    Off,
    /// Ikuti default web provider (default).
    #[default]
    Auto,
    /// Search diaktifkan (toggle deterministik per provider).
    On,
}

/// Input chat v2 yang dinormalisasi untuk semua provider.
#[derive(Debug, Clone)]
pub struct ChatInput {
    /// Histori utuh (tidak diratakan di server).
    pub messages: Vec<Message>,
    /// Nama model yang diminta client.
    pub model: String,
    /// Mode thinking.
    pub thinking: Thinking,
    /// Mode internet search.
    pub search: Search,
    /// Benar bila client meminta streaming SSE.
    pub stream: bool,
}

/// Hasil generate gambar dari provider.
pub struct GeneratedImage {
    pub bytes: Vec<u8>,
    pub mime: String,
    /// URL sumber (bila provider memberikannya; bisa terikat sesi browser).
    pub source_url: Option<String>,
}

#[async_trait::async_trait]
pub trait Provider: Send + Sync {
    fn id(&self) -> &'static str;
    /// Slug model default UI web provider ini (mis. `"auto"`,
    /// `"chatgpt-auto"`). TIDAK PERNAH model spesifik terkunci
    /// (mis. `"gemini-3-flash"`): default web mengikuti akun.
    fn web_default_model(&self) -> &'static str;
    fn models(&self) -> Vec<ModelInfo>;
    async fn chat(&self, input: &ChatInput) -> Result<EventStream, ProviderError>;
    /// Generate gambar. Default: tidak didukung.
    async fn generate_image(&self, _prompt: &str) -> Result<GeneratedImage, ProviderError> {
        Err(ProviderError::Parse(
            "provider ini tidak mendukung generate gambar".to_string(),
        ))
    }
    /// Panaskan katalog model live (dipanggil sekali saat startup
    /// oleh [`Registry::warm_all`]).
    ///
    /// Default: no-op (provider tanpa katalog live yang bisa
    /// dipanaskan). Override provider mengambil katalog live-nya;
    /// gagal upstream -> `Err` (penelepon mencatat warning,
    /// TIDAK PERNAH menggagalkan startup). Provider yang belum
    /// dikonfigurasi (kredensial kosong) HARUS mengembalikan `Err`
    /// tanpa melakukan request jaringan apa pun.
    async fn warm_catalog(&self) -> Result<(), ProviderError> {
        Ok(())
    }
}

/// Konversi [`EventStream`] menjadi [`TextStream`]: hanya event
/// [`Event::Text`] yang diteruskan, event lain (reasoning, gambar,
/// anotasi, usage) dibuang. Dipakai konsumen yang hanya butuh teks;
/// jalur chat utama memakai [`EventStream`] langsung.
//
// Phase 5: adapter kenyamanan untuk konsumen teks-saja — belum ada
// konsumen produksi yang memakainya (jalur chat memakai `EventStream`).
#[allow(dead_code)]
pub fn text_only_stream(stream: EventStream) -> TextStream {
    Box::pin(stream.filter_map(|item| async move {
        match item {
            Ok(Event::Text(t)) => Some(Ok(t)),
            Ok(_) => None,
            Err(e) => Some(Err(e)),
        }
    }))
}

/// Registry provider yang aktif (yang kredensialnya dikonfigurasi).
#[derive(Clone, Default)]
pub struct Registry {
    providers: Vec<Arc<dyn Provider>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, p: Arc<dyn Provider>) {
        tracing::info!(
            provider = p.id(),
            web_default_model = p.web_default_model(),
            "provider aktif"
        );
        self.providers.push(p);
    }

    /// Cari provider dari nama model persis.
    pub fn find_model(&self, name: &str) -> Option<Arc<dyn Provider>> {
        self.providers.iter().find_map(|p| {
            if p.models().iter().any(|m| m.id == name) {
                Some(Arc::clone(p))
            } else {
                None
            }
        })
    }

    /// Cari provider dari id-nya ("grok", "chatgpt", "claude", "gemini").
    pub fn find_provider(&self, id: &str) -> Option<Arc<dyn Provider>> {
        self.providers.iter().find(|p| p.id() == id).map(Arc::clone)
    }

    pub fn all_models(&self) -> Vec<ModelInfo> {
        let mut out: Vec<ModelInfo> = Vec::new();
        for p in &self.providers {
            out.extend(p.models());
        }
        out
    }

    /// Panaskan katalog model live semua provider secara konkuren.
    ///
    /// Best-effort: kegagalan satu provider hanya dicatat sebagai
    /// warning dan TIDAK PERNAH menggagalkan startup (provider
    /// tersebut tetap jalan; katalognya terisi saat chat pertama).
    pub async fn warm_all(&self) {
        let warmed = futures::future::join_all(self.providers.iter().map(|p| {
            let p = Arc::clone(p);
            async move { (p.id(), p.warm_catalog().await) }
        }))
        .await;
        for (id, result) in warmed {
            if let Err(e) = result {
                tracing::warn!(
                    provider = id,
                    "gagal warm_catalog saat startup (best-effort, server tetap jalan): {e}"
                );
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn text_only_stream_drops_non_text_events() {
        use async_stream::stream;
        let events: EventStream = Box::pin(stream! {
            yield Ok(Event::Text("a".to_string()));
            yield Ok(Event::Reasoning("thinking".to_string()));
            yield Ok(Event::Text("b".to_string()));
            yield Ok(Event::Usage { prompt: 1, completion: 2, reasoning: 0, estimated: true });
            yield Ok(Event::Done);
        });
        let mut texts: Vec<String> = Vec::new();
        let mut s = text_only_stream(events);
        while let Some(item) = s.next().await {
            texts.push(item.expect("tidak boleh error"));
        }
        assert_eq!(texts, vec!["a", "b"]);
    }

    #[test]
    fn providers_expose_web_default_model() {
        // Konstruksi tanpa jaringan (cookie fixture, tidak dipakai).
        let gemini = gemini::GeminiProvider::new("fixture".to_string());
        assert_eq!(gemini.web_default_model(), "gemini");
        assert!(gemini.models().iter().any(|m| m.id == "gemini"));

        let claude = claude::ClaudeProvider::new("fixture".to_string());
        assert_eq!(claude.web_default_model(), "claude");
        assert!(claude.models().iter().any(|m| m.id == "claude"));

        let chatgpt = chatgpt::ChatGptProvider::new("fixture".to_string());
        assert_eq!(chatgpt.web_default_model(), "chatgpt-auto");

        let grok = grok::GrokProvider::new("fixture".to_string());
        assert_eq!(grok.web_default_model(), "grok-auto");
    }

    #[test]
    fn chat_input_carries_thinking_search_stream() {
        let input = ChatInput {
            messages: vec![Message {
                role: Role::User,
                parts: vec![Part::Text("halo".to_string())],
            }],
            model: "gemini".to_string(),
            thinking: Thinking::On { level: Some(2) },
            search: Search::On,
            stream: true,
        };
        assert_eq!(input.thinking, Thinking::On { level: Some(2) });
        assert_eq!(input.search, Search::On);
        assert!(input.stream);
        // Semua varian enum ter-cover.
        assert_eq!(Thinking::default(), Thinking::Auto);
        assert_eq!(Search::default(), Search::Auto);
        let _ = Thinking::Off;
        let _ = Search::Off;
    }

    #[test]
    fn generated_image_and_data_uri_fields_readable() {
        let img = GeneratedImage {
            bytes: vec![1, 2, 3],
            mime: "image/png".to_string(),
            source_url: Some("https://example.com/a.png".to_string()),
        };
        assert_eq!(img.bytes.len(), 3);
        assert_eq!(img.mime, "image/png");
        assert_eq!(img.source_url.as_deref(), Some("https://example.com/a.png"));

        let src = ImageSource::DataUri {
            mime: "image/png".to_string(),
            base64: "abcd".to_string(),
        };
        match src {
            ImageSource::DataUri { mime, base64 } => {
                assert_eq!(mime, "image/png");
                assert_eq!(base64, "abcd");
            }
            _ => panic!("harus DataUri"),
        }
    }

    /// `warm_catalog` pada provider yang belum dikonfigurasi
    /// (kredensial kosong) harus mengembalikan `Err` tanpa panic
    /// dan tanpa request jaringan (jalur error fixture-level).
    #[tokio::test]
    async fn warm_catalog_on_unconfigured_providers_errors_without_network() {
        let gemini = gemini::GeminiProvider::new(String::new());
        assert!(gemini.warm_catalog().await.is_err());

        let claude = claude::ClaudeProvider::new(String::new());
        assert!(claude.warm_catalog().await.is_err());

        let chatgpt = chatgpt::ChatGptProvider::new(String::new());
        assert!(chatgpt.warm_catalog().await.is_err());

        let grok = grok::GrokProvider::new(String::new());
        assert!(grok.warm_catalog().await.is_err());
    }

    /// `warm_all` pada registry kosong adalah no-op (tidak panic,
    /// tidak ada request).
    #[tokio::test]
    async fn warm_all_on_empty_registry_is_noop() {
        Registry::new().warm_all().await;
    }

    /// Routing `find_model("grok-auto")` tetap jalan setelah daftar
    /// tier fallback Grok dihapus (hanya alias + katalog live).
    #[test]
    fn registry_routes_grok_auto_alias() {
        let mut registry = Registry::new();
        registry.add(Arc::new(grok::GrokProvider::new("fixture".to_string())));
        let provider = registry
            .find_model("grok-auto")
            .expect("grok-auto harus ter-routing ke provider Grok");
        assert_eq!(provider.id(), "grok");
        assert!(registry.find_model("grok-tidak-ada").is_none());
    }
}
