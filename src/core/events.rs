//! Event stream ternormalisasi.
//!
//! Tiap provider mem-parse format stream aslinya menjadi [`Event`],
//! sehingga lapisan OpenAI (`openai.rs`) tidak perlu tahu format
//! mentah tiap backend web.

use crate::error::ProviderError;
use futures::Stream;
use std::pin::Pin;

/// Satu unit keluaran dari provider dalam stream chat.
///
/// `Text` adalah delta jawaban; `Reasoning` adalah delta thinking
/// (datang sebelum/di luar jawaban, tidak pernah dicampur ke jawaban).
/// `Usage` selalu estimasi — tidak ada provider web yang mengembalikan
/// hitungan token asli; flag `estimated` didokumentasikan di log,
/// bukan di wire OpenAI (wire tidak punya flag itu).
#[derive(Debug, Clone)]
pub enum Event {
    /// Delta teks jawaban.
    Text(String),
    /// Delta thinking/reasoning (sebelum/di luar jawaban).
    Reasoning(String),
    /// Gambar hasil generate, sudah diunduh server-side.
    ///
    /// Phase 5: varian ini dicadangkan untuk gambar yang tiba di
    /// dalam stream chat (bukan jalur `/v1/images/generations`);
    /// belum ada provider yang meng-emit-nya di jalur chat.
    #[allow(dead_code)]
    Image {
        /// Byte gambar mentah.
        bytes: Vec<u8>,
        /// MIME gambar, mis. `"image/png"`.
        mime: String,
    },
    /// Kutipan / sumber hasil search / sitasi.
    Annotation(Annotation),
    /// Hitungan token. Selalu estimasi (`estimated` harus `true`
    /// untuk backend web; `false` hanya untuk pemakaian internal/test).
    Usage {
        /// Token prompt (estimasi).
        prompt: u64,
        /// Token completion, di luar reasoning (estimasi).
        completion: u64,
        /// Token reasoning (estimasi).
        reasoning: u64,
        /// Benar bila angka di atas adalah estimasi, bukan hitungan asli.
        estimated: bool,
    },
    /// Penanda akhir stream (sebelum `[DONE]` OpenAI).
    Done,
}

/// Kutipan/sumber yang menyertai jawaban, mis. hasil web search.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Annotation {
    /// Jenis anotasi, mis. `"citation"` atau `"search_result"`.
    pub kind: String,
    /// Teks kutipan/sumber.
    pub text: String,
    /// URL sumber, bila ada.
    pub url: Option<String>,
}

/// Aliran event dari provider: satu item per delta/error.
pub type EventStream = Pin<Box<dyn Stream<Item = Result<Event, ProviderError>> + Send>>;
