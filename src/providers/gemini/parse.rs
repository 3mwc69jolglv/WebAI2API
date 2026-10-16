//! Parsing respons mentah Gemini Web (`StreamGenerate` / `batchexecute`).
//!
//! Format wire (dari riset `docs/research-web2api-2026-10-16.md` §1):
//! respons adalah baris-baris teks; tiap frame diawali prefix XSSI
//! `)]}'` dan/atau baris length-prefix (hanya angka) lalu satu objek
//! JSON array. `frame[2]` adalah string JSON inner; teks kandidat
//! ada di `inner[4]`.
//!
//! KETERBATASAN YANG DIKETAHUI (butuh capture live, Fase 5):
//! - Slot thought eksak di dalam kandidat belum terverifikasi dari
//!   capture resmi. Parser ini hanya mengenali bentuk yang diberi
//!   tag eksplisit (`["thoughts", "..."]` atau kunci `"thoughts"`)
//!   dan TIDAK menebak index magic apa pun. Bila backend mengirim
//!   thought di slot lain, tidak ada event `Reasoning` yang di-emit
//!   (tidak ada false positive), dan slot itu ditambahkan setelah
//!   capture live.
//! - Entri katalog `otAQ7b` dikenali lewat heuristik (slug diawali
//!   `gemini`, atau id hex 16 karakter + label menyebut Gemini).
//!   Bentuk entri yang berbeda dari heuristik ini tidak akan
//!   terbaca — juga ditambahkan setelah capture live.

use serde_json::Value;

/// Satu entri katalog model dari payload `otAQ7b`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogEntry {
    /// Id model yang dipakai client (`slug` bila ada, else id hex).
    pub id: String,
    /// Hash model untuk header `x-goog-ext-525001261-jspb`, bila
    /// ditemukan berdampingan dengan id di entri yang sama.
    pub hex: Option<String>,
}

/// Parse body respons Gemini menjadi daftar inner JSON.
///
/// Baris kosong, baris length-prefix (hanya angka), dan prefix XSSI
/// `)]}'` dilewati. Baris yang bukan JSON valid dilewati. Bila satu
/// frame adalah array dengan `frame[2]` string, string itu di-parse
/// sebagai inner; bila baris itu sendiri sudah berbentuk inner
/// (punya kandidat di index 4), baris itu dipakai langsung.
pub fn parse_wrb_frames(body: &str) -> Vec<Value> {
    let mut out = Vec::new();
    for line in body.lines() {
        let t = line.trim();
        if t.is_empty() {
            continue;
        }
        if t.chars().all(|c| c.is_ascii_digit()) {
            continue; // length prefix
        }
        let t = t.strip_prefix(")]}'").unwrap_or(t);
        let Ok(v) = serde_json::from_str::<Value>(t) else {
            continue;
        };
        if let Some(inner_str) = v.as_array().and_then(|a| a.get(2)).and_then(|x| x.as_str()) {
            if let Ok(inner) = serde_json::from_str::<Value>(inner_str) {
                out.push(inner);
            }
        } else if v
            .as_array()
            .is_some_and(|a| a.get(4).is_some_and(|c| c.is_array()))
        {
            // Baris sudah berbentuk inner JSON langsung.
            out.push(v);
        }
    }
    out
}

/// Ekstrak (thought, teks body) dari satu inner JSON.
///
/// - Teks body: `inner[4][c][1][0]` per kandidat (perilaku v1 yang
///   dipertahankan), digabung antar kandidat.
/// - Thought: sub-array `["thoughts", "<teks>"]` / `["thought", ...]`
///   atau kunci objek `"thoughts"` / `"thought"` di dalam kandidat.
///   Bila thought sama persis dengan teks body, thought dibuang
///   (menghindari emit ganda konten yang sama).
/// - Urutan tuple adalah (thought, teks): penelepon WAJIB meng-emit
///   thought sebagai `Reasoning` SEBELUM teks sebagai `Text`.
pub fn segments_from_inner(inner: &Value) -> (Option<String>, Option<String>) {
    let Some(candidates) = inner.get(4).and_then(|c| c.as_array()) else {
        return (None, None);
    };
    let mut thoughts = String::new();
    let mut texts = String::new();
    for candidate in candidates {
        if let Some(text) = candidate
            .get(1)
            .and_then(|a| a.as_array())
            .and_then(|a| a.first())
            .and_then(|s| s.as_str())
        {
            if !text.is_empty() {
                texts.push_str(text);
            }
        }
        if let Some(thought) = find_thought_text(candidate) {
            if !thought.is_empty() {
                thoughts.push_str(&thought);
            }
        }
    }
    let thought = (!thoughts.is_empty()).then_some(thoughts);
    let text = (!texts.is_empty()).then_some(texts);
    // Thought yang identik dengan teks body bukan reasoning terpisah.
    if thought.is_some() && thought == text {
        return (None, text);
    }
    (thought, text)
}

/// Cari teks thought bertag eksplisit di dalam `v` (rekursif).
///
/// Mengenali: objek dengan kunci `"thoughts"`/`"thought"` bernilai
/// string, atau array `[ "thoughts" | "thought", "<teks>", ... ]`.
/// Array lain ditelusuri elemennya; string polos bukan thought.
fn find_thought_text(v: &Value) -> Option<String> {
    match v {
        Value::Object(map) => {
            for key in ["thoughts", "thought"] {
                if let Some(Value::String(s)) = map.get(key) {
                    if !s.is_empty() {
                        return Some(s.clone());
                    }
                }
            }
            map.values().find_map(find_thought_text)
        }
        Value::Array(arr) => {
            if arr.len() >= 2 {
                if let (Some(Value::String(tag)), Some(Value::String(s))) =
                    (arr.first(), arr.get(1))
                {
                    if (tag == "thoughts" || tag == "thought") && !s.is_empty() {
                        return Some(s.clone());
                    }
                }
            }
            arr.iter().find_map(find_thought_text)
        }
        _ => None,
    }
}

/// Parse payload batchexecute `otAQ7b` menjadi entri katalog.
///
/// Entri dikenali lewat heuristik (lihat doc modul): array kecil
/// (2–10 elemen) yang memuat slug `gemini...`, atau id hex 16
/// karakter berdampingan dengan label yang menyebut Gemini. Entri
/// didedup berdasarkan id, urutan upstream dipertahankan.
pub fn parse_catalog_payload(body: &str) -> Vec<CatalogEntry> {
    let mut frames = parse_wrb_frames(body);
    if frames.is_empty() {
        // Fallback: body mungkin satu JSON utuh, bukan frame-frame.
        let t = body.trim().strip_prefix(")]}'").unwrap_or(body.trim());
        if let Ok(v) = serde_json::from_str::<Value>(t) {
            frames.push(v);
        }
    }
    let mut out: Vec<CatalogEntry> = Vec::new();
    for frame in &frames {
        collect_catalog_entries(frame, &mut out);
    }
    out
}

/// Telusuri `v` secara rekursif, kumpulkan entri katalog.
fn collect_catalog_entries(v: &Value, out: &mut Vec<CatalogEntry>) {
    if let Value::Array(arr) = v {
        if (2..=10).contains(&arr.len()) {
            if let Some(entry) = entry_from_array(arr) {
                if !out.iter().any(|e| e.id == entry.id) {
                    out.push(entry);
                }
            }
        }
        for item in arr {
            collect_catalog_entries(item, out);
        }
    } else if let Value::Object(map) = v {
        for item in map.values() {
            collect_catalog_entries(item, out);
        }
    }
}

/// Coba baca satu array sebagai entri katalog; `None` bila bukan.
fn entry_from_array(arr: &[Value]) -> Option<CatalogEntry> {
    let strings: Vec<&str> = arr.iter().filter_map(|x| x.as_str()).collect();
    let slug = strings.iter().find(|s| s.starts_with("gemini")).copied();
    let hex = strings.iter().find(|s| is_hex16(s)).copied();
    if let Some(slug) = slug {
        return Some(CatalogEntry {
            id: slug.to_string(),
            hex: hex.map(str::to_string),
        });
    }
    // Entri tanpa slug: id hex + label yang menyebut Gemini.
    if let Some(hex) = hex {
        let mentions_gemini = strings.iter().any(|s| s.to_lowercase().contains("gemini"));
        if mentions_gemini {
            return Some(CatalogEntry {
                id: hex.to_string(),
                hex: Some(hex.to_string()),
            });
        }
    }
    None
}

/// Benar bila `s` tepat 16 karakter heksadesimal (hash model Gemini).
fn is_hex16(s: &str) -> bool {
    s.len() == 16 && s.chars().all(|c| c.is_ascii_hexdigit())
}

/// Kumpulkan URL gambar (`*.googleusercontent.com`) dari body
/// StreamGenerate. Gambar hasil generate ada di rich content;
/// pencarian rekursif ini cukup robust tanpa memetakan seluruh
/// struktur JSPB. (Dipindahkan utuh dari implementasi v1.)
pub fn extract_image_urls(body: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in body.lines() {
        let t = line.trim();
        if t.is_empty() || t.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        let t = t.strip_prefix(")]}'").unwrap_or(t);
        let Ok(v) = serde_json::from_str::<Value>(t) else {
            continue;
        };
        // Frame batchexecute: inner JSON string ada di index 2.
        let inner_val: Option<Value> = v
            .as_array()
            .and_then(|a| a.get(2))
            .and_then(|x| x.as_str())
            .and_then(|s| serde_json::from_str(s).ok());
        if let Some(inner) = inner_val {
            collect_image_urls(&inner, &mut out);
        } else {
            collect_image_urls(&v, &mut out);
        }
    }
    out.sort();
    out.dedup();
    out
}

fn collect_image_urls(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::String(s) => {
            if s.starts_with("https://") && s.contains("googleusercontent.com") {
                // Potong kalau URL menempel dengan teks lain.
                let end = s
                    .find(|c: char| c.is_whitespace() || c == '"' || c == '\\')
                    .unwrap_or(s.len());
                let url = s[..end].to_string();
                if !out.contains(&url) {
                    out.push(url);
                }
            }
        }
        Value::Array(a) => {
            for x in a {
                collect_image_urls(x, out);
            }
        }
        Value::Object(m) => {
            for (_, x) in m {
                collect_image_urls(x, out);
            }
        }
        _ => {}
    }
}

/// Estimasi token dari jumlah karakter: `chars / 4` (perkiraan
/// ala n0madic/go-gemini-web2api). Backend web TIDAK mengembalikan
/// hitungan token asli — semua usage adalah estimasi dan WAJIB
/// di-emit dengan `estimated: true`.
pub fn estimate_tokens(chars: usize) -> u64 {
    (chars as u64).div_ceil(4)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Inner fixture: kandidat 0 membawa thought bertag + teks body.
    fn inner_with_thought_and_text() -> Value {
        json!([
            null,
            null,
            null,
            null,
            [[null, ["Hello, world!"], ["thoughts", "Let me think first."]],],
        ])
    }

    #[test]
    fn wrb_frames_skip_length_prefix_and_xssi() {
        let inner = serde_json::to_string(&inner_with_thought_and_text()).expect("fixture valid");
        let frame = serde_json::to_string(&json!(["wrb.fr", null, inner])).expect("fixture valid");
        let body = format!(")]}}'\n12345\n{frame}\n");
        let frames = parse_wrb_frames(&body);
        assert_eq!(frames.len(), 1);
        let (thought, text) = segments_from_inner(&frames[0]);
        assert_eq!(thought.as_deref(), Some("Let me think first."));
        assert_eq!(text.as_deref(), Some("Hello, world!"));
    }

    #[test]
    fn segments_thought_before_text_ordering() {
        // Tuple (thought, teks): penelepon meng-emit thought dulu.
        let (thought, text) = segments_from_inner(&inner_with_thought_and_text());
        assert!(thought.is_some(), "thought harus ada");
        assert!(text.is_some(), "teks harus ada");
        // Thought tidak boleh sama dengan teks body.
        assert_ne!(thought, text);
    }

    #[test]
    fn segments_text_only_without_thought() {
        let inner = json!([null, null, null, null, [[null, ["Only answer"]]]]);
        let (thought, text) = segments_from_inner(&inner);
        assert_eq!(thought, None);
        assert_eq!(text.as_deref(), Some("Only answer"));
    }

    #[test]
    fn segments_thought_identical_to_text_is_dropped() {
        let inner = json!([
            null,
            null,
            null,
            null,
            [[null, ["Same"], ["thoughts", "Same"]]],
        ]);
        let (thought, text) = segments_from_inner(&inner);
        assert_eq!(thought, None);
        assert_eq!(text.as_deref(), Some("Same"));
    }

    #[test]
    fn segments_object_key_thoughts() {
        let inner = json!([
            null, null, null, null,
            [{"thoughts": "Reasoning via object", "other": [null, ["Body"]]}],
        ]);
        // Kandidat objek: teks body tidak ada di slot [1][0], tapi
        // thought dari kunci objek tetap ditemukan.
        let (thought, _text) = segments_from_inner(&inner);
        assert_eq!(thought.as_deref(), Some("Reasoning via object"));
    }

    #[test]
    fn catalog_parses_otaq7b_fixture() {
        let inner = json!([
            ["gemini-3.6-flash", "Gemini 3.6 Flash", "56fdd199312815e2"],
            ["abcdef0123456789", "Gemini 3.1 Pro"],
            ["not-a-model", "something else"],
            ["gemini-3.6-flash", "duplikat, harus didedup"],
        ]);
        let inner_str = serde_json::to_string(&inner).expect("fixture valid");
        let frame =
            serde_json::to_string(&json!(["otAQ7b", null, inner_str])).expect("fixture valid");
        let body = format!(")]}}'\n99\n{frame}\n");
        let entries = parse_catalog_payload(&body);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].id, "gemini-3.6-flash");
        assert_eq!(entries[0].hex.as_deref(), Some("56fdd199312815e2"));
        // Entri hex-only memakai hex sebagai id.
        assert_eq!(entries[1].id, "abcdef0123456789");
        assert_eq!(entries[1].hex.as_deref(), Some("abcdef0123456789"));
    }

    #[test]
    fn catalog_empty_payload_yields_empty() {
        assert!(parse_catalog_payload("").is_empty());
        assert!(parse_catalog_payload(")]}'\nnot json\n").is_empty());
        // Payload tanpa entri model -> kosong (bukan fallback statis).
        let inner = serde_json::to_string(&json!([["foo", "bar"]])).expect("fixture valid");
        let frame = serde_json::to_string(&json!(["otAQ7b", null, inner])).expect("fixture valid");
        assert!(parse_catalog_payload(&frame).is_empty());
    }

    #[test]
    fn image_urls_extracted_from_frame() {
        let url = "https://lh3.googleusercontent.com/abc123";
        let inner = serde_json::to_string(&json!([url])).expect("fixture valid");
        let frame = serde_json::to_string(&json!(["wrb.fr", null, inner])).expect("fixture valid");
        let urls = extract_image_urls(&frame);
        assert_eq!(urls, vec![url.to_string()]);
    }

    #[test]
    fn estimate_tokens_is_chars_div_4() {
        assert_eq!(estimate_tokens(0), 0);
        assert_eq!(estimate_tokens(4), 1);
        assert_eq!(estimate_tokens(8), 2);
        assert_eq!(estimate_tokens(9), 3);
    }
}
