//! Katalog model live per provider (cache in-memory + aturan default).
//!
//! Daftar model TIDAK PERNAH di-hardcode: provider mengambilnya dari
//! endpoint aslinya dan menyimpannya di [`ModelCatalog`] dengan TTL.
//! Satu-satunya nilai model yang boleh hardcoded adalah slug model
//! default web tiap provider (mis. `"auto"`), yang disuplai penelepon
//! sebagai `web_default` — bukan daftar model.

use crate::openai::ModelInfo;
use std::time::{Duration, Instant};

/// Cache katalog model satu provider: daftar model + kapan diambil.
#[derive(Debug)]
pub struct ModelCatalog {
    models: Vec<ModelInfo>,
    fetched_at: Option<Instant>,
    ttl: Duration,
}

impl ModelCatalog {
    /// Katalog kosong dengan TTL `ttl` (mis. 1 jam).
    pub fn new(ttl: Duration) -> Self {
        Self {
            models: Vec::new(),
            fetched_at: None,
            ttl,
        }
    }

    /// Ganti isi katalog dengan daftar segar dari upstream.
    pub fn update(&mut self, models: Vec<ModelInfo>) {
        self.models = models;
        self.fetched_at = Some(Instant::now());
    }

    /// Daftar model yang tersimpan (bisa kosong bila belum diambil).
    pub fn models(&self) -> &[ModelInfo] {
        &self.models
    }

    /// Benar bila katalog pernah diambil dan belum kedaluwarsa.
    pub fn is_fresh(&self) -> bool {
        match self.fetched_at {
            None => false,
            Some(at) => at.elapsed() < self.ttl,
        }
    }
}

/// Aturan MODEL=WEB-DEFAULT (kritis):
///
/// - `requested` None/kosong -> `web_default`, tanpa warning.
/// - `requested` persis cocok dengan satu entri katalog -> dipakai,
///   tanpa warning.
/// - `requested` tidak ada di katalog -> `web_default` + warning.
/// - Katalog kosong (pengambilan gagal) -> `web_default` + warning.
///
/// Mengembalikan `(model_yang_dipakai, warning_atau_None)`.
pub fn resolve_model(
    requested: Option<&str>,
    catalog: &[ModelInfo],
    web_default: &str,
) -> (String, Option<String>) {
    if catalog.is_empty() {
        return (
            web_default.to_string(),
            Some(format!(
                "katalog model kosong (gagal mengambil dari provider) — \
                 memakai model default web '{web_default}'"
            )),
        );
    }
    let requested = requested.map(str::trim).filter(|s| !s.is_empty());
    let Some(req) = requested else {
        return (web_default.to_string(), None);
    };
    if catalog.iter().any(|m| m.id == req) {
        return (req.to_string(), None);
    }
    (
        web_default.to_string(),
        Some(format!(
            "model '{req}' tidak dikenal — memakai model default web '{web_default}'"
        )),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog_of(ids: &[&str]) -> Vec<ModelInfo> {
        ids.iter().map(|id| ModelInfo::new(id, "test")).collect()
    }

    #[test]
    fn resolve_model_requested_in_catalog() {
        let catalog = catalog_of(&["auto", "fast"]);
        assert_eq!(
            resolve_model(Some("fast"), &catalog, "auto"),
            ("fast".to_string(), None)
        );
    }

    #[test]
    fn resolve_model_requested_not_in_catalog_falls_back_with_warning() {
        let catalog = catalog_of(&["auto", "fast"]);
        let (model, warning) = resolve_model(Some("nope"), &catalog, "auto");
        assert_eq!(model, "auto");
        assert!(warning.is_some(), "harus ada warning");
        assert!(warning.unwrap().contains("nope"));
    }

    #[test]
    fn resolve_model_empty_requested_uses_default_without_warning() {
        let catalog = catalog_of(&["auto", "fast"]);
        assert_eq!(
            resolve_model(None, &catalog, "auto"),
            ("auto".to_string(), None)
        );
        assert_eq!(
            resolve_model(Some(""), &catalog, "auto"),
            ("auto".to_string(), None)
        );
        assert_eq!(
            resolve_model(Some("   "), &catalog, "auto"),
            ("auto".to_string(), None)
        );
    }

    #[test]
    fn resolve_model_empty_catalog_falls_back_with_warning() {
        let (model, warning) = resolve_model(Some("fast"), &[], "auto");
        assert_eq!(model, "auto");
        assert!(warning.is_some());
        // Bahkan tanpa request eksplisit, katalog kosong tetap warning.
        let (_, warning_none) = resolve_model(None, &[], "auto");
        assert!(warning_none.is_some());
    }

    #[test]
    fn catalog_freshness() {
        let mut catalog = ModelCatalog::new(Duration::from_secs(3600));
        assert!(!catalog.is_fresh());
        assert!(catalog.models().is_empty());
        catalog.update(catalog_of(&["auto"]));
        assert!(catalog.is_fresh());
        assert_eq!(catalog.models().len(), 1);

        let mut stale = ModelCatalog::new(Duration::from_secs(0));
        stale.update(catalog_of(&["auto"]));
        assert!(!stale.is_fresh(), "TTL 0 berarti langsung basi");
    }
}
