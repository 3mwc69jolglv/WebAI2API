//! Modul bersama (shared core) untuk semua provider.
//!
//! Berisi primitif generik yang dipakai tiap provider: event stream
//! ternormalisasi, auth/cookie, HTTP client, media, katalog model,
//! dan retry/pacing. Tidak ada logika spesifik provider di sini —
//! konstanta endpoint, header, dan payload per provider tinggal di
//! file provider-nya masing-masing.

pub mod auth;
pub mod catalog;
pub mod events;
pub mod http;
pub mod media;
pub mod retry;
