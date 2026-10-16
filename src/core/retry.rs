//! Backoff, `Retry-After`, status retryable, dan pacing antar-request.
//!
//! Aturan retry: jangan pernah me-replay generasi yang sudah
//! disubmit, dan jangan retry setelah first-byte diterima (stream
//! sudah jalan). Retry hanya untuk kegagalan pre-first-byte dengan
//! status yang retryable.

use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Backoff eksponensial: percobaan ke-1 -> 2s, ke-2 -> 4s, ke-3 -> 8s,
/// ke-4 -> 16s, ke-5 dan seterusnya -> 32s (cap). `attempt == 0`
/// diperlakukan sama seperti percobaan pertama.
pub fn backoff_delay(attempt: u32) -> Duration {
    let shift = attempt.saturating_sub(1).min(4);
    Duration::from_secs(2u64 << shift)
}

/// Parse header `Retry-After`: detik (`"120"`) atau tanggal HTTP
/// (IMF-fixdate / RFC 2822). Mengembalikan `None` bila header tidak
/// ada atau tidak bisa diparse (penelepon memakai [`backoff_delay`]).
pub fn retry_after_delay(header: Option<&str>) -> Option<Duration> {
    let header = header?.trim();
    if let Ok(secs) = header.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    if let Ok(date) = chrono::DateTime::parse_from_rfc2822(header) {
        let secs = date.timestamp() - chrono::Utc::now().timestamp();
        return Some(Duration::from_secs(secs.max(0) as u64));
    }
    None
}

/// Benar bila status HTTP layak di-retry pre-first-byte:
/// 429 (rate limit), 529 (overloaded), 500/502/503.
pub fn is_retryable_status(status: u16) -> bool {
    matches!(status, 429 | 529 | 500 | 502 | 503)
}

/// Pacing antar-request: menjaga interval minimum antar request
/// berturut-turut agar traffic terlihat seperti manusia
/// (mis. lantai ~1,5 detik untuk Claude; interval bisa dikonfigurasi
/// per provider).
#[derive(Debug)]
pub struct Pacer {
    min_interval: Duration,
    last: Mutex<Option<Instant>>,
}

impl Pacer {
    /// Pacer baru dengan interval minimum `min_interval`.
    pub fn new(min_interval: Duration) -> Self {
        Self {
            min_interval,
            last: Mutex::new(None),
        }
    }

    /// Interval minimum pacer ini.
    ///
    /// Dipakai test; jalur produksi memakai [`Pacer::wait`].
    #[allow(dead_code)]
    pub fn min_interval(&self) -> Duration {
        self.min_interval
    }

    /// Hitung berapa lama lagi harus menunggu bila request terakhir
    /// dikirim pada `since` (None = belum pernah) dan sekarang `now`.
    /// Fungsi murni (tanpa tidur) agar mudah dites.
    pub fn wait_duration(&self, since: Option<Instant>, now: Instant) -> Duration {
        match since {
            None => Duration::from_secs(0),
            Some(last) => self.min_interval.saturating_sub(now.duration_since(last)),
        }
    }

    /// Tunggu sampai interval minimum sejak request terakhir terpenuhi,
    /// lalu catat waktu mulai request ini. Panggilan berurutan dari
    /// tugas yang sama dijamin berjarak minimal `min_interval`.
    pub async fn wait(&self) {
        let mut guard = self.last.lock().await;
        let now = Instant::now();
        let delay = self.wait_duration(*guard, now);
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        *guard = Some(Instant::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_then_caps() {
        assert_eq!(backoff_delay(0), Duration::from_secs(2));
        assert_eq!(backoff_delay(1), Duration::from_secs(2));
        assert_eq!(backoff_delay(2), Duration::from_secs(4));
        assert_eq!(backoff_delay(3), Duration::from_secs(8));
        assert_eq!(backoff_delay(4), Duration::from_secs(16));
        assert_eq!(backoff_delay(5), Duration::from_secs(32));
        assert_eq!(backoff_delay(6), Duration::from_secs(32));
        assert_eq!(backoff_delay(100), Duration::from_secs(32));
    }

    #[test]
    fn retry_after_parses_seconds_and_dates() {
        assert_eq!(
            retry_after_delay(Some("120")),
            Some(Duration::from_secs(120))
        );
        assert_eq!(retry_after_delay(Some("0")), Some(Duration::from_secs(0)));
        assert_eq!(
            retry_after_delay(Some(" 30 ")),
            Some(Duration::from_secs(30))
        );
        assert_eq!(retry_after_delay(None), None);
        assert_eq!(retry_after_delay(Some("abc")), None);
        // Tanggal HTTP di masa depan -> durasi positif.
        let future = retry_after_delay(Some("Fri, 01 Jan 2100 00:00:00 GMT"));
        assert!(future.is_some());
        assert!(future.unwrap() > Duration::from_secs(0));
        // Tanggal di masa lalu -> 0 (1 Jan 2000 adalah hari Sabtu).
        assert_eq!(
            retry_after_delay(Some("Sat, 01 Jan 2000 00:00:00 GMT")),
            Some(Duration::from_secs(0))
        );
    }

    #[test]
    fn retryable_statuses() {
        for s in [429u16, 529, 500, 502, 503] {
            assert!(is_retryable_status(s), "{s} harus retryable");
        }
        for s in [200u16, 400, 401, 403, 404, 422] {
            assert!(!is_retryable_status(s), "{s} tidak boleh retryable");
        }
    }

    #[test]
    fn pacer_interval_math() {
        let pacer = Pacer::new(Duration::from_millis(1500));
        assert_eq!(pacer.min_interval(), Duration::from_millis(1500));
        let now = Instant::now();
        assert_eq!(pacer.wait_duration(None, now), Duration::from_secs(0));
        assert_eq!(
            pacer.wait_duration(Some(now), now),
            Duration::from_millis(1500)
        );
        assert_eq!(
            pacer.wait_duration(Some(now - Duration::from_secs(2)), now),
            Duration::from_secs(0)
        );
        assert_eq!(
            pacer.wait_duration(Some(now - Duration::from_millis(500)), now),
            Duration::from_millis(1000)
        );
    }

    #[tokio::test]
    async fn pacer_wait_enforces_interval() {
        let pacer = Pacer::new(Duration::from_millis(100));
        let start = Instant::now();
        pacer.wait().await; // pertama: tanpa tunggu
        pacer.wait().await; // kedua: harus tunggu ~100ms
        assert!(start.elapsed() >= Duration::from_millis(100));
    }
}
