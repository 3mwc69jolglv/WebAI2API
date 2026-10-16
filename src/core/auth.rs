//! Auth generik: cookie jar, rotasi, persistensi, dan pool akun.
//!
//! Cookie adalah kredensial akun penuh: nilai cookie TIDAK PERNAH
//! di-log, dan file persistensi ditulis dengan permission 600.
//! Modul ini hanya berisi primitif generik — tidak ada endpoint,
//! nama cookie, atau alur refresh spesifik provider di sini.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

/// Kumpulan cookie `nama -> nilai`, diparse dari satu string header
/// `Cookie` (`"a=b; c=d"`) dan bisa di-render kembali ke header.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CookieJar {
    cookies: BTreeMap<String, String>,
}

impl CookieJar {
    /// Jar kosong.
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse string header `Cookie` (`"a=b; c=d"`) menjadi jar.
    ///
    /// Bagian tanpa `=` diabaikan; bila satu nama muncul dua kali,
    /// nilai terakhir yang menang; nilai boleh mengandung `=`
    /// (hanya `=` pertama yang jadi pemisah).
    pub fn parse(header: &str) -> Self {
        let mut jar = Self::new();
        for part in header.split(';') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let Some((name, value)) = part.split_once('=') else {
                continue;
            };
            let name = name.trim();
            if name.is_empty() {
                continue;
            }
            jar.cookies
                .insert(name.to_string(), value.trim().to_string());
        }
        jar
    }

    /// Ambil nilai cookie bernama, bila ada.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.cookies.get(name).map(String::as_str)
    }

    /// Set/overwrite satu cookie.
    pub fn set(&mut self, name: &str, value: &str) {
        self.cookies.insert(name.to_string(), value.to_string());
    }

    /// Hapus satu cookie, bila ada.
    pub fn remove(&mut self, name: &str) {
        self.cookies.remove(name);
    }

    /// Benar bila jar tidak berisi cookie apa pun.
    pub fn is_empty(&self) -> bool {
        self.cookies.is_empty()
    }

    /// Jumlah cookie dalam jar.
    pub fn len(&self) -> usize {
        self.cookies.len()
    }

    /// Render kembali ke nilai header `Cookie` (`"a=b; c=d"`),
    /// dengan nama terurut agar output deterministik.
    pub fn render(&self) -> String {
        self.cookies
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// Merge satu header `Set-Cookie` dari respons upstream
    /// (rotasi): hanya pasangan `nama=nilai` pertama yang dipakai,
    /// atribut (`Path`, `Expires`, `HttpOnly`, ...) diabaikan.
    pub fn merge_set_cookie(&mut self, set_cookie: &str) {
        let Some(first) = set_cookie.split(';').next() else {
            return;
        };
        let first = first.trim();
        let Some((name, value)) = first.split_once('=') else {
            return;
        };
        let name = name.trim();
        if name.is_empty() {
            return;
        }
        self.cookies
            .insert(name.to_string(), value.trim().to_string());
    }

    /// Merge banyak header `Set-Cookie` (rotasi), urut diterimanya.
    ///
    /// Dipakai test + tooling rotasi mendatang; jalur provider
    /// memakai [`CookieJar::merge_set_cookie`] per header.
    #[allow(dead_code)]
    pub fn merge_set_cookies<'a, I: IntoIterator<Item = &'a str>>(&mut self, headers: I) {
        for h in headers {
            self.merge_set_cookie(h);
        }
    }

    /// Simpan jar ke file dalam format header `Cookie`, dengan
    /// permission 600 (hanya pemilik yang bisa baca/tulis) agar
    /// rotasi tidak hilang saat restart tanpa mengekspos kredensial.
    pub fn persist(&self, path: &Path) -> std::io::Result<()> {
        fs::write(path, self.render())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    }

    /// Muat jar dari file yang ditulis [`CookieJar::persist`].
    ///
    /// Dipakai test + tooling rotasi mendatang (startup memakai
    /// cookie dari env, bukan dari file).
    #[allow(dead_code)]
    pub fn load(path: &Path) -> std::io::Result<Self> {
        let content = fs::read_to_string(path)?;
        Ok(Self::parse(&content))
    }
}

impl From<&str> for CookieJar {
    /// Parse string header `Cookie`, sama seperti [`CookieJar::parse`].
    fn from(header: &str) -> Self {
        Self::parse(header)
    }
}

/// Persistensi rotasi cookie, best-effort: dipakai provider setelah
/// merge `Set-Cookie` dari respons upstream. Gagal tulis (mis.
/// direktori tidak ada / permission) hanya dicatat di log debug —
/// TIDAK PERNAH menggagalkan request, dan TIDAK PERNAH me-log nilai
/// cookie. `path == None` berarti persistensi tidak dikonfigurasi
/// (rotasi hanya bertahan in-memory sampai restart).
pub fn persist_jar_best_effort(jar: &CookieJar, path: Option<&Path>) {
    let Some(path) = path else {
        return;
    };
    if let Err(e) = jar.persist(path) {
        // Pesan ini sengaja tidak memuat path/nilai apa pun.
        tracing::debug!(error = %e, "gagal persist rotasi cookie (best-effort, diabaikan)");
    }
}

/// State satu akun dalam pool: cookie-nya plus kesehatan runtime.
///
/// Failover hanya aman pre-first-byte (sebelum ada byte respons);
/// setelah stream jalan, request tidak boleh di-retry ke akun lain.
//
// Phase 5: pool multi-akun belum di-wire ke provider mana pun (v0.2
// hanya memakai satu akun per provider + rotasi cookie in-place);
// `AccountState`/`AccountPool` adalah primitif siap pakai untuk itu.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct AccountState {
    /// Cookie akun ini.
    pub jar: CookieJar,
    /// Sampai kapan akun ini di-cooldown (tidak dipilih selector).
    pub cooldown_until: Option<Instant>,
    /// Jumlah kegagalan berturut-turut (direset saat sukses).
    pub consecutive_failures: u32,
    /// Jumlah request yang sedang berjalan di akun ini.
    pub inflight: usize,
}

// Phase 5: lihat catatan pada `AccountState` — belum di-wire.
#[allow(dead_code)]
impl AccountState {
    /// State akun baru dari jar-nya: sehat, tanpa cooldown, tanpa inflight.
    pub fn new(jar: CookieJar) -> Self {
        Self {
            jar,
            cooldown_until: None,
            consecutive_failures: 0,
            inflight: 0,
        }
    }

    /// Benar bila akun boleh dipilih pada `now` (cooldown sudah lewat).
    pub fn is_available(&self, now: Instant) -> bool {
        match self.cooldown_until {
            None => true,
            Some(until) => now >= until,
        }
    }

    /// Durasi cooldown untuk kegagalan berikutnya: eksponensial
    /// `60s * 2^(failures-1)`, dibatasi 3600s. `failures == 0` -> 0s.
    pub fn cooldown_duration(&self) -> Duration {
        if self.consecutive_failures == 0 {
            return Duration::from_secs(0);
        }
        let shift = self.consecutive_failures.saturating_sub(1).min(6);
        Duration::from_secs(60u64.saturating_mul(1u64 << shift)).min(Duration::from_secs(3600))
    }

    /// Catat kegagalan HTTP `status`: menaikkan hitungan gagal, dan
    /// memasang cooldown eksponensial hanya untuk 429/403.
    pub fn mark_failure(&mut self, status: u16, now: Instant) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        if status == 429 || status == 403 {
            let cooldown = self.cooldown_duration();
            self.cooldown_until = Some(now + cooldown);
        }
    }

    /// Catat sukses: reset hitungan gagal dan hapus cooldown.
    pub fn mark_success(&mut self) {
        self.consecutive_failures = 0;
        self.cooldown_until = None;
    }

    /// Tandai satu request mulai berjalan di akun ini.
    pub fn acquire(&mut self) {
        self.inflight = self.inflight.saturating_add(1);
    }

    /// Tandai satu request selesai berjalan di akun ini.
    pub fn release(&mut self) {
        self.inflight = self.inflight.saturating_sub(1);
    }
}

/// Pool akun per provider.
///
/// Selector memilih akun yang tersedia (`is_available`) dengan
/// inflight terendah, dan langsung menandai inflight (`acquire`)
/// agar request berikutnya menyeimbang ke akun lain. Penelepon
/// wajib memanggil [`AccountPool::release`] saat request selesai,
/// dan [`AccountPool::mark_failure`]/[`AccountPool::mark_success`]
/// sesuai hasilnya.
// Phase 5: lihat catatan pada `AccountState` — belum di-wire.
#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct AccountPool {
    accounts: Vec<AccountState>,
}

// Phase 5: lihat catatan pada `AccountState` — belum di-wire.
#[allow(dead_code)]
impl AccountPool {
    /// Pool baru dari daftar state akun.
    pub fn new(accounts: Vec<AccountState>) -> Self {
        Self { accounts }
    }

    /// Jumlah akun dalam pool.
    pub fn len(&self) -> usize {
        self.accounts.len()
    }

    /// Benar bila pool tidak berisi akun apa pun.
    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty()
    }

    /// Pilih akun tersedia dengan inflight terendah dan tandai
    /// inflight-nya. Mengembalikan index akun, atau `None` bila
    /// semua akun sedang cooldown / pool kosong.
    pub fn pick(&mut self, now: Instant) -> Option<usize> {
        let mut best: Option<usize> = None;
        for (i, acc) in self.accounts.iter().enumerate() {
            if !acc.is_available(now) {
                continue;
            }
            let better = match best {
                None => true,
                Some(b) => acc.inflight < self.accounts[b].inflight,
            };
            if better {
                best = Some(i);
            }
        }
        if let Some(i) = best {
            self.accounts[i].acquire();
        }
        best
    }

    /// Akses state akun ke-`index` (untuk membaca/mengubah jar,
    /// mis. merge `Set-Cookie` hasil rotasi).
    pub fn get(&self, index: usize) -> Option<&AccountState> {
        self.accounts.get(index)
    }

    /// Akses mutable state akun ke-`index`.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut AccountState> {
        self.accounts.get_mut(index)
    }

    /// Tandai request di akun `index` selesai.
    pub fn release(&mut self, index: usize) {
        if let Some(acc) = self.accounts.get_mut(index) {
            acc.release();
        }
    }

    /// Catat kegagalan HTTP `status` untuk akun `index`.
    pub fn mark_failure(&mut self, index: usize, status: u16, now: Instant) {
        if let Some(acc) = self.accounts.get_mut(index) {
            acc.mark_failure(status, now);
        }
    }

    /// Catat sukses untuk akun `index`.
    pub fn mark_success(&mut self, index: usize) {
        if let Some(acc) = self.accounts.get_mut(index) {
            acc.mark_success();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cookie_jar_parse_and_render() {
        let mut jar = CookieJar::parse("b=2; a=1;  c=3 ");
        assert_eq!(jar.get("a"), Some("1"));
        assert_eq!(jar.get("b"), Some("2"));
        assert_eq!(jar.get("c"), Some("3"));
        // Render terurut berdasarkan nama.
        assert_eq!(jar.render(), "a=1; b=2; c=3");
        // set/remove ter-cover.
        jar.set("d", "4");
        assert_eq!(jar.get("d"), Some("4"));
        jar.remove("d");
        assert_eq!(jar.get("d"), None);
    }

    #[test]
    fn cookie_jar_parse_edge_cases() {
        assert!(CookieJar::parse("").is_empty());
        assert!(CookieJar::parse(";;;").is_empty());
        // Bagian tanpa '=' diabaikan; nilai boleh mengandung '='.
        let jar = CookieJar::parse("lonely; k=v=w; dup=1; dup=2");
        assert_eq!(jar.get("k"), Some("v=w"));
        assert_eq!(jar.get("dup"), Some("2"));
        assert_eq!(jar.get("lonely"), None);
        assert_eq!(jar.len(), 2);
    }

    #[test]
    fn cookie_jar_merge_set_cookie() {
        let mut jar = CookieJar::parse("a=1; b=2");
        // Rotasi: nilai lama diganti, atribut diabaikan.
        jar.merge_set_cookie("a=9; Path=/; HttpOnly");
        assert_eq!(jar.get("a"), Some("9"));
        assert_eq!(jar.get("b"), Some("2"));
        // Cookie baru dari upstream ditambahkan.
        jar.merge_set_cookies(["c=3; Secure", "b=8; Path=/"]);
        assert_eq!(jar.get("c"), Some("3"));
        assert_eq!(jar.get("b"), Some("8"));
        // Header tanpa '=' diabaikan.
        jar.merge_set_cookie("garbage");
        assert_eq!(jar.len(), 3);
    }

    #[test]
    fn cookie_jar_persist_roundtrip_with_strict_permissions() {
        let jar = CookieJar::parse("session=abc123; other=x");
        let path =
            std::env::temp_dir().join(format!("aiweb2api-test-cookies-{}.txt", std::process::id()));
        jar.persist(&path).expect("persist gagal");
        let loaded = CookieJar::load(&path).expect("load gagal");
        assert_eq!(loaded, jar);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&path).expect("metadata").permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "file cookie harus chmod 600");
        }
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn account_cooldown_exponential_on_429() {
        let mut acc = AccountState::new(CookieJar::parse("a=1"));
        let now = Instant::now();
        assert!(acc.is_available(now));
        acc.mark_failure(429, now);
        assert_eq!(acc.consecutive_failures, 1);
        assert!(!acc.is_available(now));
        // Cooldown pertama: 60 detik.
        assert!(acc.is_available(now + Duration::from_secs(61)));
        // Kegagalan kedua (setelah cooldown lewat) menggandakan cooldown.
        acc.mark_failure(429, now + Duration::from_secs(61));
        assert!(!acc.is_available(now + Duration::from_secs(61 + 119)));
        assert!(acc.is_available(now + Duration::from_secs(61 + 121)));
        // Sukses mereset semuanya.
        acc.mark_success();
        assert_eq!(acc.consecutive_failures, 0);
        assert!(acc.is_available(now));
    }

    #[test]
    fn account_failure_without_cooldown_for_other_statuses() {
        let mut acc = AccountState::new(CookieJar::parse("a=1"));
        let now = Instant::now();
        acc.mark_failure(500, now);
        assert_eq!(acc.consecutive_failures, 1);
        // 500 menaikkan hitungan tapi tidak memasang cooldown.
        assert!(acc.is_available(now));
    }

    #[test]
    fn pool_picks_lowest_inflight_and_skips_cooldown() {
        let mut pool = AccountPool::new(vec![
            AccountState::new(CookieJar::parse("a=1")),
            AccountState::new(CookieJar::parse("b=2")),
        ]);
        assert_eq!(pool.len(), 2);
        assert!(!pool.is_empty());
        assert!(AccountPool::new(vec![]).is_empty());
        let now = Instant::now();
        // Pick pertama: index 0 (seri -> yang pertama), inflight 0 -> 1.
        assert_eq!(pool.pick(now), Some(0));
        // Pick kedua: index 1 punya inflight lebih rendah.
        assert_eq!(pool.pick(now), Some(1));
        // get/get_mut membaca/mengubah jar akun.
        assert_eq!(pool.get(0).expect("akun 0 ada").jar.get("a"), Some("1"));
        pool.get_mut(1).expect("akun 1 ada").jar.set("z", "9");
        assert_eq!(pool.get(1).expect("akun 1 ada").jar.get("z"), Some("9"));
        // Cooldown akun 0 via 429; selector harus melewatinya.
        pool.mark_failure(0, 429, now);
        pool.release(0);
        pool.release(1);
        assert_eq!(pool.pick(now), Some(1));
        // Semua cooldown -> None.
        pool.mark_failure(1, 403, now);
        pool.release(1);
        assert_eq!(pool.pick(now), None);
        // mark_success mengembalikan akun 0 ke pool.
        pool.mark_success(0);
        assert_eq!(pool.pick(now), Some(0));
    }
}
