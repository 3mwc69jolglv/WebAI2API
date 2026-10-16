# Desain Refactor Modular aiweb2api (v0.2)

Tanggal: 2026-10-16. Dasar: riset GitHub web-to-API (lihat `research-web2api-2026-10-16.md`) + requirement Altria: kode rapih & modular (satu file per provider), tiap provider bisa di-tuning sendiri, fitur wajib: **vision**, **thinking/reasoning sebelum menjawab**, **internet search**.

Keputusan Altria: **refactor project lama**, bukan project baru.

---

## 1. Prinsip desain

1. **Satu provider = satu file utama** (`providers/gemini.rs`, `chatgpt.rs`, `claude.rs`, `grok.rs`). File provider boleh dibelah lagi hanya jika melewati ~600 baris (mis. `chatgpt/transport.rs` vs `chatgpt/stream.rs`) — pemisahan karena ukuran, bukan karena fitur.
2. **Semua yang shared tinggal di modul bersama**, tidak diduplikasi per provider: event stream, auth/cookie, HTTP client, upload media, katalog model, retry/rate-limit.
3. **Tidak ada hardcode yang rapuh**: daftar model diambil live dari tiap provider; hash build frontend, nama tool ter-obfuscasi, dan nilai hasil capture TIDAK di-hardcode — selalu diturunkan dari capture segar atau dari respons live.
4. **Error upstream mentah selalu dimunculkan** (status + snippet), tidak pernah ditelan. Sampling param yang tidak bisa dipenuhi Web (`temperature`, `max_tokens`) ditolak eksplisit (400 `unsupported_parameter`, gaya gptweb2api), bukan diam-diam diabaikan.
5. **Usage token selalu dilabeli estimasi** — tidak ada provider web yang mengembalikan token count asli.
6. **Jangan pernah me-replay generasi yang sudah disubmit** saat retry (aturan Grok; berlaku umum).
7. **Cookie adalah kredensial akun penuh**: tidak pernah di-log, tidak pernah dibagikan, disimpan dengan permission ketat, dan dirotasi/disimpen ulang saat upstream me-refresh-nya.

---

## 2. Keadaan sekarang (v0.1.0, hasil audit 2026-10-16)

Yang sudah ada:
- Modularity level file sudah ada: `providers/{chatgpt,claude,gemini,grok}.rs` + trait `Provider` di `providers/mod.rs`.
- `Provider` trait: `id()`, `models()`, `chat()`, `generate_image()` (default: tidak didukung).
- Grok sudah punya `x-statsig-id` (fallback lokal `x1:` + signer eksternal via env), `disableSearch: false`, dan parsing NDJSON dengan flag `isThinking`.
- Gemini sudah mengekstrak URL gambar dari respons (untuk image generation).
- Image generation endpoint `/v1/images/generations` sudah ada (ChatGPT + Gemini; Grok/Claude → 400).

Yang belum ada / masih lemah:
- `ChatInput` hanya `{prompt: String, model: String}` — prompt sudah diratakan. **Tidak ada** messages array, content parts (teks/gambar), opsi thinking, opsi search, atau tools.
- `TextStream` hanya stream `String` — **tidak ada event bertipe**, jadi thinking tidak bisa dibedakan dari jawaban (di Grok, token thinking malah dibuang: filter `!is_thinking`).
- Daftar model **hardcoded statis** per provider — tidak ada katalog live.
- Auth: string cookie mentah dari env. **Tidak ada** rotasi cookie, persistensi hasil rotasi, pool multi-akun, atau health scoring.
- HTTP: reqwest polos + UA Chrome. **Tidak ada** TLS impersonation (hanya komentar TODO soal migrasi ke `wreq`).
- Thinking: belum ada di provider mana pun (Claude tanpa `paprika_mode`; ChatGPT tanpa `thinking_effort`/slug `-thinking`; Gemini tanpa toggle thinking).
- Search: belum ada toggle deterministik di provider mana pun (hanya `disableSearch: false` di Grok).
- Vision input: belum ada di provider mana pun (tidak ada upload-then-reference).
- ChatGPT: perlu audit — apakah handshake sentinel/PoW + conduit sudah ada atau belum (tidak terlihat di grep; verifikasi di kode saat Fase 2).

---

## 3. Arsitektur target

### 3.1 Struktur file

```
src/
  main.rs            # wiring: env -> registry -> server (tetap tipis)
  server.rs          # route axum, auth Bearer lokal, SSE (tetap)
  openai.rs          # skema request/response OpenAI + translasi Event -> SSE (diperluas)
  error.rs           # ProviderError (tambah: RateLimited, AuthExpired, Upstream, UnsupportedParameter)
  providers/
    mod.rs           # trait Provider v2 + Registry + ChatInput v2 + Event (tetap jadi pintu masuk)
    gemini.rs        # Gemini saja
    chatgpt.rs       # ChatGPT saja (+ submodul bila >600 baris: transport.rs, stream.rs)
    claude.rs        # Claude saja
    grok.rs          # Grok saja
  core/              # BARU: modul bersama (bukan per provider)
    mod.rs
    events.rs        # event stream ternormalisasi (lihat 3.2)
    auth.rs          # CookieJar + rotasi + persistensi + pool akun (lihat 3.3)
    http.rs          # HTTP client builder dengan TLS impersonation (lihat 3.4)
    media.rs         # upload-then-reference generik (download URL/data-URI -> id lampiran)
    catalog.rs       # cache katalog model live per provider (TTL + refresh)
    retry.rs         # backoff eksponensial + cooldown akun + pacing antar-request
```

### 3.2 Event stream ternormalisasi (ganti `TextStream = Stream<String>`)

```rust
pub enum Event {
    Text(String),              // delta jawaban
    Reasoning(String),          // delta thinking (sebelum/di luar jawaban)
    Image { bytes: Vec<u8>, mime: String }, // gambar hasil generate (diunduh server-side)
    Annotation(Annotation),     // kutipan / sumber search / sitasi
    Usage { prompt: u64, completion: u64, reasoning: u64, estimated: bool },
    Done,                       // penanda akhir stream (sebelum [DONE] OpenAI)
}
pub struct Annotation { pub kind: String, pub text: String, pub url: Option<String> }
pub type EventStream = Pin<Box<dyn Stream<Item = Result<Event, ProviderError>> + Send>>;
```

Tiap provider mem-parse format stream aslinya menjadi `Event`:
- Gemini: frame `wrb.fr` → inner JSON; thought tiba **sebelum** teks body → `Reasoning`, lalu `Text`.
- ChatGPT: SSE JSON-patch v1 + replay segmen → ledger emisi per-turn + deteksi replay (exactly-once, berurutan).
- Claude: dua bentuk SSE (legacy `type: completion` + modern content-block). `thinking_delta` / `thinking_summary_delta` / `thinking_cut_off_delta` → `Reasoning`.
- Grok: NDJSON; token dengan `isThinking` → `Reasoning` (bukan dibuang); `isFinished: true` → `Done`.

`openai.rs` menerjemahkan `Event` ke SSE OpenAI: `Reasoning` → `reasoning_content` (delta), `Annotation` → field sitasi/kutipan, `Usage` → objek `usage` (dengan flag estimasi di log, bukan di wire — wire OpenAI tidak punya flag itu; dokumentasikan di README).

### 3.3 Input v2 (ganti `ChatInput {prompt, model}`)

```rust
pub struct ChatInput {
    pub messages: Vec<Message>,   // histori utuh, tidak diratakan di server
    pub model: String,
    pub thinking: Thinking,       // Off | Auto | On { level: Option<ThinkingLevel> }
    pub search: Search,           // Off | Auto | On
    pub stream: bool,
}
pub struct Message { pub role: Role, pub parts: Vec<Part> }
pub enum Part { Text(String), ImageUrl(ImageSource) }
pub enum ImageSource { Url(String), DataUri { mime: String, base64: String } }
pub enum Thinking { Off, Auto, On { level: Option<u8> } }  // level: mapping per provider
pub enum Search { Off, Auto, On }
```

Kompatibilitas mundur: `server.rs` tetap menerima request OpenAI lama (messages teks saja) dan membangun `ChatInput` dengan `thinking: Auto`, `search: Auto`. Client lama tidak rusak.

### 3.4 Auth (`core/auth.rs`)

- `CookieJar`: parse string cookie → map nama→nilai; render kembali ke header `Cookie`; bisa merge `Set-Cookie` dari respons (rotasi) dan **persist ke disk** (file cookie, chmod 600, format sama dengan input) agar rotasi tidak hilang saat restart.
- `AccountPool`: daftar akun per provider (tiap akun = CookieJar + state: sehat / cooldown-sampai / gagal-berturut-turut). Selector: akun sehat dengan inflight terendah; cooldown eksponensial saat 429/403; failover pre-first-byte (sebelum ada byte respons, aman untuk retry).
- Per-provider (dari riset):
  - Gemini: `__Secure-1PSID` + `__Secure-1PSIDTS` + `SAPISID` (+ scrape `SNlM0e` → `at`, `bl` dari halaman). Rotasi: keepalive `RotateCookies` berkala; export dari Firefox lebih aman daripada Chrome (DBSC). Deteksi degradasi diam-diam: kalau respons tidak lagi berisi model yang diminta / `SNlM0e` hilang → tandai akun basi.
  - ChatGPT: `access_token` JWT (mint via `/api/auth/session` dari `__Secure-next-auth.session-token`, atau refresh token OAuth) + `_puid` + `ChatGPT-Account-ID`. Refresh proaktif sebelum expiry; 401 → refresh sekali lalu retry.
  - Claude: mode full-cookie (seluruh cookie jar) sebagai jalur utama; Bearer `sessionKey` saja sebagai fallback terdegradasi. 401 → tandai akun perlu re-paste cookie (tidak ada refresh background).
  - Grok: cookie `sso` (+ `grok_device_id`, `__cf_bm`, `cf_clearance` dari pemanasan `GET grok.com/`) + header `x-statsig-id` (sudah ada: fallback lokal `x1:` + signer eksternal; pertahankan).

### 3.5 HTTP (`core/http.rs`)

- Ganti `reqwest` polos dengan client ber-impersonasi TLS. Kandidat Rust: **`rquest`** (prioritas evaluasi pertama — API mirip reqwest, tanpa libclang) atau `reqwest-impersonate` / `wreq` (butuh libclang saat build — dicatat di TODO lama). Kriteria pilih: impersonasi Chrome stabil + bisa dipakai di sandbox build + tidak menambah beban build berlebihan.
- Fallback yang direncanakan (bukan di v0.2 kecuali terbukti perlu): browser-relay via CDP/Playwright untuk ChatGPT bila POST direct 403 "Unusual activity" meski sentinel benar (bukti octo-lex). v0.2 menyiapkan titik ekstensi (trait `Transport`), bukan implementasi relay penuh.
- Header browser realistis per provider (UA + `sec-ch-ua`/`sec-fetch-*`/`Origin`/`Referer` sesuai kebutuhan provider), `oai-device-id` untuk ChatGPT, dsb. — konstanta header per provider tinggal di file provider-nya, bukan di `core`.

### 3.6 Katalog model live (`core/catalog.rs`)

- Tiap provider mengambil katalog dari endpoint aslinya (Gemini: `otAQ7b`; ChatGPT: `/backend-api/models`; Claude: `/edge-api/bootstrap/{org}/app_start`; Grok: `/rest/modes` + daftar built-in per tier).
- Cache in-memory dengan TTL (mis. 1 jam) + refresh on-demand saat model diminta tidak ditemukan di cache. `/v1/models` melayani dari cache; gagal upstream → 502 (tanpa fallback statis, gaya aurora Claude2api).
- `Registry::find_model` tetap bekerja, tapi atas daftar live, bukan hardcoded.

### 3.7 Pemetaan fitur per provider (dari riset)

| Fitur | Gemini | ChatGPT | Claude | Grok |
|---|---|---|---|---|
| **Vision** | Upload resumable ke `content-push.googleapis.com` → referensi WIZ blob. Butuh cookie. Re-upload per request. | File-service 3 langkah (`POST /backend-api/files` → `PUT upload_url` → `POST …/uploaded`) → `image_asset_pointer: file-service://<id>` di `multimodal_text`. `PUT` blob mungkin 403 di jaringan tertentu (error 1010) → fallback: laporkan error jelas. | Wiggle upload (`/conversations/{id}/wiggle/upload-file`) → dokumen: `convert_document` → `attachments`; gambar: skip convert, pakai `files` (`file_uuid`). `file_size` harus integer. | `upload-file-v2/direct` → `fileMetadataId` → `fileAttachments`. Edit Imagine: `mediaGenInput.imageToImage.inputAssets`. Terima URL atau Data-URI; base64 mentah tanpa wrapper Data-URI **gagal**. |
| **Thinking** | Toggle UI `-thinking`, ortogonal terhadap model dasar. Thought stream sebagai `Reasoning` sebelum body. **Jangan** implementasikan `@think=N` (terbukti salah — itu index turn). | Slug model `-thinking` dan/atau `thinking_effort` (`none/minimal/low/medium/high/xhigh/max`). Validasi effort terhadap katalog per-model; **jangan** percaya echo `thinking_effort` (cermin request). Stream: `thoughts` / `reasoning_recap` → `Reasoning`. | Setting `paprika_mode: "extended" \| null` (level akun/percakapan, bukan param `thinking:{budget}`). Reset ke `null` jika model baru tidak mendukung. `thinking_delta` dkk → `Reasoning`. Error khusus: `thinking_messages_rate_limit_exceeded`. | `isReasoning: true` dan/atau varian `-thinking` dan/atau `reasoning_effort`; `none` menonaktifkan. Token `isThinking` → `Reasoning`. Waspadai stream "missing-thinking" (jawaban tanpa reasoning) → perlakukan sebagai kualitas terdegradasi, bukan sukses penuh. |
| **Search** | Native (warisan backend), tanpa flag yang didokumentasikan. Mode `Search::On` = kirim seperti biasa + dokumentasikan bahwa toggle deterministik **belum terverifikasi**; butuh capture live dengan toggle UI menyala. | Model auto-putuskan via tool `search`. `Search::On` = instruksi sistem tambahan yang meminta pemakaian search + kumpulkan `citations`/`search_result_groups` dari stream → `Annotation`. **Tidak ada** flag `web_search: true` di protokol yang didokumentasikan. Opsi lanjutan (bukan v0.2): emulasi `<tool_call>` + tool search sisi-client. | Setting percakapan `enabled_web_search: true` (sticky) via settings `PUT` / `create_conversation_params`. Entri web-search di array `tools` memakai nama ter-obfuscasi — **jangan hard-code**; andalkan jalur settings. `compass_mode: "advanced"` (Deep Research) auto-mengaktifkan search — **di luar v0.2**, dicatat sebagai follow-up. | `disableSearch: false` + `toolOverrides` (web/X search) + key preset DeepSearch. Bukti konflik (capture 2026-08 tidak menemukan toggle di lane WebSocket) → verifikasi live; bila `toolOverrides` tidak berpengaruh, dokumentasikan. |

### 3.8 Retry, pacing, rate limit (`core/retry.rs`)

- Backoff eksponensial untuk `429/529/500/502/503` (2/4/8/16/32 dtk, ≤5 retry, hormati `Retry-After`).
- Pacing antar-request mirip manusia (lantai ~1,5 dtk untuk Claude; pace lembut untuk Gemini: ≤10 req/menit bila memungkinkan).
- Cooldown akun + failover pre-first-byte (lihat 3.4). Jangan retry setelah first-byte diterima (stream sudah jalan).
- 401 → alur refresh/re-paste per provider (lihat 3.4), lalu retry sekali.
- 403 Cloudflare / anti-bot → tandai transport bermasalah, jangan spam retry; munculkan pesan jelas (kemungkinan butuh clearance baru / relay browser).

---

## 4. Rencana migrasi bertahap

**Fase 0 — Baseline (tanpa mengubah perilaku).** Tambah test asap: `cargo build` bersih + smoke test route yang ada. Commit: "chore: baseline v0.1.0 sebelum refactor".

**Fase 1 — Core bersama.** Buat `src/core/` (`events.rs`, `auth.rs`, `http.rs`, `media.rs`, `catalog.rs`, `retry.rs`). Pindahkan `http_client()` dan konstanta shared dari `providers/mod.rs` ke `core/http.rs`. `Provider` trait v2 + `ChatInput` v2 + `Event` didefinisikan, tapi provider lama masih jalan lewat adapter sementara (konversi `ChatInput` v2 → prompt rata untuk provider yang belum di-migrasi). Target: build hijau, perilaku tidak berubah.

**Fase 2 — Migrasi per provider, satu per satu (urutan sesuai keyakinan).**
1. **Gemini** — protokol paling stabil & sudah dipahami (`StreamGenerate`, `wrb.fr`). Tambah: katalog live, `Event` (thought → `Reasoning`), upload gambar (vision input).
2. **Claude** — tambah: katalog live (`app_start`), `paprika_mode` (thinking), wiggle upload (vision), `enabled_web_search` (search), parsing dua bentuk SSE.
3. **ChatGPT** — paling kompleks. Audit dulu: sentinel/PoW + conduit + resume sudah ada atau belum. Tambah: katalog live, SSE patch v1 + ledger emisi, `thinking_effort` + slug `-thinking`, file-service upload (vision), pengumpulan sitasi (search).
4. **Grok** — sudah punya statsig + NDJSON. Tambah: katalog live, `Reasoning` dari `isThinking` (hentikan pembuangan token thinking), upload-file-v2 (vision), `toolOverrides` (search).
- Tiap provider: migrasi → build → smoke test → commit terpisah ("refactor(gemini): …"). Hapus adapter sementara provider itu setelah migrasinya hijau.

**Fase 3 — Fitur wajib end-to-end.** Vision, thinking, search di semua provider sesuai tabel 3.7 + test manual per fitur (prompt gambar, prompt yang memicu thinking, prompt yang butuh info terkini).

**Fase 4 — Hardening.** TLS impersonation (`rquest` dst), pool akun + rotasi cookie + persistensi, pacing + cooldown, penolakan eksplisit param tak didukung, pelabelan usage estimasi, dokumentasi README + `.env.example` diperbarui.

**Fase 5 — Validasi live (butuh Altria).** Tidak bisa dari sandbox (egress proxy MITM kena 403 di Grok/ChatGPT/Claude; Gemini juga sensitif). Validasi di jaringan/device Altria dengan cookie yang valid: tiap provider × (teks, vision, thinking, search, streaming). Hasil validasi dicatat di `docs/validasi-live.md` (pass/fail per sel).

---

## 5. Gap yang masih butuh capture live (jangan tebak — capture dulu)

1. ChatGPT: varian PoW (SHA-256 vs SHA3-512+fingerprint), diff body toggle search UI, set header `oai-client-*` saat ini, apakah `PUT` blob browserless lolos, slug model + `thinking_efforts` per-model untuk akun target.
2. Gemini: routing Pro akun free vs berbayar, guest mode tanpa cookie (klaim satu repo).
3. Claude: nama field multipart wiggle-upload, payload `tools` aurora, apakah `enabled_web_search` saja cukup untuk search end-to-end, apakah `provider-claude` chat-cli benar-benar upload file.
4. Grok: apakah `toolOverrides` search berpengaruh di lane REST saat ini, gating tier Imagine (Basic vs berbayar).
5. Umum: nilai `bl`/`SNlM0e` Gemini, `anthropic-client-sha` Claude, dan semua nilai turunan frontend — selalu dari capture segar.

---

## 6. Yang TIDAK dilakukan di refactor ini

- Tidak ada UI/dashboard baru, tidak ada database (pool akun in-memory + file cookie saja).
- Tidak ada Deep Research (Claude `compass_mode`), video generation, TTS, atau tool-calling generik — dicatat sebagai follow-up, bukan v0.2.
- Tidak ada browser-relay penuh (hanya titik ekstensi `Transport`).
- Tidak mengubah kontrak API OpenAI yang sudah ada (route + bentuk respons tetap; hanya `reasoning_content` ditambahkan untuk thinking).
- Tidak meminta/menyimpan secret produksi baru — cookie yang sudah ada dipakai; rotasi cookie lama yang pernah terlihat di chat tetap menjadi tanggung jawab Altria (status rotasi tidak diketahui sejak 2026-10-05).
