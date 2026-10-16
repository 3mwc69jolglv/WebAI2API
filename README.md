# AiWeb2API

API mandiri yang kompatibel dengan OpenAI (`/v1/chat/completions`, `/v1/models`, `/v1/images/generations`)
di atas **akun web** ChatGPT, Claude, Gemini, dan Grok. Autentikasi memakai
**cookie browser** — tanpa API key resmi, tanpa ketergantungan ke 9router.

Ditulis murni dengan **Rust** (axum + tokio + reqwest).

> ⚠️ **Penting**
> - Cookie = **kredensial penuh** akunmu. Jangan pernah membagikannya ke siapa pun,
>   jangan paste di chat, jangan commit ke git.
> - Ini memakai endpoint web **tidak resmi** yang bisa berubah sewaktu-waktu dan
>   bisa melanggar ketentuan layanan tiap provider. Risiko: logout paksa,
>   rate limit, verifikasi tambahan, sampai pemblokiran akun. Gunakan akun
>   yang kamu rela ambil risikonya, dan wajar dalam pemakaian.

## Cara kerja singkat

| Provider | Auth | Alur |
|---|---|---|
| ChatGPT | cookie `chatgpt.com` → tukar jadi access token via `/api/auth/session` | `POST /backend-api/f/conversation` (SSE patch v1) |
| Claude | cookie `claude.ai` (berisi `sessionKey`) | buat conversation → `POST .../completion` (SSE) |
| Gemini | cookie `google.com` (minimal `__Secure-1PSID`) → scrape `SNlM0e`/`cfb2h`/`FdrFJe` | `POST .../StreamGenerate` (batchexecute) |
| Grok | cookie `sso` dari `grok.com` | `POST /rest/app-chat/conversations/new` (NDJSON) |

Semua chat bersifat **stateless** (riwayat `messages` dikirim ulang tiap
request; mode temporary/riwayat mati supaya tidak mengotori histori akun).

## Instalasi & jalan

```bash
# 1. Masuk ke folder proyek
cd aiweb2api

# 2. Siapkan kredensial (JANGAN commit file ini!)
cp .env.example .env
nano .env   # isi cookie yang kamu butuhkan

# 3. Build & jalan
cargo run --release
# Server: http://127.0.0.1:8080  (atau AIW2A_PORT)
```

Atau pakai `./start.sh` (build bila perlu, lalu jalankan server dari `.env`).

### Mengambil cookie

Buka situs provider di browser **dalam keadaan login**, lalu ambil cookie
dengan salah satu cara:

**Cara cepat (disarankan):** DevTools (`F12`) → tab **Network** → refresh
halaman → klik salah satu request ke domain provider → bagian
*Request Headers* → klik kanan nilai `Cookie` → *Copy value*. Hasilnya sudah
dalam format siap tempel (`nama1=nilai1; nama2=nilai2; ...`).

**Cara manual:** DevTools → tab **Application** (Chrome) / **Storage**
(Firefox) → **Cookies** → pilih domain provider → salin tiap pasangan
nama=nilai, lalu gabung dengan `; `.

Yang diambil per provider:

| Provider | Yang disalin | Ke variabel |
|---|---|---|
| ChatGPT | **full cookie header** `chatgpt.com` — wajib ada `__Secure-next-auth.session-token` (bukan blob `ois1.` saja; itu hanya cookie `__Secure-oai-is` untuk header `x-oai-is`) | `AIW2A_CHATGPT_COOKIES` |
| Claude | nilai `sessionKey` saja, atau seluruh string cookie `claude.ai` | `AIW2A_CLAUDE_COOKIES` |
| Gemini | seluruh cookie `google.com` (wajib ada `__Secure-1PSID`) | `AIW2A_GEMINI_COOKIES` |
| Grok | **hanya nilai** cookie `sso` | `AIW2A_GROK_SSO` |

Catatan Gemini: cookie login Google tersimpan di domain `google.com`, bukan
`gemini.google.com` — kalau pakai cara manual, pastikan memilih
*Cookies → https://google.com*.

> Cookie kedaluwarsa secara berkala (terutama setelah logout/ganti password).
> Kalau API mengembalikan `401 provider_unauthorized`, ambil ulang cookie dari browser.

### Rotasi cookie

Upstream kadang me-refresh cookie lewat header `Set-Cookie` di respons.
Server me-merge rotasi itu ke cookie jar in-memory tiap provider (jadi
request berikutnya memakai cookie segar) dan — bila dikonfigurasi —
menulisnya ke file persistensi (chmod 600, format header `Cookie`):

| Variabel | Keterangan |
|---|---|
| `AIW2A_CHATGPT_COOKIES_FILE` | File persistensi rotasi cookie ChatGPT (opsional) |
| `AIW2A_CLAUDE_COOKIES_FILE` | File persistensi rotasi cookie Claude (opsional) |
| `AIW2A_GEMINI_COOKIES_FILE` | File persistensi rotasi cookie Gemini (opsional) |
| `AIW2A_GROK_COOKIES_FILE` | File persistensi rotasi cookie Grok (opsional) |

Tanpa variabel ini, rotasi hanya bertahan sampai restart. File persistensi
**tidak** dibaca saat startup (startup selalu memakai cookie dari env) —
fungsinya adalah cadangan rotasi, bukan sumber kredensial. Nilai cookie
tidak pernah di-log.

## Konfigurasi (`.env` / environment)

| Variabel | Wajib | Keterangan |
|---|---|---|
| `AIW2A_PORT` | tidak | Port server (default `8080`) |
| `AIW2A_API_KEY` | tidak | Kalau diisi, client wajib kirim `Authorization: Bearer <kunci>` |
| `AIW2A_CHATGPT_COOKIES` | opsional | Aktifkan provider ChatGPT |
| `AIW2A_CLAUDE_COOKIES` | opsional | Aktifkan provider Claude |
| `AIW2A_GEMINI_COOKIES` | opsional | Aktifkan provider Gemini |
| `AIW2A_GROK_SSO` | opsional | Aktifkan provider Grok |
| `AIW2A_GROK_STATSIG_SIGNER` | tidak | URL signer `x-statsig-id` Grok eksternal (mis. self-hosted); default: generate lokal (tidak ada pihak ketiga); kosongkan (`""`) untuk menonaktifkan signing |
| `AIW2A_*_COOKIES_FILE` | tidak | File persistensi rotasi cookie per provider (lihat di atas) |

Hanya provider yang kredensialnya diisi yang aktif dan muncul di `/v1/models`.

## Arsitektur

Satu provider = satu file utama; semua yang shared tinggal di `core/`:

```
src/
  main.rs            # bootstrap: baca .env, daftarkan provider, warm-up katalog, jalankan server
  server.rs          # route axum: /health, /v1/models, /v1/chat/completions (+SSE), /v1/images/generations
  openai.rs          # tipe request/response ala OpenAI + translasi Event -> SSE
  error.rs           # ProviderError -> ApiError (status + code OpenAI)
  core/
    events.rs        # Event stream ternormalisasi (Text/Reasoning/Image/Annotation/Usage/Done)
    auth.rs          # CookieJar + merge Set-Cookie + persistensi + pool akun (Phase 5)
    http.rs          # HTTP client bersama (reqwest + UA Chrome) + titik ekstensi Transport
    media.rs         # download/decode sumber gambar (URL / data URI)
    catalog.rs       # cache katalog model live + aturan web-default
    retry.rs         # backoff eksponensial + Retry-After + Pacer antar-request
  providers/
    mod.rs           # trait Provider v2 + Registry + ChatInput v2
    chatgpt.rs       # ChatGPT saja
    claude/          # Claude saja (mod.rs + payload.rs + sse.rs)
    gemini.rs        # Gemini saja (+ parse.rs)
    grok.rs          # Grok saja (+ ndjson.rs + payload.rs)
```

Desain lengkap (prinsip, pemetaan fitur, rencana migrasi):
`docs/design-modular-refactor.md`. Riset protokol web-to-API:
`docs/research-web2api-2026-10-16.md`. Checklist validasi live:
[`docs/validasi-live.md`](docs/validasi-live.md).

## API

### `GET /health` / `GET /`
Status server + daftar model aktif.

### `GET /v1/models`
Daftar model ala OpenAI — diambil dari **katalog live** tiap provider
(di-panaskan saat startup lewat `warm_catalog`, lalu di-refresh bila basi,
TTL 1 jam). Tidak ada daftar model hardcoded di kode.

### Aturan model: web-default (penting)

- **Jangan pernah mengunci ke satu model spesifik.** Setiap provider punya
  satu slug alias default web (`chatgpt-auto`, `claude`, `gemini`,
  `grok-auto`) — alias ini mengikuti default akun di UI web, bukan satu
  model tertentu.
- Model yang diminta client **hanya dipakai bila persis ada di katalog
  live** provider tersebut. Bila tidak ada (atau katalog gagal diambil),
  server memakai **model default web + warning di log** — request tidak
  pernah gagal hanya karena nama model tidak dikenal.
- `/v1/models` menyajikan katalog live + alias default web tiap provider
  yang aktif.

### `POST /v1/chat/completions`
Request ala OpenAI:

```bash
curl http://127.0.0.1:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $AIW2A_API_KEY" \
  -d \x27{
    "model": "grok-auto",
    "messages": [{"role": "user", "content": "Halo!"}],
    "stream": false
  }\x27
```

Streaming SSE:

```bash
curl -N http://127.0.0.1:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $AIW2A_API_KEY" \
  -d \x27{"model": "gemini", "messages": [{"role":"user","content":"Ceritakan lelucon"}], "stream": true}\x27
```

#### Thinking / reasoning

Bila provider meng-emit thinking, delta thinking dikirim sebagai
**`reasoning_content`** — di SSE sebagai field `reasoning_content` pada
delta chunk, di respons non-streaming sebagai field `reasoning_content`
pada pesan asisten. Thinking tidak pernah dicampur ke `content` jawaban.
Cara thinking diminta di sisi web per provider: lihat tabel fitur di bawah.

#### Parameter yang tidak didukung → 400 `unsupported_parameter`

Backend web **tidak bisa** memenuhi parameter sampling. Request yang
mengirim salah satu parameter ini secara eksplisit ditolak dengan
HTTP 400 + `code: "unsupported_parameter"` — bukan diam-diam diabaikan:

`temperature`, `top_p`, `max_tokens`, `frequency_penalty`,
`presence_penalty`, `logit_bias`, `seed`.

#### Usage adalah estimasi

Tidak ada provider web yang mengembalikan hitungan token asli. Field
`usage` dihitung lokal (estimasi `chars / 4`, reasoning dihitung ke
dalam `completion_tokens` mengikuti konvensi OpenAI) dan selalu
dilabeli **estimasi** di log. Jangan memperlakukan angka `usage`
sebagai hitungan token resmi.

### `POST /v1/images/generations`
Generate gambar ala OpenAI. Routing berdasarkan nama model:

```bash
curl http://127.0.0.1:8080/v1/images/generations \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $AIW2A_API_KEY" \
  -d \x27{"model":"gpt-image-1","prompt":"kucing astronot memakai helm kaca"}\x27
# {"created":...,"data":[{"b64_json":"iVBOR..."}]}
```

| Model | Provider | Cara kerja |
|---|---|---|
| `gpt-image-1`, `chatgpt-image` | ChatGPT | Satu turn "Generate an image: ..."; gambar diambil dari file reference `estuary/content` lalu diunduh server-side |
| `gemini-image-1`, `nano-banana` | Gemini | StreamGenerate; URL `googleusercontent.com` diunduh server-side |
| `grok-*` | Grok | Imagine via lane REST (`mediaGenInput`); gating tier belum terverifikasi live |
| `claude-*` | — | **404**: Claude tidak mengimplementasikan generate gambar raster |

`n` dipaksa 1 (web UI hanya 1 gambar per turn); `response_format: "url"` mengembalikan
URL sumber apa adanya (URL Gemini terikat konteks browser). Limit free-tier: ChatGPT ~2-3
gambar/hari, Gemini ~20/hari.

### Vision / thinking / search per provider

Semua provider mendukung vision input (bagian `image_url` di `messages`,
URL http(s) atau data URI) via pola upload-then-reference. Thinking dan
search mengikuti mekanisme web masing-masing. Status **UNVERIFIED**
berarti bentuk pastinya belum dikonfirmasi capture browser live —
lihat [`docs/validasi-live.md`](docs/validasi-live.md) untuk daftar
lengkap gap live-capture.

| Fitur | ChatGPT | Claude | Gemini | Grok |
|---|---|---|---|---|
| **Vision** | Upload 3 langkah file-service → `image_asset_pointer` (maks 10 lampiran/turn) | Wiggle upload → dokumen: `convert_document`, gambar: referensi `file_uuid` | Upload resumable ke `content-push` → referensi blob WIZ | Upload `upload-file-v2/direct` → `fileAttachments` |
| **Thinking** | Slug `-thinking` + `thinking_effort` (divalidasi katalog; echo upstream TIDAK dipercaya) | `paprika_mode: "extended"` (setting percakapan) | Varian katalog `<model>-thinking` (toggle ortogonal terhadap model dasar) | `isReasoning: true` + `reasoning_effort` |
| **Search** | Instruksi sistem + kumpulkan `citations`/`search_result_groups` → anotasi | Setting percakapan `enabled_web_search: true` — **UNVERIFIED** end-to-end | Native/warisan backend, tanpa flag terdokumentasi — toggle deterministik **UNVERIFIED** | `disableSearch: false` + `toolOverrides` — efektivitas **UNVERIFIED** di lane REST |

Catatan per provider:

- **ChatGPT**: PoW sentinel varian SHA3-512 + fingerprint **UNVERIFIED**
  (yang dipakai: SHA-256, capture octo-lex). Bentuk `/f/conversation/resume`
  **UNVERIFIED**. `PUT` blob file-service bisa 403 Cloudflare error 1010
  di jaringan tertentu. Prompt >96 KiB dikirim apa adanya (konversi JSONL
  **UNVERIFIED**).
- **Claude**: field multipart wiggle-upload (`"file"` best-effort),
  field `convert_document`, dan bentuk `retry_completion`
  **UNVERIFIED**. Search via settings saja belum terverifikasi end-to-end.
- **Gemini**: slot lampiran gambar `inner[61]`, bentuk respons blob WIZ,
  slot thought, dan header `SAPISIDHASH` (upload belum mengirimnya)
  **UNVERIFIED**. Nama model `-thinking` per katalog live.
- **Grok**: efektivitas `toolOverrides`, nilai preset DeepSearch,
  bentuk `fileAttachments`, field multipart upload, bentuk `/rest/modes`,
  dan bentuk + gating tier Imagine **UNVERIFIED**.

### Pacing antar-request

Tiap provider punya [`Pacer`](src/core/retry.rs) di jalur chat:
Claude memakai lantai **1500ms** (riset-backed, traffic mirip manusia);
ChatGPT, Gemini, dan Grok memakai **0ms** (mekanisme terpasang,
interval minimum 0 — angka lain tidak dikarang). Retry hanya
pre-first-byte untuk status retryable (429/529/500/502/503), dengan
backoff eksponensial; generasi yang sudah disubmit tidak pernah
di-replay.

## Batas paket gratis (free tier)

Angka di bawah adalah hasil riset web per Oktober 2026 — provider bisa
mengubahnya sewaktu-waktu, dan sebagian memang tidak dipublish resmi.

| Provider | Batas chat gratis | Reset |
|---|---|---|
| ChatGPT | Chat teks sehari-hari **unlimited**\* | — |
| Claude | Allowance per sesi, **tanpa angka pasti** | tiap 5 jam |
| Gemini | Allowance berbasis komputasi, **tanpa angka pasti** | refresh 5 jam + batas mingguan |
| Grok | **~10 pesan** | tiap 2 jam (rolling) |

\* Unlimited hanya untuk chat teks biasa; fitur lanjutan (mode reasoning,
upload file, image generation, dsb.) punya batas masing-masing.

> **Catatan untuk AiWeb2API:** server ini stateless — setiap request mengirim
> ulang seluruh riwayat `messages` sebagai satu prompt. Artinya batas hitungan
> pesan habis lebih cepat untuk percakapan panjang. Untuk Grok (~10 pesan /
> 2 jam) sebaiknya jaga riwayat tetap pendek atau ringkas manual.

## Batasan yang diketahui

- **ChatGPT**: endpoint web modern kadang menuntut proof-of-work / verifikasi
  browser (sentinel). Kalau itu terjadi kamu akan dapat error yang jelas;
  solusinya buka `chatgpt.com` di browser dan selesaikan verifikasinya.
- **Gemini**: kalau Google mengubah format `StreamGenerate`, provider perlu
  penyesuaian (parser ditulis defensif + pesan error jelas).
- **TLS fingerprint**: HTTP client memakai reqwest + UA Chrome biasa, bukan
  imitasi TLS Chrome. Evaluasi migrasi ke `rquest` (Fase 4): crate `rquest`
  tidak bisa di-resolve dari index crates.io di sandbox ini, jadi reqwest
  dipertahankan — lihat catatan lengkap di `src/core/http.rs`. Karena server
  jalan di perangkatmu sendiri (IP yang sama dengan browser), umumnya tidak
  masalah; kalau provider melempar 403 Cloudflare, buka situsnya di browser
  untuk verifikasi.
- **Multi-turn**: setiap request mandiri (tanpa memori antar-request).
  Kirim ulang riwayat di `messages` kalau butuh konteks.
- **Validasi live belum dilakukan dari sandbox** (egress proxy MITM kena
  403 di provider web). Validasi harus dilakukan di jaringan/device dengan
  cookie valid — lihat [`docs/validasi-live.md`](docs/validasi-live.md).

## Lisensi

Lisensi MIT — lihat file `LICENSE`. Kode reverse-engineering
di sini ditulis ulang dari dokumentasi publik; bukan salinan kode proyek lain.
