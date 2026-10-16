# Riset Proyek Referensi AiWeb2API

Tanggal riset: 2026-10-03. Strategi: riset dulu, implementasi kemudian
(keputusan Altria, 2026-10-02). Tiga proyek yang diminta + temuan dari
ekosistem fork-nya.

## 1. syafrieyunizar/grok-web2api (Python, single-file)

Repo: https://github.com/syafrieyunizar/grok-web2api — commit `9034c65`
(2026-08-14). Paling mirip AiWeb2API: satu akun, direct HTTP, tanpa admin UI.

**Temuan kunci:**
- **Tidak memakai `x-statsig-id` sama sekali.** Anti-bot hanya dilawan di dua
  lapis: (a) TLS fingerprint Chrome via `curl_cffi` (`impersonate=chrome131`),
  (b) pemanasan `GET grok.com/` untuk cookie `__cf_bm`/`grok_device_id`.
  Tidak ada 403-code-7 handling, tidak ada signer eksternal.
- Endpoint & body sama persis dengan kita: `POST /rest/app-chat/conversations/new`,
  `temporary: true` (tidak mengotori riwayat chat), NDJSON dengan field
  `result.response.{token,isThinking,messageTag,isFinished}`.
- Perakitan prompt multi-turn: semua messages digabung, system jadi prefix
  `[System: ...]` → **sudah paritas** dengan `flatten_messages()` kita.
- Model matching longgar: prefix match (`model_name.startswith(k)`).
- Retry 3x khusus untuk 429; 401 = token mati, langsung error.

**Implikasi untuk kita:** memperkuat catatan di `mod.rs` — migrasi client
Grok ke TLS impersonation (`wreq` + `Emulation::Chrome131`) kemungkinan bisa
**menghilangkan ketergantungan ke signer pihak ketiga** sama sekali.

## 2. syafrieyunizar/chatgpt-web2api (Python, single-file, 1200 baris)

Repo: https://github.com/syafrieyunizar/chatgpt-web2api — commit `d79ca4f`.
Direct reverse-engineer (bukan CDP/browser).

**Temuan kunci:**
- TLS impersonation `safari15_3` + Safari UA untuk chatgpt.com.
- **Auth via OAuth refresh_token** → `POST auth0.openai.com/oauth/token`
  (`client_id=pdlLIX2Y72MIl2rhLhTE9VV9bN905kB`), access token di-cache,
  auto-refresh saat 401. Alternatif yang lebih awet daripada full cookie
  header → `/api/auth/session` (cara kita sekarang).
- Endpoint: `POST /backend-api/conversation` (kita: `/backend-api/f/conversation`
  — keduanya tampaknya valid, tapi referensi memakai yang tanpa `f/`).
- **PoW config dibangun sendiri menyerupai browser**: fetch DPL (build id)
  dari halaman chatgpt.com (`/_next/static/<build>/` atau `data-build`),
  randomisasi screen res / cores / navigator keys / parse time. Solve:
  `SHA3-512(seed + base64(config))`, 500k nonce, format jawaban `gAAAAAB+...`.
  Kita: config diambil dari echo challenge + 200k nonce × 2 algoritma —
  pendekatan kita lebih sederhana, tapi DPL asli kemungkinan menaikkan
  acceptance rate.
- Sentinel: `POST /backend-api/sentinel/chat-requirements` dengan requirements
  token proaktif (`gAAAAAC+...`), lalu `chat_token` + `proof_token` di request.
  Nama header sama dengan yang kita kirim
  (`openai-sentinel-chat-requirements-token`).
- **Daftar model mereka lebih segar**: `gpt-5.6-luna → gpt-5-6`, `gpt-5.5`,
  `gpt-5.4-t-mini`, `o1/o3`, `research`, `auto`. Daftar kita
  (`gpt-5`, `gpt-5-mini`, `gpt-4o`, `gpt-4o-mini`) kemungkinan basi.
- Punya **alur upload file lengkap**: `/backend-api/files` → upload ke
  presigned Azure Blob (`x-ms-blob-type: BlockBlob`) → `/files/{id}/uploaded` →
  poll `retrieval_index_status == success`. Di luar scope kita saat ini,
  tapi polanya terdokumentasi kalau nanti dibutuhkan.
- Retry: 3x dengan delay untuk 429/5xx.

## 3. chenyme/grok2api + ekosistem fork (Go, multi-akun)

Repo utama + fork aktif: `zhujiane/grok2api`, `mrdoudou1/grok2api`, dll.
Skala produksi (admin React, account pool, quota sync) — di luar scope
AiWeb2API, tapi dua temuan teknisnya sangat relevan:

**a) `x-statsig-id` bisa dibuat lokal (PR #567, terverifikasi).**
https://github.com/chenyme/grok2api/pull/567 — JS frontend grok.com menghitung
`x-statsig-id` via Statsig SDK; saat evaluasi gagal (headless) fallback-nya:
`btoa("x1:" + error)`. Prefix lama `e:` ditolak server, prefix **`x1:`**
diterima. Divalidasi langsung ke `grok.com/rest/app-chat/conversations/new`
dengan 3 SSO token berbeda → semua 200 (sebelumnya 403).
Fork `xiaocongyu66/grok2api-sing` bahkan sudah punya implementasi
**pure-Go in-process** (`statsig/pure.go`, `svgfingerprint/compute.go`).

**Implikasi untuk kita (besar):** ketergantungan ke signer komunitas
`https://grok.wodf.de/sign` (pihak ketiga, SPOF, meta page dikirim keluar)
**bisa dihapus** — generate `x-statsig-id` lokal dengan format fallback `x1:`.
Validasi 70-byte base64 yang sudah ada di `grok.rs` tetap berlaku.

**b) Self-hosted signer = overkill untuk kita.**
`tools/statsig-signer` (zhujiane/grok2api): Playwright + WARP + FlareSolverr,
~0.5GB RAM. Cocok untuk gateway multi-akun, tidak untuk pemakaian pribadi.

**c) Pola 403-code-7:** invalidate signature → retry sekali → sisanya
dianggap masalah egress. **Sudah paritas** dengan `post_conversation()` kita.

## 4. Perbandingan ringkas vs AiWeb2API

| Aspek | AiWeb2API (sekarang) | Referensi | Status |
|---|---|---|---|
| Grok: endpoint & NDJSON | `/rest/app-chat/conversations/new`, temporary | sama | ✅ paritas |
| Grok: `x-statsig-id` | signer pihak ketiga `grok.wodf.de` | lokal `x1:` fallback (PR #567) / tanpa signer + TLS impersonation | ⚠️ bisa dilepas |
| Grok: TLS fingerprint | reqwest/rustls standar | `curl_cffi` chrome131 | ⚠️ gap (catatan sudah ada di `mod.rs`: migrasi ke `wreq`) |
| ChatGPT: endpoint | `/backend-api/f/conversation` | `/backend-api/conversation` | ⚠️ cek varian tanpa `f/` |
| ChatGPT: auth | full cookie → `/api/auth/session` | OAuth refresh_token → auth0 | ⚠️ alternatif lebih awet |
| ChatGPT: PoW | config dari echo challenge, 200k nonce | DPL asli + config browser-like, 500k nonce | ⚠️ bisa ditingkatkan |
| ChatGPT: daftar model | gpt-5, gpt-5-mini, gpt-4o... | gpt-5.6-luna, gpt-5.5, o1/o3... | ⚠️ basi |
| Prompt multi-turn | `flatten_messages` | sama persis | ✅ paritas |
| Upload file ChatGPT | belum ada | alur `/files` lengkap | 🔮 masa depan |
| Retry 429/5xx | Grok: sekali (statsig); umum: belum | 3x + delay | ⚠️ bisa ditambah |

## 5. Proposal perbaikan (urut nilai/usaha)

> **Status 2026-10-05:** proposal #1 sudah diimplementasikan — `statsig_id()`
> di `src/providers/grok.rs` kini generate `x-statsig-id` lokal secara
> default (format `btoa("x1:" + error)` per grok2api PR #567, variasi pesan
> acak); `AIW2A_GROK_STATSIG_SIGNER` diisi URL → signer eksternal (alur lama),
> dikosongkan eksplisit (`""`) → signing mati. README diperbarui.
> `cargo check` bersih; format terverifikasi via unit test sementara.
> Validasi live ke grok.com belum bisa dari sandbox (proxy MITM kena 403) —
> menunggu jaringan/device Altria.

1. **Grok: generate `x-statsig-id` lokal, hapus signer pihak ketiga.**
   Port format fallback `x1:` dari grok2api PR #567 (atau `pure.go` milik
   xiaocongyu66/grok2api-sing). Usaha sedang, nilai tinggi (hapus SPOF +
   tidak ada data yang keluar ke pihak ketiga).
2. **TLS impersonation via `wreq`** (Grok + ChatGPT). Sudah dicatat di
   `mod.rs`; riset ini mengonfirmasi kedua proyek referensi mengandalkannya
   sebagai lapis anti-bot utama. Butuh libclang saat build. Usaha besar,
   nilai tinggi.
3. **ChatGPT: jalur auth refresh_token (opsional).** Env baru mis.
   `AIW2A_CHATGPT_REFRESH_TOKEN` → exchange via auth0, fallback ke cara
   cookie yang ada. Token lebih awet daripada cookie header. Usaha sedang.
4. **ChatGPT: segarkan daftar model** (tambah slug gpt-5.5/gpt-5.6/o1/o3,
   atau discovery dinamis via `/backend-api/models`). Usaha kecil.
5. **ChatGPT: PoW pakai DPL asli** dari halaman chatgpt.com. Usaha kecil-sedang.
6. **Retry umum 429/5xx** (3x + backoff) untuk semua provider. Usaha kecil.
7. **Masa depan (di luar scope):** upload file ChatGPT (`/backend-api/files`
   flow terdokumentasi di seksi 2); image gen Grok via `enableImageGeneration`
   tetap butuh akun berbayar (Imagine), jadi tetap 400 untuk free-tier.

## 6. Yang sengaja tidak ditiru

- Arsitektur multi-akun/account pool/quota sync/admin React ala grok2api —
  target mereka produksi multi-user; AiWeb2API pemakaian pribadi satu akun.
- CDP browser automation ala `octo-lex/chatgpt-web2api` — berat, butuh Chrome
  sungguhan; direct HTTP sudah terbukti jalan.
- Self-hosted statsig-signer (Playwright+WARP, ~0.5GB) — overkill; opsi 1
  di atas lebih ringan.

## Referensi

- https://github.com/syafrieyunizar/grok-web2api (grok_web2api.py, commit 9034c65)
- https://github.com/syafrieyunizar/chatgpt-web2api (chatgpt_web2api.py, commit d79ca4f)
- https://github.com/chenyme/grok2api/pull/567 (fix format x-statsig-id `x1:`)
- https://github.com/zhujiane/grok2api/blob/HEAD/tools/statsig-signer/README.md
- https://github.com/xiaocongyu66/grok2api-sing/commit/ae5c03dd7439c2914bdaf6de3dd183ff7ff062cd (pure-Go local Statsig)
- https://github.com/sexlovr/grok-2-api (browser-based signing, penjelasan DOM fingerprint)
