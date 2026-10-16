# Validasi Live AiWeb2API (Fase 5)

Tanggal dokumen: 2026-10-16. Status: **belum divalidasi** — semua sel di
bawah masih `[ ]` sampai ada yang mengujinya di jaringan/device dengan
cookie yang valid.

## Kenapa tidak bisa divalidasi dari sandbox

Sandbox tempat kode ini dikembangkan memakai egress proxy MITM yang kena
**403** di endpoint web provider (Grok / ChatGPT / Claude; Gemini juga
sensitif). Semua test otomatis di repo ini adalah **fixture-only**
(tanpa jaringan ke provider). Validasi live HANYA bisa dilakukan dari
jaringan/device biasa (mis. laptop/HP) dengan cookie browser yang valid.

## Cara menjalankan server

```bash
cd aiweb2api
cp .env.example .env   # bila belum ada; lalu isi cookie yang dibutuhkan
./start.sh             # build bila perlu, lalu jalankan server dari .env
```

Cek server hidup dan provider aktif (tanpa kredensial asli di contoh ini —
`$AIW2A_API_KEY` adalah variabel shell-mu, bukan nilai yang ditulis di sini):

```bash
curl -s http://127.0.0.1:${AIW2A_PORT:-8080}/health
curl -s http://127.0.0.1:${AIW2A_PORT:-8080}/v1/models
```

`GET /v1/models` menampilkan katalog **live** tiap provider yang aktif
ditambah alias default web (`chatgpt-auto`, `claude`, `gemini`,
`grok-auto`). Untuk sel-sel di bawah, pakai slug yang muncul di
`/v1/models` — jangan mengarang nama model. Bila katalog live gagal
diambil, hanya alias default web yang muncul (itu perilaku yang
diharapkan, bukan bug).

> **Catatan penting — thinking/search belum diekspos ke client.**
> Server saat ini selalu mengirim `Thinking::Auto` / `Search::Auto`
> ke provider (tidak ada opsi thinking/search di request OpenAI).
> Sel thinking/search di bawah menguji perilaku **default Auto**
> (apakah provider meng-emit `reasoning_content` / sitasi secara
> native) dan logika internal provider-nya. Pengujian deterministik
> `On`/`Off` butuh opsi server — dicatat sebagai follow-up, bukan
> bagian checklist ini.

## Cara menguji tiap sel (contoh curl)

Semua contoh memakai `Authorization: Bearer $AIW2A_API_KEY` — isi
`AIW2A_API_KEY` dari `.env`-mu sebagai variabel shell, jangan paste
nilainya ke dokumen ini. Ganti `<MODEL>` dengan slug dari `/v1/models`
dan `<PORT>` dengan port server (default `8080`).

### Teks chat (non-streaming)

```bash
curl -s http://127.0.0.1:<PORT>/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $AIW2A_API_KEY" \
  -d '{"model":"<MODEL>","messages":[{"role":"user","content":"Jawab dalam satu kalimat: ibu kota Indonesia?"}],"stream":false}'
```

Lolos bila: HTTP 200, `choices[0].message.content` tidak kosong dan
benar (Jakarta), `usage` ada (angka estimasi — itu yang diharapkan).

### Streaming

```bash
curl -N http://127.0.0.1:<PORT>/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $AIW2A_API_KEY" \
  -d '{"model":"<MODEL>","messages":[{"role":"user","content":"Hitung 17 + 25, jelaskan langkahnya."}],"stream":true}'
```

Lolos bila: chunk SSE berdatangan (bukan menunggu sampai selesai baru
muncul sekaligus), ada chunk penutup + `[DONE]`, teks gabungannya utuh
dan benar (42). Catat bila ada `reasoning_content` pada delta chunk
(itu thinking yang ter-emit — bagus, bukan error).

### Vision (gambar input)

Pakai gambar KECIL (mis. foto kucing / tangkapan layar teks pendek).
Ganti `<BASE64_GAMBAR_KECIL>` dengan base64 gambar-mu, atau ganti
`data:...` dengan `{"url": "<URL_GAMBAR_PUBLIK>"}`.

```bash
curl -s http://127.0.0.1:<PORT>/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $AIW2A_API_KEY" \
  -d '{"model":"<MODEL>","messages":[{"role":"user","content":[
    {"type":"text","text":"Apa isi gambar ini? Jawab singkat."},
    {"type":"image_url","image_url":{"url":"data:image/png;base64,<BASE64_GAMBAR_KECIL>"}}
  ]}],"stream":false}'
```

Lolos bila: jawaban mendeskripsikan gambar dengan benar (bukan
jawaban generik / bukan error "gambar diabaikan"). Gagal bila:
jawaban seolah tidak melihat gambar, atau error upload (catat pesan
error persisnya — itu data untuk memperbaiki jalur upload provider).

### Thinking

```bash
curl -s http://127.0.0.1:<PORT>/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $AIW2A_API_KEY" \
  -d '{"model":"<MODEL>","messages":[{"role":"user","content":"Sebuah toko menjual 3 apel seharga 12000. Berapa harga 7 apel? Jelaskan langkahnya."}],"stream":false}'
```

Lolos bila: jawaban benar (28000) DAN field `reasoning_content` pada
pesan asisten terisi (thinking ter-emit terpisah dari jawaban).
Bila jawaban benar tapi `reasoning_content` kosong, catat sebagai
**degraded** (bukan gagal total): provider menjawab tanpa
mengekspos thinking — ini data penting, bukan sekadar "gagal".

### Search (info terkini)

Pakai pertanyaan yang jawabannya berubah-ubah / membutuhkan info
sangat baru (mis. hasil pertandingan tadi malam, harga saham hari
ini). Sesuaikan tanggalnya dengan hari pengujian.

```bash
curl -s http://127.0.0.1:<PORT>/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $AIW2A_API_KEY" \
  -d '{"model":"<MODEL>","messages":[{"role":"user","content":"Siapa yang memenangkan pertandingan sepak bola besar tadi malam? Sertakan sumbernya."}],"stream":false}'
```

Lolos bila: jawaban memakai info terkini yang benar DAN ada
`citations` (non-streaming) atau sitasi di teks. Bila jawaban benar
tapi tanpa sitasi, catat sebagai **degraded**. Bila jawaban memakai
pengetahuan basi / mengarang, catat sebagai gagal + salin jawabannya.

### Image generation

```bash
curl -s http://127.0.0.1:<PORT>/v1/images/generations \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $AIW2A_API_KEY" \
  -d '{"model":"<MODEL_GAMBAR>","prompt":"seekor kucing astronot memakai helm kaca, gaya kartun"}'
```

Model gambar: ChatGPT `gpt-image-1` / `chatgpt-image`, Gemini
`gemini-image-1` / `nano-banana`, Grok model `grok-*` (Imagine).
Claude: harapkan **404** (tidak diimplementasikan) — itu lolos bila
statusnya 404 dengan pesan jelas. Lolos untuk provider lain bila:
HTTP 200 + `data[0].b64_json` bisa di-decode menjadi file gambar
yang valid (simpan sebagai `.png` dan buka). Gagal bila: error
upstream (catat status + pesannya), atau `b64_json` tidak valid.

## Matriks validasi

Tandai `[ ]` → `[x]` saat sel lolos. Tulis tanggal + catatan singkat
di kolom Catatan (termasuk perilaku degraded). Satu baris = satu
provider.

### ChatGPT

| Sel | Status | Catatan (tanggal + hasil) |
|---|---|---|
| Teks chat | [ ] | |
| Vision | [ ] | |
| Thinking | [ ] | |
| Search | [ ] | |
| Streaming | [ ] | |
| Image gen | [ ] | |

### Claude

| Sel | Status | Catatan (tanggal + hasil) |
|---|---|---|
| Teks chat | [ ] | |
| Vision | [ ] | |
| Thinking | [ ] | |
| Search | [ ] | |
| Streaming | [ ] | |
| Image gen (harapkan 404) | [ ] | |

### Gemini

| Sel | Status | Catatan (tanggal + hasil) |
|---|---|---|
| Teks chat | [ ] | |
| Vision | [ ] | |
| Thinking | [ ] | |
| Search | [ ] | |
| Streaming | [ ] | |
| Image gen | [ ] | |

### Grok

| Sel | Status | Catatan (tanggal + hasil) |
|---|---|---|
| Teks chat | [ ] | |
| Vision | [ ] | |
| Thinking | [ ] | |
| Search | [ ] | |
| Streaming | [ ] | |
| Image gen (Imagine) | [ ] | |

## Gap live-capture (UNVERIFIED / TODO di kode)

Daftar ini dikumpulkan dari `TODO(live-capture)` / `UNVERIFIED` di
kode. Setiap item butuh capture browser live (DevTools → tab Network,
sambil memakai fitur terkait di UI web provider) sebelum bentuk
pastinya dianggap benar. Jangan tebak — capture dulu. Saat satu item
terkonfirmasi, coret dari daftar ini dan perbarui kode + README.

### ChatGPT

- [ ] **Varian PoW SHA3-512 + fingerprint array** — solver
  `solve_pow_sha3_512_fingerprint` sengaja mengembalikan `None`
  (format fingerprint array 18-field + jawaban yang diharapkan belum
  terkonfirmasi). Yang dipakai sekarang: SHA-256 (capture octo-lex).
  Capture: request sentinel prepare/finalize saat PoW diminta.
- [ ] **Diff body toggle search UI web** — tidak ada flag
  `web_search: true` di protokol yang didokumentasikan; search saat
  ini diminta lewat instruksi sistem. Capture: request
  `/f/conversation` dengan toggle search UI menyala vs mati, lalu
  bedakan body-nya.
- [ ] **Bentuk `/f/conversation/resume`** — diimplementasikan sebagai
  POST `{resume_conversation_token}`; method/parameter belum
  terverifikasi. Capture: stream panjang yang terpotong lalu
  di-resume oleh web.
- [ ] **PUT blob file-service browserless** — apakah `PUT` ke
  `upload_url` lolos tanpa browser, atau kena 403 Cloudflare error
  1010 (butuh browser-relay). Uji: sel vision ChatGPT di matriks.
- [ ] **Konversi prompt oversized (>96 KiB) ke lampiran JSONL** —
  `content_type`/`use_case` lampiran JSONL belum terdokumentasi;
  prompt besar saat ini dikirim apa adanya. Capture: kirim prompt
  >96 KiB di web, lihat apakah jadi lampiran JSONL.
- [ ] **Encoding difficulty PoW** — encoding nilai `difficulty`
  (hex 6 digit vs nol-di-depan vs numerik) belum sepenuhnya
  terdokumentasi. Capture: nilai `difficulty` + jawaban PoW yang
  diterima finalize.
- [ ] **Header `oai-client-*` saat ini** — nilai build/version tidak
  di-hardcode; set yang dipakai web saat ini belum di-capture.

### Claude

- [ ] **Field multipart wiggle-upload** — nama field `"file"` adalah
  best-effort; belum pasti apakah field tambahan diperlukan
  (`file_name`, `file_size`, `file_kind`, ...). Capture: upload file
  di web Claude, lihat body multipart-nya.
- [ ] **Field `convert_document`** — apakah endpoint mengharapkan
  byte file ulang atau `file_uuid` hasil wiggle upload, dan field
  tambahan lain. Capture: upload dokumen di web.
- [ ] **Bentuk `retry_completion`** — bentuk request/parameter
  endpoint retry belum terverifikasi. Capture: picu retry di web
  (mis. setelah error) dan lihat request-nya.
- [ ] **Search end-to-end via settings** — apakah
  `enabled_web_search: true` saja cukup untuk search end-to-end,
  atau ada mekanisme lain. Uji: sel search Claude di matriks +
  capture request settings web.

### Gemini

- [ ] **Slot lampiran gambar `inner[61]`** di payload
  `StreamGenerate` — slot ini UNVERIFIED. Capture: kirim gambar di
  web Gemini, lihat slot lampiran di `f.req`.
- [ ] **Bentuk respons upload blob WIZ** — kunci referensi blob
  (`blob`/`resource`/`id`) adalah best-effort dari riset. Capture:
  upload gambar di web, lihat respons upload-nya.
- [ ] **Slot thought eksak** — slot thought di frame StreamGenerate
  belum terverifikasi dari capture resmi. Capture: minta jawaban
  dengan thinking di web, lihat frame thought-nya.
- [ ] **`SAPISIDHASH`** — upload gambar belum mengirim header ini
  (butuh SHA-1). Bila Google mewajibkannya, upload gagal dengan
  error jelas. Capture: header upload gambar di web.
- [ ] **Toggle search deterministik** — tidak ada flag search yang
  terverifikasi di `StreamGenerate`; search dianggap warisan
  backend. Capture: request dengan toggle search UI menyala vs
  mati, bedakan body-nya.
- [ ] **Penamaan model `-thinking`** — varian `<model>-thinking`
  mengikuti katalog live; pastikan katalog live akun target
  benar-benar memuat varian thinking. Cek: `GET /v1/models` + sel
  thinking Gemini.

### Grok

- [ ] **Efektivitas `toolOverrides`** — apakah `toolOverrides`
  (web/X search) berpengaruh di lane REST saat ini. Bukti konflik:
  capture 2026-08 tidak menemukan toggle search di lane WebSocket
  yang lebih baru. Uji: sel search Grok + capture request web.
- [ ] **Nilai preset DeepSearch** — nilai `"deepsearch preset": true`
  belum terverifikasi (nama preset vs boolean). Capture: request
  dengan toggle DeepSearch menyala di web.
- [ ] **Bentuk `fileAttachments`** — bentuk entri
  (`fileMetadataId`/`fileName`/`mimeType`) belum terverifikasi
  live. Capture: kirim file di web Grok.
- [ ] **Field multipart upload** — nama field (`"file"`) dan
  field tambahan endpoint `upload-file-v2/direct` belum
  terverifikasi. Capture: upload file di web Grok.
- [ ] **Bentuk `/rest/modes`** — parser menerima beberapa bentuk
  (array string / objek / map), tapi bentuk asli endpoint ini belum
  pernah dilihat dari kode ini. Capture: `GET /rest/modes` di
  browser yang login (atau catat respons dari `warm_catalog`).
- [ ] **Bentuk + gating tier Imagine** — bentuk `mediaGenInput`
  text-to-image dianalogikan dari edit (#940) / video (#991);
  gating tier Imagine (Basic vs berbayar) belum terverifikasi.
  Uji: sel image gen Grok + capture request Imagine di web.

## Cara mencatat hasil

1. Edit file ini: ubah `[ ]` → `[x]` pada sel yang lolos, isi kolom
   Catatan dengan tanggal, model yang dipakai (slug dari
   `/v1/models`), dan hasil singkat (termasuk degraded).
2. Untuk sel yang gagal, catat status HTTP + pesan error persisnya
   (tanpa nilai cookie/token apa pun) dan, bila ada, kondisi yang
   memicunya (mis. "PUT blob 403", "respons kosong").
3. Untuk tiap gap live-capture yang terkonfirmasi, catat bentuk yang
   benar di sini, perbarui kode agar sesuai capture, dan coret
   itemnya dari daftar.
4. Simpan hasil capture (HAR / salinan request) di luar repo — jangan
   commit file yang memuat cookie/token.
