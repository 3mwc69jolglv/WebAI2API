#!/usr/bin/env bash
# Menjalankan AiWeb2API. Jalankan ulang script ini setiap VM restart
# (file proyek & .env aman tersimpan, yang hilang cuma prosesnya).
set -euo pipefail
cd "$(dirname "$0")"
export PATH="$HOME/.cargo/bin:$PATH"

if [ ! -f .env ]; then
  echo "ERROR: .env tidak ditemukan. Salin dari .env.example lalu isi cookie." >&2
  exit 1
fi
if [ ! -x target/debug/aiweb2api ]; then
  echo "Binary belum ada, build dulu..."
  cargo build
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

echo "AiWeb2API -> http://127.0.0.1:${AIW2A_PORT:-8080}"
exec ./target/debug/aiweb2api
