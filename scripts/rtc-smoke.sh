#!/usr/bin/env bash
# End-to-end smoke of the WebRTC path. Requires the engine (npm start) and
# gateway (cd gateway && go run -tags nolibopusfile .) to be running.
set -euo pipefail
cd "$(dirname "$0")/.."

WAV=$(mktemp -t rtc-smoke).wav
say -o "$WAV" --file-format=WAVE --data-format=LEI16@24000 \
  "${SMOKE_TEXT:-What is the capital of France?}"
trap 'rm -f "$WAV"' EXIT

cd gateway && go run -tags nolibopusfile ./cmd/smoke -wav "$WAV" "$@"
