# fish-bare-agent

Voice agent with **no framework** — no LiveKit, no pipecat, no agent SDK. One
Node process, two dependencies (`ws`, `@msgpack/msgpack`), three vendor
websockets:

```
browser mic ── PCM16 @16k ──> server ──> Deepgram Flux   (STT + turn-taking)
                                │            │ EndOfTurn transcript
                                │            v
                                │         LLM             (OpenAI-compatible, streamed)
                                │            │ tokens -> sentence chunker
                                │            v
browser spk <── PCM16 @24k ── server <── Fish TTS        (/v1/tts/live, msgpack)
```

## Get started

Requires Node >= 20.6.

```sh
cp .env.example .env   # fill in the three keys below
npm install
npm start              # open http://localhost:8787, hit Start, talk
```

You need:

- `DEEPGRAM_API_KEY` — [console.deepgram.com](https://console.deepgram.com)
- `LLM_BASE_URL` / `LLM_API_KEY` / `LLM_MODEL` — any OpenAI-compatible
  `/chat/completions` endpoint that supports `stream: true`
- `FISH_API_KEY` (+ optional `FISH_VOICE` reference id) — [fish.audio](https://fish.audio)

Sanity-check the whole pipeline without a mic (macOS only, uses `say`):

```sh
npm run smoke
```

## WebRTC transport (optional)

By default audio rides the websocket as raw PCM — great locally, fragile on
bad networks. The `gateway/` directory is a Go/Pion media gateway that
terminates WebRTC (Opus, jitter buffering, loss concealment) and bridges PCM
to the engine. The engine relays SDP via `POST /rtc/offer`, so the page only
talks to one origin; all calls share a single UDP port (ICE-Lite + UDP mux).

```sh
cd gateway && go run -tags nolibopusfile .   # needs Go >= 1.22 + libopus
# then open http://localhost:8787/?transport=webrtc
```

Headless end-to-end test of the WebRTC path (Pion client stands in for the
browser):

```sh
npm run smoke:rtc
```

`docker compose up --build` runs both services (see compose.yaml for the
macOS UDP caveat; on Linux set `GATEWAY_PUBLIC_IP`).

## How it works

- **Turn-taking** is Deepgram Flux's native `StartOfTurn` / `EndOfTurn` — no
  VAD, no separate turn model.
- **Speculative generation**: on `EagerEndOfTurn` (Flux is ~50% sure the user
  is done) the LLM + TTS start immediately and the output is buffered
  server-side. `EndOfTurn` flushes it to the browser; `TurnResumed` rolls it
  back silently. The model thinks while Deepgram waits out the silence.
- **Barge-in**: `StartOfTurn` during a reply aborts the LLM fetch, closes the
  Fish socket, and tells the browser to drop its playback buffer.
- **Self-interruption defenses** (header knobs, applied live per session; for
  testing echo behavior on speakerphone devices):
  - *barge-in* — `instant` cuts agent audio on `StartOfTurn`; `smart` holds
    the cut while the agent is audible until the transcript proves real
    speech (non-echo + `min words`). The engine knows the agent is audible
    from a playback horizon: bytes shipped = seconds of speaker time, on
    both transports.
  - *echo filter* — drops "user" turns whose transcript matches what the
    agent was just saying (speaker -> mic leakage). Suppressed turns show as
    dashed "echo suppressed" bubbles and are never answered.
  - *transport* — websocket vs webrtc, switchable between sessions.
  - `npm run smoke:echo` regression-tests the filter by speaking the agent's
    own reply back at it mid-stream.
- **Sentence chunking**: LLM tokens buffer until a punctuation boundary, so
  Fish synthesizes whole clauses (one Fish websocket per turn, one flush at
  end of input).
- **Latency chips** in the header, measured per turn: turn detect (how long
  after you audibly stopped speaking Flux called the turn — measured by a
  server-side energy gate on the mic, since Flux events carry no word
  timings), LLM first token, TTS first audio, and voice→voice.

Tuning (all in `.env`): `DEEPGRAM_EOT_THRESHOLD` (lower = snappier turns,
more false cut-ins), `DEEPGRAM_EAGER_EOT_THRESHOLD` (lower = earlier
speculative start, more wasted tokens), `FISH_LATENCY_MODE`
(`normal | balanced | low`).

## Layout

```
server.js                 everything server-side (~700 lines)
public/index.html         UI
public/app.js             mic/playback wiring + transcript (WS or WebRTC)
public/mic-worklet.js     16 kHz PCM16 capture (WS transport)
public/player-worklet.js  24 kHz streaming PCM player (WS transport)
scripts/smoke.js          headless end-to-end test (WS transport)
gateway/                  Go/Pion WebRTC media gateway
gateway/internal/dsp/     FIR resamplers (48k->16k, 24k->48k)
gateway/cmd/smoke/        headless end-to-end test (WebRTC transport)
```
