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

## How it works

- **Turn-taking** is Deepgram Flux's native `StartOfTurn` / `EndOfTurn` — no
  VAD, no separate turn model.
- **Speculative generation**: on `EagerEndOfTurn` (Flux is ~50% sure the user
  is done) the LLM + TTS start immediately and the output is buffered
  server-side. `EndOfTurn` flushes it to the browser; `TurnResumed` rolls it
  back silently. The model thinks while Deepgram waits out the silence.
- **Barge-in**: `StartOfTurn` during a reply aborts the LLM fetch, closes the
  Fish socket, and tells the browser to drop its playback buffer.
- **Sentence chunking**: LLM tokens buffer until a punctuation boundary, so
  Fish synthesizes whole clauses (one Fish websocket per turn, one flush at
  end of input).
- **Latency chips** in the header, measured per turn: turn detect (from when
  speech actually stopped, via Flux's `audio_window_end` audio clock), LLM
  first token, TTS first audio, and voice→voice.

Tuning (all in `.env`): `DEEPGRAM_EOT_THRESHOLD` (lower = snappier turns,
more false cut-ins), `DEEPGRAM_EAGER_EOT_THRESHOLD` (lower = earlier
speculative start, more wasted tokens), `FISH_LATENCY_MODE`
(`normal | balanced | low`).

## Layout

```
server.js                 everything server-side (~600 lines)
public/index.html         UI
public/app.js             mic/playback wiring + transcript
public/mic-worklet.js     16 kHz PCM16 capture
public/player-worklet.js  24 kHz streaming PCM player
scripts/smoke.js          headless end-to-end test
```
