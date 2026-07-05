// fish-bare-agent — framework-less voice agent.
//
// Pipeline per browser connection:
//   browser mic (PCM16 @ 16 kHz, binary WS frames)
//     -> Deepgram Flux STT (turn-taking built in: StartOfTurn / EndOfTurn,
//        plus EagerEndOfTurn for speculative generation)
//     -> Gemma LLM (OpenAI-compatible /chat/completions, streamed SSE)
//     -> sentence chunker
//     -> Fish TTS websocket (/v1/tts/live, msgpack)
//     -> browser (PCM16 @ 24 kHz, binary WS frames)
//
// Latency strategy: when Flux says "the user is probably done" (EagerEndOfTurn)
// we start the LLM + TTS immediately but buffer the output server-side. When
// the real EndOfTurn arrives we flush the buffer to the browser — the LLM and
// TTS ran during Deepgram's confirmation window, so the reply starts almost
// instantly. If the user was just pausing (TurnResumed), the speculative work
// is discarded and the browser never hears about it.
//
// No agent framework, no LiveKit — one Node process, plain http + ws.

import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { WebSocketServer, WebSocket } from "ws";
import { encode as mpEncode, decode as mpDecode } from "@msgpack/msgpack";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const PORT = Number(process.env.PORT || 8787);

const DEEPGRAM_API_KEY = required("DEEPGRAM_API_KEY");
const DEEPGRAM_MODEL = process.env.DEEPGRAM_MODEL || "flux-general-en";
// End-of-turn confidence (0.5–0.9, lower = snappier) and max-silence forcing.
const DEEPGRAM_EOT_THRESHOLD = process.env.DEEPGRAM_EOT_THRESHOLD || "0.7";
const DEEPGRAM_EOT_TIMEOUT_MS = process.env.DEEPGRAM_EOT_TIMEOUT_MS || "3000";
// Confidence at which Flux emits EagerEndOfTurn (speculative generation
// trigger). Lower = earlier head start but more wasted LLM/TTS work.
const DEEPGRAM_EAGER_EOT_THRESHOLD = process.env.DEEPGRAM_EAGER_EOT_THRESHOLD || "0.5";

const LLM_BASE_URL = required("LLM_BASE_URL");
const LLM_API_KEY = required("LLM_API_KEY");
const LLM_MODEL = process.env.LLM_MODEL || "google/gemma-4-26B-A4B-it";

// Optional WebRTC media gateway (gateway/, Go + Pion). When a client asks for
// transport=webrtc, audio flows browser <-> gateway <-> engine; this engine
// proxies the SDP offer so the browser only ever talks to one origin.
const GATEWAY_URL = process.env.GATEWAY_URL || "http://127.0.0.1:8788";

const FISH_API_KEY = required("FISH_API_KEY");
const FISH_MODEL = process.env.FISH_MODEL || "s2.1-pro";
const FISH_VOICE = process.env.FISH_VOICE || "";
const FISH_LATENCY_MODE = process.env.FISH_LATENCY_MODE || "balanced"; // normal | balanced | low

const MIC_SAMPLE_RATE = 16000; // browser -> Deepgram
const TTS_SAMPLE_RATE = 24000; // Fish -> browser

// Energy gate for latency measurement (NOT for turn-taking — that's Flux's
// job). A mic chunk whose RMS clears this is treated as "the user is audibly
// speaking"; turn-detect latency is measured from the last such chunk.
const VAD_THRESHOLD_DB = Number(process.env.VAD_THRESHOLD_DB || -40); // dBFS
const VAD_RMS = 32768 * 10 ** (VAD_THRESHOLD_DB / 20);

const SYSTEM_PROMPT =
  process.env.SYSTEM_PROMPT ||
  "You are a friendly voice assistant. Your replies are spoken aloud by a " +
    "text-to-speech engine, so answer in plain conversational prose: no " +
    "markdown, no lists, no emoji. Keep replies to one to three short " +
    "sentences unless the user asks for more detail.";

function required(name) {
  const v = process.env[name];
  if (!v) {
    console.error(`Missing required env var ${name} (see .env.example)`);
    process.exit(1);
  }
  return v;
}

// ---------------------------------------------------------------------------
// Sentence chunker — buffers streamed LLM tokens and emits complete clauses,
// so Fish receives whole sentences instead of arbitrary token boundaries.
// ---------------------------------------------------------------------------

const SENTENCE_PUNCT = new Set([...".。,，!！?？;；:：\n"]);

class SentenceChunker {
  #buf = "";

  // Returns an array of completed clauses (possibly empty).
  push(token) {
    this.#buf += token;
    const out = [];
    for (;;) {
      let idx = -1;
      for (let i = 0; i < this.#buf.length; i++) {
        if (SENTENCE_PUNCT.has(this.#buf[i])) {
          idx = i;
          break;
        }
      }
      if (idx === -1) break;
      // Extend through a punctuation run ("...", "?!") so it stays together.
      let end = idx + 1;
      while (end < this.#buf.length && SENTENCE_PUNCT.has(this.#buf[end])) end++;
      // Punctuation at the very end of the buffer: wait for the next token to
      // see whether more punctuation follows before splitting.
      if (end === this.#buf.length) break;
      out.push(this.#buf.slice(0, end));
      this.#buf = this.#buf.slice(end);
    }
    return out;
  }

  // Returns whatever is left (trailing text without final punctuation).
  flush() {
    const rest = this.#buf;
    this.#buf = "";
    return rest;
  }
}

// ---------------------------------------------------------------------------
// Gemma LLM — OpenAI-compatible streaming chat completion.
// ---------------------------------------------------------------------------

async function streamLLM(messages, signal, onDelta) {
  const res = await fetch(`${LLM_BASE_URL}/chat/completions`, {
    method: "POST",
    signal,
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${LLM_API_KEY}`,
    },
    body: JSON.stringify({
      model: LLM_MODEL,
      messages,
      stream: true,
      temperature: 0.6,
      max_tokens: 500,
    }),
  });
  if (!res.ok) {
    throw new Error(`LLM ${res.status}: ${(await res.text()).slice(0, 300)}`);
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let pending = "";
  let full = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    pending += decoder.decode(value, { stream: true });
    const lines = pending.split("\n");
    pending = lines.pop(); // keep the trailing partial line
    for (const line of lines) {
      const data = line.startsWith("data:") ? line.slice(5).trim() : null;
      if (!data || data === "[DONE]") continue;
      let delta;
      try {
        delta = JSON.parse(data).choices?.[0]?.delta?.content;
      } catch {
        continue;
      }
      if (delta) {
        full += delta;
        onDelta(delta);
      }
    }
  }
  return full;
}

// ---------------------------------------------------------------------------
// Fish TTS — one /v1/tts/live websocket per agent turn. Text goes in as
// sentence chunks, PCM16 @ 24 kHz comes out via onAudio.
// ---------------------------------------------------------------------------

function openFishTurn({ onAudio, onFinish, onError }) {
  const ws = new WebSocket("wss://api.fish.audio/v1/tts/live", {
    headers: {
      Authorization: `Bearer ${FISH_API_KEY}`,
      model: FISH_MODEL,
    },
  });

  let open = false;
  let closed = false;
  const queue = []; // events buffered until the socket opens

  const send = (event) => {
    if (closed) return;
    if (!open) {
      queue.push(event);
      return;
    }
    ws.send(mpEncode(event));
  };

  ws.on("open", () => {
    open = true;
    ws.send(
      mpEncode({
        event: "start",
        request: {
          text: "",
          chunk_length: 200,
          min_chunk_length: 20,
          format: "pcm",
          sample_rate: TTS_SAMPLE_RATE,
          references: [],
          reference_id: FISH_VOICE || null,
          normalize: true,
          latency: FISH_LATENCY_MODE,
          temperature: 0.7,
          top_p: 0.7,
        },
      }),
    );
    for (const e of queue.splice(0)) ws.send(mpEncode(e));
  });

  ws.on("message", (data) => {
    let msg;
    try {
      msg = mpDecode(data);
    } catch {
      return;
    }
    if (msg.event === "audio" && msg.audio?.length) {
      onAudio(Buffer.from(msg.audio));
    } else if (msg.event === "finish") {
      closed = true;
      ws.close();
      if (msg.reason === "error") onError(new Error("Fish TTS reported an error"));
      else onFinish();
    }
  });

  ws.on("error", (err) => {
    if (!closed) onError(err);
    closed = true;
  });

  return {
    pushText(text) {
      if (text) send({ event: "text", text });
    },
    // All text sent — synthesize the trailing buffer, then end the stream.
    endInput() {
      send({ event: "flush" });
      send({ event: "stop" });
    },
    close() {
      closed = true;
      try {
        ws.close();
      } catch {}
    },
  };
}

// ---------------------------------------------------------------------------
// Session — one per browser websocket. Owns the Deepgram connection, the
// conversation history, and at most one in-flight agent turn (which may be
// speculative, i.e. started on EagerEndOfTurn and not yet heard by the user).
// ---------------------------------------------------------------------------

class Session {
  constructor(client) {
    this.sid = randomUUID();
    this.client = client;
    // Audio transport: browser WS by default; a WebRTC gateway socket takes
    // over both directions when one attaches (JSON events stay on `client`).
    this.gatewayWs = null;
    this.history = [];
    this.turn = null;
    this.turnCounter = 0;
    // Wall time of the last mic chunk with speech-level energy. Flux's
    // TurnInfo events have no word timings (audio_window_end just tracks how
    // much audio it has processed, silence included), so this energy gate is
    // what "the user stopped speaking" is measured against.
    this.lastSpeechWall = 0;
    this.dg = this.#connectDeepgram();
  }

  sendJson(obj) {
    if (this.client.readyState === WebSocket.OPEN) {
      this.client.send(JSON.stringify(obj));
    }
  }

  sendAudio(buf) {
    const sink = this.gatewayWs ?? this.client;
    if (sink.readyState === WebSocket.OPEN) {
      sink.send(buf, { binary: true });
    }
  }

  // Barge-in flush must reach both the UI (transcript state) and whichever
  // transport is pacing audio (the gateway keeps its own outbound queue).
  sendClear() {
    this.sendJson({ type: "clear" });
    if (this.gatewayWs?.readyState === WebSocket.OPEN) {
      this.gatewayWs.send(JSON.stringify({ type: "clear" }));
    }
  }

  attachGateway(ws) {
    this.gatewayWs = ws;
    ws.on("message", (data, isBinary) => {
      if (isBinary) this.onMicAudio(data);
    });
    ws.on("close", () => {
      if (this.gatewayWs === ws) this.gatewayWs = null;
    });
    ws.on("error", () => {
      if (this.gatewayWs === ws) this.gatewayWs = null;
    });
  }

  onMicAudio(buf) {
    let sum = 0;
    const samples = buf.length >> 1;
    for (let i = 0; i < buf.length - 1; i += 2) {
      const s = buf.readInt16LE(i);
      sum += s * s;
    }
    if (samples && Math.sqrt(sum / samples) > VAD_RMS) {
      this.lastSpeechWall = Date.now();
    }
    if (this.dg.readyState === WebSocket.OPEN) this.dg.send(buf);
  }

  #connectDeepgram() {
    const params = new URLSearchParams({
      model: DEEPGRAM_MODEL,
      encoding: "linear16",
      sample_rate: String(MIC_SAMPLE_RATE),
      eot_threshold: DEEPGRAM_EOT_THRESHOLD,
      eot_timeout_ms: DEEPGRAM_EOT_TIMEOUT_MS,
      eager_eot_threshold: DEEPGRAM_EAGER_EOT_THRESHOLD,
    });
    const dg = new WebSocket(`wss://api.deepgram.com/v2/listen?${params}`, {
      headers: { Authorization: `Token ${DEEPGRAM_API_KEY}` },
    });

    dg.on("message", (data, isBinary) => {
      if (isBinary) return;
      let msg;
      try {
        msg = JSON.parse(data.toString());
      } catch {
        return;
      }
      if (msg.type === "Connected") {
        this.sendJson({ type: "ready" });
      } else if (msg.type === "TurnInfo") {
        this.#onTurnInfo(msg);
      } else if (msg.type === "FatalError" || msg.type === "Error") {
        console.error("[deepgram]", msg);
        this.sendJson({ type: "error", message: `STT error: ${msg.error || msg.description || "unknown"}` });
      }
    });
    dg.on("error", (err) => {
      console.error("[deepgram] socket error:", err.message);
      this.sendJson({ type: "error", message: "STT connection error" });
    });
    dg.on("close", () => this.sendJson({ type: "stt_closed" }));
    return dg;
  }

  #onTurnInfo(msg) {
    const transcript = (msg.transcript || "").trim();
    switch (msg.event) {
      case "StartOfTurn":
        // A new turn began: barge-in on whatever the agent was doing. Always
        // clear browser playback, even with no turn in flight — Fish
        // synthesizes faster than realtime, so the browser's audio queue can
        // outlive the server-side turn by several seconds.
        if (this.turn) this.#cancelTurn();
        this.sendClear();
        this.sendJson({ type: "user_start" });
        break;

      case "Update":
        if (transcript) this.sendJson({ type: "user_partial", text: transcript });
        break;

      case "EagerEndOfTurn":
        // Flux thinks the user is probably done — start generating now,
        // buffered, while it waits for enough silence to be sure.
        if (!transcript) break;
        if (this.turn && !this.turn.committed && this.turn.userText === transcript) break;
        if (this.turn) this.#cancelTurn();
        this.#startTurn(transcript, { speculative: true });
        break;

      case "TurnResumed":
        // False alarm — the user kept talking. Drop the speculative work;
        // nothing was sent to the browser, so it's a silent rollback.
        if (this.turn && !this.turn.committed) this.#cancelTurn();
        break;

      case "EndOfTurn": {
        if (!transcript) {
          if (this.turn && !this.turn.committed) this.#cancelTurn();
          break;
        }
        // How long Deepgram took to call the turn, measured from the last
        // mic chunk that had speech-level energy — i.e. the silence Flux
        // waited out before deciding the user was done, plus transit.
        const speechEndWall = this.lastSpeechWall || Date.now();
        const sttMs = Math.max(0, Date.now() - speechEndWall);
        this.sendJson({ type: "user_final", text: transcript });
        if (this.turn && !this.turn.committed && this.turn.userText === transcript) {
          this.#commitTurn(sttMs, speechEndWall);
        } else {
          // No usable speculation (none, or the transcript changed).
          if (this.turn) this.#cancelTurn();
          this.#startTurn(transcript, { speculative: false });
          this.#commitTurn(sttMs, speechEndWall);
        }
        break;
      }
    }
  }

  // Deliver a message to the browser through the turn: sent immediately once
  // the turn is committed, buffered until then.
  #deliver(turn, kind, data) {
    if (!turn.committed) {
      turn.outbox.push([kind, data]);
      return;
    }
    if (kind === "audio") {
      if (turn.firstDeliveredWall === 0) {
        turn.firstDeliveredWall = Date.now();
        this.#sendMetrics(turn);
      }
      this.sendAudio(data);
    } else {
      this.sendJson(data);
    }
  }

  #commitTurn(sttMs, speechEndWall) {
    const turn = this.turn;
    if (!turn || turn.committed) return;
    turn.committed = true;
    turn.sttMs = sttMs;
    turn.speechEndWall = speechEndWall;
    const buffered = turn.outbox.splice(0);
    for (const [kind, data] of buffered) this.#deliver(turn, kind, data);
    // Speculative turn that already finished synthesis while buffered.
    if (turn.finished) this.turn = null;
  }

  #sendMetrics(turn) {
    this.sendJson({
      type: "metrics",
      stt: Math.round(turn.sttMs),
      llm: turn.firstDeltaWall ? Math.round(turn.firstDeltaWall - turn.llmStartWall) : null,
      tts: turn.firstAudioWall && turn.firstTextPushWall
        ? Math.round(turn.firstAudioWall - turn.firstTextPushWall)
        : null,
      // Voice-to-voice: user stopped speaking -> first reply audio on the wire.
      total: Math.round(turn.firstDeliveredWall - turn.speechEndWall),
      eager: turn.eager,
    });
  }

  #cancelTurn() {
    const t = this.turn;
    if (!t) return;
    this.turn = null;
    t.abort.abort();
    t.fish?.close();
    if (t.committed) {
      // The user may have heard part of the reply — keep it coherent. (If the
      // LLM already finished, #runAgentTurn recorded the exchange; don't
      // record it twice.)
      if (t.spoken && !t.inHistory) {
        this.history.push(
          { role: "user", content: t.userText },
          { role: "assistant", content: t.spoken + "…" },
        );
      }
      this.sendClear(); // flush queued playback everywhere
    }
    // Speculative turns roll back silently: no client messages, no history.
  }

  #startTurn(userText, { speculative }) {
    const id = ++this.turnCounter;
    const turn = {
      id,
      userText,
      abort: new AbortController(),
      fish: null,
      spoken: "",
      committed: false,
      finished: false,
      inHistory: false,
      eager: speculative,
      outbox: [],
      // Latency bookkeeping (wall-clock ms)
      llmStartWall: Date.now(),
      firstDeltaWall: 0,
      firstTextPushWall: 0,
      firstAudioWall: 0,
      firstDeliveredWall: 0,
      sttMs: 0,
      speechEndWall: 0,
    };
    this.turn = turn;
    this.#runAgentTurn(turn);
  }

  async #runAgentTurn(turn) {
    const live = () => this.turn?.id === turn.id;

    turn.fish = openFishTurn({
      onAudio: (buf) => {
        if (!live()) return;
        if (turn.firstAudioWall === 0) turn.firstAudioWall = Date.now();
        this.#deliver(turn, "audio", buf);
      },
      onFinish: () => {
        if (live()) {
          turn.finished = true;
          this.#deliver(turn, "json", { type: "agent_done" });
          if (turn.committed) this.turn = null;
        }
      },
      onError: (err) => {
        console.error("[fish]", err.message);
        if (live()) {
          this.turn = null;
          this.sendJson({ type: "error", message: "TTS error" });
          this.sendJson({ type: "agent_done" });
        }
      },
    });

    const chunker = new SentenceChunker();
    const pushToFish = (text) => {
      if (!text) return;
      if (turn.firstTextPushWall === 0) turn.firstTextPushWall = Date.now();
      turn.spoken += text;
      turn.fish.pushText(text);
    };

    try {
      const full = await streamLLM(
        [
          { role: "system", content: SYSTEM_PROMPT },
          ...this.history,
          { role: "user", content: turn.userText },
        ],
        turn.abort.signal,
        (delta) => {
          if (!live()) return;
          if (turn.firstDeltaWall === 0) turn.firstDeltaWall = Date.now();
          this.#deliver(turn, "json", { type: "agent_text", text: delta });
          for (const sentence of chunker.push(delta)) pushToFish(sentence);
        },
      );
      if (!live()) return;
      pushToFish(chunker.flush());
      turn.fish.endInput();
      turn.inHistory = true;
      this.history.push(
        { role: "user", content: turn.userText },
        { role: "assistant", content: full },
      );
    } catch (err) {
      if (err.name === "AbortError") return; // barge-in / rollback — handled
      console.error("[llm]", err.message);
      if (live()) {
        this.turn = null;
        turn.fish.close();
        this.sendJson({ type: "error", message: "LLM error" });
        this.sendJson({ type: "agent_done" });
      }
    }
  }

  destroy() {
    this.turn?.abort.abort();
    this.turn?.fish?.close();
    this.turn = null;
    try {
      this.gatewayWs?.close();
    } catch {}
    try {
      this.dg.close();
    } catch {}
  }
}

// ---------------------------------------------------------------------------
// HTTP server (static files) + websocket endpoint
// ---------------------------------------------------------------------------

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
};

const sessions = new Map(); // sid -> Session

// Browser -> engine -> gateway SDP relay, so the page only talks to one origin.
async function handleRtcOffer(req, res) {
  let body = "";
  req.on("data", (c) => (body += c));
  req.on("end", async () => {
    try {
      const { sid, sdp } = JSON.parse(body);
      if (!sessions.has(sid)) {
        res.writeHead(404).end(JSON.stringify({ error: "unknown session" }));
        return;
      }
      const upstream = await fetch(`${GATEWAY_URL}/offer`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sid, sdp }),
      });
      const answer = await upstream.text();
      res.writeHead(upstream.status, { "Content-Type": "application/json" });
      res.end(answer);
    } catch (err) {
      console.error("[rtc] offer relay failed:", err.message);
      res.writeHead(502).end(JSON.stringify({ error: "gateway unreachable" }));
    }
  });
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, "http://x");
  if (req.method === "POST" && url.pathname === "/rtc/offer") {
    handleRtcOffer(req, res);
    return;
  }
  let file = url.pathname === "/" ? "/index.html" : url.pathname;
  file = path.normalize(file).replace(/^(\.\.[/\\])+/, "");
  const full = path.join(__dirname, "public", file);
  if (!full.startsWith(path.join(__dirname, "public"))) {
    res.writeHead(403).end();
    return;
  }
  fs.readFile(full, (err, data) => {
    if (err) {
      res.writeHead(404).end("not found");
      return;
    }
    res.writeHead(200, { "Content-Type": MIME[path.extname(full)] || "application/octet-stream" });
    res.end(data);
  });
});

const wss = new WebSocketServer({ noServer: true });

server.on("upgrade", (req, socket, head) => {
  const url = new URL(req.url, "http://x");

  if (url.pathname === "/ws") {
    // Browser (or smoke client): owns the session and all JSON events.
    wss.handleUpgrade(req, socket, head, (client) => {
      const session = new Session(client);
      sessions.set(session.sid, session);
      console.log(`[session] ${session.sid} connected`);
      session.sendJson({ type: "session", sid: session.sid });
      client.on("message", (data, isBinary) => {
        if (isBinary) session.onMicAudio(data);
      });
      client.on("close", () => {
        console.log(`[session] ${session.sid} closed`);
        sessions.delete(session.sid);
        session.destroy();
      });
      client.on("error", () => {
        sessions.delete(session.sid);
        session.destroy();
      });
    });
    return;
  }

  if (url.pathname === "/gateway") {
    // WebRTC gateway attaching as the audio transport for an existing session.
    const session = sessions.get(url.searchParams.get("sid"));
    if (!session) {
      socket.destroy();
      return;
    }
    wss.handleUpgrade(req, socket, head, (gw) => {
      console.log(`[session] ${session.sid} gateway attached`);
      session.attachGateway(gw);
    });
    return;
  }

  socket.destroy();
});

server.listen(PORT, () => {
  console.log(`fish-bare-agent listening on http://localhost:${PORT}`);
  console.log(`  stt:  deepgram ${DEEPGRAM_MODEL} (eot ${DEEPGRAM_EOT_THRESHOLD}, eager ${DEEPGRAM_EAGER_EOT_THRESHOLD}, timeout ${DEEPGRAM_EOT_TIMEOUT_MS}ms)`);
  console.log(`  llm:  ${LLM_MODEL} @ ${LLM_BASE_URL}`);
  console.log(`  tts:  fish ${FISH_MODEL} latency=${FISH_LATENCY_MODE} voice=${FISH_VOICE || "(default)"}`);
});
