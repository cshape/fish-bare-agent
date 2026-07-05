// Load/latency test for the WS transport: N concurrent single-turn sessions,
// each speaking the same question and timing the reply.
//
//   node --env-file=.env scripts/loadtest.js --sessions 8 --stagger 250
//
// Reports server-side per-stage metrics plus a client-observed response gap
// (last speech sample sent -> first reply audio byte received), which is the
// transport-comparable number.

import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import WebSocket from "ws";

const args = process.argv.slice(2);
const flag = (name, def) => {
  const i = args.indexOf(`--${name}`);
  return i === -1 ? def : Number(args[i + 1]);
};
const SESSIONS = flag("sessions", 4);
const STAGGER_MS = flag("stagger", 250);
const PORT = Number(process.env.PORT || 8787);
const QUESTION = "What is the capital of France?";

const wav = path.join(os.tmpdir(), `loadtest-${process.pid}.wav`);
execFileSync("say", ["-o", wav, "--file-format=WAVE", "--data-format=LEI16@16000", QUESTION]);
const speech = Buffer.concat([Buffer.alloc(16000), fs.readFileSync(wav).subarray(44)]);
fs.unlinkSync(wav);

const CHUNK = 3200; // 100 ms @ 16 kHz

function runSession(id) {
  return new Promise((resolve) => {
    const result = { id, ok: false };
    const ws = new WebSocket(`ws://localhost:${PORT}/ws`);
    let tSpeechEnd = 0;
    let tFirstAudio = 0;
    let done = false;

    const finish = (ok, why) => {
      if (done) return;
      done = true;
      result.ok = ok;
      if (!ok) result.error = why;
      try {
        ws.close();
      } catch {}
      resolve(result);
    };
    const timeout = setTimeout(() => finish(false, "timeout"), 60000);

    ws.on("open", async () => {
      let off = 0;
      const silence = Buffer.alloc(CHUNK);
      while (!done && ws.readyState === WebSocket.OPEN) {
        if (off < speech.length) {
          ws.send(speech.subarray(off, off + CHUNK));
          off += CHUNK;
          if (off >= speech.length) tSpeechEnd = Date.now();
        } else {
          ws.send(silence);
        }
        await new Promise((r) => setTimeout(r, 95));
      }
    });

    ws.on("message", (data, isBinary) => {
      if (isBinary) {
        if (!tFirstAudio) {
          tFirstAudio = Date.now();
          result.clientGapMs = tFirstAudio - tSpeechEnd;
        }
        return;
      }
      const msg = JSON.parse(data.toString());
      if (msg.type === "metrics") {
        Object.assign(result, { sttMs: msg.stt, llmMs: msg.llm, ttsMs: msg.tts, totalMs: msg.total, eager: msg.eager });
      } else if (msg.type === "agent_done") {
        clearTimeout(timeout);
        finish(Boolean(result.totalMs && tFirstAudio), "no metrics/audio");
      } else if (msg.type === "error") {
        clearTimeout(timeout);
        finish(false, msg.message);
      }
    });
    ws.on("error", (e) => finish(false, e.message));
  });
}

function pct(sorted, p) {
  if (!sorted.length) return null;
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
}

function summarize(results, field) {
  const vals = results.filter((r) => r.ok && r[field] != null).map((r) => r[field]).sort((a, b) => a - b);
  if (!vals.length) return "n/a";
  const mean = Math.round(vals.reduce((a, b) => a + b, 0) / vals.length);
  return `mean ${mean}  p50 ${pct(vals, 50)}  p95 ${pct(vals, 95)}`;
}

const started = Date.now();
const runs = [];
for (let i = 0; i < SESSIONS; i++) {
  runs.push(new Promise((r) => setTimeout(() => runSession(i).then(r), i * STAGGER_MS)));
}
const results = await Promise.all(runs);

const ok = results.filter((r) => r.ok);
console.log(`\n[ws loadtest] ${ok.length}/${SESSIONS} sessions ok in ${((Date.now() - started) / 1000).toFixed(1)}s`);
for (const f of ["sttMs", "llmMs", "ttsMs", "totalMs", "clientGapMs"]) {
  console.log(`  ${f.padEnd(12)} ${summarize(results, f)}`);
}
for (const r of results.filter((r) => !r.ok)) console.log(`  session ${r.id} FAILED: ${r.error}`);
process.exit(ok.length === SESSIONS ? 0 : 1);
