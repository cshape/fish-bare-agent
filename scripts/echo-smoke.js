// Echo-suppression smoke test (macOS, uses `say`).
//
// Simulates speaker->mic leakage: asks for a long story, and once the reply
// is streaming, "speaks" the agent's own opening words back into the mic
// (smart barge-in + echo filter enabled). Passes iff the engine flags it as
// echo (echo_suppressed), never cuts the story (no clear), and never answers
// the echo as a real turn.

import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import WebSocket from "ws";

const PORT = Number(process.env.PORT || 8787);
const CHUNK = 3200; // 100 ms @ 16 kHz
const silence = Buffer.alloc(CHUNK);

function synth16k(text) {
  const wav = path.join(os.tmpdir(), `echo-smoke-${process.pid}.wav`);
  execFileSync("say", ["-o", wav, "--file-format=WAVE", "--data-format=LEI16@16000", text]);
  const pcm = Buffer.concat([Buffer.alloc(8000), fs.readFileSync(wav).subarray(44)]);
  fs.unlinkSync(wav);
  return pcm;
}

const question = synth16k("Please tell me a nice long story about a fish.");

const ws = new WebSocket(`ws://localhost:${PORT}/ws`);
const outQueue = [];
const enqueue = (pcm) => {
  for (let off = 0; off < pcm.length; off += CHUNK) outQueue.push(pcm.subarray(off, off + CHUNK));
};

let agentText = "";
let echoInjected = false;
let echoSuppressed = false;
let clears = 0;
let finals = [];
let done = false;

const timeout = setTimeout(() => {
  console.error("[echo-smoke] FAIL: timeout");
  process.exit(1);
}, 90000);

ws.on("open", async () => {
  ws.send(JSON.stringify({ type: "config", bargeMode: "smart", echoFilter: true, minWords: 2 }));
  console.log("[echo-smoke] speaking question…");
  enqueue(question);
  while (!done && ws.readyState === WebSocket.OPEN) {
    ws.send(outQueue.shift() ?? silence);
    await new Promise((r) => setTimeout(r, 95));
  }
});

ws.on("message", (data, isBinary) => {
  if (isBinary) {
    // Reply audio is flowing. Once we have enough agent text, leak it back.
    if (!echoInjected && agentText.split(/\s+/).length >= 8) {
      echoInjected = true;
      const words = agentText.split(/\s+/).slice(0, 10).join(" ");
      console.log(`[echo-smoke] injecting echo: "${words}"`);
      enqueue(synth16k(words));
    }
    return;
  }
  const msg = JSON.parse(data.toString());
  switch (msg.type) {
    case "agent_text":
      agentText += msg.text;
      break;
    case "user_final":
      finals.push(msg.text);
      console.log(`[echo-smoke] user_final: "${msg.text}"`);
      break;
    case "clear":
      if (echoInjected) clears++;
      break;
    case "echo_suppressed":
      echoSuppressed = true;
      console.log(`[echo-smoke] echo suppressed: "${msg.text}"`);
      break;
    case "agent_done": {
      if (!echoInjected) break; // story ended before we injected — inconclusive
      done = true;
      clearTimeout(timeout);
      const answeredEcho = finals.length > 1;
      console.log(
        `[echo-smoke] suppressed=${echoSuppressed} clears-after-echo=${clears} extra-turns=${finals.length - 1}`,
      );
      const ok = echoSuppressed && clears === 0 && !answeredEcho;
      console.log(ok ? "[echo-smoke] PASS" : "[echo-smoke] FAIL");
      process.exit(ok ? 0 : 1);
    }
  }
});

ws.on("error", (e) => {
  console.error(`[echo-smoke] ws error: ${e.message}`);
  process.exit(1);
});
