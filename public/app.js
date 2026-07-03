// Browser side: mic -> WS (PCM16 @ 16 kHz), WS -> speaker (PCM16 @ 24 kHz),
// plus a running transcript from the server's JSON events.

const $ = (id) => document.getElementById(id);
const log = $("log");
const orb = $("orb");
const statusEl = $("status");
const btn = $("btn");

let ws = null;
let inCtx = null;
let outCtx = null;
let player = null;
let micStream = null;
let running = false;

let userBubble = null; // live (partial) user bubble
let agentBubble = null; // streaming agent bubble
let lastAgentBubble = null; // most recent agent bubble (for interrupt marking)
let agentSpeaking = false;

const HINT_HTML =
  '<div id="hint">Tap <b>Start</b>, allow the mic, and just talk.<br />' +
  "Interrupt it mid-sentence &mdash; it will stop and listen.</div>";

function setStatus(text) {
  statusEl.textContent = text;
}

function setOrb(state) {
  orb.className = state;
}

function bubble(cls) {
  document.getElementById("hint")?.remove();
  const el = document.createElement("div");
  el.className = `msg ${cls}`;
  log.appendChild(el);
  log.scrollTop = log.scrollHeight;
  return el;
}

function fmtMs(v) {
  if (v == null) return "—";
  return v >= 1000
    ? `${(v / 1000).toFixed(2)}<span class="unit"> s</span>`
    : `${v}<span class="unit"> ms</span>`;
}

function showMetrics(m) {
  document.getElementById("m-stt").innerHTML = fmtMs(m.stt);
  document.getElementById("m-llm").innerHTML = fmtMs(m.llm);
  document.getElementById("m-tts").innerHTML = fmtMs(m.tts);
  document.getElementById("m-total").innerHTML = fmtMs(m.total);
  document.getElementById("eager-badge").style.display = m.eager ? "inline" : "none";
}

async function start() {
  btn.disabled = true;
  try {
    micStream = await navigator.mediaDevices.getUserMedia({
      audio: { echoCancellation: true, noiseSuppression: true, channelCount: 1 },
    });
  } catch {
    setStatus("mic permission denied");
    btn.disabled = false;
    return;
  }

  // Capture context pinned to 16 kHz (the browser resamples the mic for us).
  inCtx = new AudioContext({ sampleRate: 16000 });
  await inCtx.audioWorklet.addModule("/mic-worklet.js");
  const src = inCtx.createMediaStreamSource(micStream);
  const mic = new AudioWorkletNode(inCtx, "mic-capture");
  src.connect(mic);

  // Playback context pinned to Fish's 24 kHz output.
  outCtx = new AudioContext({ sampleRate: 24000 });
  await outCtx.audioWorklet.addModule("/player-worklet.js");
  player = new AudioWorkletNode(outCtx, "pcm-player");
  player.connect(outCtx.destination);
  player.port.onmessage = (e) => {
    agentSpeaking = e.data.playing;
    if (running) setOrb(agentSpeaking ? "speaking" : "listening");
  };

  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws`);
  ws.binaryType = "arraybuffer";

  mic.port.onmessage = (e) => {
    if (ws.readyState === WebSocket.OPEN) ws.send(e.data.buffer);
  };

  ws.onmessage = (e) => {
    if (e.data instanceof ArrayBuffer) {
      player.port.postMessage(new Int16Array(e.data));
      return;
    }
    handleEvent(JSON.parse(e.data));
  };
  ws.onclose = () => stop("connection closed");
  ws.onerror = () => stop("connection error");

  running = true;
  btn.textContent = "Stop";
  btn.className = "stop";
  btn.disabled = false;
  setStatus("connecting…");
}

function handleEvent(msg) {
  switch (msg.type) {
    case "ready":
      setStatus("listening — say something");
      setOrb("listening");
      break;
    case "user_start":
      userBubble = bubble("user partial");
      break;
    case "user_partial":
      if (!userBubble) userBubble = bubble("user partial");
      userBubble.textContent = msg.text;
      log.scrollTop = log.scrollHeight;
      break;
    case "user_final":
      if (!userBubble) userBubble = bubble("user");
      userBubble.textContent = msg.text;
      userBubble.classList.remove("partial");
      userBubble = null;
      agentBubble = null;
      setStatus("thinking…");
      break;
    case "agent_text":
      if (!agentBubble) agentBubble = lastAgentBubble = bubble("agent");
      agentBubble.textContent += msg.text;
      log.scrollTop = log.scrollHeight;
      setStatus("speaking");
      break;
    case "clear": // barge-in: stop playback immediately
      // Only mark the reply interrupted if it was actually cut off — "clear"
      // also arrives on every normal turn start as a safety flush.
      if (agentBubble || agentSpeaking) lastAgentBubble?.classList.add("interrupted");
      player?.port.postMessage({ cmd: "clear" });
      agentBubble = null;
      setStatus("listening");
      break;
    case "agent_done":
      agentBubble = null;
      setStatus("listening");
      break;
    case "metrics":
      showMetrics(msg);
      break;
    case "error":
      setStatus(msg.message || "error");
      break;
    case "stt_closed":
      if (running) setStatus("STT connection closed — restart");
      break;
  }
}

function stop(reason) {
  if (!running) return;
  running = false;
  ws?.close();
  micStream?.getTracks().forEach((t) => t.stop());
  inCtx?.close();
  outCtx?.close();
  ws = null;
  inCtx = outCtx = player = micStream = null;
  // Fresh slate: empty the transcript and reset the latency chips.
  log.innerHTML = HINT_HTML;
  userBubble = agentBubble = lastAgentBubble = null;
  agentSpeaking = false;
  for (const id of ["m-stt", "m-llm", "m-tts", "m-total"]) {
    document.getElementById(id).innerHTML = "&mdash;";
  }
  document.getElementById("eager-badge").style.display = "none";
  btn.textContent = "Start";
  btn.className = "";
  btn.disabled = false;
  setOrb("");
  setStatus(reason || "stopped");
}

btn.onclick = () => (running ? stop() : start());
