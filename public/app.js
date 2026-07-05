// Browser side. Two transports:
//   default        mic -> WS (PCM16 @ 16 kHz), WS -> speaker (PCM16 @ 24 kHz)
//   ?transport=webrtc   audio rides a PeerConnection through the Go/Pion
//                       gateway (Opus @ 48 kHz); the WS carries JSON only.
// JSON events (transcripts, metrics, clear) arrive on the WS either way.

const $ = (id) => document.getElementById(id);

// --- knobs: transport + self-interruption defenses --------------------------
// Persisted in localStorage; ?transport=webrtc overrides the saved transport.
const settings = Object.assign(
  { transport: "ws", bargeMode: "instant", echoFilter: true, minWords: 2 },
  JSON.parse(localStorage.getItem("fish-bare-settings") || "{}"),
);
const urlTransport = new URLSearchParams(location.search).get("transport");
if (urlTransport === "webrtc" || urlTransport === "ws") settings.transport = urlTransport;

const useRtc = () => settings.transport === "webrtc";

function saveSettings() {
  localStorage.setItem("fish-bare-settings", JSON.stringify(settings));
}

function sendConfig() {
  if (ws?.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({
      type: "config",
      bargeMode: settings.bargeMode,
      echoFilter: settings.echoFilter,
      minWords: settings.minWords,
    }));
  }
}

function renderKnobs() {
  const mark = (id, value) => {
    for (const b of $(id).querySelectorAll("button")) {
      b.classList.toggle("on", b.dataset.v === value);
    }
  };
  mark("k-transport", settings.transport);
  mark("k-barge", settings.bargeMode);
  mark("k-echo", settings.echoFilter ? "on" : "off");
  mark("k-words", String(settings.minWords));
}

function wireKnob(id, apply) {
  $(id).addEventListener("click", (e) => {
    const v = e.target.dataset?.v;
    if (!v) return;
    apply(v);
    saveSettings();
    renderKnobs();
    sendConfig();
  });
}
wireKnob("k-transport", (v) => {
  if (v !== settings.transport && running) stop("transport changed — press start");
  settings.transport = v;
});
wireKnob("k-barge", (v) => (settings.bargeMode = v));
wireKnob("k-echo", (v) => (settings.echoFilter = v === "on"));
wireKnob("k-words", (v) => (settings.minWords = Number(v)));
const log = $("log");
const orb = $("orb");
const statusEl = $("status");
const btn = $("btn");

let ws = null;
let inCtx = null;
let outCtx = null;
let player = null;
let micStream = null;
let pc = null; // RTCPeerConnection (webrtc transport only)
let remoteAudio = null; // hidden muted element that keeps the remote stream alive
let rtcCtx = null; // WebAudio graph for webrtc playback (gain = ducking)
let rtcGain = null;
let rtcActive = false; // audio currently riding webrtc (false after fallback)
let rtcWatchdog = null;
let sid = null;
let iceServers = null; // from the session message (STUN + optional TURN)
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

  if (!useRtc()) {
    await initWsAudio();
  } else {
    // Create the playback context inside the click gesture so iOS lets it run.
    rtcCtx = new AudioContext();
    rtcCtx.resume();
  }

  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws`);
  ws.binaryType = "arraybuffer";

  ws.onmessage = (e) => {
    if (e.data instanceof ArrayBuffer) {
      player?.port.postMessage(new Int16Array(e.data));
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

// WS transport audio: mic worklet up, PCM player worklet down. Also used as
// the fallback when the WebRTC path can't connect (firewalls, cell networks).
async function initWsAudio() {
  // Capture context pinned to 16 kHz (the browser resamples the mic for us).
  inCtx = new AudioContext({ sampleRate: 16000 });
  await inCtx.audioWorklet.addModule("/mic-worklet.js");
  const src = inCtx.createMediaStreamSource(micStream);
  const mic = new AudioWorkletNode(inCtx, "mic-capture");
  src.connect(mic);
  mic.port.onmessage = (e) => {
    if (ws?.readyState === WebSocket.OPEN) ws.send(e.data.buffer);
  };

  // Playback context pinned to Fish's 24 kHz output.
  outCtx = new AudioContext({ sampleRate: 24000 });
  await outCtx.audioWorklet.addModule("/player-worklet.js");
  player = new AudioWorkletNode(outCtx, "pcm-player");
  player.connect(outCtx.destination);
  player.port.onmessage = (e) => {
    agentSpeaking = e.data.playing;
    if (running) setOrb(agentSpeaking ? "speaking" : "listening");
  };
}

// WebRTC transport: mic and speaker ride a PeerConnection; the Go gateway
// terminates it and bridges PCM to the engine. No STUN needed — the gateway
// answers with host candidates (ICE-Lite).
async function setupRtc(id) {
  rtcActive = true;
  // ICE servers come from the engine: STUN for hole punching, plus TURN when
  // configured — required for cellular/CGNAT, where direct paths don't exist.
  pc = new RTCPeerConnection({
    iceServers: iceServers ?? [{ urls: "stun:stun.l.google.com:19302" }],
  });
  for (const track of micStream.getAudioTracks()) pc.addTrack(track, micStream);
  pc.ontrack = (e) => {
    // Muted element keeps the remote stream flowing; audible playback goes
    // through a WebAudio gain node so the server can duck it (barge-in hold).
    remoteAudio = new Audio();
    remoteAudio.srcObject = e.streams[0];
    remoteAudio.muted = true;
    remoteAudio.play().catch(() => {});
    const src = rtcCtx.createMediaStreamSource(e.streams[0]);
    rtcGain = rtcCtx.createGain();
    src.connect(rtcGain);
    rtcGain.connect(rtcCtx.destination);
  };
  pc.onconnectionstatechange = () => {
    const st = pc?.connectionState;
    if (!st) return;
    if (ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: "rtc_state", state: st }));
    if (rtcActive && st !== "connected") setStatus(`webrtc: ${st}`);
    if (st === "connected") {
      clearTimeout(rtcWatchdog);
      setStatus("listening — say something");
    } else if (st === "failed") {
      fallbackToWs("webrtc failed");
    }
  };
  // If ICE can't get through (firewall, AP isolation, cellular), don't strand
  // the user — drop to websocket audio on the connection we already have.
  rtcWatchdog = setTimeout(() => {
    if (pc && pc.connectionState !== "connected") fallbackToWs("webrtc timeout");
  }, 7000);

  const offer = await pc.createOffer();
  await pc.setLocalDescription(offer);
  await new Promise((resolve) => {
    // Non-trickle: wait for gathering so the offer carries all candidates —
    // but cap the wait so a dead STUN/TURN server can't stall setup.
    const cap = setTimeout(resolve, 3000);
    if (pc.iceGatheringState === "complete") {
      clearTimeout(cap);
      return resolve();
    }
    pc.onicegatheringstatechange = () => {
      if (pc.iceGatheringState === "complete") {
        clearTimeout(cap);
        resolve();
      }
    };
  });
  const res = await fetch("/rtc/offer", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sid: id, sdp: pc.localDescription.sdp }),
  });
  if (!res.ok) {
    fallbackToWs("webrtc gateway unavailable");
    return;
  }
  const { sdp } = await res.json();
  await pc.setRemoteDescription({ type: "answer", sdp });
}

async function fallbackToWs(why) {
  if (!running || !rtcActive) return;
  rtcActive = false;
  clearTimeout(rtcWatchdog);
  console.warn(`${why} — falling back to websocket audio`);
  setStatus(`${why} — using websocket audio`);
  ws?.send(JSON.stringify({ type: "use_ws_audio" }));
  pc?.close();
  pc = null;
  rtcCtx?.close();
  rtcCtx = rtcGain = remoteAudio = null;
  await initWsAudio();
}

function setDuck(on) {
  const v = on ? 0.15 : 1;
  player?.port.postMessage({ cmd: "gain", value: v });
  if (rtcGain) rtcGain.gain.value = v;
}

function handleEvent(msg) {
  switch (msg.type) {
    case "session":
      sid = msg.sid;
      if (msg.iceServers) iceServers = msg.iceServers;
      sendConfig();
      if (useRtc()) setupRtc(sid).catch(() => fallbackToWs("webrtc setup failed"));
      break;
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
      // No player worklet on the webrtc path — approximate the orb from events.
      if (rtcActive) setOrb("speaking");
      break;
    case "duck": // barge-in being evaluated — soften playback instantly
      setDuck(true);
      break;
    case "unduck": // false alarm (our own echo) — swell back
      setDuck(false);
      break;
    case "clear": // barge-in: stop playback immediately
      // Only mark the reply interrupted if it was actually cut off — "clear"
      // also arrives on every normal turn start as a safety flush. On the
      // webrtc path the gateway drops its paced queue; nothing to do here.
      if (agentBubble || agentSpeaking) lastAgentBubble?.classList.add("interrupted");
      player?.port.postMessage({ cmd: "clear" });
      setDuck(false);
      agentBubble = null;
      setStatus("listening");
      if (rtcActive) setOrb("listening");
      break;
    case "agent_done":
      agentBubble = null;
      setStatus("listening");
      if (rtcActive) setOrb("listening");
      break;
    case "echo_suppressed":
      // The engine decided this "user turn" was the agent's own voice.
      if (userBubble) {
        userBubble.textContent = msg.text;
        userBubble.classList.remove("partial");
        userBubble.classList.add("echo");
        userBubble = null;
      }
      setStatus("echo suppressed");
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
  clearTimeout(rtcWatchdog);
  rtcActive = false;
  ws?.close();
  pc?.close();
  remoteAudio?.pause();
  micStream?.getTracks().forEach((t) => t.stop());
  inCtx?.close();
  outCtx?.close();
  rtcCtx?.close();
  ws = null;
  pc = remoteAudio = rtcCtx = rtcGain = sid = null;
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
renderKnobs();
