// Captures mic audio at the context sample rate (16 kHz — the AudioContext is
// created with that rate) and posts PCM16 chunks to the main thread.
class MicCapture extends AudioWorkletProcessor {
  constructor() {
    super();
    this.buf = new Int16Array(512); // 32 ms @ 16 kHz — small chunks keep STT latency low
    this.n = 0;
  }

  process(inputs) {
    const ch = inputs[0]?.[0];
    if (!ch) return true;
    for (let i = 0; i < ch.length; i++) {
      const s = Math.max(-1, Math.min(1, ch[i]));
      this.buf[this.n++] = s < 0 ? s * 0x8000 : s * 0x7fff;
      if (this.n === this.buf.length) {
        this.port.postMessage(this.buf.slice(0));
        this.n = 0;
      }
    }
    return true;
  }
}

registerProcessor("mic-capture", MicCapture);
