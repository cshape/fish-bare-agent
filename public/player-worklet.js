// Streaming PCM16 player: queues chunks from the main thread and feeds the
// output, emitting silence on underrun. "clear" drops everything (barge-in).
class PcmPlayer extends AudioWorkletProcessor {
  constructor() {
    super();
    this.queue = []; // Float32Array chunks
    this.offset = 0; // read position within queue[0]
    this.playing = false;
    this.gain = 1; // ducked during held barge-in evaluation
    this.port.onmessage = (e) => {
      if (e.data.cmd === "clear") {
        this.queue = [];
        this.offset = 0;
        this.gain = 1;
        return;
      }
      if (e.data.cmd === "gain") {
        this.gain = e.data.value;
        return;
      }
      const pcm = e.data; // Int16Array
      const f = new Float32Array(pcm.length);
      for (let i = 0; i < pcm.length; i++) f[i] = pcm[i] / 0x8000;
      this.queue.push(f);
    };
  }

  process(_inputs, outputs) {
    const out = outputs[0][0];
    let i = 0;
    while (i < out.length && this.queue.length) {
      const head = this.queue[0];
      const n = Math.min(out.length - i, head.length - this.offset);
      for (let k = 0; k < n; k++) out[i + k] = head[this.offset + k] * this.gain;
      i += n;
      this.offset += n;
      if (this.offset === head.length) {
        this.queue.shift();
        this.offset = 0;
      }
    }
    out.fill(0, i);

    const playing = i > 0;
    if (playing !== this.playing) {
      this.playing = playing;
      this.port.postMessage({ playing });
    }
    return true;
  }
}

registerProcessor("pcm-player", PcmPlayer);
