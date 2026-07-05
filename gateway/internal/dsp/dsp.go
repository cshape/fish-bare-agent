// Package dsp provides the two fixed-ratio resamplers the gateway needs:
// 48 kHz -> 16 kHz (mic toward Deepgram) and 24 kHz -> 48 kHz (Fish toward
// Opus). Both are windowed-sinc FIR polyphase filters — small, allocation-
// light, and good enough that the codec is the quality bottleneck, not us.
package dsp

import "math"

// makeLowpass designs a Hamming-windowed sinc FIR. cutoff is normalized to
// the sample rate the filter runs at (0..0.5); gain scales the passband.
func makeLowpass(taps int, cutoff, gain float64) []float64 {
	h := make([]float64, taps)
	center := float64(taps-1) / 2
	var sum float64
	for i := range h {
		x := float64(i) - center
		var s float64
		if x == 0 {
			s = 2 * cutoff
		} else {
			s = math.Sin(2*math.Pi*cutoff*x) / (math.Pi * x)
		}
		w := 0.54 - 0.46*math.Cos(2*math.Pi*float64(i)/float64(taps-1)) // Hamming
		h[i] = s * w
		sum += h[i]
	}
	for i := range h {
		h[i] *= gain / sum // normalize DC gain
	}
	return h
}

func clip16(v float64) int16 {
	if v > 32767 {
		return 32767
	}
	if v < -32768 {
		return -32768
	}
	return int16(v)
}

// Decimator downsamples by an integer factor with an anti-alias FIR.
type Decimator struct {
	factor int
	taps   []float64
	buf    []float64
	next   int // index in buf whose sample is the newest tap of the next output
}

// NewDecimator48to16 converts 48 kHz mono PCM to 16 kHz.
func NewDecimator48to16() *Decimator {
	factor := 3
	// Cutoff at 0.9 * Nyquist of the output rate, normalized to input rate.
	taps := makeLowpass(48, 0.9*0.5/float64(factor), 1.0)
	return &Decimator{factor: factor, taps: taps, next: len(taps) - 1}
}

func (d *Decimator) Process(in []int16) []int16 {
	for _, s := range in {
		d.buf = append(d.buf, float64(s))
	}
	n := len(d.taps)
	var out []int16
	for d.next < len(d.buf) {
		var acc float64
		for k := 0; k < n; k++ {
			acc += d.taps[k] * d.buf[d.next-k]
		}
		out = append(out, clip16(acc))
		d.next += d.factor
	}
	// Keep only what future outputs still need.
	keep := d.next - (n - 1)
	if keep > 0 {
		d.buf = append(d.buf[:0], d.buf[keep:]...)
		d.next -= keep
	}
	return out
}

// Interpolator upsamples by 2 with a polyphase image-rejection FIR.
type Interpolator struct {
	phases [2][]float64
	buf    []float64
	next   int // index in buf of the next input sample to expand
}

// NewInterpolator24to48 converts 24 kHz mono PCM to 48 kHz. (Also used for
// any x2 upsample, e.g. the smoke client's 24 kHz test audio.)
func NewInterpolator24to48() *Interpolator {
	// Prototype filter at the output rate: cutoff 0.9 * input Nyquist,
	// gain 2 to compensate for zero-stuffing.
	h := makeLowpass(32, 0.9*0.25, 2.0)
	it := &Interpolator{}
	for i, c := range h {
		it.phases[i%2] = append(it.phases[i%2], c)
	}
	it.next = len(it.phases[0]) - 1
	return it
}

func (u *Interpolator) Process(in []int16) []int16 {
	for _, s := range in {
		u.buf = append(u.buf, float64(s))
	}
	n := len(u.phases[0])
	var out []int16
	for u.next < len(u.buf) {
		for p := 0; p < 2; p++ {
			var acc float64
			for k := 0; k < n; k++ {
				acc += u.phases[p][k] * u.buf[u.next-k]
			}
			out = append(out, clip16(acc))
		}
		u.next++
	}
	keep := u.next - (n - 1)
	if keep > 0 {
		u.buf = append(u.buf[:0], u.buf[keep:]...)
		u.next -= keep
	}
	return out
}

// Bytes <-> int16 helpers (little-endian, the wire format everywhere here).

func BytesToPCM(b []byte) []int16 {
	pcm := make([]int16, len(b)/2)
	for i := range pcm {
		pcm[i] = int16(uint16(b[2*i]) | uint16(b[2*i+1])<<8)
	}
	return pcm
}

func PCMToBytes(pcm []int16) []byte {
	b := make([]byte, len(pcm)*2)
	for i, s := range pcm {
		b[2*i] = byte(uint16(s))
		b[2*i+1] = byte(uint16(s) >> 8)
	}
	return b
}
