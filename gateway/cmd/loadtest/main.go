// Load/latency test for the WebRTC transport: N concurrent Pion clients,
// each speaking one question through the gateway and timing the reply.
//
//	go run -tags nolibopusfile ./cmd/loadtest -wav q.wav -n 8 -stagger 250ms
//
// Reports the engine's per-stage metrics plus the client-observed response
// gap (last speech frame paced out -> first reply RTP packet), comparable
// with scripts/loadtest.js on the WS path.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	opus "gopkg.in/hraban/opus.v2"

	"fish-bare-agent/gateway/internal/dsp"
)

type result struct {
	id          int
	ok          bool
	err         string
	sttMs       float64
	llmMs       float64
	ttsMs       float64
	totalMs     float64
	clientGapMs float64
}

func main() {
	host := flag.String("host", "localhost:8787", "engine host:port")
	wavPath := flag.String("wav", "", "24 kHz mono PCM16 WAV to speak")
	n := flag.Int("n", 4, "concurrent sessions")
	stagger := flag.Duration("stagger", 250*time.Millisecond, "start stagger")
	flag.Parse()
	if *wavPath == "" {
		log.Fatal("-wav is required")
	}
	wav, err := os.ReadFile(*wavPath)
	if err != nil {
		log.Fatal(err)
	}
	pcm24 := append(make([]int16, 12000), dsp.BytesToPCM(wav[44:])...) // 500ms lead-in

	started := time.Now()
	results := make([]result, *n)
	var wg sync.WaitGroup
	for i := 0; i < *n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			time.Sleep(time.Duration(id) * *stagger)
			results[id] = runSession(id, *host, pcm24)
		}(i)
	}
	wg.Wait()

	okCount := 0
	for _, r := range results {
		if r.ok {
			okCount++
		}
	}
	fmt.Printf("\n[webrtc loadtest] %d/%d sessions ok in %.1fs\n", okCount, *n, time.Since(started).Seconds())
	summarize(results, "sttMs", func(r result) float64 { return r.sttMs })
	summarize(results, "llmMs", func(r result) float64 { return r.llmMs })
	summarize(results, "ttsMs", func(r result) float64 { return r.ttsMs })
	summarize(results, "totalMs", func(r result) float64 { return r.totalMs })
	summarize(results, "clientGapMs", func(r result) float64 { return r.clientGapMs })
	for _, r := range results {
		if !r.ok {
			fmt.Printf("  session %d FAILED: %s\n", r.id, r.err)
		}
	}
	if okCount != *n {
		os.Exit(1)
	}
}

func summarize(rs []result, name string, get func(result) float64) {
	var vals []float64
	for _, r := range rs {
		if r.ok && get(r) > 0 {
			vals = append(vals, get(r))
		}
	}
	if len(vals) == 0 {
		fmt.Printf("  %-12s n/a\n", name)
		return
	}
	sort.Float64s(vals)
	var sum float64
	for _, v := range vals {
		sum += v
	}
	p := func(q float64) float64 {
		i := int(q * float64(len(vals)))
		if i >= len(vals) {
			i = len(vals) - 1
		}
		return vals[i]
	}
	fmt.Printf("  %-12s mean %.0f  p50 %.0f  p95 %.0f\n", name, sum/float64(len(vals)), p(0.5), p(0.95))
}

func fail(r result, err string) result {
	r.err = err
	return r
}

func runSession(id int, host string, pcm24 []int16) result {
	r := result{id: id}

	ws, _, err := websocket.DefaultDialer.Dial("ws://"+host+"/ws", nil)
	if err != nil {
		return fail(r, "engine ws: "+err.Error())
	}
	defer ws.Close()

	events := make(chan map[string]any, 64)
	go func() {
		defer close(events)
		for {
			_, data, err := ws.ReadMessage()
			if err != nil {
				return
			}
			var msg map[string]any
			if json.Unmarshal(data, &msg) == nil {
				events <- msg
			}
		}
	}()

	sid := ""
	for msg := range events {
		if msg["type"] == "session" {
			sid = msg["sid"].(string)
			break
		}
	}
	if sid == "" {
		return fail(r, "no session id")
	}

	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return fail(r, err.Error())
	}
	defer pc.Close()
	micTrack, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		"audio", "load-mic",
	)
	if err != nil {
		return fail(r, err.Error())
	}
	if _, err := pc.AddTrack(micTrack); err != nil {
		return fail(r, err.Error())
	}

	connected := make(chan struct{})
	var connectedOnce sync.Once
	pc.OnConnectionStateChange(func(st webrtc.PeerConnectionState) {
		if st == webrtc.PeerConnectionStateConnected {
			connectedOnce.Do(func() { close(connected) })
		}
	})

	firstReply := make(chan time.Time, 1)
	pc.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		sent := false
		for {
			if _, _, err := remote.ReadRTP(); err != nil {
				return
			}
			if !sent {
				sent = true
				firstReply <- time.Now()
			}
		}
	})

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return fail(r, err.Error())
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		return fail(r, err.Error())
	}
	<-gathered

	body, _ := json.Marshal(map[string]string{"sid": sid, "sdp": pc.LocalDescription().SDP})
	res, err := http.Post("http://"+host+"/rtc/offer", "application/json", bytes.NewReader(body))
	if err != nil {
		return fail(r, err.Error())
	}
	ansBody, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 {
		return fail(r, fmt.Sprintf("offer relay %d", res.StatusCode))
	}
	var ans struct {
		SDP string `json:"sdp"`
	}
	json.Unmarshal(ansBody, &ans)
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: ans.SDP}); err != nil {
		return fail(r, err.Error())
	}

	// Don't start "speaking" until DTLS/SRTP is actually up — samples written
	// before then are silently dropped and the whole question can be lost.
	select {
	case <-connected:
	case <-time.After(15 * time.Second):
		return fail(r, "webrtc never connected")
	}

	// Mic: pace speech then silence; note when the last speech frame goes out.
	speechEnd := make(chan time.Time, 1)
	stopMic := make(chan struct{})
	defer close(stopMic)
	go func() {
		enc, err := opus.NewEncoder(48000, 1, opus.AppVoIP)
		if err != nil {
			return
		}
		pcm48 := dsp.NewInterpolator24to48().Process(pcm24)
		out := make([]byte, 1500)
		silence := make([]int16, 960)
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stopMic:
				return
			case <-tick.C:
			}
			frame := silence
			if len(pcm48) >= 960 {
				frame, pcm48 = pcm48[:960], pcm48[960:]
				if len(pcm48) < 960 {
					select {
					case speechEnd <- time.Now():
					default:
					}
				}
			}
			n, err := enc.Encode(frame, out)
			if err != nil {
				continue
			}
			if micTrack.WriteSample(media.Sample{Data: append([]byte(nil), out[:n]...), Duration: 20 * time.Millisecond}) != nil {
				return
			}
		}
	}()

	num := func(v any) float64 {
		f, _ := v.(float64)
		return f
	}
	var tSpeechEnd, tFirstReply time.Time
	timeout := time.After(60 * time.Second)
	for {
		select {
		case t := <-speechEnd:
			tSpeechEnd = t
		case t := <-firstReply:
			if tFirstReply.IsZero() {
				tFirstReply = t
			}
		case msg, ok := <-events:
			if !ok {
				return fail(r, "engine ws closed")
			}
			switch msg["type"] {
			case "metrics":
				r.sttMs, r.llmMs, r.ttsMs, r.totalMs = num(msg["stt"]), num(msg["llm"]), num(msg["tts"]), num(msg["total"])
			case "agent_done":
				if r.totalMs == 0 || tFirstReply.IsZero() || tSpeechEnd.IsZero() {
					return fail(r, "missing metrics or reply audio")
				}
				r.clientGapMs = float64(tFirstReply.Sub(tSpeechEnd).Milliseconds())
				r.ok = true
				return r
			case "error":
				return fail(r, fmt.Sprint(msg["message"]))
			}
		case <-timeout:
			return fail(r, "timeout")
		}
	}
}
