// Headless end-to-end test of the WebRTC path — a Pion client standing in
// for the browser. Speaks a WAV (24 kHz mono PCM16) into the gateway as an
// Opus track, listens to the reply track, and watches the engine's JSON
// events on the websocket. Passes when the turn is transcribed, answered,
// and >1s of reply audio arrives over WebRTC.
//
// Usage: go run -tags nolibopusfile ./cmd/smoke -wav /path/to/question.wav
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
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	opus "gopkg.in/hraban/opus.v2"

	"fish-bare-agent/gateway/internal/dsp"
)

func main() {
	host := flag.String("host", "localhost:8787", "engine host:port")
	wavPath := flag.String("wav", "", "24 kHz mono PCM16 WAV to speak")
	flag.Parse()
	if *wavPath == "" {
		log.Fatal("-wav is required")
	}

	wav, err := os.ReadFile(*wavPath)
	if err != nil {
		log.Fatal(err)
	}
	pcm24 := dsp.BytesToPCM(wav[44:]) // strip canonical WAV header
	// 500 ms lead-in silence so Flux has context, like a live mic.
	pcm24 = append(make([]int16, 12000), pcm24...)
	log.Printf("input: %.1fs of audio", float64(len(pcm24))/24000)

	// --- engine websocket: session + JSON events ---------------------------
	ws, _, err := websocket.DefaultDialer.Dial("ws://"+*host+"/ws", nil)
	if err != nil {
		log.Fatalf("engine ws: %v (is the server running?)", err)
	}

	events := make(chan map[string]any, 64)
	go func() {
		for {
			_, data, err := ws.ReadMessage()
			if err != nil {
				close(events)
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
	log.Printf("session %s", sid)

	// --- peer connection ----------------------------------------------------
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		log.Fatal(err)
	}
	micTrack, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		"audio", "smoke-mic",
	)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := pc.AddTrack(micTrack); err != nil {
		log.Fatal(err)
	}

	connected := make(chan struct{})
	var connectedOnce sync.Once
	pc.OnConnectionStateChange(func(st webrtc.PeerConnectionState) {
		if st == webrtc.PeerConnectionStateConnected {
			connectedOnce.Do(func() { close(connected) })
		}
	})

	replySamples := make(chan int, 1024) // decoded sample counts per packet
	pc.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		dec, err := opus.NewDecoder(48000, 1)
		if err != nil {
			return
		}
		buf := make([]int16, 5760)
		for {
			pkt, _, err := remote.ReadRTP()
			if err != nil {
				return
			}
			if n, err := dec.Decode(pkt.Payload, buf); err == nil {
				replySamples <- n
			}
		}
	})

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		log.Fatal(err)
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		log.Fatal(err)
	}
	<-gathered

	body, _ := json.Marshal(map[string]string{"sid": sid, "sdp": pc.LocalDescription().SDP})
	res, err := http.Post("http://"+*host+"/rtc/offer", "application/json", bytes.NewReader(body))
	if err != nil {
		log.Fatal(err)
	}
	ansBody, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 {
		log.Fatalf("offer relay: %d %s", res.StatusCode, ansBody)
	}
	var ans struct {
		SDP string `json:"sdp"`
	}
	json.Unmarshal(ansBody, &ans)
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer, SDP: ans.SDP,
	}); err != nil {
		log.Fatal(err)
	}
	// Samples written before DTLS/SRTP is up are silently dropped — wait for
	// the connection before "speaking" or the question can be lost.
	select {
	case <-connected:
	case <-time.After(15 * time.Second):
		log.Fatal("FAIL: webrtc never connected")
	}
	log.Print("webrtc connected, speaking…")

	// --- "mic": pace the WAV as 20 ms Opus frames, then silence -------------
	go func() {
		enc, err := opus.NewEncoder(48000, 1, opus.AppVoIP)
		if err != nil {
			log.Fatal(err)
		}
		up := dsp.NewInterpolator24to48()
		pcm48 := up.Process(pcm24)
		out := make([]byte, 1500)
		silence := make([]int16, 960)
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for range tick.C {
			frame := silence
			if len(pcm48) >= 960 {
				frame, pcm48 = pcm48[:960], pcm48[960:]
			}
			n, err := enc.Encode(frame, out)
			if err != nil {
				continue
			}
			if micTrack.WriteSample(media.Sample{
				Data: append([]byte(nil), out[:n]...), Duration: 20 * time.Millisecond,
			}) != nil {
				return
			}
		}
	}()

	// --- judge ---------------------------------------------------------------
	timeout := time.After(90 * time.Second)
	var transcript, agentText string
	replyTotal := 0
	done := false
	for !done {
		select {
		case n := <-replySamples:
			replyTotal += n
		case msg, ok := <-events:
			if !ok {
				log.Fatal("FAIL: engine websocket closed")
			}
			switch msg["type"] {
			case "user_final":
				transcript = msg["text"].(string)
				log.Printf("transcript: %q", transcript)
			case "agent_text":
				agentText += msg["text"].(string)
			case "metrics":
				log.Printf("latency: turn-detect %v ms | llm %v ms | tts %v ms | voice->voice %v ms (eager=%v)",
					msg["stt"], msg["llm"], msg["tts"], msg["total"], msg["eager"])
			case "agent_done":
				done = true
			}
		case <-timeout:
			log.Fatal("FAIL: timed out")
		}
	}
	// Reply audio keeps arriving in real time after agent_done; drain briefly.
	drain := time.After(3 * time.Second)
	for {
		select {
		case n := <-replySamples:
			replyTotal += n
		case <-drain:
			secs := float64(replyTotal) / 48000
			log.Printf("agent said: %q", agentText)
			log.Printf("reply audio over webrtc: %.1fs", secs)
			if transcript == "" || agentText == "" || secs < 1 {
				log.Fatal("FAIL")
			}
			fmt.Println("PASS")
			os.Exit(0)
		}
	}
}
