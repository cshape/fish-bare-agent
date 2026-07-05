package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/pion/webrtc/v4/pkg/media/samplebuilder"
	opus "gopkg.in/hraban/opus.v2"

	"fish-bare-agent/gateway/internal/dsp"
)

const (
	frame48    = 960 // 20 ms @ 48 kHz
	frame16    = 320 // 20 ms @ 16 kHz
)

// session bridges one PeerConnection to one engine websocket:
//
//	browser --Opus 48k--> [samplebuilder -> decode -> 48->16] --PCM16--> engine
//	browser <--Opus 48k-- [24->48 -> encode -> 20ms pacing]  <--PCM16-- engine
type session struct {
	sid    string
	pc     *webrtc.PeerConnection
	engine *websocket.Conn
	track  *webrtc.TrackLocalStaticSample

	mu        sync.Mutex // guards engine writes + outbound buffer
	outPCM    []int16    // 48 kHz PCM awaiting encode/pacing
	closed    bool
	closeOnce sync.Once
	gotAudio  bool // first engine audio seen (timing log)
	sentRTP   bool // first RTP frame sent (timing log)
}

func newSession(sid string, pc *webrtc.PeerConnection, engine *websocket.Conn, track *webrtc.TrackLocalStaticSample) *session {
	return &session{sid: sid, pc: pc, engine: engine, track: track}
}

func (s *session) close() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		s.engine.Close()
		s.pc.Close()
		log.Printf("[%s] session closed", s.sid)
	})
}

// --- inbound: browser mic -> engine ---------------------------------------

func (s *session) handleInboundTrack(remote *webrtc.TrackRemote) {
	dec, err := opus.NewDecoder(48000, 1)
	if err != nil {
		log.Printf("[%s] opus decoder: %v", s.sid, err)
		return
	}
	sb := samplebuilder.New(10, &codecs.OpusPacket{}, 48000)
	down := dsp.NewDecimator48to16()
	pcm48 := make([]int16, 5760) // max opus frame
	var toEngine []int16

	for {
		pkt, _, err := remote.ReadRTP()
		if err != nil {
			return // track closed
		}
		sb.Push(pkt)
		for sample := sb.Pop(); sample != nil; sample = sb.Pop() {
			n, err := dec.Decode(sample.Data, pcm48)
			if err != nil {
				continue
			}
			toEngine = append(toEngine, down.Process(pcm48[:n])...)
			// Ship in 20 ms chunks — same cadence as the browser worklet path.
			for len(toEngine) >= frame16 {
				if !s.writeEngine(dsp.PCMToBytes(toEngine[:frame16])) {
					return
				}
				toEngine = toEngine[frame16:]
			}
		}
	}
}

func (s *session) writeEngine(b []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	return s.engine.WriteMessage(websocket.BinaryMessage, b) == nil
}

// --- outbound: engine TTS -> browser ---------------------------------------

// readEngine consumes the engine socket: binary frames are 24 kHz reply PCM,
// text frames are control JSON (only "clear" matters to us).
func (s *session) readEngine() {
	up := dsp.NewInterpolator24to48()
	for {
		mt, data, err := s.engine.ReadMessage()
		if err != nil {
			s.close()
			return
		}
		switch mt {
		case websocket.BinaryMessage:
			pcm48 := up.Process(dsp.BytesToPCM(data))
			s.mu.Lock()
			if !s.gotAudio {
				s.gotAudio = true
				log.Printf("[%s] first engine audio t=%d", s.sid, time.Now().UnixMilli())
			}
			s.outPCM = append(s.outPCM, pcm48...)
			s.mu.Unlock()
		case websocket.TextMessage:
			var msg struct {
				Type string `json:"type"`
			}
			if json.Unmarshal(data, &msg) == nil && msg.Type == "clear" {
				// Barge-in: drop everything not yet on the wire. The browser
				// holds only its jitter buffer (~tens of ms), so the cut is
				// near-instant without any client-side flush protocol.
				s.mu.Lock()
				s.outPCM = s.outPCM[:0]
				s.mu.Unlock()
			}
		}
	}
}

// pace encodes and sends one 20 ms Opus frame per tick while audio is queued.
// Real-time pacing (vs dumping the whole reply) is what makes barge-in cheap.
func (s *session) pace() {
	enc, err := opus.NewEncoder(48000, 1, opus.AppVoIP)
	if err != nil {
		log.Printf("[%s] opus encoder: %v", s.sid, err)
		return
	}
	enc.SetBitrate(32000)
	enc.SetInBandFEC(true)
	buf := make([]byte, 1500)

	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for range tick.C {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		var frame []int16
		if len(s.outPCM) >= frame48 {
			frame = append([]int16(nil), s.outPCM[:frame48]...)
			s.outPCM = s.outPCM[frame48:]
		}
		s.mu.Unlock()
		if frame == nil {
			continue // gap in the stream — RTP tolerates silence gaps fine
		}
		n, err := enc.Encode(frame, buf)
		if err != nil {
			continue
		}
		if err := s.track.WriteSample(media.Sample{
			Data:     append([]byte(nil), buf[:n]...),
			Duration: 20 * time.Millisecond,
		}); err != nil {
			return
		}
		s.mu.Lock()
		if !s.sentRTP {
			s.sentRTP = true
			log.Printf("[%s] first rtp out t=%d", s.sid, time.Now().UnixMilli())
		}
		s.mu.Unlock()
	}
}
