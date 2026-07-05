// fish-bare-agent WebRTC media gateway.
//
// A thin Pion-based server-side peer: terminates the browser's WebRTC
// connection (ICE/DTLS/SRTP/Opus) and bridges plain PCM to the Node engine
// over a local websocket, speaking the engine's existing audio protocol.
// The engine relays SDP offers here via POST /offer; all calls share one
// UDP port (ICE-Lite + UDP mux), so deployment is one TCP port for
// signaling + one UDP port for media.
//
// Config (env):
//   PORT        HTTP signaling port                      (default 8788)
//   UDP_PORT    shared media port                        (default 7881)
//   ENGINE_URL  engine websocket base                    (default ws://127.0.0.1:8787)
//   PUBLIC_IP   advertise this IP in candidates (deploys behind NAT / k8s)
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	port := env("PORT", "8788")
	udpPort := env("UDP_PORT", "7881")
	engineURL := env("ENGINE_URL", "ws://127.0.0.1:8787")
	publicIP := os.Getenv("PUBLIC_IP")

	// One UDP socket for every call.
	addr, err := net.ResolveUDPAddr("udp", ":"+udpPort)
	if err != nil {
		log.Fatal(err)
	}
	udpConn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatal(err)
	}

	se := webrtc.SettingEngine{}
	se.SetICEUDPMux(webrtc.NewICEUDPMux(nil, udpConn))
	se.SetLite(true) // we have a reachable address; the browser does the ICE work
	if publicIP != "" {
		se.SetNAT1To1IPs([]string{publicIP}, webrtc.ICECandidateTypeHost)
	} else {
		// Local dev: allow 127.0.0.1 candidates so localhost works offline.
		se.SetIncludeLoopbackCandidate(true)
		se.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	}

	api := webrtc.NewAPI(webrtc.WithSettingEngine(se))

	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	http.HandleFunc("/offer", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			SID string `json:"sid"`
			SDP string `json:"sdp"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.SID == "" {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		answer, err := startCall(api, engineURL, req.SID, req.SDP)
		if err != nil {
			log.Printf("[%s] call setup failed: %v", req.SID, err)
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"sdp": answer})
	})

	log.Printf("gateway: signaling :%s, media :%s/udp, engine %s", port, udpPort, engineURL)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func startCall(api *webrtc.API, engineURL, sid, offerSDP string) (string, error) {
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return "", err
	}

	track, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		"audio", "fish-agent",
	)
	if err != nil {
		pc.Close()
		return "", err
	}
	if _, err := pc.AddTrack(track); err != nil {
		pc.Close()
		return "", err
	}

	// Attach to the engine as this session's audio transport.
	u, err := url.Parse(engineURL)
	if err != nil {
		pc.Close()
		return "", err
	}
	u.Path = "/gateway"
	u.RawQuery = "sid=" + url.QueryEscape(sid)
	engine, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		pc.Close()
		return "", fmt.Errorf("engine dial: %w", err)
	}

	sess := newSession(sid, pc, engine, track)

	pc.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		if remote.Kind() == webrtc.RTPCodecTypeAudio {
			log.Printf("[%s] mic track up (%s)", sid, remote.Codec().MimeType)
			sess.handleInboundTrack(remote)
		}
	})
	pc.OnConnectionStateChange(func(st webrtc.PeerConnectionState) {
		log.Printf("[%s] pc state: %s", sid, st)
		switch st {
		case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed,
			webrtc.PeerConnectionStateDisconnected:
			sess.close()
		}
	})

	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer, SDP: offerSDP,
	}); err != nil {
		sess.close()
		return "", err
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		sess.close()
		return "", err
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		sess.close()
		return "", err
	}
	select {
	case <-gathered: // instant with a UDP mux
	case <-time.After(3 * time.Second):
	}

	go sess.readEngine()
	go sess.pace()

	return pc.LocalDescription().SDP, nil
}
