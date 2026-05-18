package webwidget

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
	"github.com/madmike/go-infra/telemetry"
	"github.com/madmike/go-voip/core"
)

type webhookHandler struct {
	protocol *Protocol
}

func (h *webhookHandler) Path() string { return "/connect" }

func (h *webhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !isWebSocketUpgrade(r) {
		http.Error(w, "Expected WebSocket upgrade", http.StatusBadRequest)
		return
	}

	h.serveWS(w, r)
}

func (h *webhookHandler) ServeHTTPRaw(ctx context.Context, req core.HTTPRequest) (core.HTTPResponse, error) {
	return core.HTTPResponse{Status: http.StatusBadRequest, Body: []byte("WebWidget requires native HTTP for WebSocket upgrade")}, nil
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func (h *webhookHandler) serveWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.protocol.logger.Error("webwidget: ws upgrade failed", telemetry.Err(err))
		return
	}

	// 1. Handshake: Read first message for 'start' event
	_, message, err := conn.ReadMessage()
	if err != nil {
		_ = conn.Close()
		return
	}

	var startMsg struct {
		Event string `json:"event"`
		To    string `json:"to"`
		From  string `json:"from"`
		Token string `json:"site_token"`
	}
	if err := json.Unmarshal(message, &startMsg); err != nil || startMsg.Event != "start" {
		h.protocol.logger.Error("webwidget: invalid handshake", telemetry.String("raw", string(message)))
		_ = conn.Close()
		return
	}

	// 2. Create Call and AudioStream
	to := startMsg.To
	if startMsg.Token != "" && to == "" {
		to = startMsg.Token
	}
	call := newCall(h.protocol, startMsg.From, to)
	stream := newAudioStream(conn)
	call.attachAudio(stream)

	h.protocol.mu.Lock()
	h.protocol.calls[call.id] = call
	handler := h.protocol.handler
	h.protocol.mu.Unlock()

	if handler == nil {
		h.protocol.logger.Warn("webwidget: no handler registered for inbound call")
		_ = call.Hangup(context.Background())
		return
	}

	// 3. Dispatch to manager
	go handler(context.Background(), call)
}

func isWebSocketUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}
