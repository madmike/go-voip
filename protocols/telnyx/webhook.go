package telnyx

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/madmike/go-infra/telemetry"
	"github.com/madmike/go-voip/core"
)

// webhookHandler satisfies core.HTTPHandler plus exposes a native
// http.Handler so services can mount it directly on their mux. It covers
// both:
//
//  1. Telnyx call-event webhook (POST JSON): dispatches inbound calls to
//     the registered CallHandler and drives Call.Events().
//  2. Telnyx media streaming WebSocket (GET + Upgrade): attaches a
//     mediaStream to the matching call.
type webhookHandler struct {
	protocol *Protocol
}

func (h *webhookHandler) Path() string { return "/" } // mount anywhere

// ServeHTTP implements net/http.Handler for native mounting.
func (h *webhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Distinguish WebSocket upgrade (media) from plain webhook (events).
	if isWebSocketUpgrade(r) {
		h.serveMediaWS(w, r)
		return
	}
	h.serveEventWebhook(w, r)
}

// ServeHTTPRaw is used when callers can't mount an http.Handler (rare, but
// required by core.HTTPHandler so every provider can be driven headlessly).
// It handles event webhooks only — media streaming requires a real
// net/http hijackable connection.
func (h *webhookHandler) ServeHTTPRaw(ctx context.Context, req core.HTTPRequest) (core.HTTPResponse, error) {
	if strings.EqualFold(req.Method, "GET") {
		return core.HTTPResponse{Status: http.StatusBadRequest, Body: []byte("media streaming requires native HTTP")}, nil
	}
	evt, err := parseTelnyxEvent(req.Body)
	if err != nil {
		return core.HTTPResponse{Status: http.StatusBadRequest, Body: []byte(err.Error())}, nil
	}
	h.handleEvent(ctx, evt)
	return core.HTTPResponse{Status: http.StatusOK, Body: []byte(`{"ok":true}`)}, nil
}

// --- event webhook ---

func (h *webhookHandler) serveEventWebhook(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1 MB cap
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	evt, err := parseTelnyxEvent(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	h.handleEvent(r.Context(), evt)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok":true}`))
}

// telnyxEvent covers the subset of webhook payloads we react to.
type telnyxEvent struct {
	Data struct {
		EventType  string          `json:"event_type"`
		ID         string          `json:"id"`
		OccurredAt time.Time       `json:"occurred_at"`
		Payload    json.RawMessage `json:"payload"`
	} `json:"data"`
}

type telnyxCallInitiated struct {
	CallControlID string `json:"call_control_id"`
	CallLegID     string `json:"call_leg_id"`
	CallSessionID string `json:"call_session_id"`
	From          string `json:"from"`
	To            string `json:"to"`
	Direction     string `json:"direction"`
	ConnectionID  string `json:"connection_id"`
}

type telnyxDTMFPayload struct {
	CallControlID string `json:"call_control_id"`
	Digit         string `json:"digit"`
}

type telnyxHangupPayload struct {
	CallControlID string `json:"call_control_id"`
	HangupCause   string `json:"hangup_cause"`
	HangupSource  string `json:"hangup_source"`
}

func parseTelnyxEvent(raw []byte) (telnyxEvent, error) {
	var evt telnyxEvent
	if err := json.Unmarshal(raw, &evt); err != nil {
		return evt, fmt.Errorf("telnyx: invalid webhook body: %w", err)
	}
	if evt.Data.EventType == "" {
		return evt, fmt.Errorf("telnyx: missing event_type")
	}
	return evt, nil
}

func (h *webhookHandler) handleEvent(ctx context.Context, evt telnyxEvent) {
	switch evt.Data.EventType {
	case "call.initiated":
		h.handleCallInitiated(ctx, evt.Data.Payload)

	case "call.answered":
		h.emitCallEvent(evt.Data.Payload, core.Event{Type: core.EventAnswered, Timestamp: evt.Data.OccurredAt})
		h.updateCallState(evt.Data.Payload, core.CallStateAnswered)

	case "call.hangup":
		var p telnyxHangupPayload
		_ = json.Unmarshal(evt.Data.Payload, &p)
		h.protocol.logger.Info("telnyx call hangup",
			telemetry.String("call_control_id", p.CallControlID),
			telemetry.String("cause", p.HangupCause),
		)
		if call := h.findCall(p.CallControlID); call != nil {
			call.emit(core.Event{Type: core.EventHangup, Reason: p.HangupCause, Timestamp: evt.Data.OccurredAt})
			call.setState(core.CallStateHangup)
			call.finish()
		}

	case "call.dtmf.received":
		var p telnyxDTMFPayload
		_ = json.Unmarshal(evt.Data.Payload, &p)
		if call := h.findCall(p.CallControlID); call != nil {
			call.emit(core.Event{Type: core.EventDTMF, Digits: p.Digit, Timestamp: evt.Data.OccurredAt})
		}

	case "streaming.started":
		h.emitCallEvent(evt.Data.Payload, core.Event{Type: core.EventMediaStarted, Timestamp: evt.Data.OccurredAt})
	case "streaming.stopped":
		h.emitCallEvent(evt.Data.Payload, core.Event{Type: core.EventMediaStopped, Timestamp: evt.Data.OccurredAt})
	}
}

func (h *webhookHandler) handleCallInitiated(ctx context.Context, raw json.RawMessage) {
	var p telnyxCallInitiated
	if err := json.Unmarshal(raw, &p); err != nil {
		h.protocol.logger.Error("telnyx call.initiated decode", telemetry.Err(err))
		return
	}

	direction := core.DirectionInbound
	if strings.EqualFold(p.Direction, "outgoing") || strings.EqualFold(p.Direction, "outbound") {
		// This is our own outbound call acking — it was registered at Dial time.
		if call := h.findCall(p.CallControlID); call != nil {
			call.emit(core.Event{Type: core.EventAnswered})
		}
		return
	}

	h.protocol.mu.RLock()
	handler := h.protocol.handler
	h.protocol.mu.RUnlock()
	if handler == nil {
		h.protocol.logger.Warn("telnyx: inbound call but no handler registered",
			telemetry.String("call_control_id", p.CallControlID))
		return
	}

	call := newCall(h.protocol, direction, p.From, p.To)
	call.callControlID = p.CallControlID
	call.callLegID = p.CallLegID
	call.callSessionID = p.CallSessionID

	h.protocol.mu.Lock()
	h.protocol.calls[call.callControlID] = call
	h.protocol.mu.Unlock()

	// Dispatch on a dedicated goroutine — the handler owns the call's
	// lifetime and may block for the duration of the conversation.
	go handler(ctx, call)
}

func (h *webhookHandler) emitCallEvent(raw json.RawMessage, evt core.Event) {
	var p struct {
		CallControlID string `json:"call_control_id"`
	}
	_ = json.Unmarshal(raw, &p)
	if call := h.findCall(p.CallControlID); call != nil {
		call.emit(evt)
	}
}

func (h *webhookHandler) updateCallState(raw json.RawMessage, state core.CallState) {
	var p struct {
		CallControlID string `json:"call_control_id"`
	}
	_ = json.Unmarshal(raw, &p)
	if call := h.findCall(p.CallControlID); call != nil {
		call.setState(state)
	}
}

func (h *webhookHandler) findCall(id string) *telnyxCall {
	if id == "" {
		return nil
	}
	h.protocol.mu.RLock()
	defer h.protocol.mu.RUnlock()
	return h.protocol.calls[id]
}

// --- media WebSocket upgrade ---

var mediaUpgrader = websocket.Upgrader{
	ReadBufferSize:  4 << 10,
	WriteBufferSize: 4 << 10,
	// Telnyx will connect from its own infrastructure; accept any origin.
	CheckOrigin: func(r *http.Request) bool { return true },
}

func (h *webhookHandler) serveMediaWS(w http.ResponseWriter, r *http.Request) {
	conn, err := mediaUpgrader.Upgrade(w, r, nil)
	if err != nil {
		h.protocol.logger.Error("telnyx: media ws upgrade failed", telemetry.Err(err))
		return
	}

	// Telnyx sends a "start" event as the first message; it contains the
	// call_control_id so we can associate the stream with the right call.
	// We peek once, attach, then hand the conn to mediaStream.readLoop by
	// reusing the same conn (mediaStream re-reads too, so we defer reading
	// start to its loop by pre-wiring via ClientState instead).
	//
	// Simpler: read first event ourselves.
	_, raw, err := conn.ReadMessage()
	if err != nil {
		_ = conn.Close()
		return
	}
	var env mediaEnvelope
	if err := json.Unmarshal(raw, &env); err != nil || env.Event != "start" || env.Start == nil {
		_ = conn.Close()
		return
	}

	callID := env.Start.CallControlID
	if callID == "" {
		// fall back to ClientState
		callID = decodeClientState(r.URL.Query().Get("client_state"))
	}

	call := h.findCall(callID)
	if call == nil {
		h.protocol.logger.Warn("telnyx: media ws for unknown call", telemetry.String("call_control_id", callID))
		_ = conn.Close()
		return
	}

	codec := codecFromEncoding(env.Start.MediaFormat.Encoding)
	stream := newMediaStream(conn, codec)
	stream.streamID = env.Start.StreamID
	if env.Start.MediaFormat.SampleRate > 0 {
		stream.sampleRate = env.Start.MediaFormat.SampleRate
	}
	call.attachAudio(stream)
	call.emit(core.Event{Type: core.EventMediaStarted})
}

func isWebSocketUpgrade(r *http.Request) bool {
	if !strings.EqualFold(r.Method, "GET") {
		return false
	}
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}

func decodeClientState(s string) string {
	if s == "" {
		return ""
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return s
	}
	return string(b)
}

func codecFromEncoding(encoding string) core.AudioCodec {
	switch strings.ToUpper(encoding) {
	case "PCMU":
		return core.CodecPCMU
	case "PCMA":
		return core.CodecPCMA
	case "OPUS":
		return core.CodecOpus
	case "L16":
		return core.CodecL16
	default:
		return core.CodecPCMU
	}
}
