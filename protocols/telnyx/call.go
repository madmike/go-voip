package telnyx

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/madmike/go-voip/core"
)

// telnyxCall implements core.Call on top of the Telnyx Call Control API.
//
// Call control (accept/hangup/dtmf/transfer) goes over REST; media flows
// through the bidirectional Media Streaming WebSocket, which is attached
// by the webhook handler once Telnyx connects inbound.
type telnyxCall struct {
	protocol *Protocol

	callControlID string
	callLegID     string
	callSessionID string

	direction core.Direction
	from      string
	to        string
	startedAt time.Time

	mu    sync.RWMutex
	state core.CallState

	audio    *mediaStream
	audioSet chan struct{} // closed once audio stream is attached

	events chan core.Event
	once   sync.Once
	closed bool
}

func newCall(p *Protocol, direction core.Direction, from, to string) *telnyxCall {
	return &telnyxCall{
		protocol:  p,
		direction: direction,
		from:      from,
		to:        to,
		startedAt: time.Now(),
		state:     initialState(direction),
		audioSet:  make(chan struct{}),
		events:    make(chan core.Event, 32),
	}
}

func initialState(d core.Direction) core.CallState {
	if d == core.DirectionInbound {
		return core.CallStateRinging
	}
	return core.CallStateDialing
}

// --- core.Call ---

func (c *telnyxCall) ID() string                { return c.callControlID }
func (c *telnyxCall) Direction() core.Direction { return c.direction }
func (c *telnyxCall) From() string              { return c.from }
func (c *telnyxCall) To() string                { return c.to }
func (c *telnyxCall) StartedAt() time.Time      { return c.startedAt }

func (c *telnyxCall) State() core.CallState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

func (c *telnyxCall) Accept(ctx context.Context, opts core.AcceptOptions) error {
	if c.direction == core.DirectionOutbound {
		return nil // already answered by remote
	}

	streamURL := opts.StreamURL
	if streamURL == "" {
		streamURL = c.protocol.mediaWSURL
	}
	if opts.EnableMediaStreaming && streamURL == "" {
		return fmt.Errorf("telnyx: media streaming requested but no stream URL configured")
	}

	req := telnyxAnswerRequest{
		ClientState: encodeClientState(c.callControlID),
	}
	if opts.EnableMediaStreaming {
		req.StreamURL = streamURL
		req.StreamTrack = "both_tracks"
		req.StreamBidirectional = "rtp"
		req.StreamCodec = codecToTelnyx(opts.Codec)
	}
	for k, v := range opts.CustomHeaders {
		req.CustomHeaders = append(req.CustomHeaders, telnyxSIPHeader{Name: k, Value: v})
	}

	path := fmt.Sprintf("/calls/%s/actions/answer", c.callControlID)
	if err := c.protocol.apiPOST(ctx, path, req, nil); err != nil {
		c.setState(core.CallStateFailed)
		return err
	}
	return nil
}

func (c *telnyxCall) Reject(ctx context.Context, reason string) error {
	req := telnyxRejectRequest{Cause: firstNonEmpty(reason, "CALL_REJECTED")}
	path := fmt.Sprintf("/calls/%s/actions/reject", c.callControlID)
	if err := c.protocol.apiPOST(ctx, path, req, nil); err != nil {
		return err
	}
	c.setState(core.CallStateHangup)
	c.finish()
	return nil
}

func (c *telnyxCall) Audio() core.AudioStream {
	// Block until the media WebSocket has attached. Callers typically invoke
	// Audio() only after receiving EventMediaStarted, but a short wait is
	// safer than returning a nil stream.
	select {
	case <-c.audioSet:
	case <-time.After(5 * time.Second):
		// Return a stream that errors on use, so callers get a clear signal
		// instead of a panic.
		return &mediaStream{detached: true}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.audio
}

func (c *telnyxCall) Events() <-chan core.Event { return c.events }

func (c *telnyxCall) SendDTMF(ctx context.Context, digits string) error {
	if digits == "" {
		return nil
	}
	path := fmt.Sprintf("/calls/%s/actions/send_dtmf", c.callControlID)
	return c.protocol.apiPOST(ctx, path, telnyxDTMFRequest{Digits: digits}, nil)
}

func (c *telnyxCall) Transfer(ctx context.Context, destination string) error {
	path := fmt.Sprintf("/calls/%s/actions/transfer", c.callControlID)
	if err := c.protocol.apiPOST(ctx, path, telnyxTransferRequest{To: destination, From: c.from}, nil); err != nil {
		return err
	}
	c.setState(core.CallStateBridged)
	return nil
}

func (c *telnyxCall) Hangup(ctx context.Context) error {
	c.mu.RLock()
	state := c.state
	c.mu.RUnlock()
	if state == core.CallStateHangup || state == core.CallStateFailed {
		return nil
	}

	path := fmt.Sprintf("/calls/%s/actions/hangup", c.callControlID)
	err := c.protocol.apiPOST(ctx, path, telnyxHangupRequest{}, nil)

	c.setState(core.CallStateHangup)
	c.finish()
	return err
}

// --- internals ---

func (c *telnyxCall) attachAudio(stream *mediaStream) {
	c.mu.Lock()
	c.audio = stream
	c.mu.Unlock()
	// Signal Audio() waiters exactly once.
	select {
	case <-c.audioSet:
	default:
		close(c.audioSet)
	}
}

func (c *telnyxCall) setState(s core.CallState) {
	c.mu.Lock()
	c.state = s
	c.mu.Unlock()
}

func (c *telnyxCall) emit(evt core.Event) {
	if evt.Timestamp.IsZero() {
		evt.Timestamp = time.Now()
	}
	select {
	case c.events <- evt:
	default:
		// drop on overflow — consumers that care should drain
	}
}

// finish closes the events channel and tears down audio. Idempotent.
func (c *telnyxCall) finish() {
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		audio := c.audio
		c.mu.Unlock()
		if audio != nil {
			_ = audio.Close()
		}
		close(c.events)
		// remove from provider registry
		c.protocol.mu.Lock()
		delete(c.protocol.calls, c.callControlID)
		c.protocol.mu.Unlock()
	})
}

func (c *telnyxCall) closeLocked() error {
	// Called while protocol.mu is held (see Protocol.Close).
	c.once.Do(func() {
		c.closed = true
		if c.audio != nil {
			_ = c.audio.Close()
		}
		close(c.events)
	})
	return nil
}

// --- Dial entrypoint ---

// Dial places an outbound call.
func (p *Protocol) Dial(ctx context.Context, req core.DialRequest) (core.Call, error) {
	p.mu.RLock()
	initialized := p.initialized
	p.mu.RUnlock()
	if !initialized {
		return nil, fmt.Errorf("telnyx: not initialized")
	}

	connID := req.ConnectionID
	if connID == "" {
		connID = p.connectionID
	}
	if connID == "" {
		return nil, fmt.Errorf("telnyx: connection_id required (pass in DialRequest or config)")
	}

	dialReq := telnyxDialRequest{
		ConnectionID:    connID,
		To:              req.To,
		From:            req.From,
		FromDisplayName: req.CallerName,
		WebhookURL:      p.publicWebhookURL,
	}
	if req.Timeout > 0 {
		dialReq.TimeoutSecs = int(req.Timeout / time.Second)
	}
	for k, v := range req.Headers {
		dialReq.CustomHeaders = append(dialReq.CustomHeaders, telnyxSIPHeader{Name: k, Value: v})
	}

	var resp telnyxDialResponse
	if err := p.apiPOST(ctx, "/calls", dialReq, &resp); err != nil {
		return nil, err
	}

	call := newCall(p, core.DirectionOutbound, req.From, req.To)
	call.callControlID = resp.Data.CallControlID
	call.callLegID = resp.Data.CallLegID
	call.callSessionID = resp.Data.CallSessionID

	p.mu.Lock()
	p.calls[call.callControlID] = call
	p.mu.Unlock()

	return call, nil
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func codecToTelnyx(c core.AudioCodec) string {
	switch c {
	case core.CodecPCMU, "":
		return "PCMU"
	case core.CodecPCMA:
		return "PCMA"
	case core.CodecOpus:
		return "OPUS"
	case core.CodecL16, core.CodecPCM16:
		return "L16"
	default:
		return "PCMU"
	}
}

// encodeClientState is a small wrapper: Telnyx requires client_state to be
// base64. We simply stash the call_control_id so the media WebSocket handler
// can correlate without parsing anything fancy. Exposed as a plain function
// so tests can override.
var encodeClientState = func(callID string) string {
	// Use raw bytes wrapped in base64 via stdlib; implemented in webhook.go to
	// keep this file focused on call lifecycle.
	return base64Encode([]byte(callID))
}

// io.EOF is re-exported for callers matching stream termination.
var _ = io.EOF
