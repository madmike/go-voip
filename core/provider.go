// Package core defines protocol-agnostic interfaces for VOIP providers
// (Telnyx, Twilio, Plivo, Vonage, …). A VOIP provider is responsible for:
//
//   - Accepting inbound SIP/WebRTC calls and dispatching them to a CallHandler.
//   - Placing outbound calls.
//   - Exposing a bidirectional Call session with audio frames and control events
//     (DTMF, hangup, answered, …).
//
// The library is intentionally transport-agnostic: Telnyx/Twilio use
// REST-for-control + WebSocket-for-media, but a pure-SIP or LiveKit-style
// provider could implement the same interfaces.
package core

import (
	"context"
	"time"
)

// ProviderType identifies the underlying VOIP backend.
type ProviderType string

const (
	ProviderTypeTelnyx ProviderType = "telnyx"
	ProviderTypeTwilio ProviderType = "twilio"
)

// Provider is the base interface every VOIP backend implements.
//
// Lifecycle mirrors the go-ai-providers Provider interface so services can
// register VOIP providers next to LLM/STT/TTS ones without special-casing.
type Provider interface {
	Name() string
	Type() ProviderType

	Initialize(ctx context.Context, config ProviderConfig) error
	Close() error
	HealthCheck(ctx context.Context) error

	// Dial places an outbound call. Returns a Call that is in state
	// CallStateDialing until the remote party answers.
	Dial(ctx context.Context, req DialRequest) (Call, error)

	// Listen starts accepting inbound calls. The handler is invoked for each
	// accepted call; it must return quickly (spawn a goroutine for long-lived
	// agents).
	//
	// How inbound calls are surfaced is provider-specific: Telnyx/Twilio
	// require a publicly reachable webhook that the provider protocol
	// translates into Accept() calls. For that reason some providers expose
	// an auxiliary http.Handler via HTTPHandler().
	Listen(ctx context.Context, handler CallHandler) error

	// HTTPHandler returns an optional HTTP handler that services can mount on
	// their own webhook endpoint (e.g. "/voip/telnyx/webhook"). Providers
	// that don't need webhooks return nil.
	HTTPHandler() HTTPHandler
}

// HTTPHandler is a minimal abstraction over net/http without forcing the core
// package to import it. Implementations adapt their native http.Handler.
type HTTPHandler interface {
	// Path is the relative URL path the handler expects to be mounted at.
	Path() string
	// ServeHTTPRaw is invoked with the raw request body and headers.
	// Returns response body + status. Providers translate this to the
	// provider's REST/webhook protocol.
	ServeHTTPRaw(ctx context.Context, req HTTPRequest) (HTTPResponse, error)
}

// HTTPRequest is a provider-neutral webhook request.
type HTTPRequest struct {
	Method  string
	Path    string
	Headers map[string][]string
	Body    []byte
}

// HTTPResponse is a provider-neutral webhook response.
type HTTPResponse struct {
	Status  int
	Headers map[string]string
	Body    []byte
}

// ProviderConfig configures a VOIP provider instance.
type ProviderConfig struct {
	Name       string
	Type       ProviderType
	APIKey     string // Telnyx v2 API key / Twilio auth token
	APISecret  string // Twilio account SID (ignored by Telnyx)
	BaseURL    string // Override REST endpoint (testing)
	MediaWSURL string // Override media streaming URL (testing / region routing)

	// PublicWebhookURL is the externally reachable URL the provider will POST
	// call events to. Required for Listen() to work with webhook-based
	// providers.
	PublicWebhookURL string

	// DefaultConnectionID / DefaultAppID scoping for outbound dials, if the
	// provider requires it (Telnyx: `connection_id`).
	DefaultConnectionID string

	Options map[string]any
	Timeout time.Duration
	Logger  any // telemetry.Logger
}

// DialRequest describes an outbound call.
type DialRequest struct {
	From         string            // E.164 "+12025550100"
	To           string            // E.164 or SIP URI
	ConnectionID string            // Telnyx connection (overrides config default)
	CallerName   string            // CNAM display name
	Timeout      time.Duration     // ring timeout
	Headers      map[string]string // Custom SIP headers (X-*)
	Metadata     map[string]any    // Opaque data echoed back in call events
}

// CallHandler handles an inbound (or freshly dialed) call session.
//
// The handler owns the Call: it MUST call Accept() or Reject() for inbound
// calls, drive audio via the Call.Audio() stream, and eventually call
// Hangup() or rely on remote hangup detection.
type CallHandler func(ctx context.Context, call Call)
