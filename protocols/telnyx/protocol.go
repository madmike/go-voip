// Package telnyx implements the core.Provider interface on top of the
// Telnyx Call Control v2 REST API and Telnyx Media Streaming WebSocket.
//
// Reference:
//   - https://developers.telnyx.com/api/call-control
//   - https://developers.telnyx.com/docs/voice/programmable-voice/media-streaming
package telnyx

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/madmike/go-infra/telemetry"
	"github.com/madmike/go-voip/core"
)

// Protocol is the Telnyx implementation of core.Provider.
type Protocol struct {
	name             string
	apiKey           string
	baseURL          string
	mediaWSURL       string
	publicWebhookURL string
	connectionID     string
	options          map[string]any
	logger           telemetry.Logger

	httpClient *http.Client

	mu          sync.RWMutex
	initialized bool
	handler     core.CallHandler
	// calls indexed by Telnyx call_control_id, used to route webhook events
	// and incoming media WebSocket frames to the right session.
	calls map[string]*telnyxCall
}

// NewProtocol is the factory entrypoint registered with voip/factory.
func NewProtocol(config core.ProviderConfig) (core.Provider, error) {
	logger := resolveLogger(config.Logger)

	timeout := config.Timeout
	if timeout == 0 {
		timeout = 15 * time.Second
	}

	p := &Protocol{
		name:             config.Name,
		apiKey:           config.APIKey,
		baseURL:          config.BaseURL,
		mediaWSURL:       config.MediaWSURL,
		publicWebhookURL: config.PublicWebhookURL,
		connectionID:     config.DefaultConnectionID,
		options:          config.Options,
		logger:           logger,
		httpClient:       &http.Client{Timeout: timeout},
		calls:            make(map[string]*telnyxCall),
	}
	return p, nil
}

func (p *Protocol) Name() string            { return p.name }
func (p *Protocol) Type() core.ProviderType { return core.ProviderTypeTelnyx }

func (p *Protocol) Initialize(ctx context.Context, config core.ProviderConfig) error {
	if config.APIKey == "" {
		return fmt.Errorf("telnyx: API key is required")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.apiKey = config.APIKey
	if config.BaseURL != "" {
		p.baseURL = config.BaseURL
	}
	if p.baseURL == "" {
		p.baseURL = "https://api.telnyx.com/v2"
	}
	if config.MediaWSURL != "" {
		p.mediaWSURL = config.MediaWSURL
	}
	if config.PublicWebhookURL != "" {
		p.publicWebhookURL = config.PublicWebhookURL
	}
	if config.DefaultConnectionID != "" {
		p.connectionID = config.DefaultConnectionID
	}
	if cid, ok := config.Options["connection_id"].(string); ok && cid != "" {
		p.connectionID = cid
	}
	p.initialized = true
	return nil
}

func (p *Protocol) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.calls {
		_ = c.closeLocked()
	}
	p.calls = make(map[string]*telnyxCall)
	p.initialized = false
	return nil
}

func (p *Protocol) HealthCheck(ctx context.Context) error {
	p.mu.RLock()
	initialized := p.initialized
	p.mu.RUnlock()
	if !initialized {
		return fmt.Errorf("telnyx: not initialized")
	}
	// Telnyx has no dedicated health endpoint; a GET on /balance is the
	// canonical "is my key alive" probe.
	_, err := p.apiGET(ctx, "/balance")
	return err
}

// Listen registers a CallHandler for inbound calls routed through the
// Telnyx webhook. The provider itself does not bind a socket: services mount
// HTTPHandler() on their own router and Telnyx POSTs here for every call
// event.
func (p *Protocol) Listen(ctx context.Context, handler core.CallHandler) error {
	if handler == nil {
		return fmt.Errorf("telnyx: nil call handler")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.initialized {
		return fmt.Errorf("telnyx: not initialized")
	}
	if p.publicWebhookURL == "" {
		return fmt.Errorf("telnyx: PublicWebhookURL required for Listen()")
	}
	p.handler = handler
	return nil
}

// HTTPHandler exposes both the REST webhook (for call events) and the
// WebSocket upgrade for media streaming. Services mount this on their own
// router.
func (p *Protocol) HTTPHandler() core.HTTPHandler {
	return &webhookHandler{protocol: p}
}

// Capabilities-style helpers so callers can introspect without a type switch.
func (p *Protocol) MediaWSURL() string       { return p.mediaWSURL }
func (p *Protocol) PublicWebhookURL() string { return p.publicWebhookURL }

func resolveLogger(l any) telemetry.Logger {
	if l == nil {
		return &telemetry.NoOpLogger{}
	}
	if logger, ok := l.(telemetry.Logger); ok {
		return logger
	}
	return &telemetry.NoOpLogger{}
}
