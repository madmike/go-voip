package webwidget

import (
	"context"
	"fmt"
	"sync"

	"github.com/madmike/go-infra/telemetry"
	"github.com/madmike/go-voip/core"
)

// Protocol implements core.Provider as a WebSocket-based bridge for web widgets.
type Protocol struct {
	name        string
	logger      telemetry.Logger
	initialized bool
	handler     core.CallHandler

	mu    sync.RWMutex
	calls map[string]*webwidgetCall
}

func NewProtocol(config core.ProviderConfig) (core.Provider, error) {
	logger := resolveLogger(config.Logger)
	return &Protocol{
		name:   config.Name,
		logger: logger,
		calls:  make(map[string]*webwidgetCall),
	}, nil
}

func (p *Protocol) Name() string            { return p.name }
func (p *Protocol) Type() core.ProviderType { return "webwidget" }

func (p *Protocol) Initialize(ctx context.Context, config core.ProviderConfig) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.initialized = true
	return nil
}

func (p *Protocol) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.calls {
		_ = c.Hangup(context.Background())
	}
	p.calls = make(map[string]*webwidgetCall)
	return nil
}

func (p *Protocol) HealthCheck(ctx context.Context) error {
	return nil
}

func (p *Protocol) Dial(ctx context.Context, req core.DialRequest) (core.Call, error) {
	return nil, fmt.Errorf("webwidget: Dial not supported (inbound only)")
}

func (p *Protocol) Listen(ctx context.Context, handler core.CallHandler) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.handler = handler
	return nil
}

func (p *Protocol) HTTPHandler() core.HTTPHandler {
	return &webhookHandler{protocol: p}
}

func (p *Protocol) findCall(id string) *webwidgetCall {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.calls[id]
}

func resolveLogger(l any) telemetry.Logger {
	if logger, ok := l.(telemetry.Logger); ok {
		return logger
	}
	return &telemetry.NoOpLogger{}
}
