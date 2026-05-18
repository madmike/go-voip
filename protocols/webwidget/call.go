package webwidget

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/madmike/go-voip/core"
)

type webwidgetCall struct {
	protocol  *Protocol
	id        string
	direction core.Direction
	from      string
	to        string
	state     core.CallState
	startedAt time.Time

	audio   *audioStream
	eventCh chan core.Event

	mu sync.RWMutex
}

func newCall(p *Protocol, from, to string) *webwidgetCall {
	return &webwidgetCall{
		protocol:  p,
		id:        fmt.Sprintf("ww-%d", time.Now().UnixNano()),
		direction: core.DirectionInbound,
		from:      from,
		to:        to,
		state:     core.CallStateRinging,
		startedAt: time.Now(),
		eventCh:   make(chan core.Event, 32),
	}
}

func (c *webwidgetCall) ID() string                { return c.id }
func (c *webwidgetCall) Direction() core.Direction { return c.direction }
func (c *webwidgetCall) From() string              { return c.from }
func (c *webwidgetCall) To() string                { return c.to }
func (c *webwidgetCall) State() core.CallState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}
func (c *webwidgetCall) StartedAt() time.Time { return c.startedAt }

func (c *webwidgetCall) Accept(ctx context.Context, opts core.AcceptOptions) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = core.CallStateAnswered
	c.emit(core.Event{Type: core.EventAnswered, Timestamp: time.Now()})
	return nil
}

func (c *webwidgetCall) Reject(ctx context.Context, reason string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = core.CallStateFailed
	c.finish()
	return nil
}

func (c *webwidgetCall) Audio() core.AudioStream { return c.audio }

func (c *webwidgetCall) Events() <-chan core.Event { return c.eventCh }

func (c *webwidgetCall) SendDTMF(ctx context.Context, digits string) error      { return nil }
func (c *webwidgetCall) Transfer(ctx context.Context, destination string) error { return nil }

func (c *webwidgetCall) Hangup(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == core.CallStateHangup {
		return nil
	}
	c.state = core.CallStateHangup
	c.emit(core.Event{Type: core.EventHangup, Timestamp: time.Now()})
	c.finish()
	return nil
}

func (c *webwidgetCall) emit(evt core.Event) {
	select {
	case c.eventCh <- evt:
	default:
	}
}

func (c *webwidgetCall) finish() {
	if c.audio != nil {
		_ = c.audio.Close()
	}
	close(c.eventCh)
}

func (c *webwidgetCall) attachAudio(s *audioStream) {
	c.mu.Lock()
	c.audio = s
	c.mu.Unlock()
}
