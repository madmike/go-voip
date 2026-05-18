package webwidget

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/madmike/go-voip/core"
)

type audioStream struct {
	conn       *websocket.Conn
	codec      core.AudioCodec
	sampleRate int

	recvCh chan core.AudioFrame
	errCh  chan error
	done   chan struct{}

	mu     sync.Mutex
	closed bool
}

func newAudioStream(conn *websocket.Conn) *audioStream {
	s := &audioStream{
		conn:       conn,
		codec:      core.CodecPCM16, // High quality PCM16 for web widget
		sampleRate: 16000,
		recvCh:     make(chan core.AudioFrame, 1000),
		errCh:      make(chan error, 10),
		done:       make(chan struct{}),
	}
	go s.readLoop()
	return s
}

func (s *audioStream) Codec() core.AudioCodec { return s.codec }
func (s *audioStream) SampleRate() int        { return s.sampleRate }

func (s *audioStream) Send(ctx context.Context, frame core.AudioFrame) error {
	select {
	case <-s.done:
		return io.EOF
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Debug log for outgoing audio (only every 50 frames to avoid spam)
		// We use a counter in the caller (greeting.go) for this usually,
		// but let's add it here too if needed for pipeline tracing.
		return s.conn.WriteMessage(websocket.BinaryMessage, frame.Payload)
	}
}

func (s *audioStream) Receive(ctx context.Context) (core.AudioFrame, error) {
	select {
	case f, ok := <-s.recvCh:
		if !ok {
			return core.AudioFrame{}, io.EOF
		}
		return f, nil
	case err := <-s.errCh:
		return core.AudioFrame{}, err
	case <-s.done:
		return core.AudioFrame{}, io.EOF
	case <-ctx.Done():
		return core.AudioFrame{}, ctx.Err()
	}
}

func (s *audioStream) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	close(s.done)
	return s.conn.Close()
}

func (s *audioStream) readLoop() {
	defer close(s.recvCh)
	i := 0
	for {
		msgType, message, err := s.conn.ReadMessage()
		if err != nil {
			select {
			case s.errCh <- err:
			default:
			}
			return
		}

		if msgType == websocket.BinaryMessage {
			i++
			select {
			case s.recvCh <- core.AudioFrame{
				Payload:   message,
				Track:     "inbound",
				Timestamp: time.Now(),
			}:
			default:
				fmt.Printf("[webwidget] WARNING: Receive buffer full, dropping packet\n")
			}
		} else if msgType == websocket.TextMessage {
			// Support JSON for signaling/control messages
			var msg struct {
				Event string `json:"event"`
			}
			if err := json.Unmarshal(message, &msg); err == nil && msg.Event == "stop" {
				close(s.done)
				return
			}
		}
	}
}
