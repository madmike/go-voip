package telnyx

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/madmike/go-voip/core"
)

// mediaStream implements core.AudioStream on a Telnyx Media Streaming
// bidirectional WebSocket. Frame protocol:
//
//	{ "event": "media",
//	  "stream_id": "...",
//	  "media": { "track": "inbound|outbound", "chunk": "1",
//	             "timestamp": "...", "payload": "<base64>" } }
//
// The payload is already codec-encoded (PCMU 8kHz by default). We pass it
// straight through to consumers.
type mediaStream struct {
	conn       *websocket.Conn
	streamID   string
	codec      core.AudioCodec
	sampleRate int

	recvCh chan core.AudioFrame
	errCh  chan error
	done   chan struct{}

	mu       sync.Mutex
	closed   bool
	detached bool // true for the "no media streaming attached" placeholder
	seqOut   uint64
}

func newMediaStream(conn *websocket.Conn, codec core.AudioCodec) *mediaStream {
	s := &mediaStream{
		conn:       conn,
		codec:      codec,
		sampleRate: sampleRateForCodec(codec),
		recvCh:     make(chan core.AudioFrame, 128),
		errCh:      make(chan error, 1),
		done:       make(chan struct{}),
	}
	go s.readLoop()
	return s
}

func sampleRateForCodec(c core.AudioCodec) int {
	switch c {
	case core.CodecOpus:
		return 48000
	case core.CodecL16, core.CodecPCM16:
		return 16000
	default: // PCMU / PCMA / unknown → PSTN default
		return 8000
	}
}

// --- core.AudioStream ---

func (s *mediaStream) Codec() core.AudioCodec { return s.codec }
func (s *mediaStream) SampleRate() int        { return s.sampleRate }

func (s *mediaStream) Send(ctx context.Context, frame core.AudioFrame) error {
	if s.detached {
		return fmt.Errorf("telnyx: media stream not attached")
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return io.ErrClosedPipe
	}
	s.seqOut++
	seq := s.seqOut
	s.mu.Unlock()

	msg := map[string]any{
		"event":     "media",
		"stream_id": s.streamID,
		"media": map[string]any{
			"track":   "outbound",
			"chunk":   fmt.Sprintf("%d", seq),
			"payload": base64.StdEncoding.EncodeToString(frame.Payload),
		},
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = s.conn.SetWriteDeadline(deadline)
	} else {
		_ = s.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	}
	if err := s.conn.WriteJSON(msg); err != nil {
		return fmt.Errorf("telnyx media send: %w", err)
	}
	return nil
}

func (s *mediaStream) Receive(ctx context.Context) (core.AudioFrame, error) {
	if s.detached {
		return core.AudioFrame{}, fmt.Errorf("telnyx: media stream not attached")
	}
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

func (s *mediaStream) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	if s.conn != nil {
		_ = s.conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
			time.Now().Add(time.Second),
		)
		_ = s.conn.Close()
	}
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	return nil
}

// --- reader goroutine ---

func (s *mediaStream) readLoop() {
	defer func() {
		s.mu.Lock()
		alreadyClosed := s.closed
		s.mu.Unlock()
		if !alreadyClosed {
			close(s.recvCh)
		}
	}()

	for {
		_, raw, err := s.conn.ReadMessage()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if !closed {
				select {
				case s.errCh <- err:
				default:
				}
			}
			return
		}

		var env mediaEnvelope
		if err := json.Unmarshal(raw, &env); err != nil {
			continue
		}

		switch env.Event {
		case "start":
			if env.Start != nil {
				s.streamID = env.Start.StreamID
			}
		case "media":
			if env.Media == nil {
				continue
			}
			payload, err := base64.StdEncoding.DecodeString(env.Media.Payload)
			if err != nil || len(payload) == 0 {
				continue
			}
			seq, _ := parseUint64(env.Media.Chunk)
			ts := parseTelnyxTimestamp(env.Media.Timestamp)
			frame := core.AudioFrame{
				Payload:        payload,
				Timestamp:      ts,
				Track:          env.Media.Track,
				SequenceNumber: seq,
			}
			select {
			case s.recvCh <- frame:
			case <-s.done:
				return
			}
		case "stop":
			// Telnyx signals the end of the media stream — exit cleanly so
			// Receive() returns io.EOF on next call.
			return
		}
	}
}

// mediaEnvelope mirrors Telnyx Media Streaming WebSocket payloads. We only
// unmarshal the fields we consume.
type mediaEnvelope struct {
	Event string             `json:"event"`
	Start *mediaStartPayload `json:"start,omitempty"`
	Media *mediaMediaPayload `json:"media,omitempty"`
}

type mediaStartPayload struct {
	StreamID      string `json:"stream_id"`
	CallControlID string `json:"call_control_id"`
	MediaFormat   struct {
		Encoding   string `json:"encoding"`
		SampleRate int    `json:"sample_rate"`
		Channels   int    `json:"channels"`
	} `json:"media_format"`
}

type mediaMediaPayload struct {
	Track     string `json:"track"`
	Chunk     string `json:"chunk"`
	Timestamp string `json:"timestamp"`
	Payload   string `json:"payload"`
}

func parseUint64(s string) (uint64, error) {
	var n uint64
	for _, r := range s {
		if r < '0' || r > '9' {
			return n, fmt.Errorf("not a number: %q", s)
		}
		n = n*10 + uint64(r-'0')
	}
	return n, nil
}

// Telnyx sends timestamps as millisecond offsets from stream start (string).
func parseTelnyxTimestamp(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	ms, err := parseUint64(s)
	if err != nil {
		return time.Time{}
	}
	return time.Unix(0, int64(ms)*int64(time.Millisecond))
}

// base64Encode is exposed for call.go without introducing an import cycle.
func base64Encode(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}
