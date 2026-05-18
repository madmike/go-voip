package core

import (
	"context"
	"time"
)

// CallState is the lifecycle state of a Call.
type CallState string

const (
	CallStateDialing  CallState = "dialing"  // outbound, ringing
	CallStateRinging  CallState = "ringing"  // inbound, not yet answered
	CallStateAnswered CallState = "answered" // media path open
	CallStateBridged  CallState = "bridged"  // bridged to another leg
	CallStateHangup   CallState = "hangup"   // remote or local hangup
	CallStateFailed   CallState = "failed"   // terminated with error
)

// Direction indicates inbound vs outbound.
type Direction string

const (
	DirectionInbound  Direction = "inbound"
	DirectionOutbound Direction = "outbound"
)

// Call is an active VOIP call session with bidirectional audio and control.
//
// Calls are single-use: once hung up, create a new Call for the next
// conversation. Implementations are safe for concurrent calls to Audio()
// send/receive, but Accept/Reject/Hangup/Transfer are expected to be
// serialized by the caller.
type Call interface {
	// ID is the provider-assigned call identifier (e.g. Telnyx call_control_id).
	ID() string
	Direction() Direction
	From() string
	To() string
	State() CallState
	StartedAt() time.Time

	// Accept picks up an inbound call and opens the media path. For outbound
	// calls this is a no-op (the call is already answered by the remote).
	Accept(ctx context.Context, opts AcceptOptions) error

	// Reject declines an inbound call with an optional SIP status code
	// (default 603 Decline).
	Reject(ctx context.Context, reason string) error

	// Audio returns the bidirectional audio stream. Calling Audio() before
	// Accept() is provider-specific: most providers only allow it once the
	// media path is open.
	Audio() AudioStream

	// Events returns a channel of call-level events (DTMF, state changes,
	// transcription-ready, …). The channel is closed when the call ends.
	Events() <-chan Event

	// SendDTMF injects DTMF digits toward the remote party.
	SendDTMF(ctx context.Context, digits string) error

	// Transfer hands the call off to a new destination (blind transfer).
	Transfer(ctx context.Context, destination string) error

	// Hangup terminates the call. Safe to call multiple times.
	Hangup(ctx context.Context) error
}

// AcceptOptions controls how an inbound call is answered.
type AcceptOptions struct {
	// Codec requests a specific media codec. Empty = provider default
	// (Telnyx: PCMU 8kHz).
	Codec AudioCodec

	// EnableMediaStreaming opens the bidirectional media WebSocket. For
	// Telnyx this corresponds to `stream_url` + `stream_track=both_tracks`.
	// Defaults to true for agent use.
	EnableMediaStreaming bool

	// StreamURL overrides the media WebSocket URL (useful for regionally
	// distributed deployments). Empty = use provider config default.
	StreamURL string

	// CustomHeaders are SIP headers to echo in the 200 OK.
	CustomHeaders map[string]string
}

// AudioCodec identifies a wire-format audio codec.
type AudioCodec string

const (
	CodecPCMU  AudioCodec = "pcmu"  // G.711 μ-law 8kHz, PSTN default
	CodecPCMA  AudioCodec = "pcma"  // G.711 A-law 8kHz
	CodecPCM16 AudioCodec = "pcm16" // Raw 16-bit LE PCM
	CodecOpus  AudioCodec = "opus"  // WebRTC default
	CodecL16   AudioCodec = "l16"   // 16 kHz linear (Telnyx)
)

// AudioStream is bidirectional audio. Send and Receive are safe to call
// concurrently from different goroutines.
type AudioStream interface {
	// Send pushes an audio frame toward the remote party. The frame MUST be
	// encoded in the stream's negotiated codec. Frames are typically 20ms.
	Send(ctx context.Context, frame AudioFrame) error

	// Receive blocks until the next audio frame from the remote party, or
	// the context is cancelled, or the stream is closed (returns io.EOF).
	Receive(ctx context.Context) (AudioFrame, error)

	// Codec returns the negotiated codec for this stream.
	Codec() AudioCodec

	// SampleRate in Hz (8000 for PSTN, 16000/48000 for WebRTC).
	SampleRate() int

	// Close stops the stream. The underlying call is NOT hung up — call
	// Call.Hangup() for that.
	Close() error
}

// AudioFrame is a single codec-encoded audio frame.
type AudioFrame struct {
	// Payload is the encoded frame. For PCMU/PCMA this is 160 bytes for 20ms
	// at 8kHz; for L16 16kHz it's 640 bytes.
	Payload []byte

	// Timestamp is the sender-side capture time. Zero if unknown.
	Timestamp time.Time

	// Track distinguishes inbound vs outbound track in a mixed stream.
	// Most consumers can ignore this.
	Track string // "inbound" | "outbound"

	// SequenceNumber is a monotonically increasing frame counter. Zero if
	// not provided by the provider.
	SequenceNumber uint64
}

// EventType classifies call-level events.
type EventType string

const (
	EventAnswered       EventType = "answered"
	EventHangup         EventType = "hangup"
	EventDTMF           EventType = "dtmf"
	EventSpeechStarted  EventType = "speech_started"
	EventSpeechStopped  EventType = "speech_stopped"
	EventMediaStarted   EventType = "media_started"
	EventMediaStopped   EventType = "media_stopped"
	EventTransferred    EventType = "transferred"
	EventRecordingSaved EventType = "recording_saved"
	EventError          EventType = "error"
)

// Event is a call-level control event delivered on Call.Events().
type Event struct {
	Type      EventType
	Timestamp time.Time
	Digits    string         // populated for DTMF
	Reason    string         // populated for hangup, error
	Metadata  map[string]any // provider-specific extras
}
