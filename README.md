# VOIP Library

`go-voip` is a protocol-agnostic VOIP integration layer for the Aulinq platform. It mirrors the design of [`go-ai-providers`](../providers/README.md): a small `core` interface set, pluggable `protocols`, and a `factory` that instantiates providers from DB-loaded configuration.

The first consumer is the **VOIP Agent** — a low-latency telephony bot that pipes:

```
Caller ─► Telnyx ─► go-voip ─► Deepgram STT ─► Groq/Fireworks LLM ─► Cartesia TTS ─► Telnyx ─► Caller
                                                                      (ElevenLabs fallback)
```

## Core Concepts

| Concept | Description |
| --- | --- |
| **Provider** | A VOIP backend (`Telnyx`, future: Twilio, Plivo). Implements `core.Provider`. |
| **Call** | A live session with bidirectional audio + control events (DTMF, hangup, …). |
| **AudioStream** | Codec-encoded frames (PCMU 8 kHz PSTN, Opus 48 kHz WebRTC, L16). |
| **CallHandler** | User-supplied function that owns an accepted call for its entire lifetime. |
| **HTTPHandler** | Mountable HTTP + WebSocket handler providers expose for webhooks/media. |

## Supported Protocols

| Preset | Protocol | Description |
| --- | --- | --- |
| `telnyx` | `telnyx` | Telnyx Call Control v2 REST + Media Streaming WebSocket. |

Add a new protocol by implementing `core.Provider` in `protocols/<name>/` and registering its factory in `factory/factory.go`.

## Quick Start

```go
import (
    "github.com/madmike/go-voip/core"
    "github.com/madmike/go-voip/factory"
)

provider, err := factory.CreateFromPreset(
    "telnyx",
    "Telnyx Production",
    os.Getenv("TELNYX_API_KEY"),
    "https://agent.example.com/voip/telnyx", // PublicWebhookURL
    map[string]any{
        "connection_id": os.Getenv("TELNYX_CONNECTION_ID"),
    },
    logger,
)
if err != nil { log.Fatal(err) }

// Accept inbound calls
provider.Listen(ctx, func(ctx context.Context, call core.Call) {
    if err := call.Accept(ctx, core.AcceptOptions{
        Codec:                core.CodecPCMU,
        EnableMediaStreaming: true,
    }); err != nil {
        return
    }
    audio := call.Audio()
    for {
        frame, err := audio.Receive(ctx)
        if err != nil { return }
        // → pass frame.Payload to Deepgram STT stream
        _ = frame
    }
})

// Mount the provider's webhook + media WebSocket on your HTTP server
http.Handle("/voip/telnyx", provider.HTTPHandler().(http.Handler))
```

### Outbound Call

```go
call, err := provider.Dial(ctx, core.DialRequest{
    From:         "+12025550100",
    To:           "+12025550199",
    ConnectionID: "<telnyx-connection-id>",
    Timeout:      30 * time.Second,
})
```

## Directory Layout

```
services/libraries/voip/
├── core/                 # Protocol-agnostic interfaces & types
│   ├── provider.go
│   └── call.go
├── factory/              # Preset-based instantiation
│   └── factory.go
├── protocols/
│   └── telnyx/
│       ├── protocol.go   # core.Provider implementation
│       ├── call.go       # core.Call + Dial
│       ├── rest.go       # Call Control REST client
│       ├── media_ws.go   # Bidirectional media streaming
│       └── webhook.go    # HTTP webhook + WS upgrade
├── go.mod
└── README.md             # This file
```

## Integration Checklist (for services)

1. Add `github.com/madmike/go-voip` to the service `go.mod`.
2. Persist provider credentials in a `voip_providers` table shaped like `factory.DBProviderConfig` (`preset_name`, `api_key`, `public_webhook_url`, `options`).
3. Use `factory.CreateFromDB(...)` in bootstrap, cache the resulting `core.Provider` per tenant.
4. Mount `provider.HTTPHandler()` on a public route (Telnyx needs it to POST events + upgrade the media WebSocket).
5. Hand each accepted `core.Call` to a pipeline stage that bridges `AudioStream` ↔ `go-ai-providers` STT/TTS.
