package telnyx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// apiPOST issues a POST against the Telnyx Call Control REST API and
// decodes the JSON response into `out` (may be nil).
func (p *Protocol) apiPOST(ctx context.Context, path string, body any, out any) error {
	return p.apiCall(ctx, http.MethodPost, path, body, out)
}

func (p *Protocol) apiGET(ctx context.Context, path string) ([]byte, error) {
	var raw json.RawMessage
	if err := p.apiCall(ctx, http.MethodGet, path, nil, &raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func (p *Protocol) apiCall(ctx context.Context, method, path string, body any, out any) error {
	var buf io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("telnyx: marshal request: %w", err)
		}
		buf = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, p.baseURL+path, buf)
	if err != nil {
		return fmt.Errorf("telnyx: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+p.apiKey)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("telnyx: http error: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("telnyx: read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return fmt.Errorf("telnyx: %s %s -> HTTP %d: %s", method, path, resp.StatusCode, truncate(string(data), 512))
	}
	if out == nil || len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("telnyx: decode response: %w", err)
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// Telnyx "Dial" request shape. Only the fields we actually set are listed;
// see https://developers.telnyx.com/api/call-control/dial for the full set.
type telnyxDialRequest struct {
	ConnectionID    string            `json:"connection_id"`
	To              string            `json:"to"`
	From            string            `json:"from"`
	FromDisplayName string            `json:"from_display_name,omitempty"`
	TimeoutSecs     int               `json:"timeout_secs,omitempty"`
	WebhookURL      string            `json:"webhook_url,omitempty"`
	CustomHeaders   []telnyxSIPHeader `json:"custom_headers,omitempty"`
	ClientState     string            `json:"client_state,omitempty"`
}

type telnyxSIPHeader struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type telnyxDialResponse struct {
	Data struct {
		CallControlID string `json:"call_control_id"`
		CallLegID     string `json:"call_leg_id"`
		CallSessionID string `json:"call_session_id"`
		IsAlive       bool   `json:"is_alive"`
	} `json:"data"`
}

type telnyxAnswerRequest struct {
	StreamURL           string            `json:"stream_url,omitempty"`
	StreamTrack         string            `json:"stream_track,omitempty"`              // "inbound_track" | "outbound_track" | "both_tracks"
	StreamCodec         string            `json:"stream_codec,omitempty"`              // PCMU | OPUS | L16
	StreamBidirectional string            `json:"stream_bidirectional_mode,omitempty"` // "rtp" | "mp3"
	ClientState         string            `json:"client_state,omitempty"`
	CustomHeaders       []telnyxSIPHeader `json:"custom_headers,omitempty"`
}

type telnyxRejectRequest struct {
	Cause string `json:"cause,omitempty"` // "USER_BUSY" | "CALL_REJECTED" | ...
}

type telnyxHangupRequest struct {
	ClientState string `json:"client_state,omitempty"`
}

type telnyxDTMFRequest struct {
	Digits string `json:"digits"`
}

type telnyxTransferRequest struct {
	To   string `json:"to"`
	From string `json:"from,omitempty"`
}
