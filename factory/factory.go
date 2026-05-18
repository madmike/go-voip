// Package factory instantiates VOIP providers from named presets, matching
// the ergonomics of go-ai-providers/factory.
package factory

import (
	"context"
	"fmt"

	"github.com/madmike/go-voip/core"
	"github.com/madmike/go-voip/protocols/telnyx"
	"github.com/madmike/go-voip/protocols/webwidget"
)

// ProtocolFactory constructs a Provider from a ProviderConfig.
type ProtocolFactory func(config core.ProviderConfig) (core.Provider, error)

// ProtocolRegistry holds all available VOIP protocol factories.
var ProtocolRegistry = map[string]ProtocolFactory{
	"telnyx":    telnyx.NewProtocol,
	"webwidget": webwidget.NewProtocol,
}

// Preset is a named, pre-wired VOIP provider configuration template.
type Preset struct {
	Protocol        string
	Name            string
	BaseURL         string
	MediaWSURL      string
	RequiredOptions []string
	Description     string
}

// Presets contains predefined VOIP provider configurations.
var Presets = map[string]Preset{
	"telnyx": {
		Protocol:    "telnyx",
		Name:        "Telnyx",
		BaseURL:     "https://api.telnyx.com/v2",
		MediaWSURL:  "wss://rtc.telnyx.com/v2/media", // Call Control media streaming
		Description: "Telnyx Call Control v2 + Media Streaming (SIP/WebRTC) — primary VOIP backend.",
	},
	"webwidget": {
		Protocol:    "webwidget",
		Name:        "Web Widget (Test)",
		Description: "WebSocket-based VOIP bridge for browser-based testing via web widget.",
	},
}

// DBProviderConfig mirrors go-ai-providers/factory.DBProviderConfig so
// services can persist VOIP provider credentials the same way.
type DBProviderConfig struct {
	PresetName       string
	DisplayName      string
	APIKey           string
	APISecret        string
	BaseURL          string
	MediaWSURL       string
	PublicWebhookURL string
	Options          map[string]any
	Logger           any
}

// CreateFromDB instantiates a VOIP provider from DB-loaded config.
func CreateFromDB(cfg DBProviderConfig) (core.Provider, error) {
	preset, ok := Presets[cfg.PresetName]
	if !ok {
		return nil, fmt.Errorf("unknown voip preset: %s", cfg.PresetName)
	}

	factory, ok := ProtocolRegistry[preset.Protocol]
	if !ok {
		return nil, fmt.Errorf("unknown voip protocol: %s", preset.Protocol)
	}

	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = preset.BaseURL
	}
	mediaURL := cfg.MediaWSURL
	if mediaURL == "" {
		mediaURL = preset.MediaWSURL
	}

	providerCfg := core.ProviderConfig{
		Name:             cfg.DisplayName,
		Type:             core.ProviderType(preset.Protocol),
		APIKey:           cfg.APIKey,
		APISecret:        cfg.APISecret,
		BaseURL:          baseURL,
		MediaWSURL:       mediaURL,
		PublicWebhookURL: cfg.PublicWebhookURL,
		Options:          cfg.Options,
		Logger:           cfg.Logger,
	}

	for _, key := range preset.RequiredOptions {
		if providerCfg.Options == nil || providerCfg.Options[key] == nil {
			return nil, fmt.Errorf("required option %s missing for preset %s", key, cfg.PresetName)
		}
	}

	provider, err := factory(providerCfg)
	if err != nil {
		return nil, err
	}
	if err := provider.Initialize(context.Background(), providerCfg); err != nil {
		return nil, fmt.Errorf("initialize %s: %w", cfg.DisplayName, err)
	}
	return provider, nil
}

// CreateFromPreset is the convenience wrapper for static configuration.
func CreateFromPreset(preset, displayName, apiKey, publicWebhookURL string, options map[string]any, logger any) (core.Provider, error) {
	return CreateFromDB(DBProviderConfig{
		PresetName:       preset,
		DisplayName:      displayName,
		APIKey:           apiKey,
		PublicWebhookURL: publicWebhookURL,
		Options:          options,
		Logger:           logger,
	})
}

// GetPreset returns a preset by name.
func GetPreset(name string) (Preset, bool) {
	p, ok := Presets[name]
	return p, ok
}

// ListPresets returns all known VOIP presets.
func ListPresets() map[string]Preset {
	return Presets
}
