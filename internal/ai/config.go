package ai

import (
	"fmt"
	"strings"
	"time"

	"github.com/Hoverhuang-er/godis/internal/config"
)

// Chunking defaults, in characters. Roughly four characters per token, so the
// default is a ~200-token chunk with ~30 tokens of overlap.
const (
	DefaultChunkSize    = 800
	DefaultChunkOverlap = 120
)

// Options is the runtime AI configuration.
type Options struct {
	Provider string
	LocalDim int
	BaseURL  string
	APIKey   string
	Model    string
	Dim      int
	Timeout  time.Duration
	Size     int
	Overlap  int
}

// OptionsFromConfig maps the [ai] section onto runtime options, filling in
// defaults so an unconfigured server still answers knowledge-base queries.
func OptionsFromConfig(props *config.ServerProperties) Options {
	opts := Options{
		Provider: props.AIEmbedProvider,
		LocalDim: props.AIEmbedLocalDim,
		BaseURL:  props.AIEmbedBaseURL,
		APIKey:   props.AIEmbedAPIKey,
		Model:    props.AIEmbedModel,
		Dim:      props.AIEmbedDim,
		Timeout:  time.Duration(props.AIEmbedTimeout) * time.Second,
		Size:     props.AIChunkSize,
		Overlap:  props.AIChunkOverlap,
	}
	if opts.Provider == "" {
		opts.Provider = ProviderLocal
	}
	if opts.LocalDim <= 0 {
		opts.LocalDim = DefaultLocalDim
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 30 * time.Second
	}
	if opts.Size <= 0 {
		opts.Size = DefaultChunkSize
	}
	if opts.Overlap < 0 || opts.Overlap >= opts.Size {
		opts.Overlap = DefaultChunkOverlap
	}
	return opts
}

// Providers supported by Options.Provider.
const (
	ProviderLocal = "local"
	ProviderHTTP  = "http"
)

// NewEmbedder builds the embedder described by opts. A misconfigured HTTP
// provider is an error rather than a silent fallback to the local embedder,
// which would quietly mix two incompatible vector spaces.
func NewEmbedder(opts Options) (Embedder, error) {
	switch strings.ToLower(opts.Provider) {
	case "", ProviderLocal:
		return NewLocalEmbedder(opts.LocalDim), nil
	case ProviderHTTP:
		return NewHTTPEmbedder(HTTPEmbedderOptions{
			BaseURL: opts.BaseURL,
			APIKey:  opts.APIKey,
			Model:   opts.Model,
			Dim:     opts.Dim,
			Timeout: opts.Timeout,
		})
	default:
		return nil, fmt.Errorf("unknown embed provider %q, want %q or %q", opts.Provider, ProviderLocal, ProviderHTTP)
	}
}
