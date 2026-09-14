package ai

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// HTTPEmbedder calls an OpenAI-compatible embeddings endpoint, which covers
// OpenAI, Azure OpenAI, Ollama, vLLM, LM Studio and most hosted gateways.
type HTTPEmbedder struct {
	baseURL string
	apiKey  string
	model   string
	dim     int
	client  *http.Client
	// batchSize caps how many inputs go into one request.
	batchSize int
}

// HTTPEmbedderOptions configures NewHTTPEmbedder.
type HTTPEmbedderOptions struct {
	BaseURL string
	APIKey  string
	Model   string
	Dim     int
	Timeout time.Duration
}

// NewHTTPEmbedder builds an embedder against an OpenAI-compatible /embeddings
// endpoint. BaseURL is the API root, e.g. "https://api.openai.com/v1".
func NewHTTPEmbedder(opts HTTPEmbedderOptions) (*HTTPEmbedder, error) {
	if opts.BaseURL == "" {
		return nil, fmt.Errorf("embedding base URL is required")
	}
	if opts.Model == "" {
		return nil, fmt.Errorf("embedding model is required")
	}
	if opts.Dim <= 0 {
		return nil, fmt.Errorf("embedding dimension must be positive")
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &HTTPEmbedder{
		baseURL:   trimSlash(opts.BaseURL),
		apiKey:    opts.APIKey,
		model:     opts.Model,
		dim:       opts.Dim,
		client:    &http.Client{Timeout: timeout},
		batchSize: 64,
	}, nil
}

func (e *HTTPEmbedder) Dim() int { return e.dim }

func (e *HTTPEmbedder) Name() string { return "http:" + e.model + ":" + fmt.Sprint(e.dim) }

type embedRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type embedResponse struct {
	Data []struct {
		Index     int       `json:"index"`
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

func (e *HTTPEmbedder) Embed(texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}
	out := make([][]float32, 0, len(texts))
	for start := 0; start < len(texts); start += e.batchSize {
		end := min(start+e.batchSize, len(texts))
		vecs, err := e.embedBatch(texts[start:end])
		if err != nil {
			return nil, err
		}
		out = append(out, vecs...)
	}
	return out, nil
}

func (e *HTTPEmbedder) embedBatch(texts []string) ([][]float32, error) {
	body, err := json.Marshal(embedRequest{Model: e.model, Input: texts})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, e.baseURL+"/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if e.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+e.apiKey)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embedding request failed: %w", err)
	}
	defer resp.Body.Close()

	// Cap the error body: a misconfigured URL can return an entire HTML page.
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embedding endpoint returned %s: %s", resp.Status, truncate(string(raw), 300))
	}

	var parsed embedResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("embedding response is not valid JSON: %w", err)
	}
	if parsed.Error != nil {
		return nil, fmt.Errorf("embedding endpoint error: %s", parsed.Error.Message)
	}
	if len(parsed.Data) != len(texts) {
		return nil, fmt.Errorf("embedding endpoint returned %d vectors for %d inputs", len(parsed.Data), len(texts))
	}

	// The API may return results out of order; Index says where each belongs.
	out := make([][]float32, len(texts))
	for _, item := range parsed.Data {
		if item.Index < 0 || item.Index >= len(out) {
			return nil, fmt.Errorf("embedding endpoint returned index %d out of range", item.Index)
		}
		if len(item.Embedding) != e.dim {
			return nil, fmt.Errorf("embedding dimension mismatch: configured %d, endpoint returned %d",
				e.dim, len(item.Embedding))
		}
		out[item.Index] = item.Embedding
	}
	for i, v := range out {
		if v == nil {
			return nil, fmt.Errorf("embedding endpoint omitted input %d", i)
		}
	}
	return out, nil
}

func trimSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
