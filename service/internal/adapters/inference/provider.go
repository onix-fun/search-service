package inference

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

var ErrUnavailable = errors.New("embedding provider unavailable")

type Provider interface {
	Embed(context.Context, string) ([]float32, error)
}

type OpenAICompatible struct {
	endpoint, model string
	client          *http.Client
}

func New(endpoint, model string, timeout time.Duration) Provider {
	return &OpenAICompatible{endpoint: strings.TrimRight(endpoint, "/"), model: model, client: &http.Client{Timeout: timeout}}
}

func (p *OpenAICompatible) Embed(ctx context.Context, text string) ([]float32, error) {
	if p.endpoint == "" {
		return nil, ErrUnavailable
	}
	body, _ := json.Marshal(map[string]any{"model": p.model, "input": []string{text}})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint+"/v1/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		return nil, fmt.Errorf("%w: http %d", ErrUnavailable, response.StatusCode)
	}
	var decoded struct {
		Data []struct {
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil || len(decoded.Data) == 0 {
		return nil, fmt.Errorf("%w: invalid response", ErrUnavailable)
	}
	return decoded.Data[0].Embedding, nil
}
