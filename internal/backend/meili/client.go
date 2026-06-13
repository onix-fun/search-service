package meili

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/company/search-service/internal/config"
	"github.com/company/search-service/internal/model"
)

type Client struct {
	baseURL      string
	apiKey       string
	index        string
	pollInterval time.Duration
	taskTimeout  time.Duration
	httpClient   *http.Client
}

type taskResponse struct {
	TaskUID int64     `json:"taskUid"`
	UID     int64     `json:"uid"`
	Status  string    `json:"status"`
	Error   taskError `json:"error"`
}

type taskError struct {
	Message string `json:"message"`
	Code    string `json:"code"`
}

type searchResponse struct {
	Results []struct {
		Hits []struct {
			UUID string `json:"uuid"`
		} `json:"hits"`
	} `json:"results"`
}

func New(cfg config.MeilisearchConfig) *Client {
	return &Client{
		baseURL:      strings.TrimRight(cfg.Host, "/"),
		apiKey:       cfg.APIKey,
		index:        cfg.Index,
		pollInterval: cfg.TaskPollInterval,
		taskTimeout:  cfg.TaskTimeout,
		httpClient:   &http.Client{Timeout: cfg.TaskTimeout},
	}
}

func (c *Client) Health(ctx context.Context) error {
	return c.do(ctx, http.MethodGet, "/health", nil, nil, http.StatusOK)
}

func (c *Client) Upsert(ctx context.Context, docs []model.Document) error {
	if len(docs) == 0 {
		return nil
	}
	task, err := c.enqueue(ctx, http.MethodPost, "/indexes/"+url.PathEscape(c.index)+"/documents", docs, http.StatusAccepted)
	if err != nil {
		return err
	}
	return c.waitTask(ctx, task)
}

func (c *Client) Delete(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	task, err := c.enqueue(ctx, http.MethodPost, "/indexes/"+url.PathEscape(c.index)+"/documents/delete-batch", ids, http.StatusAccepted)
	if err != nil {
		return err
	}
	return c.waitTask(ctx, task)
}

func (c *Client) Search(ctx context.Context, variants []string, limit int, entityType string) ([]string, error) {
	if len(variants) == 0 {
		return nil, nil
	}
	queries := make([]map[string]any, 0, len(variants))
	for _, variant := range variants {
		q := map[string]any{
			"indexUid":             c.index,
			"q":                    variant,
			"limit":                limit,
			"attributesToRetrieve": []string{"uuid"},
		}
		if entityType != "" {
			q["filter"] = "entity_type = " + entityType
		}
		queries = append(queries, q)
	}
	var response searchResponse
	if err := c.do(ctx, http.MethodPost, "/multi-search", map[string]any{"queries": queries}, &response, http.StatusOK); err != nil {
		return nil, err
	}

	seen := make(map[string]struct{}, limit)
	uuids := make([]string, 0, limit)
	for _, result := range response.Results {
		for _, hit := range result.Hits {
			if hit.UUID == "" {
				continue
			}
			if _, ok := seen[hit.UUID]; ok {
				continue
			}
			seen[hit.UUID] = struct{}{}
			uuids = append(uuids, hit.UUID)
			if len(uuids) == limit {
				return uuids, nil
			}
		}
	}
	return uuids, nil
}

func (c *Client) Migrate(ctx context.Context, synonymsFile string) error {
	if err := c.ensureIndex(ctx); err != nil {
		return err
	}
	synonyms, err := loadSynonyms(synonymsFile)
	if err != nil {
		return err
	}
	settings := map[string]any{
		"searchableAttributes": []string{"title", "keywords", "stems", "translit", "description", "text"},
		"filterableAttributes": []string{"entity_type"},
		"displayedAttributes":  []string{"uuid"},
		"rankingRules":         []string{"words", "typo", "proximity", "attribute", "sort", "exactness"},
		"typoTolerance": map[string]any{
			"enabled":             true,
			"minWordSizeForTypos": map[string]int{"oneTypo": 4, "twoTypos": 8},
		},
		"synonyms": synonyms,
	}
	task, err := c.enqueue(ctx, http.MethodPatch, "/indexes/"+url.PathEscape(c.index)+"/settings", settings, http.StatusAccepted)
	if err != nil {
		return err
	}
	return c.waitTask(ctx, task)
}

func (c *Client) ensureIndex(ctx context.Context) error {
	var existing map[string]any
	err := c.do(ctx, http.MethodGet, "/indexes/"+url.PathEscape(c.index), nil, &existing, http.StatusOK)
	if err == nil {
		return nil
	}
	var statusErr *HTTPError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusNotFound {
		return err
	}

	task, err := c.enqueue(ctx, http.MethodPost, "/indexes", map[string]string{"uid": c.index, "primaryKey": "id"}, http.StatusAccepted)
	if err == nil {
		return c.waitTask(ctx, task)
	}
	if errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusConflict {
		return nil
	}
	return err
}

func (c *Client) enqueue(ctx context.Context, method, path string, body any, expectedStatus int) (int64, error) {
	var response taskResponse
	if err := c.do(ctx, method, path, body, &response, expectedStatus); err != nil {
		return 0, err
	}
	if response.TaskUID == 0 {
		return 0, errors.New("meilisearch response does not include taskUid")
	}
	return response.TaskUID, nil
}

func (c *Client) waitTask(ctx context.Context, uid int64) error {
	ctx, cancel := context.WithTimeout(ctx, c.taskTimeout)
	defer cancel()
	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()

	for {
		var task taskResponse
		if err := c.do(ctx, http.MethodGet, fmt.Sprintf("/tasks/%d", uid), nil, &task, http.StatusOK); err != nil {
			return err
		}
		switch task.Status {
		case "succeeded":
			return nil
		case "failed", "canceled":
			return fmt.Errorf("meilisearch task %d %s: %s (%s)", uid, task.Status, task.Error.Message, task.Error.Code)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for meilisearch task %d: %w", uid, ctx.Err())
		case <-ticker.C:
		}
	}
}

type HTTPError struct {
	StatusCode int
	Body       string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("meilisearch returned HTTP %d: %s", e.StatusCode, e.Body)
}

func (c *Client) do(ctx context.Context, method, path string, body, response any, expectedStatus int) error {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode meilisearch request: %w", err)
		}
		reader = bytes.NewReader(data)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return fmt.Errorf("create meilisearch request: %w", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if c.apiKey != "" {
		request.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	result, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("call meilisearch: %w", err)
	}
	defer result.Body.Close()
	data, err := io.ReadAll(io.LimitReader(result.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("read meilisearch response: %w", err)
	}
	if result.StatusCode != expectedStatus {
		return &HTTPError{StatusCode: result.StatusCode, Body: string(data)}
	}
	if response != nil && len(data) > 0 {
		if err := json.Unmarshal(data, response); err != nil {
			return fmt.Errorf("decode meilisearch response: %w", err)
		}
	}
	return nil
}

func loadSynonyms(path string) (map[string][]string, error) {
	if path == "" {
		return map[string][]string{}, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read synonyms file: %w", err)
	}
	var synonyms map[string][]string
	if err := yaml.Unmarshal(data, &synonyms); err != nil {
		return nil, fmt.Errorf("decode synonyms file: %w", err)
	}
	if synonyms == nil {
		synonyms = map[string][]string{}
	}
	return synonyms, nil
}
