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

	"github.com/onix-fun/search/service/internal/domain"
	"github.com/onix-fun/search/service/internal/platform/config"
)

type Client struct {
	baseURL      string
	apiKey       string
	pollInterval time.Duration
	taskTimeout  time.Duration
	httpClient   *http.Client
}

type taskResponse struct {
	TaskUID *int64    `json:"taskUid"`
	UID     int64     `json:"uid"`
	Status  string    `json:"status"`
	Error   taskError `json:"error"`
}

type taskError struct {
	Message string `json:"message"`
	Code    string `json:"code"`
}

type searchResponse struct {
	Hits             []map[string]any `json:"hits"`
	Offset           int              `json:"offset"`
	Limit            int              `json:"limit"`
	EstimatedTotal   int              `json:"estimatedTotalHits"`
	ProcessingTimeMs int              `json:"processingTimeMs"`
}

type documentsResponse struct {
	Results []map[string]any `json:"results"`
	Offset  int              `json:"offset"`
	Limit   int              `json:"limit"`
	Total   int              `json:"total"`
}

func New(cfg config.MeilisearchConfig) *Client {
	return &Client{
		baseURL:      strings.TrimRight(cfg.Host, "/"),
		apiKey:       cfg.APIKey,
		pollInterval: cfg.TaskPollInterval,
		taskTimeout:  cfg.TaskTimeout,
		httpClient:   &http.Client{Timeout: cfg.TaskTimeout},
	}
}

func (c *Client) Health(ctx context.Context) error {
	return c.do(ctx, http.MethodGet, "/health", nil, nil, http.StatusOK)
}

func (c *Client) Upsert(ctx context.Context, collection string, docs []domain.Document) error {
	if len(docs) == 0 {
		return nil
	}
	task, err := c.enqueue(ctx, http.MethodPost, "/indexes/"+url.PathEscape(collection)+"/documents", docs, http.StatusAccepted)
	if err != nil {
		return err
	}
	return c.waitTask(ctx, task)
}

func (c *Client) Delete(ctx context.Context, collection string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	task, err := c.enqueue(ctx, http.MethodPost, "/indexes/"+url.PathEscape(collection)+"/documents/delete-batch", ids, http.StatusAccepted)
	if err != nil {
		return err
	}
	return c.waitTask(ctx, task)
}

func (c *Client) Search(ctx context.Context, collection string, request domain.SearchRequest) (domain.SearchResult, error) {
	body := map[string]any{"q": request.Query, "limit": request.Limit, "offset": request.Offset, "showRankingScore": true}
	if len(request.Vector) > 0 {
		body["vector"] = request.Vector
	}
	if request.SemanticRatio > 0 {
		body["hybrid"] = map[string]any{"semanticRatio": request.SemanticRatio, "embedder": request.Embedder}
	}
	if request.Filter != nil {
		body["filter"] = request.Filter
	}
	if len(request.Sort) > 0 {
		body["sort"] = request.Sort
	}
	var response searchResponse
	if err := c.do(ctx, http.MethodPost, "/indexes/"+url.PathEscape(collection)+"/search", body, &response, http.StatusOK); err != nil {
		return domain.SearchResult{}, err
	}
	result := domain.SearchResult{Offset: response.Offset, Limit: response.Limit, EstimatedTotal: response.EstimatedTotal, ProcessingTimeMs: response.ProcessingTimeMs}
	for _, raw := range response.Hits {
		id, _ := raw["id"].(string)
		score, _ := raw["_rankingScore"].(float64)
		delete(raw, "_rankingScore")
		if id != "" {
			result.Hits = append(result.Hits, domain.SearchHit{ID: id, Score: score, Data: raw})
		}
	}
	return result, nil
}

func (c *Client) Document(ctx context.Context, collection, id string) (map[string]any, error) {
	var document map[string]any
	if err := c.do(ctx, http.MethodGet, "/indexes/"+url.PathEscape(collection)+"/documents/"+url.PathEscape(id), nil, &document, http.StatusOK); err != nil {
		return nil, err
	}
	return document, nil
}

func (c *Client) Migrate(ctx context.Context, collections []config.CollectionConfig) error {
	for _, collection := range collections {
		if err := c.migrateCollection(ctx, collection); err != nil {
			return fmt.Errorf("migrate collection %s: %w", collection.Name, err)
		}
	}
	// Index retirement is configured by operators; the generic Search service
	// does not know which domain previously owned an index.
	return nil
}

func (c *Client) RetireIndexes(ctx context.Context, indexes []string) error {
	for _, index := range indexes {
		var existing map[string]any
		if err := c.do(ctx, http.MethodGet, "/indexes/"+url.PathEscape(index), nil, &existing, http.StatusOK); err != nil {
			var statusErr *HTTPError
			if errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusNotFound {
				continue
			}
			return err
		}
		task, err := c.enqueue(ctx, http.MethodDelete, "/indexes/"+url.PathEscape(index), nil, http.StatusAccepted)
		if err != nil {
			var statusErr *HTTPError
			if errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusNotFound {
				continue
			}
			return err
		}
		if err := c.waitTask(ctx, task); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) migrateCollection(ctx context.Context, collection config.CollectionConfig) error {
	if err := c.ensureIndex(ctx, collection.Index); err != nil {
		return err
	}
	if collection.Embedder != "" {
		if err := c.prepareUserProvidedEmbedder(ctx, collection.Index, collection.Embedder); err != nil {
			return fmt.Errorf("prepare user-provided embedder %s: %w", collection.Embedder, err)
		}
	}
	// Synonyms are intentionally collection-neutral in v1; per-collection files can be added without changing the API.
	synonymsFile := ""
	synonyms, err := loadSynonyms(synonymsFile)
	if err != nil {
		return err
	}
	settings := map[string]any{
		"searchableAttributes": collection.Searchable,
		"filterableAttributes": collection.Filterable,
		"sortableAttributes":   collection.Sortable,
		"displayedAttributes":  append([]string{"id"}, collection.Returnable...),
		"rankingRules":         []string{"words", "typo", "proximity", "attribute", "sort", "exactness"},
		"typoTolerance": map[string]any{
			"enabled":             true,
			"minWordSizeForTypos": map[string]int{"oneTypo": 4, "twoTypos": 8},
		},
		"synonyms": synonyms,
	}
	if collection.Embedder != "" {
		settings["embedders"] = map[string]any{collection.Embedder: map[string]any{"source": "userProvided", "dimensions": 384}}
	}
	task, err := c.enqueue(ctx, http.MethodPatch, "/indexes/"+url.PathEscape(collection.Index)+"/settings", settings, http.StatusAccepted)
	if err != nil {
		return err
	}
	return c.waitTask(ctx, task)
}

// prepareUserProvidedEmbedder makes adding semantic search to an existing
// lexical index safe. Meilisearch reindexes every document when an embedder is
// added and rejects documents without an explicit vector decision. Existing
// documents opt out with a null vector until the durable embedding worker
// enriches them. Once the embedder exists this method must be a no-op, or a
// service restart would erase already generated vectors.
func (c *Client) prepareUserProvidedEmbedder(ctx context.Context, index, embedder string) error {
	var embedders map[string]json.RawMessage
	if err := c.do(ctx, http.MethodGet, "/indexes/"+url.PathEscape(index)+"/settings/embedders", nil, &embedders, http.StatusOK); err != nil {
		return err
	}
	if _, exists := embedders[embedder]; exists {
		return nil
	}

	const pageSize = 1000
	for offset := 0; ; offset += pageSize {
		path := fmt.Sprintf("/indexes/%s/documents?limit=%d&offset=%d&fields=id", url.PathEscape(index), pageSize, offset)
		var page documentsResponse
		if err := c.do(ctx, http.MethodGet, path, nil, &page, http.StatusOK); err != nil {
			return err
		}
		if len(page.Results) == 0 {
			return nil
		}

		updates := make([]map[string]any, 0, len(page.Results))
		for _, document := range page.Results {
			id, exists := document["id"]
			if !exists {
				return errors.New("meilisearch document does not include id")
			}
			updates = append(updates, map[string]any{
				"id":       id,
				"_vectors": map[string]any{embedder: nil},
			})
		}

		task, err := c.enqueue(ctx, http.MethodPut, "/indexes/"+url.PathEscape(index)+"/documents", updates, http.StatusAccepted)
		if err != nil {
			return err
		}
		if err := c.waitTask(ctx, task); err != nil {
			return err
		}
		if offset+len(page.Results) >= page.Total {
			return nil
		}
	}
}

func (c *Client) ensureIndex(ctx context.Context, index string) error {
	var existing map[string]any
	err := c.do(ctx, http.MethodGet, "/indexes/"+url.PathEscape(index), nil, &existing, http.StatusOK)
	if err == nil {
		return nil
	}
	var statusErr *HTTPError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusNotFound {
		return err
	}

	task, err := c.enqueue(ctx, http.MethodPost, "/indexes", map[string]string{"uid": index, "primaryKey": "id"}, http.StatusAccepted)
	if err != nil {
		// Another replica may create the index between the GET and POST.
		if errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusConflict {
			return c.waitIndex(ctx, index)
		}
		return err
	}
	return c.waitTask(ctx, task)
}

func (c *Client) waitIndex(ctx context.Context, index string) error {
	ctx, cancel := context.WithTimeout(ctx, c.taskTimeout)
	defer cancel()
	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()

	for {
		var existing map[string]any
		err := c.do(ctx, http.MethodGet, "/indexes/"+url.PathEscape(index), nil, &existing, http.StatusOK)
		if err == nil {
			return nil
		}
		var statusErr *HTTPError
		if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusNotFound {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for meilisearch index %s: %w", index, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (c *Client) enqueue(ctx context.Context, method, path string, body any, expectedStatus int) (int64, error) {
	var response taskResponse
	if err := c.do(ctx, method, path, body, &response, expectedStatus); err != nil {
		return 0, err
	}
	if response.TaskUID == nil {
		return 0, errors.New("meilisearch response does not include taskUid")
	}
	return *response.TaskUID, nil
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
