package httpapi

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/onix-fun/search-service/internal/backend"
	"github.com/onix-fun/search-service/internal/config"
	"github.com/onix-fun/search-service/internal/model"
)

type API struct {
	backend      backend.SearchBackend
	cfg          config.Config
	defaultLimit int
	maxLimit     int
}

func New(searchBackend backend.SearchBackend, cfg config.Config) http.Handler {
	api := &API{backend: searchBackend, cfg: cfg, defaultLimit: cfg.Search.DefaultLimit, maxLimit: cfg.Search.MaxLimit}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/collections/{collection}/search", api.search)
	return requireBearer(cfg, mux)
}

// search godoc
// @Summary Perform a search query
// @Description Executes a search against a specific Meilisearch collection with filtering, sorting, and pagination.
// @Tags search
// @Accept json
// @Produce json
// @Param collection path string true "Collection Name"
// @Param request body model.SearchRequest true "Search Request"
// @Success 200 {object} model.SearchResult
// @Failure 400 {string} string "Invalid request"
// @Failure 404 {string} string "Unknown collection"
// @Failure 503 {string} string "Search backend unavailable"
// @Security ApiKeyAuth
// @Router /v1/collections/{collection}/search [post]
func (a *API) search(w http.ResponseWriter, r *http.Request) {
	collectionName := strings.TrimSpace(r.PathValue("collection"))
	collection, ok := a.cfg.Collection(collectionName)
	if !ok {
		http.Error(w, "unknown collection", http.StatusNotFound)
		return
	}
	var request model.SearchRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		http.Error(w, "invalid request: "+err.Error(), http.StatusBadRequest)
		return
	}
	if request.Offset < 0 || request.Limit < 0 {
		http.Error(w, "offset and limit must not be negative", http.StatusBadRequest)
		return
	}
	if request.Limit == 0 {
		request.Limit = a.defaultLimit
	}
	if request.Limit > a.maxLimit {
		request.Limit = a.maxLimit
	}
	result, err := a.backend.Search(r.Context(), collection.Index, request)
	if err != nil {
		http.Error(w, "search backend unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func requireBearer(cfg config.Config, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		value := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		collection := r.PathValue("collection")
		for _, key := range cfg.APIKeys {
			if subtle.ConstantTimeCompare([]byte(value), []byte(key.Value)) != 1 {
				continue
			}
			if allowed(key.Scopes, "search:read") && allowed(key.Collections, collection) {
				next.ServeHTTP(w, r)
				return
			}
		}
		if cfg.Service.InternalAuthSecret != "" && subtle.ConstantTimeCompare([]byte(value), []byte(cfg.Service.InternalAuthSecret)) == 1 {
			next.ServeHTTP(w, r)
			return
		}
		http.Error(w, "forbidden", http.StatusForbidden)
	})
}

func allowed(values []string, expected string) bool {
	for _, value := range values {
		if value == "*" || value == expected {
			return true
		}
	}
	return false
}
