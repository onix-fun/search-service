package grpcapi

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/onix-fun/search/service/internal/application"
	"github.com/onix-fun/search/service/internal/application/indexer"
	"github.com/onix-fun/search/service/internal/domain"
	searchpb "github.com/onix-fun/search/service/internal/gen/search"
	"github.com/onix-fun/search/service/internal/platform/config"
)

type Server struct {
	searchpb.UnimplementedSearchServiceServer
	backend    application.SearchBackend
	store      *indexer.Store
	cfg        config.Config
	embeddings application.EmbeddingProvider
}

func New(searchBackend application.SearchBackend, store *indexer.Store, cfg config.Config, embeddings application.EmbeddingProvider) *Server {
	return &Server{backend: searchBackend, store: store, cfg: cfg, embeddings: embeddings}
}

func (s *Server) Search(ctx context.Context, request *searchpb.SearchRequest) (*searchpb.SearchResponse, error) {
	collection, ok := s.cfg.Collection(strings.TrimSpace(request.GetCollection()))
	if !ok {
		return nil, status.Error(codes.NotFound, "unknown collection")
	}
	if !s.authorizedScope(ctx, "search:read", request.GetCollection()) {
		return nil, status.Error(codes.PermissionDenied, "forbidden")
	}
	weight, mode := effectiveMode(request.GetMode(), request.GetQuery(), request.GetSemanticWeight())
	fallback := false
	var vector []float32
	if weight > 0 && s.embeddings != nil {
		embedded, err := s.embeddings.Embed(ctx, "query: "+request.GetQuery())
		if err != nil {
			weight, mode, fallback = 0, searchpb.SearchMode_SEARCH_MODE_LEXICAL, true
		} else {
			vector = embedded
		}
	}
	filters, err := buildFilters(request.GetFilters(), collection.Filterable)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	result, err := s.backend.Search(ctx, collection.Index, domain.SearchRequest{
		Query: request.GetQuery(), Filter: filters, Limit: s.limit(int(request.GetPageSize())),
		Offset: decodeOffset(request.GetPageToken()), Vector: vector, SemanticRatio: weight, Embedder: collection.Embedder,
	})
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "search backend unavailable: %v", err)
	}
	return responseFromResult(result, request.GetQuery(), mode, weight, fallback, request.GetExplain()), nil
}

func (s *Server) Similar(ctx context.Context, request *searchpb.SimilarRequest) (*searchpb.SearchResponse, error) {
	collection, ok := s.cfg.Collection(strings.TrimSpace(request.GetCollection()))
	if !ok {
		return nil, status.Error(codes.NotFound, "unknown collection")
	}
	if !s.authorizedScope(ctx, "search:read", request.GetCollection()) {
		return nil, status.Error(codes.PermissionDenied, "forbidden")
	}
	text := strings.TrimSpace(request.GetText())
	if text == "" && request.GetDocumentId() != "" {
		resolver, ok := s.backend.(interface {
			Document(context.Context, string, string) (map[string]any, error)
		})
		if !ok {
			return nil, status.Error(codes.Unimplemented, "document similarity is unavailable")
		}
		document, err := resolver.Document(ctx, collection.Index, request.GetDocumentId())
		if err != nil {
			return nil, status.Error(codes.NotFound, "document not found")
		}
		text = semanticText(document, collection.Semantic)
	}
	if text == "" {
		return nil, status.Error(codes.InvalidArgument, "text or document_id is required")
	}
	if s.embeddings == nil {
		return nil, status.Error(codes.Unavailable, "semantic inference unavailable")
	}
	vector, err := s.embeddings.Embed(ctx, "query: "+text)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "semantic inference unavailable")
	}
	filters, err := buildFilters(request.GetFilters(), collection.Filterable)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if request.GetDocumentId() != "" {
		filters = append(filters, fmt.Sprintf("id != %s", quoteFilter(request.GetDocumentId())))
	}
	result, err := s.backend.Search(ctx, collection.Index, domain.SearchRequest{
		Filter: filters, Limit: s.limit(int(request.GetPageSize())), Offset: decodeOffset(request.GetPageToken()),
		Vector: vector, SemanticRatio: 1, Embedder: collection.Embedder,
	})
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "search backend unavailable: %v", err)
	}
	return responseFromResult(result, text, searchpb.SearchMode_SEARCH_MODE_SEMANTIC, 1, false, request.GetExplain()), nil
}

func (s *Server) IngestEvents(ctx context.Context, request *searchpb.IngestEventsRequest) (*searchpb.IngestEventsResponse, error) {
	if !s.authorizedScope(ctx, "search:write", "") {
		return nil, status.Error(codes.PermissionDenied, "forbidden")
	}
	response := &searchpb.IngestEventsResponse{}
	for _, incoming := range request.GetEvents() {
		event, source, err := eventFromProto(incoming)
		if err != nil {
			response.Results = append(response.Results, &searchpb.IngestEventResult{EventId: incoming.GetEventId(), Status: searchpb.IngestStatus_INGEST_STATUS_REJECTED, Message: err.Error()})
			continue
		}
		result, err := s.store.Ingest(ctx, source, event)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "persist index event: %v", err)
		}
		response.Results = append(response.Results, &searchpb.IngestEventResult{EventId: result.EventID, Status: ingestStatus(result.Status), Message: result.Message})
	}
	return response, nil
}

func eventFromProto(incoming *searchpb.IndexEvent) (domain.IndexEvent, string, error) {
	operation := domain.Operation("")
	switch incoming.GetOperation() {
	case searchpb.IndexOperation_INDEX_OPERATION_UPSERT:
		operation = domain.OperationUpsert
	case searchpb.IndexOperation_INDEX_OPERATION_DELETE:
		operation = domain.OperationDelete
	default:
		return domain.IndexEvent{}, "", fmt.Errorf("operation is required")
	}
	var document map[string]any
	if operation == domain.OperationUpsert {
		if err := json.Unmarshal([]byte(incoming.GetDocumentJson()), &document); err != nil {
			return domain.IndexEvent{}, "", fmt.Errorf("decode document_json: %w", err)
		}
	}
	source := strings.TrimSpace(incoming.GetSourceService())
	return domain.IndexEvent{
		EventID: incoming.GetEventId(), SourceService: source, Operation: operation,
		Collection: incoming.GetCollection(), DocumentID: incoming.GetAggregateId(), Revision: incoming.GetRevision(),
		Document: document, OccurredAt: incoming.GetOccurredAt(),
	}, source, nil
}

func ingestStatus(value indexer.IngestStatus) searchpb.IngestStatus {
	switch value {
	case indexer.IngestAccepted:
		return searchpb.IngestStatus_INGEST_STATUS_ACCEPTED
	case indexer.IngestDuplicate:
		return searchpb.IngestStatus_INGEST_STATUS_DUPLICATE
	default:
		return searchpb.IngestStatus_INGEST_STATUS_REJECTED
	}
}

func (s *Server) authorizedScope(ctx context.Context, scope, collection string) bool {
	value := bearer(ctx)
	if value == "" {
		return false
	}
	if subtle.ConstantTimeCompare([]byte(value), []byte(s.cfg.Service.InternalAuthSecret)) == 1 {
		return true
	}
	for _, key := range s.cfg.APIKeys {
		if subtle.ConstantTimeCompare([]byte(value), []byte(key.Value)) == 1 && allowed(key.Scopes, scope) && (collection == "" || allowed(key.Collections, collection)) {
			return true
		}
	}
	return false
}

func (s *Server) limit(value int) int {
	if value <= 0 {
		return s.cfg.Search.DefaultLimit
	}
	if value > s.cfg.Search.MaxLimit {
		return s.cfg.Search.MaxLimit
	}
	return value
}

func effectiveMode(mode searchpb.SearchMode, query string, provided float64) (float64, searchpb.SearchMode) {
	if mode == searchpb.SearchMode_SEARCH_MODE_LEXICAL {
		return 0, mode
	}
	if mode == searchpb.SearchMode_SEARCH_MODE_SEMANTIC {
		return 1, mode
	}
	if provided > 0 && provided <= 1 {
		return provided, searchpb.SearchMode_SEARCH_MODE_HYBRID
	}
	weight := .2
	if tokens := len(strings.Fields(query)); !strings.Contains(query, "\"") && tokens >= 3 && tokens <= 5 {
		weight = .45
	} else if tokens >= 6 {
		weight = .7
	}
	return weight, searchpb.SearchMode_SEARCH_MODE_HYBRID
}

func buildFilters(filters []*searchpb.SearchFilter, allowedFields []string) ([]string, error) {
	allow := map[string]bool{}
	for _, field := range allowedFields {
		allow[field] = true
	}
	result := []string{}
	for _, filter := range filters {
		if !allow[filter.GetField()] {
			return nil, fmt.Errorf("filter %s is not allowed", filter.GetField())
		}
		values := make([]string, 0, len(filter.GetValues()))
		for _, value := range filter.GetValues() {
			values = append(values, quoteFilter(value))
		}
		if len(values) > 0 {
			result = append(result, fmt.Sprintf("%s IN [%s]", filter.GetField(), strings.Join(values, ",")))
		}
	}
	return result, nil
}

func responseFromResult(result domain.SearchResult, query string, mode searchpb.SearchMode, weight float64, fallback, explain bool) *searchpb.SearchResponse {
	response := &searchpb.SearchResponse{EffectiveMode: mode, SemanticWeight: weight, LexicalFallback: fallback}
	for _, hit := range result.Hits {
		matched := []string{}
		if explain {
			needle := strings.ToLower(query)
			for field, value := range hit.Data {
				if text, ok := value.(string); ok && needle != "" && strings.Contains(strings.ToLower(text), needle) {
					matched = append(matched, field)
				}
			}
		}
		response.Candidates = append(response.Candidates, &searchpb.SearchCandidate{
			ProviderKey: stringValue(hit.Data, "provider_key", "_provider_key"),
			ItemType:    stringValue(hit.Data, "item_type", "_item_type"), ItemId: hit.ID,
			Score: hit.Score, LexicalScore: hit.Score * (1 - weight), SemanticScore: hit.Score * weight,
			MatchedFields: matched, Revision: int64Value(hit.Data, "_revision"),
		})
	}
	if result.Offset+len(result.Hits) < result.EstimatedTotal {
		response.NextPageToken = encodeOffset(result.Offset + len(result.Hits))
	}
	return response
}

func semanticText(document map[string]any, fields []string) string {
	parts := []string{}
	for _, field := range fields {
		if value, ok := document[field].(string); ok && value != "" {
			parts = append(parts, value)
		}
	}
	return strings.Join(parts, "\n")
}

func stringValue(data map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := data[key].(string); ok {
			return value
		}
	}
	return ""
}

func int64Value(data map[string]any, key string) int64 {
	switch value := data[key].(type) {
	case float64:
		return int64(value)
	case int64:
		return value
	}
	return 0
}

func quoteFilter(value string) string { encoded, _ := json.Marshal(value); return string(encoded) }
func decodeOffset(token string) int {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0
	}
	value, _ := strconv.Atoi(string(raw))
	return value
}
func encodeOffset(value int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(value)))
}
func bearer(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	values := md.Get("authorization")
	if len(values) == 0 {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(values[0], "Bearer "))
}
func allowed(values []string, expected string) bool {
	for _, value := range values {
		if value == "*" || value == expected {
			return true
		}
	}
	return false
}
