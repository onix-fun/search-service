package grpcserver

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	searchv1 "github.com/company/search-service/api/search/v1"
	"github.com/company/search-service/internal/backend"
	"github.com/company/search-service/internal/enrichment"
)

type SearchServer struct {
	searchv1.UnimplementedSearchServiceServer
	backend      backend.SearchBackend
	processor    *enrichment.Processor
	defaultLimit int
	maxLimit     int
}

func New(searchBackend backend.SearchBackend, processor *enrichment.Processor, defaultLimit, maxLimit int) *SearchServer {
	return &SearchServer{backend: searchBackend, processor: processor, defaultLimit: defaultLimit, maxLimit: maxLimit}
}

func (s *SearchServer) Search(ctx context.Context, request *searchv1.SearchRequest) (*searchv1.SearchResponse, error) {
	query := strings.TrimSpace(request.GetQuery())
	if query == "" {
		return &searchv1.SearchResponse{}, nil
	}
	entityType := strings.TrimSpace(request.GetEntityType())
	limit := s.Limit(request.GetLimit())
	uuids, err := s.backend.Search(ctx, s.processor.QueryVariants(query), limit, entityType)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "search backend unavailable")
	}
	return &searchv1.SearchResponse{Uuids: uuids}, nil
}

func (s *SearchServer) Limit(requested int32) int {
	if requested <= 0 {
		return s.defaultLimit
	}
	if int(requested) > s.maxLimit {
		return s.maxLimit
	}
	return int(requested)
}
