package model

type Document map[string]any

type SearchRequest struct {
	Query  string   `json:"query"`
	Filter any      `json:"filter,omitempty"`
	Sort   []string `json:"sort,omitempty"`
	Offset int      `json:"offset,omitempty"`
	Limit  int      `json:"limit,omitempty"`
}

type SearchHit struct {
	ID    string         `json:"id"`
	Score float64        `json:"score,omitempty"`
	Data  map[string]any `json:"data,omitempty"`
}

type SearchResult struct {
	Hits             []SearchHit `json:"hits"`
	Offset           int         `json:"offset"`
	Limit            int         `json:"limit"`
	EstimatedTotal   int         `json:"estimated_total"`
	ProcessingTimeMs int         `json:"processing_time_ms"`
}
