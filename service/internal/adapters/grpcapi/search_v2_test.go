package grpcapi

import (
	"testing"

	searchpb "github.com/onix-fun/search/service/internal/gen/search"
)

func TestAutoSemanticWeight(t *testing.T) {
	cases := []struct {
		query string
		want  float64
	}{{"кот", .2}, {"цветной проект дизайн", .45}, {"покажи мне яркие проекты про современный дизайн", .7}}
	for _, item := range cases {
		weight, mode := effectiveMode(searchpb.SearchMode_SEARCH_MODE_UNSPECIFIED, item.query, 0)
		if weight != item.want {
			t.Fatalf("%q: got %v want %v", item.query, weight, item.want)
		}
		if mode != searchpb.SearchMode_SEARCH_MODE_HYBRID {
			t.Fatalf("auto must resolve to hybrid")
		}
	}
}

func TestExplicitLexicalDisablesSemantic(t *testing.T) {
	weight, mode := effectiveMode(searchpb.SearchMode_SEARCH_MODE_LEXICAL, "длинный естественный запрос который обычно semantic", .9)
	if weight != 0 || mode != searchpb.SearchMode_SEARCH_MODE_LEXICAL {
		t.Fatalf("got %v %v", weight, mode)
	}
}
