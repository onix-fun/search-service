package enrichment_test

import (
	"slices"
	"testing"

	"github.com/company/search-service/internal/enrichment"
	"github.com/company/search-service/internal/model"
)

func TestNormalize(t *testing.T) {
	if got := enrichment.Normalize("  ИВАН   Петров  "); got != "иван петров" {
		t.Fatalf("Normalize() = %q", got)
	}
}

func TestTransliterateBothDirections(t *testing.T) {
	if got := enrichment.Transliterate("иван петров"); got != "ivan petrov" {
		t.Fatalf("Transliterate(cyrillic) = %q", got)
	}
	if got := enrichment.Transliterate("ivan"); got != "иван" {
		t.Fatalf("Transliterate(latin) = %q", got)
	}
}

func TestEnrichAndQueryVariants(t *testing.T) {
	processor := enrichment.New(true, true)
	doc := processor.Enrich(model.IndexEvent{
		EntityType: "users",
		UUID:       "9dd2e47e-7a2d-4b99-b7a1-ff0d94b7e301", Revision: 1, Source: "users",
		Title: "Иван Петров", Description: "Backend разработчик", Keywords: []string{"GOLANG"},
	})
	if doc.Title != "иван петров" || doc.Translit == "" || len(doc.Stems) == 0 {
		t.Fatalf("Enrich() = %#v", doc)
	}
	variants := processor.QueryVariants("Ivan")
	if !slices.Contains(variants, "иван") {
		t.Fatalf("QueryVariants() = %v, want transliterated variant", variants)
	}
}
