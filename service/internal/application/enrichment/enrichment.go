package enrichment

import (
	"regexp"
	"strings"
	"unicode"

	"github.com/kljensen/snowball"
	"golang.org/x/text/unicode/norm"

	"github.com/onix-fun/search/service/internal/domain"
)

var wordPattern = regexp.MustCompile(`[\p{L}\p{N}]+`)

type Processor struct {
	transliteration bool
	morphology      bool
}

func New(transliteration, morphology bool) *Processor {
	return &Processor{transliteration: transliteration, morphology: morphology}
}

func (p *Processor) Enrich(event domain.IndexEvent) domain.Document {
	doc := event.SearchDocument()
	var values []string
	collectStrings(event.Document, &values)
	allText := strings.Join(values, " ")
	doc["_search_text"] = Normalize(allText)
	if p.morphology {
		doc["_stems"] = stems(allText)
	}
	if p.transliteration {
		doc["_translit"] = Transliterate(allText)
	}
	return doc
}

func collectStrings(value any, result *[]string) {
	switch typed := value.(type) {
	case string:
		*result = append(*result, typed)
	case []any:
		for _, item := range typed {
			collectStrings(item, result)
		}
	case map[string]any:
		for _, item := range typed {
			collectStrings(item, result)
		}
	}
}

func (p *Processor) QueryVariants(query string) []string {
	original := Normalize(query)
	if original == "" {
		return nil
	}

	variants := []string{original}
	if p.morphology {
		variants = append(variants, strings.Join(stems(original), " "))
	}
	if p.transliteration {
		translit := Transliterate(original)
		variants = append(variants, translit)
		if p.morphology {
			variants = append(variants, strings.Join(stems(translit), " "))
		}
	}
	return uniqueNonEmpty(variants)
}

func Normalize(value string) string {
	return strings.Join(strings.Fields(strings.ToLower(norm.NFKC.String(value))), " ")
}

func normalizeList(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if normalized := Normalize(value); normalized != "" {
			result = append(result, normalized)
		}
	}
	return uniqueNonEmpty(result)
}

func stems(value string) []string {
	words := wordPattern.FindAllString(Normalize(value), -1)
	result := make([]string, 0, len(words))
	for _, word := range words {
		language := "english"
		if containsCyrillic(word) {
			language = "russian"
		}
		stem, err := snowball.Stem(word, language, true)
		if err != nil || stem == "" {
			stem = word
		}
		result = append(result, stem)
	}
	return uniqueNonEmpty(result)
}

func containsCyrillic(value string) bool {
	for _, r := range value {
		if unicode.In(r, unicode.Cyrillic) {
			return true
		}
	}
	return false
}

func uniqueNonEmpty(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = Normalize(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}
