package enrichment

import (
	"regexp"
	"strings"
	"unicode"

	"github.com/kljensen/snowball"
	"golang.org/x/text/unicode/norm"

	"github.com/company/search-service/internal/model"
)

var wordPattern = regexp.MustCompile(`[\p{L}\p{N}]+`)

type Processor struct {
	transliteration bool
	morphology      bool
}

func New(transliteration, morphology bool) *Processor {
	return &Processor{transliteration: transliteration, morphology: morphology}
}

func (p *Processor) Enrich(event model.IndexEvent) model.Document {
	title := Normalize(event.Title)
	description := Normalize(event.Description)
	text := Normalize(event.Text)
	keywords := normalizeList(event.Keywords)
	allText := strings.Join(append([]string{title, description, text}, keywords...), " ")

	doc := model.Document{
		ID:          event.UUID,
		UUID:        event.UUID,
		EntityType:  event.EntityType,
		Revision:    event.Revision,
		Source:      event.Source,
		Title:       title,
		Description: description,
		Text:        text,
		Keywords:    keywords,
		Metadata:    event.Metadata,
		UpdatedAt:   event.UpdatedAt,
	}
	if p.morphology {
		doc.Stems = stems(allText)
	}
	if p.transliteration {
		doc.Translit = Transliterate(allText)
	}
	return doc
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
