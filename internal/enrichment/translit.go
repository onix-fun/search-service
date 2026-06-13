package enrichment

import (
	"sort"
	"strings"
	"unicode"
)

var cyrillicToLatin = map[rune]string{
	'а': "a", 'б': "b", 'в': "v", 'г': "g", 'д': "d", 'е': "e", 'ё': "yo",
	'ж': "zh", 'з': "z", 'и': "i", 'й': "y", 'к': "k", 'л': "l", 'м': "m",
	'н': "n", 'о': "o", 'п': "p", 'р': "r", 'с': "s", 'т': "t", 'у': "u",
	'ф': "f", 'х': "kh", 'ц': "ts", 'ч': "ch", 'ш': "sh", 'щ': "shch",
	'ъ': "", 'ы': "y", 'ь': "", 'э': "e", 'ю': "yu", 'я': "ya",
}

var latinToCyrillic = map[string]string{
	"shch": "щ", "yo": "ё", "zh": "ж", "kh": "х", "ts": "ц", "ch": "ч",
	"sh": "ш", "yu": "ю", "ya": "я", "a": "а", "b": "б", "v": "в",
	"g": "г", "d": "д", "e": "е", "z": "з", "i": "и", "y": "й",
	"k": "к", "l": "л", "m": "м", "n": "н", "o": "о", "p": "п",
	"r": "р", "s": "с", "t": "т", "u": "у", "f": "ф",
}

var latinTokens = sortedLatinTokens()

func Transliterate(value string) string {
	value = Normalize(value)
	if containsCyrillic(value) {
		return cyrillicAsLatin(value)
	}
	return latinAsCyrillic(value)
}

func cyrillicAsLatin(value string) string {
	var result strings.Builder
	for _, r := range value {
		if replacement, ok := cyrillicToLatin[unicode.ToLower(r)]; ok {
			result.WriteString(replacement)
			continue
		}
		result.WriteRune(r)
	}
	return result.String()
}

func latinAsCyrillic(value string) string {
	var result strings.Builder
	for offset := 0; offset < len(value); {
		matched := false
		for _, token := range latinTokens {
			if strings.HasPrefix(value[offset:], token) {
				result.WriteString(latinToCyrillic[token])
				offset += len(token)
				matched = true
				break
			}
		}
		if matched {
			continue
		}
		result.WriteByte(value[offset])
		offset++
	}
	return result.String()
}

func sortedLatinTokens() []string {
	tokens := make([]string, 0, len(latinToCyrillic))
	for token := range latinToCyrillic {
		tokens = append(tokens, token)
	}
	sort.Slice(tokens, func(i, j int) bool {
		return len(tokens[i]) > len(tokens[j])
	})
	return tokens
}
