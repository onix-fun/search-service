package model

type Document struct {
	ID          string            `json:"id"`
	UUID        string            `json:"uuid"`
	EntityType  string            `json:"entity_type"`
	Revision    int64             `json:"revision"`
	Source      string            `json:"source"`
	Title       string            `json:"title"`
	Description string            `json:"description"`
	Text        string            `json:"text"`
	Keywords    []string          `json:"keywords"`
	Stems       []string          `json:"stems"`
	Translit    string            `json:"translit"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	UpdatedAt   string            `json:"updated_at,omitempty"`
}
