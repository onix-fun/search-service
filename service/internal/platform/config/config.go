package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Service     ServiceConfig      `yaml:"service"`
	Database    DatabaseConfig     `yaml:"database"`
	Meilisearch MeilisearchConfig  `yaml:"meilisearch"`
	Search      SearchConfig       `yaml:"search"`
	Embedding   EmbeddingConfig    `yaml:"embedding"`
	Enrichment  EnrichmentConfig   `yaml:"enrichment"`
	Indexer     IndexerConfig      `yaml:"indexer"`
	Collections []CollectionConfig `yaml:"collections"`
	APIKeys     []APIKey           `yaml:"api_keys"`
}

type DatabaseConfig struct {
	URL           string `yaml:"url"`
	AutoMigrate   bool   `yaml:"auto_migrate"`
	MigrationPath string `yaml:"migration_path"`
}

type ServiceConfig struct {
	Name               string `yaml:"name"`
	GRPCAddr           string `yaml:"grpc_addr"`
	HTTPAddr           string `yaml:"http_addr"`
	InternalAuthSecret string `yaml:"internal_auth_secret"`
	GRPCTLS            bool   `yaml:"grpc_tls"`
	GRPCCertFile       string `yaml:"grpc_cert_file"`
	GRPCKeyFile        string `yaml:"grpc_key_file"`
	GRPCClientCAFile   string `yaml:"grpc_client_ca_file"`
}

type MeilisearchConfig struct {
	Host             string        `yaml:"host"`
	APIKey           string        `yaml:"api_key"`
	Index            string        `yaml:"index"`
	TaskPollInterval time.Duration `yaml:"task_poll_interval"`
	TaskTimeout      time.Duration `yaml:"task_timeout"`
	RetiredIndexes   []string      `yaml:"retired_indexes"`
}

type SearchConfig struct {
	DefaultLimit int `yaml:"default_limit"`
	MaxLimit     int `yaml:"max_limit"`
}

type EmbeddingConfig struct {
	Endpoint   string        `yaml:"endpoint"`
	Model      string        `yaml:"model"`
	Dimensions int           `yaml:"dimensions"`
	Timeout    time.Duration `yaml:"timeout"`
}

type EnrichmentConfig struct {
	Transliteration bool   `yaml:"transliteration"`
	Morphology      bool   `yaml:"morphology"`
	SynonymsFile    string `yaml:"synonyms_file"`
}

type IndexerConfig struct {
	Shards        int           `yaml:"shards"`
	QueueSize     int           `yaml:"queue_size"`
	FlushInterval time.Duration `yaml:"flush_interval"`
	MaxRetries    int64         `yaml:"max_retries"`
	LeaseKey      string        `yaml:"lease_key"`
	LeaseDuration time.Duration `yaml:"lease_duration"`
	LeaseRenew    time.Duration `yaml:"lease_renew"`
}

type CollectionConfig struct {
	Name            string             `yaml:"name"`
	Index           string             `yaml:"index"`
	RevisionPrefix  string             `yaml:"revision_prefix"`
	Searchable      []string           `yaml:"searchable_fields"`
	Filterable      []string           `yaml:"filterable_fields"`
	Sortable        []string           `yaml:"sortable_fields"`
	Returnable      []string           `yaml:"returnable_fields"`
	Semantic        []string           `yaml:"semantic_fields"`
	SemanticWeights map[string]float64 `yaml:"semantic_weights"`
	Embedder        string             `yaml:"embedder"`
}

type APIKey struct {
	Name        string   `yaml:"name"`
	Value       string   `yaml:"value"`
	Scopes      []string `yaml:"scopes"`
	Collections []string `yaml:"collections"`
}

func Defaults() Config {
	return Config{
		Service: ServiceConfig{Name: "search", GRPCAddr: ":9090", HTTPAddr: ":8080"},
		Database: DatabaseConfig{
			AutoMigrate:   true,
			MigrationPath: "file://migrations",
		},
		Meilisearch: MeilisearchConfig{
			Host:             "http://localhost:7700",
			Index:            "global_search",
			TaskPollInterval: 100 * time.Millisecond,
			TaskTimeout:      30 * time.Second,
		},
		Search:     SearchConfig{DefaultLimit: 20, MaxLimit: 100},
		Embedding:  EmbeddingConfig{Model: "intfloat/multilingual-e5-small", Dimensions: 384, Timeout: 8 * time.Second},
		Enrichment: EnrichmentConfig{Transliteration: true, Morphology: true, SynonymsFile: "config/synonyms.yaml"},
		Indexer:    IndexerConfig{Shards: 8, QueueSize: 1000, FlushInterval: 500 * time.Millisecond, MaxRetries: 5, LeaseKey: "search:indexer", LeaseDuration: 15 * time.Second, LeaseRenew: 5 * time.Second},
		Collections: []CollectionConfig{
			{Name: "default", Index: "default", RevisionPrefix: "search:revision:", Searchable: []string{"*"}, Returnable: []string{"*"}},
		},
	}
}

func (c Config) Collection(name string) (CollectionConfig, bool) {
	for _, e := range c.Collections {
		if e.Name == name {
			return e, true
		}
	}
	return CollectionConfig{}, false
}

func Load(path string) (Config, error) {
	cfg := Defaults()
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read config: %w", err)
		}
		if err := yaml.Unmarshal([]byte(os.ExpandEnv(string(data))), &cfg); err != nil {
			return Config{}, fmt.Errorf("decode config: %w", err)
		}
	}
	if err := applyEnv(&cfg); err != nil {
		return Config{}, err
	}
	for _, target := range []*string{&cfg.Service.InternalAuthSecret, &cfg.Database.URL, &cfg.Meilisearch.APIKey} {
		if strings.HasPrefix(*target, "file:") {
			data, err := os.ReadFile(strings.TrimPrefix(*target, "file:"))
			if err != nil {
				return Config{}, fmt.Errorf("read secret: %w", err)
			}
			*target = strings.TrimSpace(string(data))
		}
	}
	for i := range cfg.APIKeys {
		if cfg.APIKeys[i].Value == "" {
			continue
		}
		target := &cfg.APIKeys[i].Value
		if strings.HasPrefix(*target, "file:") {
			data, err := os.ReadFile(strings.TrimPrefix(*target, "file:"))
			if err != nil {
				return Config{}, fmt.Errorf("read API key: %w", err)
			}
			*target = strings.TrimSpace(string(data))
		}
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if c.Service.HTTPAddr == "" {
		return errors.New("service http_addr is required")
	}
	if c.Service.GRPCAddr == "" {
		return errors.New("service grpc_addr is required")
	}
	if c.Service.GRPCTLS && (c.Service.GRPCCertFile == "" || c.Service.GRPCKeyFile == "" || c.Service.GRPCClientCAFile == "") {
		return errors.New("grpc tls requires cert, key and client ca files")
	}
	if c.Service.InternalAuthSecret == "" {
		return errors.New("service internal_auth_secret is required")
	}
	if c.Database.URL == "" {
		return errors.New("database url is required")
	}
	if c.Indexer.LeaseKey == "" || c.Indexer.LeaseRenew <= 0 || c.Indexer.LeaseDuration <= c.Indexer.LeaseRenew {
		return errors.New("indexer lease_key is required and lease_duration must be greater than lease_renew")
	}
	if c.Meilisearch.Host == "" {
		return errors.New("meilisearch host is required")
	}
	if c.Search.DefaultLimit <= 0 || c.Search.MaxLimit < c.Search.DefaultLimit {
		return errors.New("search limits are invalid")
	}
	if c.Embedding.Endpoint != "" && (c.Embedding.Dimensions <= 0 || c.Embedding.Timeout <= 0 || c.Embedding.Model == "") {
		return errors.New("embedding model, dimensions and timeout are required when endpoint is configured")
	}
	if c.Indexer.Shards <= 0 || c.Indexer.QueueSize <= 0 || c.Indexer.FlushInterval <= 0 || c.Indexer.MaxRetries <= 0 {
		return errors.New("indexer values must be greater than zero")
	}
	if len(c.Collections) == 0 {
		return errors.New("at least one collection is required")
	}
	if len(c.APIKeys) == 0 && c.Service.InternalAuthSecret == "" {
		return errors.New("at least one api key or service.internal_auth_secret is required")
	}
	seen := map[string]struct{}{}
	for i, collection := range c.Collections {
		if collection.Name == "" || collection.Index == "" {
			return fmt.Errorf("collections[%d].name and index are required", i)
		}
		if _, ok := seen[collection.Name]; ok {
			return fmt.Errorf("duplicate collection %q", collection.Name)
		}
		seen[collection.Name] = struct{}{}
	}
	return nil
}

func applyEnv(cfg *Config) error {
	stringVars := map[string]*string{
		"SEARCH_SERVICE_NAME":             &cfg.Service.Name,
		"SEARCH_SERVICE_GRPC_ADDR":        &cfg.Service.GRPCAddr,
		"SEARCH_SERVICE_HTTP_ADDR":        &cfg.Service.HTTPAddr,
		"SEARCH_INTERNAL_AUTH_SECRET":     &cfg.Service.InternalAuthSecret,
		"SEARCH_DATABASE_URL":             &cfg.Database.URL,
		"SEARCH_DATABASE_MIGRATION_PATH":  &cfg.Database.MigrationPath,
		"SEARCH_GRPC_CERT_FILE":           &cfg.Service.GRPCCertFile,
		"SEARCH_GRPC_KEY_FILE":            &cfg.Service.GRPCKeyFile,
		"SEARCH_GRPC_CLIENT_CA_FILE":      &cfg.Service.GRPCClientCAFile,
		"SEARCH_INDEXER_LEASE_KEY":        &cfg.Indexer.LeaseKey,
		"SEARCH_MEILISEARCH_HOST":         &cfg.Meilisearch.Host,
		"SEARCH_MEILISEARCH_API_KEY":      &cfg.Meilisearch.APIKey,
		"SEARCH_MEILISEARCH_INDEX":        &cfg.Meilisearch.Index,
		"SEARCH_ENRICHMENT_SYNONYMS_FILE": &cfg.Enrichment.SynonymsFile,
		"SEARCH_EMBEDDING_ENDPOINT":       &cfg.Embedding.Endpoint,
		"SEARCH_EMBEDDING_MODEL":          &cfg.Embedding.Model,
	}
	for name, target := range stringVars {
		if value, ok := os.LookupEnv(name); ok {
			*target = value
		}
	}

	intVars := map[string]*int{
		"SEARCH_INDEXER_SHARDS":       &cfg.Indexer.Shards,
		"SEARCH_INDEXER_QUEUE_SIZE":   &cfg.Indexer.QueueSize,
		"SEARCH_SEARCH_DEFAULT_LIMIT": &cfg.Search.DefaultLimit,
		"SEARCH_SEARCH_MAX_LIMIT":     &cfg.Search.MaxLimit,
		"SEARCH_EMBEDDING_DIMENSIONS": &cfg.Embedding.Dimensions,
	}
	for name, target := range intVars {
		if err := setInt(name, target); err != nil {
			return err
		}
	}
	int64Vars := map[string]*int64{
		"SEARCH_INDEXER_MAX_RETRIES": &cfg.Indexer.MaxRetries,
	}
	for name, target := range int64Vars {
		if err := setInt64(name, target); err != nil {
			return err
		}
	}
	durationVars := map[string]*time.Duration{
		"SEARCH_INDEXER_LEASE_DURATION":         &cfg.Indexer.LeaseDuration,
		"SEARCH_INDEXER_LEASE_RENEW":            &cfg.Indexer.LeaseRenew,
		"SEARCH_MEILISEARCH_TASK_POLL_INTERVAL": &cfg.Meilisearch.TaskPollInterval,
		"SEARCH_MEILISEARCH_TASK_TIMEOUT":       &cfg.Meilisearch.TaskTimeout,
		"SEARCH_INDEXER_FLUSH_INTERVAL":         &cfg.Indexer.FlushInterval,
		"SEARCH_EMBEDDING_TIMEOUT":              &cfg.Embedding.Timeout,
	}
	for name, target := range durationVars {
		if err := setDuration(name, target); err != nil {
			return err
		}
	}
	boolVars := map[string]*bool{
		"SEARCH_ENRICHMENT_TRANSLITERATION": &cfg.Enrichment.Transliteration,
		"SEARCH_ENRICHMENT_MORPHOLOGY":      &cfg.Enrichment.Morphology,
		"SEARCH_GRPC_TLS":                   &cfg.Service.GRPCTLS,
		"SEARCH_DATABASE_AUTO_MIGRATE":      &cfg.Database.AutoMigrate,
	}
	for name, target := range boolVars {
		if err := setBool(name, target); err != nil {
			return err
		}
	}
	return nil
}

func setInt(name string, target *int) error {
	value, ok := os.LookupEnv(name)
	if !ok {
		return nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("%s must be an integer", name)
	}
	*target = parsed
	return nil
}

func setInt64(name string, target *int64) error {
	value, ok := os.LookupEnv(name)
	if !ok {
		return nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fmt.Errorf("%s must be an integer", name)
	}
	*target = parsed
	return nil
}

func setDuration(name string, target *time.Duration) error {
	value, ok := os.LookupEnv(name)
	if !ok {
		return nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("%s must be a duration", name)
	}
	*target = parsed
	return nil
}

func setBool(name string, target *bool) error {
	value, ok := os.LookupEnv(name)
	if !ok {
		return nil
	}
	parsed, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return fmt.Errorf("%s must be a boolean", name)
	}
	*target = parsed
	return nil
}
