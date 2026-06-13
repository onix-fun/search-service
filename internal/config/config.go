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
	Redis       RedisConfig        `yaml:"redis"`
	RabbitMQ    RabbitMQConfig     `yaml:"rabbitmq"`
	Meilisearch MeilisearchConfig  `yaml:"meilisearch"`
	Search      SearchConfig       `yaml:"search"`
	Enrichment  EnrichmentConfig   `yaml:"enrichment"`
	Indexer     IndexerConfig      `yaml:"indexer"`
	Collections []CollectionConfig `yaml:"collections"`
	APIKeys     []APIKey           `yaml:"api_keys"`
}

type ServiceConfig struct {
	Name               string `yaml:"name"`
	GRPCAddr           string `yaml:"grpc_addr"`
	HTTPAddr           string `yaml:"http_addr"`
	InternalAuthSecret string `yaml:"internal_auth_secret"`
}

type RedisConfig struct {
	Addr          string        `yaml:"addr"`
	Password      string        `yaml:"password"`
	DB            int           `yaml:"db"`
	LeaseKey      string        `yaml:"lease_key"`
	LeaseDuration time.Duration `yaml:"lease_duration"`
	LeaseRenew    time.Duration `yaml:"lease_renew"`
}

type RabbitMQConfig struct {
	URL           string `yaml:"url"`
	DLQExchange   string `yaml:"dlq_exchange"`
	DLQQueue      string `yaml:"dlq_queue"`
	PrefetchCount int    `yaml:"prefetch_count"`
}

type MeilisearchConfig struct {
	Host             string        `yaml:"host"`
	APIKey           string        `yaml:"api_key"`
	Index            string        `yaml:"index"`
	TaskPollInterval time.Duration `yaml:"task_poll_interval"`
	TaskTimeout      time.Duration `yaml:"task_timeout"`
}

type SearchConfig struct {
	DefaultLimit int `yaml:"default_limit"`
	MaxLimit     int `yaml:"max_limit"`
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
}

type CollectionConfig struct {
	Name           string   `yaml:"name"`
	Index          string   `yaml:"index"`
	Exchange       string   `yaml:"exchange"`
	Queue          string   `yaml:"queue"`
	RoutingKey     string   `yaml:"routing_key"`
	RevisionPrefix string   `yaml:"revision_prefix"`
	Searchable     []string `yaml:"searchable_fields"`
	Filterable     []string `yaml:"filterable_fields"`
	Sortable       []string `yaml:"sortable_fields"`
	Returnable     []string `yaml:"returnable_fields"`
}

type APIKey struct {
	Name        string   `yaml:"name"`
	Value       string   `yaml:"value"`
	Scopes      []string `yaml:"scopes"`
	Collections []string `yaml:"collections"`
}

func Defaults() Config {
	return Config{
		Service: ServiceConfig{Name: "search-service", GRPCAddr: ":9090", HTTPAddr: ":8080"},
		Redis: RedisConfig{
			Addr:          "localhost:6379",
			LeaseKey:      "search:index:leader",
			LeaseDuration: 15 * time.Second,
			LeaseRenew:    5 * time.Second,
		},
		RabbitMQ: RabbitMQConfig{
			URL:           "amqp://guest:guest@localhost:5672/",
			DLQExchange:   "search.index.dlq",
			DLQQueue:      "search.index.dlq.queue",
			PrefetchCount: 100,
		},
		Meilisearch: MeilisearchConfig{
			Host:             "http://localhost:7700",
			Index:            "global_search",
			TaskPollInterval: 100 * time.Millisecond,
			TaskTimeout:      30 * time.Second,
		},
		Search:     SearchConfig{DefaultLimit: 20, MaxLimit: 100},
		Enrichment: EnrichmentConfig{Transliteration: true, Morphology: true, SynonymsFile: "config/synonyms.yaml"},
		Indexer:    IndexerConfig{Shards: 8, QueueSize: 1000, FlushInterval: 500 * time.Millisecond, MaxRetries: 5},
		Collections: []CollectionConfig{
			{Name: "default", Index: "default", Exchange: "search.index.events", Queue: "search.index.events.queue", RoutingKey: "search.index.events.queue", RevisionPrefix: "search:revision:", Searchable: []string{"*"}, Returnable: []string{"*"}},
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
	for _, target := range []*string{&cfg.Service.InternalAuthSecret, &cfg.Redis.Password, &cfg.RabbitMQ.URL, &cfg.Meilisearch.APIKey} {
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
	if c.Service.InternalAuthSecret == "" {
		return errors.New("service internal_auth_secret is required")
	}
	if c.Redis.Addr == "" {
		return errors.New("redis addr is required")
	}
	if c.Redis.LeaseRenew <= 0 || c.Redis.LeaseDuration <= c.Redis.LeaseRenew {
		return errors.New("redis lease_duration must be greater than lease_renew")
	}
	if c.RabbitMQ.URL == "" {
		return errors.New("rabbitmq url is required")
	}
	if c.Meilisearch.Host == "" {
		return errors.New("meilisearch host is required")
	}
	if c.Search.DefaultLimit <= 0 || c.Search.MaxLimit < c.Search.DefaultLimit {
		return errors.New("search limits are invalid")
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
		if collection.Exchange == "" || collection.Queue == "" || collection.RoutingKey == "" {
			return fmt.Errorf("collections[%d].exchange, queue and routing_key are required", i)
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
		"SEARCH_REDIS_ADDR":               &cfg.Redis.Addr,
		"SEARCH_REDIS_PASSWORD":           &cfg.Redis.Password,
		"SEARCH_REDIS_LEASE_KEY":          &cfg.Redis.LeaseKey,
		"SEARCH_RABBITMQ_URL":             &cfg.RabbitMQ.URL,
		"SEARCH_RABBITMQ_DLQ_EXCHANGE":    &cfg.RabbitMQ.DLQExchange,
		"SEARCH_RABBITMQ_DLQ_QUEUE":       &cfg.RabbitMQ.DLQQueue,
		"SEARCH_MEILISEARCH_HOST":         &cfg.Meilisearch.Host,
		"SEARCH_MEILISEARCH_API_KEY":      &cfg.Meilisearch.APIKey,
		"SEARCH_MEILISEARCH_INDEX":        &cfg.Meilisearch.Index,
		"SEARCH_ENRICHMENT_SYNONYMS_FILE": &cfg.Enrichment.SynonymsFile,
	}
	for name, target := range stringVars {
		if value, ok := os.LookupEnv(name); ok {
			*target = value
		}
	}

	intVars := map[string]*int{
		"SEARCH_REDIS_DB":                &cfg.Redis.DB,
		"SEARCH_INDEXER_SHARDS":          &cfg.Indexer.Shards,
		"SEARCH_INDEXER_QUEUE_SIZE":      &cfg.Indexer.QueueSize,
		"SEARCH_SEARCH_DEFAULT_LIMIT":    &cfg.Search.DefaultLimit,
		"SEARCH_SEARCH_MAX_LIMIT":        &cfg.Search.MaxLimit,
		"SEARCH_RABBITMQ_PREFETCH_COUNT": &cfg.RabbitMQ.PrefetchCount,
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
		"SEARCH_REDIS_LEASE_DURATION":           &cfg.Redis.LeaseDuration,
		"SEARCH_REDIS_LEASE_RENEW":              &cfg.Redis.LeaseRenew,
		"SEARCH_MEILISEARCH_TASK_POLL_INTERVAL": &cfg.Meilisearch.TaskPollInterval,
		"SEARCH_MEILISEARCH_TASK_TIMEOUT":       &cfg.Meilisearch.TaskTimeout,
		"SEARCH_INDEXER_FLUSH_INTERVAL":         &cfg.Indexer.FlushInterval,
	}
	for name, target := range durationVars {
		if err := setDuration(name, target); err != nil {
			return err
		}
	}
	boolVars := map[string]*bool{
		"SEARCH_ENRICHMENT_TRANSLITERATION": &cfg.Enrichment.Transliteration,
		"SEARCH_ENRICHMENT_MORPHOLOGY":      &cfg.Enrichment.Morphology,
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
