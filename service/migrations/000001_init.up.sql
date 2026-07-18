-- Generated from the final pre-production schema for search.
-- Data is intentionally reset; historical compatibility DDL is forbidden here.

CREATE SCHEMA IF NOT EXISTS search;

CREATE TABLE search.applied_revisions (
    collection text NOT NULL,
    document_id text NOT NULL,
    revision bigint NOT NULL,
    payload_digest text NOT NULL,
    payload_json jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT applied_revisions_collection_not_null NOT NULL collection,
    CONSTRAINT applied_revisions_document_id_not_null NOT NULL document_id,
    CONSTRAINT applied_revisions_payload_digest_not_null NOT NULL payload_digest,
    CONSTRAINT applied_revisions_payload_json_not_null NOT NULL payload_json,
    CONSTRAINT applied_revisions_pkey PRIMARY KEY (collection, document_id),
    CONSTRAINT applied_revisions_revision_not_null NOT NULL revision,
    CONSTRAINT applied_revisions_updated_at_not_null NOT NULL updated_at
);

CREATE TABLE search.dead_letters (
    id uuid NOT NULL,
    source_service text,
    event_id text,
    collection text,
    document_id text,
    revision bigint,
    payload_json jsonb,
    attempts bigint DEFAULT 0 NOT NULL,
    reason text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT dead_letters_attempts_not_null NOT NULL attempts,
    CONSTRAINT dead_letters_created_at_not_null NOT NULL created_at,
    CONSTRAINT dead_letters_id_not_null NOT NULL id,
    CONSTRAINT dead_letters_pkey PRIMARY KEY (id),
    CONSTRAINT dead_letters_reason_not_null NOT NULL reason
);

CREATE TABLE search.embedding_cache (
    content_hash text NOT NULL,
    model_version text NOT NULL,
    index_name text,
    embedder text,
    semantic_text text,
    dimensions integer NOT NULL,
    vector_json jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT embedding_cache_content_hash_not_null NOT NULL content_hash,
    CONSTRAINT embedding_cache_created_at_not_null NOT NULL created_at,
    CONSTRAINT embedding_cache_dimensions_not_null NOT NULL dimensions,
    CONSTRAINT embedding_cache_model_version_not_null NOT NULL model_version,
    CONSTRAINT embedding_cache_pkey PRIMARY KEY (content_hash, model_version),
    CONSTRAINT embedding_cache_vector_json_not_null NOT NULL vector_json
);

CREATE TABLE search.embedding_tasks (
    id uuid NOT NULL,
    collection text NOT NULL,
    document_id text NOT NULL,
    revision bigint NOT NULL,
    content_hash text NOT NULL,
    model_version text NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    next_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    leased_until timestamp with time zone,
    last_error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    index_name text NOT NULL,
    embedder text NOT NULL,
    semantic_text text NOT NULL,
    CONSTRAINT embedding_tasks_attempts_not_null NOT NULL attempts,
    CONSTRAINT embedding_tasks_collection_document_id_revision_model_versi_key UNIQUE (collection, document_id, revision, model_version),
    CONSTRAINT embedding_tasks_collection_not_null NOT NULL collection,
    CONSTRAINT embedding_tasks_content_hash_not_null NOT NULL content_hash,
    CONSTRAINT embedding_tasks_created_at_not_null NOT NULL created_at,
    CONSTRAINT embedding_tasks_document_id_not_null NOT NULL document_id,
    CONSTRAINT embedding_tasks_embedder_not_null NOT NULL embedder,
    CONSTRAINT embedding_tasks_id_not_null NOT NULL id,
    CONSTRAINT embedding_tasks_index_name_not_null NOT NULL index_name,
    CONSTRAINT embedding_tasks_model_version_not_null NOT NULL model_version,
    CONSTRAINT embedding_tasks_next_attempt_at_not_null NOT NULL next_attempt_at,
    CONSTRAINT embedding_tasks_pkey PRIMARY KEY (id),
    CONSTRAINT embedding_tasks_revision_not_null NOT NULL revision,
    CONSTRAINT embedding_tasks_semantic_text_not_null NOT NULL semantic_text,
    CONSTRAINT embedding_tasks_status_check CHECK (status = ANY (ARRAY['pending'::text, 'leased'::text, 'retry'::text, 'indexed'::text, 'dead'::text])),
    CONSTRAINT embedding_tasks_status_not_null NOT NULL status,
    CONSTRAINT embedding_tasks_updated_at_not_null NOT NULL updated_at
);

CREATE TABLE search.inbox_events (
    id uuid NOT NULL,
    source_service text NOT NULL,
    event_id text NOT NULL,
    operation text NOT NULL,
    collection text NOT NULL,
    document_id text NOT NULL,
    revision bigint NOT NULL,
    payload_json jsonb NOT NULL,
    payload_digest text NOT NULL,
    occurred_at timestamp with time zone,
    received_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT inbox_events_collection_not_null NOT NULL collection,
    CONSTRAINT inbox_events_document_id_not_null NOT NULL document_id,
    CONSTRAINT inbox_events_event_id_not_null NOT NULL event_id,
    CONSTRAINT inbox_events_id_not_null NOT NULL id,
    CONSTRAINT inbox_events_operation_not_null NOT NULL operation,
    CONSTRAINT inbox_events_payload_digest_not_null NOT NULL payload_digest,
    CONSTRAINT inbox_events_payload_json_not_null NOT NULL payload_json,
    CONSTRAINT inbox_events_pkey PRIMARY KEY (id),
    CONSTRAINT inbox_events_received_at_not_null NOT NULL received_at,
    CONSTRAINT inbox_events_revision_not_null NOT NULL revision,
    CONSTRAINT inbox_events_source_service_event_id_key UNIQUE (source_service, event_id),
    CONSTRAINT inbox_events_source_service_not_null NOT NULL source_service
);

CREATE TABLE search.indexing_tasks (
    id uuid NOT NULL,
    inbox_event_id uuid NOT NULL,
    collection text NOT NULL,
    document_id text NOT NULL,
    operation text NOT NULL,
    revision bigint NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    attempts bigint DEFAULT 0 NOT NULL,
    next_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    leased_until timestamp with time zone,
    last_error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT indexing_tasks_attempts_not_null NOT NULL attempts,
    CONSTRAINT indexing_tasks_collection_not_null NOT NULL collection,
    CONSTRAINT indexing_tasks_created_at_not_null NOT NULL created_at,
    CONSTRAINT indexing_tasks_document_id_not_null NOT NULL document_id,
    CONSTRAINT indexing_tasks_id_not_null NOT NULL id,
    CONSTRAINT indexing_tasks_inbox_event_id_fkey FOREIGN KEY (inbox_event_id) REFERENCES search.inbox_events(id) ON DELETE CASCADE,
    CONSTRAINT indexing_tasks_inbox_event_id_key UNIQUE (inbox_event_id),
    CONSTRAINT indexing_tasks_inbox_event_id_not_null NOT NULL inbox_event_id,
    CONSTRAINT indexing_tasks_next_attempt_at_not_null NOT NULL next_attempt_at,
    CONSTRAINT indexing_tasks_operation_check CHECK (operation = ANY (ARRAY['upsert'::text, 'delete'::text])),
    CONSTRAINT indexing_tasks_operation_not_null NOT NULL operation,
    CONSTRAINT indexing_tasks_pkey PRIMARY KEY (id),
    CONSTRAINT indexing_tasks_revision_not_null NOT NULL revision,
    CONSTRAINT indexing_tasks_status_check CHECK (status = ANY (ARRAY['pending'::text, 'leased'::text, 'retry'::text, 'transforming'::text, 'submitted_to_meili'::text, 'indexed'::text, 'dead'::text])),
    CONSTRAINT indexing_tasks_status_not_null NOT NULL status,
    CONSTRAINT indexing_tasks_updated_at_not_null NOT NULL updated_at
);

CREATE TABLE search.worker_leases (
    lease_key text PRIMARY KEY,
    holder_id text NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_embedding_tasks_ready ON search.embedding_tasks USING btree (status, next_attempt_at, created_at) WHERE (status = ANY (ARRAY['pending'::text, 'retry'::text]));

CREATE INDEX idx_indexing_tasks_ready ON search.indexing_tasks USING btree (status, next_attempt_at, created_at) WHERE (status = ANY (ARRAY['pending'::text, 'retry'::text]));
