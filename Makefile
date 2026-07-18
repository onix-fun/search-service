PATH := /usr/local/go/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:$(HOME)/go/bin:$(PATH)

.PHONY: generate test race build docker-build compose-up migrate

generate:
	cd service && protoc -I ../api/proto \
		--go_out=. --go_opt=module=github.com/onix-fun/search/service \
		--go-grpc_out=. --go-grpc_opt=module=github.com/onix-fun/search/service \
		../api/proto/onix/search/search.proto

test:
	cd service && go test ./...

race:
	cd service && go test -race ./...

build:
	cd service && go build ./cmd/search

docker-build:
	docker build -t onix-search:local service

compose-up:
	cd service && docker compose up --build -d postgres meilisearch search

migrate:
	cd service && docker compose --profile tools run --rm migrate-index
