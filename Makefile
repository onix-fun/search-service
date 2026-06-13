PATH := /usr/local/go/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:$(HOME)/go/bin:$(PATH)

.PHONY: generate test race build docker-build compose-up migrate

generate:
	protoc --go_out=. --go_opt=module=github.com/onix-fun/search-service \
		--go-grpc_out=. --go-grpc_opt=module=github.com/onix-fun/search-service \
		api/proto/search/v1/search.proto

test:
	go test ./...

race:
	go test -race ./...

build:
	go build ./cmd/search-service

docker-build:
	docker build -t search-service:local .

compose-up:
	docker compose up --build -d redis meilisearch search-service

migrate:
	docker compose --profile tools run --rm migrate-index
