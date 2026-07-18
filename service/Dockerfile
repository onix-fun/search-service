FROM golang:1.26.3-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    for attempt in 1 2 3 4 5; do \
        go mod download && exit 0; \
        [ "$attempt" -eq 5 ] && exit 1; \
        sleep $((attempt * 3)); \
    done
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/search ./cmd/search

FROM alpine:3.23

RUN apk add --no-cache ca-certificates
RUN adduser -D -u 10001 app
WORKDIR /app
COPY --from=build /out/search /usr/local/bin/search
COPY config ./config
COPY migrations ./migrations
USER app
EXPOSE 8080 9094
ENTRYPOINT ["search"]
