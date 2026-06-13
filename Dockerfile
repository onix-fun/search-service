FROM golang:1.26.3-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/search-service ./cmd/search-service

FROM alpine:3.23

RUN apk add --no-cache ca-certificates
RUN adduser -D -u 10001 app
WORKDIR /app
COPY --from=build /out/search-service /usr/local/bin/search-service
COPY config ./config
USER app
ENTRYPOINT ["search-service"]
