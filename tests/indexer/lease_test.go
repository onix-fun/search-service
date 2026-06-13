package indexer_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/company/search-service/internal/indexer"
)

func TestLeaseTokenOwnership(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	lease := indexer.NewLease(client, "leader", 15*time.Second)
	ctx := context.Background()

	ok, err := lease.Acquire(ctx, "first")
	if err != nil || !ok {
		t.Fatalf("Acquire(first) = %v, %v", ok, err)
	}
	ok, err = lease.Acquire(ctx, "second")
	if err != nil || ok {
		t.Fatalf("Acquire(second) = %v, %v", ok, err)
	}
	ok, err = lease.Refresh(ctx, "second")
	if err != nil || ok {
		t.Fatalf("Refresh(second) = %v, %v", ok, err)
	}
	if err := lease.Release(ctx, "second"); err != nil {
		t.Fatal(err)
	}
	if !server.Exists("leader") {
		t.Fatal("non-owner released lease")
	}
	if err := lease.Release(ctx, "first"); err != nil {
		t.Fatal(err)
	}
	if server.Exists("leader") {
		t.Fatal("owner did not release lease")
	}
}
