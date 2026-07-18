package indexer_test

import (
	"testing"

	"github.com/onix-fun/search/service/internal/application/indexer"
)

func TestLeaseTokenIsRandomAndOpaque(t *testing.T) {
	first, err := indexer.NewLeaseToken()
	if err != nil {
		t.Fatal(err)
	}
	second, err := indexer.NewLeaseToken()
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 32 || len(second) != 32 {
		t.Fatalf("token lengths = %d and %d, want 32", len(first), len(second))
	}
	if first == second {
		t.Fatal("consecutive lease tokens must differ")
	}
}
