package indexer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Lease coordinates the singleton indexer through PostgreSQL. The lease row is
// durable, but an expired holder can always be replaced after a crash.
type Lease struct {
	pool     *pgxpool.Pool
	key      string
	duration time.Duration
}

func NewLease(pool *pgxpool.Pool, key string, duration time.Duration) *Lease {
	return &Lease{pool: pool, key: key, duration: duration}
}

func NewLeaseToken() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func (l *Lease) Acquire(ctx context.Context, token string) (bool, error) {
	var acquired bool
	err := l.pool.QueryRow(ctx, `
		INSERT INTO worker_leases (lease_key, holder_id, expires_at, updated_at)
		VALUES ($1, $2, NOW() + ($3 * INTERVAL '1 millisecond'), NOW())
		ON CONFLICT (lease_key) DO UPDATE
		SET holder_id = EXCLUDED.holder_id,
		    expires_at = EXCLUDED.expires_at,
		    updated_at = NOW()
		WHERE worker_leases.expires_at <= NOW()
		RETURNING true
	`, l.key, token, l.duration.Milliseconds()).Scan(&acquired)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return acquired, err
}

func (l *Lease) Refresh(ctx context.Context, token string) (bool, error) {
	tag, err := l.pool.Exec(ctx, `
		UPDATE worker_leases
		SET expires_at = NOW() + ($3 * INTERVAL '1 millisecond'), updated_at = NOW()
		WHERE lease_key = $1 AND holder_id = $2 AND expires_at > NOW()
	`, l.key, token, l.duration.Milliseconds())
	return err == nil && tag.RowsAffected() == 1, err
}

func (l *Lease) Release(ctx context.Context, token string) error {
	_, err := l.pool.Exec(ctx, `DELETE FROM worker_leases WHERE lease_key = $1 AND holder_id = $2`, l.key, token)
	return err
}
