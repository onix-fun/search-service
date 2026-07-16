package indexer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

var refreshLeaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`)

var releaseLeaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)

type Lease struct {
	client   *redis.Client
	key      string
	duration time.Duration
}

func NewLease(client *redis.Client, key string, duration time.Duration) *Lease {
	return &Lease{client: client, key: key, duration: duration}
}

func NewLeaseToken() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func (l *Lease) Acquire(ctx context.Context, token string) (bool, error) {
	return l.client.SetNX(ctx, l.key, token, l.duration).Result()
}

func (l *Lease) Refresh(ctx context.Context, token string) (bool, error) {
	result, err := refreshLeaseScript.Run(ctx, l.client, []string{l.key}, token, l.duration.Milliseconds()).Int64()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (l *Lease) Release(ctx context.Context, token string) error {
	_, err := releaseLeaseScript.Run(ctx, l.client, []string{l.key}, token).Result()
	if errors.Is(err, redis.Nil) {
		return nil
	}
	return err
}
