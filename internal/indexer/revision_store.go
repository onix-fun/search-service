package indexer

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"

	"github.com/onix-fun/search-service/internal/model"
)

var saveRevisionScript = redis.NewScript(`
local current = redis.call("HGET", KEYS[1], "revision")
if current then
  if tonumber(current) > tonumber(ARGV[1]) then
    return 0
  end
  if tonumber(current) == tonumber(ARGV[1]) then
    if redis.call("HGET", KEYS[1], "digest") == ARGV[2] then
      return 2
    end
    return -1
  end
end
redis.call("HSET", KEYS[1], "revision", ARGV[1], "digest", ARGV[2], "payload", ARGV[3])
return 1
`)

type RevisionDecision int

const (
	RevisionNew RevisionDecision = iota
	RevisionStale
	RevisionDuplicate
	RevisionConflict
)

type AppliedState struct {
	Revision int64
	Digest   string
	Payload  string
}

type RevisionStore struct {
	client *redis.Client
	prefix string
}

func NewRevisionStore(client *redis.Client, prefix string) *RevisionStore {
	return &RevisionStore{client: client, prefix: prefix}
}

func (s *RevisionStore) Check(ctx context.Context, event model.IndexEvent) (RevisionDecision, error) {
	state, ok, err := s.Get(ctx, event.DocumentID)
	if err != nil || !ok {
		return RevisionNew, err
	}
	digest, err := event.Digest()
	if err != nil {
		return RevisionNew, err
	}
	switch {
	case state.Revision > event.Revision:
		return RevisionStale, nil
	case state.Revision < event.Revision:
		return RevisionNew, nil
	case state.Digest == digest:
		return RevisionDuplicate, nil
	default:
		return RevisionConflict, nil
	}
}

func (s *RevisionStore) Save(ctx context.Context, event model.IndexEvent) (RevisionDecision, error) {
	payload, err := event.CanonicalPayload()
	if err != nil {
		return RevisionNew, err
	}
	digest, err := event.Digest()
	if err != nil {
		return RevisionNew, err
	}
	result, err := saveRevisionScript.Run(ctx, s.client, []string{s.key(event.DocumentID)}, event.Revision, digest, payload).Int()
	if err != nil {
		return RevisionNew, fmt.Errorf("save revision: %w", err)
	}
	switch result {
	case 1:
		return RevisionNew, nil
	case 2:
		return RevisionDuplicate, nil
	case 0:
		return RevisionStale, nil
	default:
		return RevisionConflict, nil
	}
}

func (s *RevisionStore) Get(ctx context.Context, uuid string) (AppliedState, bool, error) {
	values, err := s.client.HGetAll(ctx, s.key(uuid)).Result()
	if err != nil {
		return AppliedState{}, false, fmt.Errorf("get revision: %w", err)
	}
	if len(values) == 0 {
		return AppliedState{}, false, nil
	}
	revision, err := strconv.ParseInt(values["revision"], 10, 64)
	if err != nil {
		return AppliedState{}, false, fmt.Errorf("decode revision for %s: %w", uuid, err)
	}
	return AppliedState{Revision: revision, Digest: values["digest"], Payload: values["payload"]}, true, nil
}

func (s *RevisionStore) key(uuid string) string {
	return s.prefix + uuid
}
