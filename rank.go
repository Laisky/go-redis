package redis

import (
	"context"
	"fmt"

	"github.com/Laisky/zap"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

// Rank Use the ordered set of redis to implement dynamic ranking.
//
// When a user's score changes, a snapshot is taken of the user's
// current information and status to get a snapshot ID,
// and then the user's key, score, and snapshot ID are stored
// in the ordered set. member is the key, score is the (score * 100000 + snapshot ID).
type Rank interface {
	// Set set/update someone's score and snapshotID
	Set(ctx context.Context, key string, score, snapshotID int) error
	// Del delete a key
	Del(ctx context.Context, key string) error
	// List get top N scores
	List(ctx context.Context, limit uint) ([]redis.Z, error)
	// Get get someone's score and snapshotID
	Get(ctx context.Context, key string) (snapshotID int, err error)
}

type rank struct {
	rdb           *Utils
	dataKey       string
	maxSnapshotID int
}

// NewRank create a new rank
func (u *Utils) NewRank(name string, maxSnapshotID int) (Rank, error) {
	if maxSnapshotID <= 0 {
		return nil, errors.Errorf("maxSnapshotID must greater than 0")
	}

	if maxSnapshotID%10 != 0 {
		return nil, errors.Errorf("maxSnapshotID must be multiple of 10")
	}

	return &rank{
		rdb:           u,
		dataKey:       fmt.Sprintf(defaultKeyRankData, name),
		maxSnapshotID: maxSnapshotID,
	}, nil
}

// Set set/update someone's score and snapshotID
func (r *rank) Set(ctx context.Context, key string, score, snapshotID int) error {
	if key == "" {
		return errors.Errorf("key must not be empty")
	}
	if snapshotID >= r.maxSnapshotID {
		return errors.Errorf("ver's length must shorter than %d", r.maxSnapshotID)
	}

	v := score*r.maxSnapshotID + snapshotID
	if err := r.rdb.ZAdd(ctx, r.dataKey, &redis.Z{Score: float64(v), Member: key}).Err(); err != nil {
		return errors.Wrapf(err, "zadd %s.%s", r.dataKey, key)
	}

	logger.Debug("set rank", zap.String("key", r.dataKey),
		zap.String("member", key),
		zap.Int("score", score),
		zap.Int("ver", snapshotID))
	return nil
}

// Del delete a key
func (r *rank) Del(ctx context.Context, key string) error {
	return r.rdb.ZRem(ctx, r.dataKey, key).Err()
}

// List get top N scores
func (r *rank) List(ctx context.Context, limit uint) ([]redis.Z, error) {
	return r.rdb.ZRevRangeWithScores(ctx, r.dataKey, 0, int64(limit-1)).Result()
}

// Get get someone's score and snapshotID
func (r *rank) Get(ctx context.Context, key string) (snapshotID int, err error) {
	ret, err := r.rdb.ZScore(ctx, r.dataKey, key).Result()
	if err != nil {
		return 0, errors.Wrapf(err, "zrank %s.%s", r.dataKey, key)
	}

	return int(ret) % r.maxSnapshotID, nil
}
