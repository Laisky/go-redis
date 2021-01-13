package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	gutils "github.com/Laisky/go-utils"
	"github.com/Laisky/zap"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

const (
	defaultSemaphoreHeartbeatInterval = time.Second
	defaultSemaphoreTTL               = 5 * time.Second
)

// Semaphore distributed fair semaphore
//
// Redis keys:
//
//   `/rtils/sync/sema/<lock_id>/`
//
//   * cids/: client_id -> ts, all clients
//   * owners/: client_id -> counter, all clients acquired lock
//   * counter:
//
// you can specified client_id by `WithSemaphoreClientID`.
// will auto generate client_id by UUID4 if not set.
//
// Implementations:
//
//   1. generate client id(cid)
//   2. delete all expired clients in `cids`,
//      by `ZREMRANGEBYSCORE cids -inf <now - ttl>`
//   3. delete all expired clients in `owners`,
//      by `ZINTERSTORE owners 2 owners cids WEIGHTS 1 0`
//   4. increment semaphore's counter
//   5. add cid:counter to `owners`, get rank (smaller is better).
// 	   5-1. delete from `owners` if the rank is over the limit of semaphore
//   6. add cid:timestamp to `cids`
type Semaphore struct {
	semaOption
	rdb    *Utils
	logger *gutils.LoggerType
	cancel context.CancelFunc

	// limit of semaphore
	limit int

	// cids name lock name
	//   cliend_id -> timestamp
	cids,
	// owners clients acquired lock
	//   client_id -> counter
	owners,
	// counter current count number of the lock
	counter string
}

type semaOption struct {
	mutexOption
}

// SemaphoreOptionFunc options for semaphore
type SemaphoreOptionFunc func(*Semaphore) error

// WithSemaphoreRefreshInterval set lock refreshing interval
func WithSemaphoreRefreshInterval(interval time.Duration) SemaphoreOptionFunc {
	return func(mu *Semaphore) error {
		mu.heartbeatInterval = interval
		return nil
	}
}

// WithSemaphoreTTL set lock's expiration
func WithSemaphoreTTL(ttl time.Duration) SemaphoreOptionFunc {
	return func(mu *Semaphore) error {
		mu.ttl = ttl
		return nil
	}
}

// WithSemaphoreLogger set lock's expiration
func WithSemaphoreLogger(logger *gutils.LoggerType) SemaphoreOptionFunc {
	return func(mu *Semaphore) error {
		mu.logger = logger
		return nil
	}
}

// WithSemaphoreClientID set client id
func WithSemaphoreClientID(clientID string) SemaphoreOptionFunc {
	return func(mu *Semaphore) error {
		mu.clientID = clientID
		return nil
	}
}

// NewSemaphore new semaphore
func (u *Utils) NewSemaphore(lockName string, limit int, opts ...SemaphoreOptionFunc) (sema *Semaphore, err error) {
	sema = &Semaphore{
		limit:   limit,
		rdb:     u,
		logger:  u.logger,
		cids:    fmt.Sprintf(DefaultKeySyncSemaphoreLocks, lockName),
		owners:  fmt.Sprintf(DefaultKeySyncSemaphoreOwners, lockName),
		counter: fmt.Sprintf(DefaultKeySyncSemaphoreCounter, lockName),
		semaOption: semaOption{mutexOption{
			heartbeatInterval: defaultSemaphoreHeartbeatInterval,
			ttl:               defaultSemaphoreTTL,
		}},
	}

	for _, optf := range opts {
		if err = optf(sema); err != nil {
			return nil, err
		}
	}

	if sema.clientID == "" {
		sema.clientID = uuid.New().String()
	}

	return
}

// Lock acquire lock
//
// if succeed acquired lock,
//   * locked == true
//   * lockCtx is context of lock, this context will be set to done when lock is expired
func (s *Semaphore) Lock(ctx context.Context) (locked bool, lockCtx context.Context, err error) {
	var acquiredLock bool
	if _, err = s.rdb.Pipelined(ctx, func(pp redis.Pipeliner) (err error) {
		expiredAt := strconv.Itoa(int(gutils.Clock.GetUTCNow().Add(-s.ttl).Unix()))
		pp.ZRemRangeByScore(ctx, s.cids, "-inf", expiredAt)
		pp.ZInterStore(ctx,
			s.owners,
			&redis.ZStore{
				Keys:      []string{s.owners, s.cids},
				Weights:   []float64{1, 0},
				Aggregate: "MAX",
			},
		)
		pp.Incr(ctx, s.counter)

		var cnt int64
		if rets, err := pp.Exec(ctx); err != nil {
			return errors.WithStack(err)
		} else if cnt, err = rets[len(rets)-1].(*redis.IntCmd).Result(); err != nil {
			return errors.Wrapf(err, "get counter `%s`", rets[len(rets)-1].String())
		}

		pp.ZAdd(ctx, s.owners, &redis.Z{
			Member: s.clientID,
			Score:  float64(cnt),
		})
		pp.ZAdd(ctx, s.cids, &redis.Z{
			Member: s.clientID,
			Score:  float64(float64(gutils.Clock.GetUTCNow().Unix())),
		})
		pp.ZRank(ctx, s.owners, s.clientID)
		s.logger.Debug("add new client", zap.Int64("score", cnt))

		if rets, err := pp.Exec(ctx); err != nil {
			return errors.WithStack(err)
		} else if cnt, err = rets[len(rets)-1].(*redis.IntCmd).Result(); err != nil {
			return errors.Wrapf(err, "get counter `%s`", rets[len(rets)-1].String())
		}

		if int(cnt)+1 > s.limit {
			// failed to acquire lock
			pp.ZRem(ctx, s.owners, s.clientID)
			pp.ZRem(ctx, s.cids, s.clientID)
		} else {
			acquiredLock = true
		}

		return nil
	}); err != nil {
		return false, nil, errors.WithStack(err)
	}

	lockCtx, s.cancel = context.WithCancel(context.Background())
	go s.refreshLock(lockCtx, s.cancel)

	return acquiredLock, lockCtx, nil
}

// Unlock release lock
func (s *Semaphore) Unlock(ctx context.Context) (err error) {
	if _, err = s.rdb.TxPipelined(ctx, func(pp redis.Pipeliner) error {
		pp.ZRem(ctx, s.owners, s.clientID)
		pp.ZRem(ctx, s.cids, s.clientID)
		return nil
	}); err != nil {
		return errors.WithStack(err)
	}

	s.cancel()
	return
}

func (s *Semaphore) refreshLock(ctx context.Context, cancel func()) {
	defer cancel()
	ticker := time.NewTicker(s.heartbeatInterval)
	defer ticker.Stop()

	logger := s.logger.With(zap.String("lock", s.cids))
	defer logger.Debug("release key")
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if err := s.rdb.ZAddXX(ctx, s.cids, &redis.Z{
			Member: s.clientID,
			Score:  float64(gutils.Clock.GetUTCNow().Unix()),
		}).Err(); err != nil {
			if IsNil(err) {
				logger.Warn("lock not exists")
				return
			}

			logger.Error("refresh semaphore", zap.Error(err))
			return
		}

		logger.Debug("succeed renew lock")
	}
}
