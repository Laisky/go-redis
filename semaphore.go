package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	gutils "github.com/Laisky/go-utils"
	"github.com/Laisky/zap"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

// semaphore distributed fair semaphore
//
// Redis keys:
//
//	`/rtils/sync/sema/<lock_id>/`
//
//	* cids/: client_id -> ts, all clients
//	* owners/: client_id -> counter, all clients acquired lock
//	* counter:
//
// you can specified client_id by `WithSemaphoreClientID`.
// will auto generate client_id by UUID4 if not set.
//
// Implementations:
//
//  1. generate client id(cid)
//  2. delete all expired clients in `cids`,
//     by `ZREMRANGEBYSCORE cids -inf <now - ttl>`
//  3. delete all expired clients in `owners`,
//     by `ZINTERSTORE owners 2 owners cids WEIGHTS 1 0`
//  4. increment semaphore's counter
//  5. add cid:counter to `owners`, get rank (smaller is better).
//     5-1. delete from `owners` if the rank is over the limit of semaphore
//  6. add cid:timestamp to `cids`
type Semaphore interface {
	// Lock acquire a recursive lock
	//
	// if succeed acquired lock,
	//   * locked == true
	//   * lockCtx is context of lock, this context will be set to done when lock is expired
	Lock(ctx context.Context) (locked bool, lockCtx context.Context, err error)
	// Unlock release lock
	Unlock(ctx context.Context) (err error)
}

type semaphore struct {
	semaOption
	rdb    *Utils
	logger gutils.LoggerItf
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
	*mutexOption
}

// SemaphoreOptionFunc options for semaphore
type SemaphoreOptionFunc func(*semaphore) error

// WithSemaphoreRefreshInterval set lock refreshing interval
func WithSemaphoreRefreshInterval(interval time.Duration) SemaphoreOptionFunc {
	return func(mu *semaphore) error {
		mu.heartbeatInterval = interval
		return nil
	}
}

// WithSemaphoreSpinInterval set lock spin interval
func WithSemaphoreSpinInterval(interval time.Duration) SemaphoreOptionFunc {
	return func(mu *semaphore) error {
		mu.spinInterval = interval
		return nil
	}
}

// WithSemaphoreBlockingLock set whether blocking lock
func WithSemaphoreBlockingLock(blocking bool) SemaphoreOptionFunc {
	return func(mu *semaphore) error {
		mu.blocking = blocking
		return nil
	}
}

// WithSemaphoreTTL set lock's expiration
func WithSemaphoreTTL(ttl time.Duration) SemaphoreOptionFunc {
	return func(mu *semaphore) error {
		mu.ttl = ttl
		return nil
	}
}

// WithSemaphoreLogger set lock's expiration
func WithSemaphoreLogger(logger *gutils.LoggerType) SemaphoreOptionFunc {
	return func(mu *semaphore) error {
		mu.logger = logger
		return nil
	}
}

// WithSemaphoreClientID set client id
func WithSemaphoreClientID(clientID string) SemaphoreOptionFunc {
	return func(mu *semaphore) error {
		mu.clientID = clientID
		return nil
	}
}

// NewSemaphore new semaphore
func (u *Utils) NewSemaphore(lockName string, limit int, opts ...SemaphoreOptionFunc) (Semaphore, error) {
	sema := &semaphore{
		limit:      limit,
		rdb:        u,
		logger:     u.logger,
		cids:       fmt.Sprintf(defaultKeySyncSemaphoreLocks, lockName),
		owners:     fmt.Sprintf(defaultKeySyncSemaphoreOwners, lockName),
		counter:    fmt.Sprintf(defaultKeySyncSemaphoreCounter, lockName),
		semaOption: semaOption{newMutexOption()},
	}

	for _, optf := range opts {
		if err := optf(sema); err != nil {
			return nil, err
		}
	}

	return sema, nil
}

// Lock acquire a recursive lock
//
// if succeed acquired lock,
//   - locked == true
//   - lockCtx is context of lock, this context will be set to done when lock is expired
func (s *semaphore) Lock(ctx context.Context) (locked bool, lockCtx context.Context, err error) {
	for {
		select {
		case <-ctx.Done():
			return locked, lockCtx, ctx.Err()
		default:
		}

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
				locked = true
			}

			return nil
		}); err != nil {
			return false, nil, errors.WithStack(err)
		}

		if !locked {
			if !s.blocking {
				return false, nil, nil
			}

			time.Sleep(s.spinInterval)
			continue
		}

		if s.cancel != nil {
			s.cancel()
		}

		lockCtx, s.cancel = context.WithCancel(ctx)
		go s.refreshLock(lockCtx, s.cancel)

		return locked, lockCtx, nil
	}
}

// Unlock release lock
func (s *semaphore) Unlock(ctx context.Context) (err error) {
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

func (s *semaphore) refreshLock(ctx context.Context, cancel func()) {
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
