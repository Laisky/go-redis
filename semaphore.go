package redis

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"sync/atomic"
	"time"

	gutils "github.com/Laisky/go-utils/v5"
	glog "github.com/Laisky/go-utils/v5/log"
	"github.com/pkg/errors"
)

// Semaphore bounds concurrent leases using an atomic Redis decision.
// Expired membership is pruned, capacity checked and a distinct token inserted
// in one script. Waiting callers retry; strict FIFO scheduling is not promised.
// Recursion is explicit: pass a live returned lockCtx to Lock and balance every
// successful acquisition with Unlock using its own returned context.
type Semaphore interface {
	// Lock acquires a distinct permit. Recursion requires a returned live lockCtx.
	//
	// if succeed acquired lock,
	//   * locked == true
	//   * lockCtx is context of lock, this context will be set to done when lock is expired
	Lock(ctx context.Context) (locked bool, lockCtx context.Context, err error)
	// Unlock releases exactly the acquisition identified by a returned lockCtx.
	Unlock(ctx context.Context) (err error)
}

type semaphore struct {
	semaOption
	rdb    *Utils
	logger glog.Logger
	active atomic.Int64

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
func WithSemaphoreLogger(logger glog.Logger) SemaphoreOptionFunc {
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

	if limit <= 0 {
		return nil, fmt.Errorf("semaphore limit must be positive")
	}
	if err := validateLockOptions(sema.mutexOption); err != nil {
		return nil, err
	}
	return sema, nil
}

func (s *semaphore) Lock(ctx context.Context) (bool, context.Context, error) {
	if handled, lctx, err := recursiveAcquisition(ctx, s); handled {
		return err == nil, lctx, err
	}
	token := uuid.NewString()
	for {
		if err := ctx.Err(); err != nil {
			return false, nil, err
		}
		result, err := semaphoreAcquireScript.Run(ctx, s.rdb.Client, []string{s.cids, s.owners, s.counter}, token, s.ttl.Seconds(), s.limit).Int64()
		if err != nil {
			return false, nil, errors.WithStack(err)
		}
		if result == 1 {
			s.active.Add(1)
			lctx := newLockLease(ctx, s, token, s.heartbeatInterval, s.renew, s.release, func() { s.active.Add(-1) })
			return true, lctx, nil
		}
		if !s.blocking {
			return false, nil, nil
		}
		gutils.SleepWithContext(ctx, s.spinInterval)
	}
}
func (s *semaphore) renew(ctx context.Context, token string) (bool, error) {
	result, err := semaphoreRenewScript.Run(ctx, s.rdb.Client, []string{s.cids, s.owners}, token, s.ttl.Seconds()).Int64()
	return result == 1, err
}
func (s *semaphore) release(ctx context.Context, token string) (bool, error) {
	result, err := semaphoreReleaseScript.Run(ctx, s.rdb.Client, []string{s.cids, s.owners}, token).Int64()
	return result == 1, err
}
func (s *semaphore) Unlock(ctx context.Context) error {
	return releaseAcquisition(ctx, s, s.active.Load())
}
