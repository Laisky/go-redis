package redis

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	gutils "github.com/Laisky/go-utils/v5"
	glog "github.com/Laisky/go-utils/v5/log"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

const (
	defaultMutexHeartbeatInterval = time.Second
	defaultMutexTTL               = 3 * time.Second
	defaultSpinInterval           = 100 * time.Millisecond
	defaultBlocking               = true
)

type mutexOption struct {
	heartbeatInterval time.Duration
	ttl               time.Duration
	spinInterval      time.Duration
	blocking          bool
	clientID          string
}

func newMutexOption() *mutexOption {
	return &mutexOption{
		ttl:               defaultMutexTTL,
		heartbeatInterval: defaultMutexHeartbeatInterval,
		spinInterval:      defaultSpinInterval,
		blocking:          defaultBlocking,
		clientID:          uuid.New().String(),
	}
}

// Mutex protects one Redis-backed lease using a unique token per logical
// acquisition. Recursive ownership is explicit through a returned lockCtx;
// each successful Lock must be balanced with Unlock of that exact context.
// A parent context without an acquisition reference cannot release an active
// lease. ClientID is retained as a configuration label, not authorization.
type Mutex interface {
	// Lock acquires a unique lease. Recursion requires a returned live lockCtx.
	//
	// if succeed acquired lock,
	//   * locked == true
	//   * lockCtx is context of lock, this context will be set to done when lock is expired
	Lock(ctx context.Context) (locked bool, lockCtx context.Context, err error)
	// Unlock releases exactly the acquisition identified by a returned lockCtx.
	Unlock(ctx context.Context) error
}

type mutex struct {
	*mutexOption
	rdb    *Utils
	logger glog.Logger
	active atomic.Int64

	// name unique lock id
	name string
}

// MutexOptionFunc options for mutex
type MutexOptionFunc func(*mutex) error

// WithMutexSpinInterval set lock spin interval
func WithMutexSpinInterval(interval time.Duration) MutexOptionFunc {
	return func(mu *mutex) error {
		mu.spinInterval = interval
		return nil
	}
}

// WithMutexBlockingLock set whether blocking lock
func WithMutexBlockingLock(blocking bool) MutexOptionFunc {
	return func(mu *mutex) error {
		mu.blocking = blocking
		return nil
	}
}

// WithMutexRefreshInterval set lock refreshing interval
func WithMutexRefreshInterval(interval time.Duration) MutexOptionFunc {
	return func(mu *mutex) error {
		mu.heartbeatInterval = interval
		return nil
	}
}

// WithMutexTTL set lock's expiration
func WithMutexTTL(ttl time.Duration) MutexOptionFunc {
	return func(mu *mutex) error {
		mu.ttl = ttl
		return nil
	}
}

// WithMutexLogger set lock's expiration
func WithMutexLogger(logger glog.Logger) MutexOptionFunc {
	return func(mu *mutex) error {
		mu.logger = logger
		return nil
	}
}

// WithMutexClientID set client id
func WithMutexClientID(clientID string) MutexOptionFunc {
	return func(mu *mutex) error {
		mu.clientID = clientID
		return nil
	}
}

// NewMutex new mutex
func (u *Utils) NewMutex(lockName string, opts ...MutexOptionFunc) (Mutex, error) {
	mu := &mutex{
		logger:      u.logger,
		rdb:         u,
		name:        fmt.Sprintf(defaultKeySyncMutex, lockName),
		mutexOption: newMutexOption(),
	}
	for _, optf := range opts {
		if err := optf(mu); err != nil {
			return nil, err
		}
	}

	if err := validateLockOptions(mu.mutexOption); err != nil {
		return nil, err
	}
	return mu, nil
}

func (m *mutex) Lock(ctx context.Context) (bool, context.Context, error) {
	if handled, lctx, err := recursiveAcquisition(ctx, m); handled {
		return err == nil, lctx, err
	}
	token := uuid.NewString()
	for {
		if err := ctx.Err(); err != nil {
			return false, nil, err
		}
		result, err := mutexAcquireScript.Run(ctx, m.rdb.Client, []string{m.name}, token, m.ttl.Milliseconds()).Int64()
		if err != nil {
			return false, nil, errors.WithStack(err)
		}
		if result == 1 {
			m.active.Add(1)
			lctx := newLockLease(ctx, m, token, m.heartbeatInterval, m.renew, m.release, func() { m.active.Add(-1) })
			return true, lctx, nil
		}
		if !m.blocking {
			return false, nil, nil
		}
		gutils.SleepWithContext(ctx, m.spinInterval)
	}
}
func (m *mutex) renew(ctx context.Context, token string) (bool, error) {
	result, err := mutexRenewScript.Run(ctx, m.rdb.Client, []string{m.name}, token, m.ttl.Milliseconds()).Int64()
	return result == 1, err
}
func (m *mutex) release(ctx context.Context, token string) (bool, error) {
	result, err := mutexReleaseScript.Run(ctx, m.rdb.Client, []string{m.name}, token).Int64()
	return result == 1, err
}
func (m *mutex) Unlock(ctx context.Context) error {
	return releaseAcquisition(ctx, m, m.active.Load())
}
