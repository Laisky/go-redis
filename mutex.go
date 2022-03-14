package redis

import (
	"context"
	"fmt"
	"time"

	gutils "github.com/Laisky/go-utils"
	"github.com/Laisky/zap"
	"github.com/go-redis/redis/v8"
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

// mutexType distributed mutex
//
// Redis keys:
//
//   `/rtils/sync/mutex/<lock_id>/<client_id>`
//
// Implementations:
//
//   1. generate client id(cid)
//   2. set if not exists by `SETNX` with ttl: lock_name -> cid
//   3. if succeeded set, auto refresh lock's ttl
type Mutex interface {
	// Lock acquire a recursive lock
	//
	// if succeed acquired lock,
	//   * locked == true
	//   * lockCtx is context of lock, this context will be set to done when lock is expired
	Lock(ctx context.Context) (locked bool, lockCtx context.Context, err error)
	// Unlock release lock
	Unlock(ctx context.Context) error
}

type mutex struct {
	*mutexOption
	rdb    *Utils
	logger gutils.LoggerItf
	cancel context.CancelFunc

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
func WithMutexLogger(logger *gutils.LoggerType) MutexOptionFunc {
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

	return mu, nil
}

func (m *mutex) refreshLock(ctx context.Context, cancel func()) {
	defer cancel()
	ticker := time.NewTicker(m.heartbeatInterval)
	defer ticker.Stop()
	defer m.logger.Debug("release key", zap.String("dbkey", m.name))
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if err := m.rdb.Watch(ctx, func(tx *redis.Tx) (err error) {
			if val, err := tx.Get(ctx, m.name).Result(); err != nil {
				return errors.Wrapf(err, "get key `%s`", m.name)
			} else if val != m.clientID {
				return errors.Errorf("another process `%s` take over this lock `%s`", val, m.name)
			}

			_, err = tx.TxPipelined(ctx, func(pp redis.Pipeliner) error {
				pp.Expire(ctx, m.name, m.ttl)
				return nil
			})

			err = errors.WithStack(err)
			return
		}, m.name); err != nil {
			m.logger.Warn("renew lock", zap.String("dbkey", m.name), zap.Error(err))
			return
		}

		m.logger.Debug("succeed renew lock", zap.String("lock", m.name))
	}
}

// Lock acquire a recursive lock
//
// if succeed acquired lock,
//   * locked == true
//   * lockCtx is context of lock, this context will be set to done when lock is expired
func (m *mutex) Lock(ctx context.Context) (locked bool, lockCtx context.Context, err error) {
	for {
		select {
		case <-ctx.Done():
			return locked, lockCtx, ctx.Err()
		default:
		}

		if locked, err = m.rdb.SetNX(ctx, m.name, m.clientID, m.ttl).Result(); err != nil {
			return false, nil, errors.WithStack(err)
		} else if !locked {
			if val, err := m.rdb.Get(ctx, m.name).Result(); err != nil {
				return false, nil, errors.Wrapf(err, "get `%s`", m.name)
			} else if val != m.clientID {
				// if val == m.clientID, means this client already acquired lock
				if !m.blocking {
					return false, nil, nil
				}

				time.Sleep(m.spinInterval)
				continue
			}
		}

		if m.cancel != nil {
			m.cancel()
		}

		lockCtx, m.cancel = context.WithCancel(ctx)
		go m.refreshLock(lockCtx, m.cancel)
		return true, lockCtx, nil
	}
}

// Unlock release lock
func (m *mutex) Unlock(ctx context.Context) error {
	return errors.WithStack(m.rdb.Watch(ctx, func(tx *redis.Tx) (err error) {
		if val, err := tx.Get(ctx, m.name).Result(); err != nil {
			if !IsNil(err) {
				return errors.Wrapf(err, "get key `%s`", m.name)
			}

			m.logger.Warn("lock not exists")
			return nil
		} else if val != m.clientID {
			m.logger.Warn("another process already acquired this lock")
			return nil
		}

		if _, err = tx.TxPipelined(ctx, func(pp redis.Pipeliner) error {
			pp.Del(ctx, m.name)
			return nil
		}); err != nil {
			return errors.WithStack(err)
		}

		m.cancel()
		m.cancel = nil
		return
	}, m.name))
}
