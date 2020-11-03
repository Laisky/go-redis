package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/Laisky/zap"
	"github.com/go-redis/redis/v7"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

const (
	defaultMutexHeartbeatInterval = time.Second
	defaultMutexTimeout           = 5 * time.Second
)

type mutexOption struct {
	heartbeatInterval time.Duration
	timeout           time.Duration
	identifier        string
}

// Mutex distributed mutex
type Mutex struct {
	mutexOption

	rdb  *Utils
	name string
}

// MutexOptionFunc options for mutex
type MutexOptionFunc func(*Mutex) error

// WithMutexHeartbeatInterval set heartbeat interval
func WithMutexHeartbeatInterval(interval time.Duration) MutexOptionFunc {
	return func(mu *Mutex) error {
		mu.heartbeatInterval = interval
		return nil
	}
}

// WithMutexTimeout set expiration
func WithMutexTimeout(timeout time.Duration) MutexOptionFunc {
	return func(mu *Mutex) error {
		mu.timeout = timeout
		return nil
	}
}

// NewMutex new mutex
func (u *Utils) NewMutex(lockName string, opts ...MutexOptionFunc) (mu *Mutex, err error) {
	mu = &Mutex{
		rdb:  u,
		name: fmt.Sprintf(DefaultKeySyncMutex, lockName),
		mutexOption: mutexOption{
			heartbeatInterval: defaultMutexHeartbeatInterval,
			timeout:           defaultMutexTimeout,
			identifier:        uuid.New().String(),
		},
	}
	for _, optf := range opts {
		if err = optf(mu); err != nil {
			return nil, err
		}
	}

	return
}

func (m *Mutex) heartbeat(ctx context.Context, cancel func()) {
	defer cancel()
	ticker := time.NewTicker(m.heartbeatInterval)
	defer ticker.Stop()
	defer GetLogger().Debug("release key", zap.String("dbkey", m.name))
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if err := m.rdb.Watch(func(tx *redis.Tx) (err error) {
			if val, err := tx.Get(m.name).Result(); err != nil {
				return errors.Wrapf(err, "get key `%s`", m.name)
			} else if val != m.identifier {
				return errors.Errorf("another process `%s` take over this lock `%s`", val, m.name)
			}

			if ok, err := tx.Expire(m.name, m.timeout).Result(); err != nil {
				return errors.Wrapf(err, "reset expires `%s`", m.name)
			} else if !ok {
				return errors.Errorf("key `%s` not exists", m.name)
			}

			return nil
		}, m.name); err != nil {
			GetLogger().Warn("renew lock", zap.String("dbkey", m.name), zap.Error(err))
			return
		}

		GetLogger().Debug("succeed renew lock", zap.String("lock", m.name))
	}
}

// Lock lock
func (m *Mutex) Lock(ctx context.Context) (bool, context.Context, error) {
	if ok, err := m.rdb.SetNX(m.name, m.identifier, m.timeout).Result(); err != nil || !ok {
		return false, nil, errors.WithStack(err)
	}

	ctxLock, cancel := context.WithCancel(ctx)
	go m.heartbeat(ctxLock, cancel)
	return true, ctxLock, nil
}

// Unlock release lock
func (m *Mutex) Unlock() error {
	return errors.WithStack(m.rdb.Watch(func(tx *redis.Tx) error {
		if val, err := tx.Get(m.name).Result(); err != nil {
			if !IsNil(err) {
				return errors.Wrapf(err, "get key `%s`", m.name)
			}

			GetLogger().Warn("lock not exists")
			return nil
		} else if val != m.identifier {
			GetLogger().Warn("another process already acquired this lock")
			return nil
		}

		return errors.WithStack(tx.Del(m.name).Err())
	}, m.name))
}
