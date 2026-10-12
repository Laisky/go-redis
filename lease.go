package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrLockContextRequired = errors.New("Unlock requires the context returned by Lock")
var ErrLeaseLost = errors.New("lock acquisition is no longer owned")

type acquisitionContextKey struct{}
type lockAcquisition struct {
	lease    *lockLease
	released bool
}
type lockLease struct {
	owner      interface{}
	token      string
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	references int
	release    func(context.Context, string) (bool, error)
}

// Each logical acquisition has a unique server token and immutable context
// reference. Explicit recursion shares that lease with balanced references.
func newLockLease(parent context.Context, owner interface{}, token string, interval time.Duration,
	renew func(context.Context, string) (bool, error), release func(context.Context, string) (bool, error), ended func()) context.Context {
	ctx, cancel := context.WithCancel(parent)
	lease := &lockLease{owner: owner, token: token, ctx: ctx, cancel: cancel, references: 1, release: release}
	ref := &lockAcquisition{lease: lease}
	go func() {
		defer ended()
		defer cancel()
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, _ = release(cleanup, token)
		}()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			found, err := renew(ctx, token)
			if err != nil || !found {
				cancel()
				return
			}
		}
	}()
	return context.WithValue(ctx, acquisitionContextKey{}, ref)
}
func recursiveAcquisition(ctx context.Context, owner interface{}) (bool, context.Context, error) {
	ref, _ := ctx.Value(acquisitionContextKey{}).(*lockAcquisition)
	if ref == nil || ref.lease.owner != owner {
		return false, nil, nil
	}
	lease := ref.lease
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if ref.released || lease.ctx.Err() != nil {
		return true, nil, ErrLeaseLost
	}
	if err := ctx.Err(); err != nil {
		return true, nil, err
	}
	lease.references++
	return true, context.WithValue(lease.ctx, acquisitionContextKey{}, &lockAcquisition{lease: lease}), nil
}
func releaseAcquisition(ctx context.Context, owner interface{}, active int64) error {
	ref, _ := ctx.Value(acquisitionContextKey{}).(*lockAcquisition)
	if ref == nil {
		if active == 0 {
			return nil
		}
		return ErrLockContextRequired
	}
	lease := ref.lease
	if lease.owner != owner {
		return fmt.Errorf("%w: context belongs to another lock", ErrLockContextRequired)
	}
	lease.mu.Lock()
	if ref.released {
		lease.mu.Unlock()
		return nil
	}
	ref.released = true
	lease.references--
	final := lease.references == 0
	lease.mu.Unlock()
	if !final {
		return nil
	}
	lease.cancel()
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	_, err := lease.release(cleanup, lease.token)
	return err
}
func validateLockOptions(opt *mutexOption) error {
	if opt.ttl < time.Millisecond || opt.heartbeatInterval <= 0 || opt.heartbeatInterval >= opt.ttl || opt.spinInterval <= 0 {
		return fmt.Errorf("lock TTL, heartbeat and spin must be positive; heartbeat must be shorter than TTL")
	}
	return nil
}
