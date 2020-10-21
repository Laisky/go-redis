package redis

import (
	"sync"

	"github.com/go-redis/redis/v7"
	"github.com/pkg/errors"
)

var (
	nilFunc = func(err error) bool {
		return errors.Is(err, redis.Nil)
	}
	nilMux sync.RWMutex
)

// IsNil is nil in redis
func IsNil(err error) (is bool) {
	nilMux.RLock()
	is = nilFunc(err)
	nilMux.RUnlock()
	return
}

// SetNilFunc set nil func
func SetNilFunc(f func(error) bool) {
	nilMux.Lock()
	nilFunc = f
	nilMux.Unlock()
}
