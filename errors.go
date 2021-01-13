package redis

import (
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

// IsNil is nil in redis
func IsNil(err error) bool {
	return errors.Is(err, redis.Nil)
}
