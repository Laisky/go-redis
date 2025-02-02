package redis

import (
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

// IsNil is nil in redis
func IsNil(err error) bool {
	return errors.Is(err, redis.Nil)
}
