package redis

import "github.com/go-redis/redis/v7"

// Utils utils enhancemant for redis
type Utils struct {
	*redis.Client
}

// NewRedisUtils wrap redis client with utils
func NewRedisUtils(rdb *redis.Client) *Utils {
	return &Utils{rdb}
}
