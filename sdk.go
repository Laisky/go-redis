package redis

import (
	glog "github.com/Laisky/go-utils/v5/log"
	"github.com/go-redis/redis/v8"
)

// Utils utils enhancemant for redis
type Utils struct {
	*redis.Client
	logger glog.Logger
}

// NewRedisUtils wrap redis client with utils
func NewRedisUtils(rdb *redis.Client) *Utils {
	return &Utils{
		Client: rdb,
		logger: logger,
	}
}
