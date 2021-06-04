package redis

import (
	gutils "github.com/Laisky/go-utils"
	"github.com/go-redis/redis/v8"
)

// Utils utils enhancemant for redis
type Utils struct {
	*redis.Client
	logger *gutils.LoggerType
}

// NewRedisUtils wrap redis client with utils
func NewRedisUtils(rdb *redis.Client) *Utils {
	return &Utils{
		Client: rdb,
		logger: logger,
	}
}
