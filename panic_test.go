package redis

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestMutex_Unlock_Panic(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{})
	u := NewRedisUtils(rdb)

	mu, err := u.NewMutex("test-lock")
	require.NoError(t, err)

	// Calling Unlock before Lock should not panic
	// Even if it returns an error due to redis connection, it shouldn't panic
	_ = mu.Unlock(context.Background())
}

func TestSemaphore_Unlock_Panic(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{})
	u := NewRedisUtils(rdb)

	sema, err := u.NewSemaphore("test-sema", 2)
	require.NoError(t, err)

	// Calling Unlock before Lock should not panic
	_ = sema.Unlock(context.Background())
}
