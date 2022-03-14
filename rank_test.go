package redis

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

func ExampleRank() {
	rdb := redis.NewClient(&redis.Options{})
	rtils := NewRedisUtils(rdb)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	r, _ := rtils.NewRank("test", 10000)
	_ = r.Set(ctx, "user-1", 100, 1)
	_ = r.Set(ctx, "user-2", 101, 1)
	_ = r.Set(ctx, "user-3", 511, 1)
	_ = r.Set(ctx, "user-4", 201, 1)
	_ = r.Set(ctx, "user-5", 301, 1)

	ret, _ := r.List(ctx, 1)
	fmt.Println(ret[0].Member)
	// Output:
	// user-3
}

func TestUtils_NewRank(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{})
	rtils := NewRedisUtils(rdb)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := rtils.NewRank("test", 0)
	require.Error(t, err)
	_, err = rtils.NewRank("test", -2)
	require.Error(t, err)
	_, err = rtils.NewRank("test", 11)
	require.Error(t, err)

	r, err := rtils.NewRank("laisky", 10000)
	require.NoError(t, err)

	t.Run("getset", func(t *testing.T) {
		require.Error(t, r.Set(ctx, "", 0, 0))
		require.Error(t, r.Set(ctx, "123", 0, 10000))
		require.Error(t, r.Set(ctx, "123", 0, 10001))
		require.NoError(t, r.Set(ctx, "1", 1, 1))
		ret, err := r.Get(ctx, "1")
		require.NoError(t, err)
		require.Equal(t, 1, ret)

		require.NoError(t, r.Set(ctx, "1", 14, 123))
		ret, err = r.Get(ctx, "1")
		require.NoError(t, err)
		require.Equal(t, 123, ret)
	})

	require.NoError(t, r.Set(ctx, "1", 1, 3))
	for i := 0; i <= 100; i++ {
		n := rand.Intn(100)
		require.NoError(t, r.Set(ctx, strconv.Itoa(n), n, 1))
	}

	t.Run("list", func(t *testing.T) {
		ret, err := r.List(ctx, 5)
		require.NoError(t, err)
		require.Equal(t, 5, len(ret))
		for i := 1; i < len(ret)-1; i++ {
			require.Greater(t, ret[i-1].Score, ret[i].Score)
		}
	})

	t.Run("del", func(t *testing.T) {
		require.NoError(t, r.Del(ctx, "1"))
		_, err := r.Get(ctx, "1")
		require.Error(t, err)
		require.True(t, IsNil(err))
	})

}
