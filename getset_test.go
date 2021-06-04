package redis

import (
	"context"
	"testing"
	"time"

	gutils "github.com/Laisky/go-utils"
	"github.com/Laisky/zap"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestGetSet(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{})
	rtils := NewRedisUtils(rdb)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dbkey := gutils.RandomStringWithLength(20)

	if _, err := rtils.GetItem(ctx, dbkey); err != nil {
		if !IsNil(err) {
			t.Fatalf("%+v", err)
		}
	}

	val := "4ij234j23l4"
	if err := rtils.SetItem(ctx, dbkey, val, KeyExpImmortal); err != nil {
		t.Fatalf("%+v", err)
	}
	if _, err := rtils.GetItem(ctx, dbkey); err != nil {
		t.Fatalf("%+v", err)
	}
	time.Sleep(1 * time.Second)
	if _, err := rtils.GetItem(ctx, dbkey); err != nil {
		if !IsNil(err) {
			t.Fatalf("%+v", err)
		}
	}
}

func TestPopPush(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{})
	rtils := NewRedisUtils(rdb)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dbkey := gutils.RandomStringWithLength(20)

	val := "4ij234j23l4"
	if err := rtils.RPush(ctx, dbkey, val); err != nil {
		t.Fatalf("%+v", err)
	}
	if k, v, err := rtils.LPopKeysBlocking(ctx, dbkey); err != nil {
		t.Fatalf("%+v", err)
	} else {
		if k != dbkey || v != val {
			t.Fatalf("got %v:%v", k, v)
		}
	}

	ctxTimeout, cancelTimeout := context.WithTimeout(ctx, 2*time.Second)
	defer cancelTimeout()

	if _, _, err := rtils.LPopKeysBlocking(ctxTimeout, dbkey); err != nil {
		if !IsNil(err) && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%+v", err)
		}
	}
}

// get item and delelte
func TestUtils_GetItemBlockingWithDelete(t *testing.T) {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{})
	rtils := NewRedisUtils(rdb)

	dbkey := "/TestUtils_GetItemBlockingWithDelete"
	go func() {
		ctxWrite, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		for {
			select {
			case <-ctxWrite.Done():
				return
			default:
			}

			if err := rdb.Set(ctxWrite, dbkey, gutils.RandomStringWithLength(8), KeyExpImmortal).Err(); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				gutils.Logger.Panic("set", zap.Error(err))
			}
		}
	}()

	err := rdb.Set(ctx, dbkey, gutils.RandomStringWithLength(8), KeyExpImmortal).Err()
	require.NoError(t, err)

	data, err := rtils.GetItemBlocking(ctx, dbkey)
	require.NoError(t, err)
	t.Logf("got: %+v", data)

	data, err = rdb.Get(ctx, dbkey).Result()
	require.NoError(t, err)
	require.NotEqual(t, "", data)

	// =====================================
	// case: get and delete
	// =====================================
	err = rdb.Set(ctx, dbkey, gutils.RandomStringWithLength(8), KeyExpImmortal).Err()
	require.NoError(t, err)

	data, err = rtils.GetItemBlocking(ctx, dbkey)
	require.NoError(t, err)
	t.Logf("got: %+v", data)

	data, err = rdb.Get(ctx, dbkey).Result()
	require.True(t, IsNil(err))
	require.Equal(t, "", data)
}
