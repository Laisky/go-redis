package redis

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gutils "github.com/Laisky/go-utils/v5"
	"github.com/Laisky/zap"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

type oneShotGetNilHook struct {
	key  string
	done atomic.Bool
}

func (h *oneShotGetNilHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *oneShotGetNilHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *oneShotGetNilHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if !strings.EqualFold(cmd.Name(), "get") {
			return next(ctx, cmd)
		}

		args := cmd.Args()
		if len(args) < 2 {
			return next(ctx, cmd)
		}

		key, ok := args[1].(string)
		if !ok || key != h.key {
			return next(ctx, cmd)
		}

		if h.done.CompareAndSwap(false, true) {
			cmd.SetErr(redis.Nil)
			return redis.Nil
		}

		return next(ctx, cmd)
	}
}

func TestUtils_NewMutex_lock(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{})
	rtils := NewRedisUtils(rdb)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	mu, err := rtils.NewMutex("laisky")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	ok, ctxLock, err := mu.Lock(ctx)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	if !ok {
		t.Fatalf("not ok")
	}

	// RLock
	var ctxLockR context.Context
	for i := 0; i < 5; i++ {
		if ok, ctxLockR, err = mu.Lock(ctx); err != nil {
			t.Fatalf("%+v", err)
		} else if !ok {
			t.Fatal("should ok")
		}
	}

	if err = ctxLock.Err(); err != nil && err != context.Canceled {
		t.Fatalf("%+v", err)
	}

	{
		mu2, err := rtils.NewMutex("laisky")
		if err != nil {
			t.Fatalf("%+v", err)
		}

		ctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		if ok, _, err = mu2.Lock(ctx); err != nil {
			if err != context.DeadlineExceeded {
				t.Fatalf("%+v", err)
			}
		} else if ok {
			t.Fatalf("should be not ok")
		}
		cancel()
	}

	time.Sleep(1 * time.Second)
	if err = ctxLockR.Err(); err != nil {
		t.Fatalf("lock released %+v", err)
	}

	// should released
	time.Sleep(2 * time.Second)
	if err = ctxLockR.Err(); err == nil {
		t.Fatalf("lock released")
	} else if err != context.DeadlineExceeded {
		t.Fatalf("unlnown err %+v", err)
	}

	// wait redis lock exceeds ttl
	time.Sleep(2 * time.Second)
	{
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if ok, _, err = mu.Lock(ctx); err != nil {
			t.Fatalf("%+v", err)
		} else if !ok {
			t.Fatalf("should be ok")
		}

		if err = mu.Unlock(ctx); err != nil {
			t.Fatalf("%+v", err)
		}

		if err = ctxLock.Err(); err == nil {
			t.Fatal("ctx should exit")
		}
		cancel()
	}
}

func TestUtils_NewMutex_unlock(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{})
	rtils := NewRedisUtils(rdb)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mu, err := rtils.NewMutex("laisky")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	for i := 0; i < 10; i++ {
		if ok, _, err := mu.Lock(ctx); err != nil {
			t.Fatalf("%+v", err)
		} else if !ok {
			t.Fatalf("not ok")
		}

		if err = mu.Unlock(ctx); err != nil {
			t.Fatalf("not ok")
		}
	}
}

func TestMutex_Lock_GetNilBetweenSetNXAndGet(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{})
	rtils := NewRedisUtils(rdb)

	lockName := "lock-getnil"
	lockKey := fmt.Sprintf(defaultKeySyncMutex, lockName)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	t.Cleanup(func() {
		_ = rdb.Del(context.Background(), lockKey).Err()
	})

	if err := rdb.Set(ctx, lockKey, "owner-1", 10*time.Second).Err(); err != nil {
		t.Fatalf("prepare lock key: %+v", err)
	}

	rdb.AddHook(&oneShotGetNilHook{key: lockKey})

	mu, err := rtils.NewMutex(lockName, WithMutexBlockingLock(false))
	if err != nil {
		t.Fatalf("new mutex: %+v", err)
	}

	locked, _, err := mu.Lock(ctx)
	if err != nil {
		t.Fatalf("lock should not fail on transient redis nil: %+v", err)
	}

	if locked {
		t.Fatalf("lock should not be acquired while another owner holds it")
	}
}

func TestMutex_Unlock_WithoutLock_NoPanic(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{})
	rtils := NewRedisUtils(rdb)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	mu, err := rtils.NewMutex("unlock-without-lock")
	if err != nil {
		t.Fatalf("new mutex: %+v", err)
	}

	if err = mu.Unlock(ctx); err != nil {
		t.Fatalf("unlock without lock should not panic, got error: %+v", err)
	}
}

// BenchmarkUtils_NewMutex_unlock-8   	   35546	     32872 ns/op	     488 B/op	      10 allocs/op
func BenchmarkUtils_NewMutex_unlock(b *testing.B) {
	rdb := redis.NewClient(&redis.Options{})
	rtils := NewRedisUtils(rdb)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	b.RunParallel(func(pb *testing.PB) {
		mu, err := rtils.NewMutex("laisky")
		if err != nil {
			b.Fatalf("%+v", err)
		}

		for pb.Next() {
			if locked, _, err := mu.Lock(ctx); err != nil {
				b.Fatalf("%+v", err)
			} else if locked {
				if err = mu.Unlock(ctx); err != nil {
					b.Fatalf("%+v", err)
				}
			}
		}
	})
}

func TestUtils_NewMutex_race(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{})
	rtils := NewRedisUtils(rdb)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	lockid := "laisky" + gutils.RandomStringWithLength(10)

	var locked int32
	var pool errgroup.Group
	for i := 0; i < 10; i++ {
		mu, err := rtils.NewMutex(lockid,
			WithMutexSpinInterval(time.Millisecond),
		)
		if err != nil {
			t.Fatalf("%+v", err)
		}

		pool.Go(func() error {
			if _, _, err := mu.Lock(ctx); err != nil && err != context.DeadlineExceeded {
				logger.Panic("lock", zap.Error(err))
				return nil
			}

			if got := atomic.AddInt32(&locked, 1); got > 1 {
				logger.Panic("locked ", zap.Int32("got", got))
			}

			if got := atomic.AddInt32(&locked, -1); got < 0 {
				logger.Panic("locked ", zap.Int32("got", got))
			}

			time.Sleep(time.Duration(100+rand.Intn(300)) * time.Millisecond)
			if err := mu.Unlock(ctx); err != nil {
				t.Fatalf("%+v", err)
			}

			return nil
		})
	}

	_ = pool.Wait()
}
