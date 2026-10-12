package redis

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gutils "github.com/Laisky/go-utils/v5"
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
	rdb := redis.NewClient(testRedisOptions())
	defer rdb.Close()
	rtils := NewRedisUtils(rdb)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	name := "recursive-" + gutils.RandomStringWithLength(10)
	mu, err := rtils.NewMutex(name, WithMutexBlockingLock(false))
	if err != nil {
		t.Fatal(err)
	}
	ok, lctx, err := mu.Lock(ctx)
	if err != nil || !ok {
		t.Fatal("initial lock failed")
	}
	references := []context.Context{lctx}
	for i := 0; i < 5; i++ {
		ok, lctx, err = mu.Lock(lctx)
		if err != nil || !ok {
			t.Fatal("explicit recursive lock failed")
		}
		references = append(references, lctx)
	}
	contender, err := rtils.NewMutex(name, WithMutexBlockingLock(false))
	if err != nil {
		t.Fatal(err)
	}
	for i := len(references) - 1; i > 0; i-- {
		if err := mu.Unlock(references[i]); err != nil {
			t.Fatal(err)
		}
		if references[0].Err() != nil {
			t.Fatal("unbalanced recursive release revoked outer lease")
		}
		if ok, _, err := contender.Lock(ctx); err != nil || ok {
			t.Fatal("recursive release exposed protected state")
		}
	}
	if err := mu.Unlock(references[0]); err != nil {
		t.Fatal(err)
	}
	if references[0].Err() == nil {
		t.Fatal("final release did not revoke lease")
	}
	ok, other, err := contender.Lock(ctx)
	if err != nil || !ok {
		t.Fatal("balanced release did not free lock")
	}
	if err := contender.Unlock(other); err != nil {
		t.Fatal(err)
	}
}

func TestUtils_NewMutex_unlock(t *testing.T) {
	rdb := redis.NewClient(testRedisOptions())
	rtils := NewRedisUtils(rdb)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mu, err := rtils.NewMutex("laisky")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	for i := 0; i < 10; i++ {
		ok, lockCtx, err := mu.Lock(ctx)
		if err != nil {
			t.Fatalf("%+v", err)
		} else if !ok {
			t.Fatalf("not ok")
		}

		if err = mu.Unlock(lockCtx); err != nil {
			t.Fatalf("not ok")
		}
	}
}

func TestMutex_Lock_GetNilBetweenSetNXAndGet(t *testing.T) {
	rdb := redis.NewClient(testRedisOptions())
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
	rdb := redis.NewClient(testRedisOptions())
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
	rdb := redis.NewClient(testRedisOptions())
	rtils := NewRedisUtils(rdb)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	b.RunParallel(func(pb *testing.PB) {
		mu, err := rtils.NewMutex("laisky")
		if err != nil {
			b.Fatalf("%+v", err)
		}

		for pb.Next() {
			locked, lockCtx, err := mu.Lock(ctx)
			if err != nil {
				b.Fatalf("%+v", err)
			} else if locked {
				if err = mu.Unlock(lockCtx); err != nil {
					b.Fatalf("%+v", err)
				}
			}
		}
	})
}

func TestUtils_NewMutex_race(t *testing.T) {
	rdb := redis.NewClient(testRedisOptions())
	defer rdb.Close()
	rtils := NewRedisUtils(rdb)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	name := "mutex-race-" + gutils.RandomStringWithLength(10)
	mu, err := rtils.NewMutex(name, WithMutexSpinInterval(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	var active atomic.Int32
	var pool errgroup.Group
	for i := 0; i < 8; i++ {
		pool.Go(func() error {
			ok, lctx, err := mu.Lock(ctx)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("missing acquisition")
			}
			entered := active.Add(1)
			time.Sleep(time.Millisecond)
			active.Add(-1)
			unlockErr := mu.Unlock(lctx)
			if entered > 1 {
				return fmt.Errorf("simultaneous critical sections=%d", entered)
			}
			return unlockErr
		})
	}
	if err := pool.Wait(); err != nil {
		t.Fatal(err)
	}
}
