package redis

import (
	"context"
	"fmt"
	"github.com/Laisky/zap"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestSemaphore_Lock(t *testing.T) {
	rdb := redis.NewClient(testRedisOptions())
	rtils := NewRedisUtils(rdb)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	sema1, err := rtils.NewSemaphore("laisky", 2)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	// client 1
	locked1, lease1, err := sema1.Lock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer sema1.Unlock(lease1)
	if locked1, _, err := locked1, lease1, err; err != nil {
		t.Fatalf("%+v", err)
	} else if !locked1 {
		t.Fatal("should locked")
	}

	sema2, err := rtils.NewSemaphore("laisky", 2)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	// client 2
	locked2, lease2, err := sema2.Lock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if locked2, _, err := locked2, lease2, err; err != nil {
		t.Fatalf("%+v", err)
	} else if !locked2 {
		t.Fatal("should locked")
	}

	// client 3
	sema3, err := rtils.NewSemaphore("laisky", 2)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	ctx3, cancel3 := context.WithTimeout(ctx, time.Second)
	if locked3, _, err := sema3.Lock(ctx3); err != nil {
		if err != context.DeadlineExceeded {
			t.Fatalf("%+v", err)
		}
	} else if locked3 {
		t.Fatal("should not locked")
	}
	cancel3()

	if err = sema2.Unlock(lease2); err != nil {
		t.Fatal("should not locked")
	}

	if locked3, _, err := sema3.Lock(ctx); err != nil {
		t.Fatalf("%+v", err)
	} else if !locked3 {
		t.Fatal("should  locked")
	}
}

func TestSemaphore_race(t *testing.T) {
	for _, limit := range []int{1, 2, 4} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			rdb := redis.NewClient(testRedisOptions())
			defer rdb.Close()
			rtils := NewRedisUtils(rdb)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			sema, err := rtils.NewSemaphore("semaphore-race-"+uuid.NewString(), limit, WithSemaphoreSpinInterval(time.Millisecond))
			if err != nil {
				t.Fatal(err)
			}
			var active atomic.Int32
			var group errgroup.Group
			for worker := 0; worker < 8; worker++ {
				group.Go(func() error {
					for i := 0; i < 8; i++ {
						locked, lctx, err := sema.Lock(ctx)
						if err != nil {
							return err
						}
						if !locked {
							return fmt.Errorf("missing permit")
						}
						entered := active.Add(1)
						time.Sleep(time.Millisecond)
						active.Add(-1)
						releaseErr := sema.Unlock(lctx)
						if entered > int32(limit) {
							return fmt.Errorf("active holders=%d limit=%d", entered, limit)
						}
						if releaseErr != nil {
							return releaseErr
						}
					}
					return nil
				})
			}
			if err := group.Wait(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func BenchmarkSemaphore(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		rdb := redis.NewClient(testRedisOptions())
		rtils := NewRedisUtils(rdb)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()

		sema, err := rtils.NewSemaphore("laisky", 2)
		if err != nil {
			logger.Panic("new", zap.Error(err))
			return
		}

		for pb.Next() {
			locked, lockCtx, err := sema.Lock(ctx)
			if err != nil {
				logger.Panic("lock", zap.Error(err))
				return
			} else if locked {
				b.Log(locked)
				if err := sema.Unlock(lockCtx); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

func TestSemaphore_Unlock_WithoutLock_NoPanic(t *testing.T) {
	rdb := redis.NewClient(testRedisOptions())
	rtils := NewRedisUtils(rdb)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sema, err := rtils.NewSemaphore("unlock-without-lock", 1)
	if err != nil {
		t.Fatalf("new semaphore: %+v", err)
	}

	if err = sema.Unlock(ctx); err != nil {
		t.Fatalf("unlock without lock should not panic, got error: %+v", err)
	}
}
