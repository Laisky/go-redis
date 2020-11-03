package redis

import (
	"context"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"golang.org/x/sync/errgroup"
)

func TestUtils_NewMutex_lock(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{})
	rtils := NewRedisUtils(rdb)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

	if ok, _, err = mu.Lock(ctx); err != nil {
		t.Fatalf("%+v", err)
	} else if ok {
		t.Fatalf("should be not ok")
	}

	time.Sleep(10 * time.Second)

	if err = ctxLock.Err(); err != nil {
		t.Fatalf("lock released")
	}

	if ok, _, err = mu.Lock(ctx); err != nil {
		t.Fatalf("%+v", err)
	} else if ok {
		t.Fatalf("should be not ok")
	}

	cancel()
	if err = ctxLock.Err(); err == nil {
		t.Fatal("ctx should exit")
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

		if err = mu.Unlock(); err != nil {
			t.Fatalf("not ok")
		}
	}
}

// BenchmarkUtils_NewMutex_unlock-8   	    2824	   2396695 ns/op	    8900 B/op	     195 allocs/op
func BenchmarkUtils_NewMutex_unlock(b *testing.B) {
	rdb := redis.NewClient(&redis.Options{})
	rtils := NewRedisUtils(rdb)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mu, err := rtils.NewMutex("laisky")
	if err != nil {
		b.Fatalf("%+v", err)
	}

	for i := 0; i < 10; i++ {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if rand.Intn(2) == 0 {
					if _, _, err := mu.Lock(ctx); err != nil {
						b.Fatalf("%+v", err)
					}
				} else {
					if err := mu.Unlock(); err != nil {
						b.Fatalf("not ok: %+v", err)
					}
				}
			}
		})
	}
}

func TestUtils_NewMutex_race(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{})
	rtils := NewRedisUtils(rdb)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mu, err := rtils.NewMutex("laisky")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	var locked int32
	var pool errgroup.Group
	for i := 0; i < 10; i++ {
		pool.Go(func() error {
			if _, _, err := mu.Lock(ctx); err != nil {
				t.Fatalf("%+v", err)
			}

			if got := atomic.AddInt32(&locked, 1); got > 1 {
				t.Fatalf("locked %d", got)
			}

			if got := atomic.AddInt32(&locked, -1); got < 0 {
				t.Fatalf("locked %d", got)
			}

			time.Sleep(time.Duration(100+rand.Intn(300)) * time.Millisecond)
			if err := mu.Unlock(); err != nil {
				t.Fatalf("%+v", err)
			}

			return nil
		})
	}

	_ = pool.Wait()
}
