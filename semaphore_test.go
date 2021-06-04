package redis

import (
	"context"
	"testing"
	"time"

	gutils "github.com/Laisky/go-utils"
	"github.com/Laisky/zap"
	"github.com/go-redis/redis/v8"
)

func TestSemaphore_Lock(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{})
	rtils := NewRedisUtils(rdb)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	sema1, err := rtils.NewSemaphore("laisky", 2)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	// client 1
	if locked1, _, err := sema1.Lock(ctx); err != nil {
		t.Fatalf("%+v", err)
	} else if !locked1 {
		t.Fatal("should locked")
	}

	sema2, err := rtils.NewSemaphore("laisky", 2)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	// client 2
	if locked2, _, err := sema2.Lock(ctx); err != nil {
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

	if err = sema2.Unlock(ctx); err != nil {
		t.Fatal("should not locked")
	}

	if locked3, _, err := sema3.Lock(ctx); err != nil {
		t.Fatalf("%+v", err)
	} else if !locked3 {
		t.Fatal("should  locked")
	}
}

func TestSemaphore_race(t *testing.T) {
	run := func(ctx context.Context, s *Semaphore) {
		if locked, _, err := s.Lock(ctx); err != nil {
			t.Fatalf("%+v", err)
		} else if locked {
			if err = s.Unlock(ctx); err != nil {
				t.Fatalf("%+v", err)
			}
		}
	}

	for i := 0; i < 10; i++ {
		go func() {
			rdb := redis.NewClient(&redis.Options{})
			rtils := NewRedisUtils(rdb)

			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()

			sema, err := rtils.NewSemaphore("laisky", 2)
			if err != nil {
				gutils.Logger.Panic("new semaphore", zap.Error(err))
			}

			for i := 0; i < 1000; i++ {
				run(ctx, sema)
			}
		}()
	}
}

func BenchmarkSemaphore(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		rdb := redis.NewClient(&redis.Options{})
		rtils := NewRedisUtils(rdb)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()

		sema, err := rtils.NewSemaphore("laisky", 2)
		if err != nil {
			logger.Panic("new", zap.Error(err))
			return
		}

		for pb.Next() {
			if locked, _, err := sema.Lock(ctx); err != nil {
				logger.Panic("lock", zap.Error(err))
				return
			} else if locked {
				b.Log(locked)
				// TODO
				// ...
			}
		}
	})
}
