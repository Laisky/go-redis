package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	gutils "github.com/Laisky/go-utils"
	"github.com/go-redis/redis/v8"
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
