package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	gutils "github.com/Laisky/go-utils"
	"github.com/go-redis/redis/v7"
)

func TestGetSet(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dbkey := gutils.RandomStringWithLength(20)

	if _, err := GetItem(ctx, rdb, dbkey); err != nil {
		if !IsNil(err) {
			t.Fatalf("%+v", err)
		}
	}

	val := "4ij234j23l4"
	if err := SetItem(ctx, rdb, dbkey, val, KeyExpImmortal); err != nil {
		t.Fatalf("%+v", err)
	}
	if _, err := GetItem(ctx, rdb, dbkey); err != nil {
		t.Fatalf("%+v", err)
	}
	time.Sleep(1 * time.Second)
	if _, err := GetItem(ctx, rdb, dbkey); err != nil {
		if !IsNil(err) {
			t.Fatalf("%+v", err)
		}
	}
}

func TestPopPush(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dbkey := gutils.RandomStringWithLength(20)

	val := "4ij234j23l4"
	if err := RPush(rdb, dbkey, val); err != nil {
		t.Fatalf("%+v", err)
	}
	if k, v, err := LPopKeysBlocking(ctx, rdb, dbkey); err != nil {
		t.Fatalf("%+v", err)
	} else {
		if k != dbkey || v != val {
			t.Fatalf("got %v:%v", k, v)
		}
	}

	ctxTimeout, cancelTimeout := context.WithTimeout(ctx, 2*time.Second)
	defer cancelTimeout()

	if _, _, err := LPopKeysBlocking(ctxTimeout, rdb, dbkey); err != nil {
		if !IsNil(err) && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%+v", err)
		}
	}
}
