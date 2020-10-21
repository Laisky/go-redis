package redis

// =====================================
// Some simple get/set/pop/push utils
// =====================================

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/Laisky/zap"
	"github.com/go-redis/redis/v7"
	"github.com/pkg/errors"
)

// GetItem get item from redis
func GetItem(ctx context.Context, rdb *redis.Client, key string) (string, error) {
	GetLogger().Debug("get redis item", zap.String("key", key))
	return rdb.Get(key).Result()
}

type getItemBlockingOption struct {
	del bool
}

// GetItemBlockingOptionFunc optional arguments for GetItemBlocking
type GetItemBlockingOptionFunc func(*getItemBlockingOption) error

// WithGetItemBlockingDel delete after get
func WithGetItemBlockingDel(del bool) GetItemBlockingOptionFunc {
	return func(opt *getItemBlockingOption) error {
		opt.del = del
		return nil
	}
}

// GetItemBlocking get key blocking
//
// will delete key after get in default.
func GetItemBlocking(ctx context.Context, rdb *redis.Client, dbkey string, opts ...GetItemBlockingOptionFunc) (string, error) {
	opt := &getItemBlockingOption{
		del: true,
	}
	for _, optf := range opts {
		if err := optf(opt); err != nil {
			return "", err
		}
	}

	for {
		select {
		case <-ctx.Done():
			return "", errors.Wrapf(ctx.Err(), "get key `%s`", dbkey)
		default:
		}

		data, err := rdb.Get(dbkey).Result()
		if err != nil {
			if IsNil(err) {
				time.Sleep(WaitDBKeyDuration)
				continue
			}

			return "", err
		}

		if opt.del {
			if err = rdb.Del(dbkey).Err(); err != nil {
				GetLogger().Error("del key", zap.String("dbkey", dbkey))
			}
		}

		return data, nil
	}

}

// SetItem set item
func SetItem(ctx context.Context, rdb *redis.Client, key, val string, exp time.Duration) error {
	GetLogger().Debug("put redis item", zap.String("key", key))
	return rdb.Set(key, val, exp).Err()
}

// GetItemWithPrefix get item with prefix, return `map[key]: val`
func GetItemWithPrefix(ctx context.Context, rdb *redis.Client, keyPrefix string) (map[string]string, error) {
	GetLogger().Debug("get redis item with prefix", zap.String("key_prefix", keyPrefix))
	if keyPrefix == "" {
		return nil, fmt.Errorf("do not scan all keys")
	}

	var (
		err           error
		keys, newKeys []string
		cursor        uint64
	)
	for {
		if newKeys, cursor, err = rdb.Scan(cursor, keyPrefix+"*", ScanCount).Result(); err != nil {
			return nil, errors.Wrapf(err, "scan redis with key_prefix `%s`", keyPrefix)
		}

		keys = append(keys, newKeys...)
		if cursor == 0 {
			break
		}
	}

	item := make(map[string]string)
	if len(keys) == 0 {
		return item, nil
	}

	res, err := rdb.MGet(keys...).Result()
	if err != nil {
		return nil, errors.Wrapf(err, "mget keys `%v`", keys)
	}

	for i, v := range res {
		if v == nil {
			continue
		}

		item[keys[i]] = v.(string)
	}

	return item, nil
}

// LPopKeysBlocking LPop from mutiple keys
func LPopKeysBlocking(ctx context.Context, rdb *redis.Client, keys ...string) (key, val string, err error) {
	for {
		select {
		case <-ctx.Done():
			return "", "", errors.Wrapf(ctx.Err(), "lpop `%v`", keys)
		default:
		}

		for _, key = range keys {
			if val, err = rdb.LPop(key).Result(); err != nil {
				if !IsNil(err) {
					return "", "", errors.Wrapf(err, "lpop `%v`", keys)
				}

				continue
			}

			return key, val, nil
		}

		time.Sleep(WaitDBKeyDuration)
	}
}

// RPush rpush keys and truncate its length
//
// default max length is 100
func RPush(rdb *redis.Client, key string, payloads ...interface{}) (err error) {
	var length int64
	if rand.Intn(100) == 0 {
		if length, err = rdb.LLen(key).Result(); err != nil {
			return errors.Wrapf(err, "get len `%s`", key)
		}
	}

	if length >= 100 {
		if err = rdb.LTrim(key, -10, -1).Err(); err != nil {
			GetLogger().Error("trim", zap.String("key", key), zap.Error(err))
		}

		GetLogger().Info("trim array", zap.String("key", key))
	}

	return rdb.RPush(key, payloads...).Err()
}
