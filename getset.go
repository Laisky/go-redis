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
	"github.com/pkg/errors"
)

// GetItem get item from redis
func (u *Utils) GetItem(ctx context.Context, key string) (string, error) {
	u.logger.Debug("get redis item", zap.String("key", key))
	return u.Client.Get(ctx, key).Result()
}

type getItemBlockingOption struct {
	del bool
}

// GetItemBlockingOptionFunc optional arguments for GetItemBlocking
type GetItemBlockingOptionFunc func(*getItemBlockingOption) error

// WithGetItemBlockingDel delete after get
func (u *Utils) WithGetItemBlockingDel(del bool) GetItemBlockingOptionFunc {
	return func(opt *getItemBlockingOption) error {
		opt.del = del
		return nil
	}
}

// GetItemBlocking get key blocking
//
// will delete key after get in default.
func (u *Utils) GetItemBlocking(ctx context.Context, dbkey string, opts ...GetItemBlockingOptionFunc) (string, error) {
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

		data, err := u.Client.Get(ctx, dbkey).Result()
		if err != nil {
			if IsNil(err) {
				time.Sleep(WaitDBKeyDuration)
				continue
			}

			return "", err
		}

		if opt.del {
			if err = u.Client.Del(ctx, dbkey).Err(); err != nil {
				u.logger.Error("del key", zap.String("dbkey", dbkey))
			}
		}

		return data, nil
	}

}

// SetItem set item
func (u *Utils) SetItem(ctx context.Context, key, val string, exp time.Duration) error {
	u.logger.Debug("put redis item", zap.String("key", key))
	return u.Client.Set(ctx, key, val, exp).Err()
}

// GetItemWithPrefix get item with prefix, return `map[key]: val`
func (u *Utils) GetItemWithPrefix(ctx context.Context, keyPrefix string) (map[string]string, error) {
	u.logger.Debug("get redis item with prefix", zap.String("key_prefix", keyPrefix))
	if keyPrefix == "" {
		return nil, fmt.Errorf("do not scan all keys")
	}

	var (
		err           error
		keys, newKeys []string
		cursor        uint64
	)
	for {
		if newKeys, cursor, err = u.Client.Scan(ctx, cursor, keyPrefix+"*", ScanCount).Result(); err != nil {
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

	res, err := u.Client.MGet(ctx, keys...).Result()
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
func (u *Utils) LPopKeysBlocking(ctx context.Context, keys ...string) (key, val string, err error) {
	for {
		select {
		case <-ctx.Done():
			return "", "", errors.Wrapf(ctx.Err(), "lpop `%v`", keys)
		default:
		}

		for _, key = range keys {
			if val, err = u.Client.LPop(ctx, key).Result(); err != nil {
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
func (u *Utils) RPush(ctx context.Context, key string, payloads ...interface{}) (err error) {
	var length int64
	if rand.Intn(100) == 0 {
		if length, err = u.Client.LLen(ctx, key).Result(); err != nil {
			return errors.Wrapf(err, "get len `%s`", key)
		}
	}

	if length >= 100 {
		if err = u.Client.LTrim(ctx, key, -10, -1).Err(); err != nil {
			u.logger.Error("trim", zap.String("key", key), zap.Error(err))
		}

		u.logger.Info("trim array", zap.String("key", key))
	}

	return u.Client.RPush(ctx, key, payloads...).Err()
}
