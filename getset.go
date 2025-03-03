package redis

// =====================================
// Some simple get/set/pop/push utils
// =====================================

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	gutils "github.com/Laisky/go-utils/v5"
	"github.com/Laisky/zap"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
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

// WithDel delete after get
func (u *Utils) WithDel() GetItemBlockingOptionFunc {
	return func(opt *getItemBlockingOption) error {
		opt.del = true
		return nil
	}
}

// GetItemBlocking get key blocking
//
// will delete key after get in default.
func (u *Utils) GetItemBlocking(ctx context.Context, dbkey string, opts ...GetItemBlockingOptionFunc) (data string, err error) {
	opt := &getItemBlockingOption{
		del: false,
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

		if !opt.del {
			if data, err = u.Client.Get(ctx, dbkey).Result(); err != nil {
				if IsNil(err) {
					gutils.SleepWithContext(ctx, WaitDBKeyDuration)
					continue
				}

				return "", err
			}

			return data, nil
		}

		err = u.Client.Watch(ctx, func(tx *redis.Tx) (err error) {
			if data, err = tx.Get(ctx, dbkey).Result(); err != nil {
				return errors.Wrapf(err, "get key `%s`", dbkey)
			}

			_, err = tx.TxPipelined(ctx, func(p redis.Pipeliner) (err error) {
				return p.Del(ctx, dbkey).Err()
			})
			if err != nil {
				return errors.Wrapf(err, "del key `%s`", dbkey)
			}

			return nil
		}, dbkey)

		if err != nil {
			// If it's a transaction failure, don't log and retry immediately
			if strings.Contains(err.Error(), "redis: transaction failed") {
				continue
			}

			gutils.SleepWithContext(ctx, WaitDBKeyDuration)
			continue
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

		// Convert value to string safely
		switch val := v.(type) {
		case string:
			item[keys[i]] = val
		case []byte:
			item[keys[i]] = string(val)
		default:
			// Convert any other type to string representation
			item[keys[i]] = fmt.Sprintf("%v", val)
		}
	}

	return item, nil
}

// LPopKeysBlocking LPop from mutiple keys
func (u *Utils) LPopKeysBlocking(ctx context.Context, keys ...string) (key, val string, err error) {
	for {
		select {
		case <-ctx.Done():
			return key, "", errors.Wrapf(ctx.Err(), "lpop `%v`", keys)
		default:
		}

		for _, key = range keys {
			if val, err = u.Client.LPop(ctx, key).Result(); err != nil {
				if !IsNil(err) {
					return key, "", errors.Wrapf(err, "lpop `%v`", keys)
				}

				continue
			}

			return key, val, nil
		}

		gutils.SleepWithContext(ctx, WaitDBKeyDuration)
	}
}

// RPush options
type rpushOption struct {
	maxLength  int64
	trimSize   int64
	forceCheck bool
}

// RPushOptionFunc optional arguments for RPush
type RPushOptionFunc func(*rpushOption) error

// WithMaxLength sets the maximum list length before truncating
func (u *Utils) WithMaxLength(maxLength int64) RPushOptionFunc {
	return func(opt *rpushOption) error {
		if maxLength <= 0 {
			return errors.New("maxLength must be positive")
		}
		opt.maxLength = maxLength
		return nil
	}
}

// WithTrimSize sets how many items to keep when truncating
func (u *Utils) WithTrimSize(size int64) RPushOptionFunc {
	return func(opt *rpushOption) error {
		if size <= 0 {
			return errors.New("trimSize must be positive")
		}
		opt.trimSize = size
		return nil
	}
}

// WithForceCheck forces a length check regardless of random chance
func (u *Utils) WithForceCheck() RPushOptionFunc {
	return func(opt *rpushOption) error {
		opt.forceCheck = true
		return nil
	}
}

// RPush rpush keys and truncate its length
//
// default max length is 100, default trim size is 10
func (u *Utils) RPush(ctx context.Context, key string, payloads []interface{}, opts ...RPushOptionFunc) (err error) {
	// Use default options
	opt := &rpushOption{
		maxLength:  100,
		trimSize:   10,
		forceCheck: false,
	}

	// Apply all option functions
	for _, optFunc := range opts {
		if err = optFunc(opt); err != nil {
			return errors.Wrap(err, "apply RPush option")
		}
	}

	var length int64
	if opt.forceCheck || rand.Intn(100) == 0 {
		if length, err = u.Client.LLen(ctx, key).Result(); err != nil {
			return errors.Wrapf(err, "get len `%s`", key)
		}

		if length >= opt.maxLength {
			if err = u.Client.LTrim(ctx, key, -opt.trimSize, -1).Err(); err != nil {
				u.logger.Error("trim", zap.String("key", key), zap.Error(err))
			}
			u.logger.Info("trim array",
				zap.String("key", key),
				zap.Int64("length", length),
				zap.Int64("max_length", opt.maxLength),
				zap.Int64("trim_size", opt.trimSize))
		}
	}

	if len(payloads) == 0 {
		return nil
	}

	return u.Client.RPush(ctx, key, payloads...).Err()
}
