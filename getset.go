package redis

// =====================================
// Some simple get/set/pop/push utils
// =====================================

import (
	"context"
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

// GetItemWithPrefix reads a complete literal-prefix result within conservative
// default key, byte, work and deadline budgets. Use GetItemWithPrefixLimited
// for explicit trusted limits. A budget failure returns no partial result.
func (u *Utils) GetItemWithPrefix(ctx context.Context, keyPrefix string) (map[string]string, error) {
	u.logger.Debug("get redis item with prefix", zap.String("key_prefix", keyPrefix))
	return u.GetItemWithPrefixLimited(ctx, keyPrefix, PrefixLookupLimits{})
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
	maxLength   int64
	trimSize    int64
	forceCheck  bool
	trimSizeSet bool
}

// RPushOptionFunc optional arguments for RPush
type RPushOptionFunc func(*rpushOption) error

// WithMaxLength sets the hard post-append list length limit.
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
		opt.trimSizeSet = true
		return nil
	}
}

// WithForceCheck is retained for compatibility; every RPush now enforces its limit.
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

	if opt.maxLength > (1<<53)-1 {
		return errors.New("maxLength exceeds exact Redis script range")
	}
	if opt.trimSize > opt.maxLength {
		if opt.trimSizeSet {
			return errors.New("trimSize must not exceed maxLength")
		}
		opt.trimSize = opt.maxLength
	}
	if int64(len(payloads)) > opt.maxLength {
		return errors.New("payload batch exceeds maxLength")
	}
	args := make([]interface{}, 0, len(payloads)+2)
	args = append(args, opt.maxLength, opt.trimSize)
	args = append(args, payloads...)
	if _, err = boundedRPush.Run(ctx, u.Client, []string{key}, args...).Result(); err != nil {
		return errors.Wrap(err, "append bounded Redis list")
	}
	return nil
}
