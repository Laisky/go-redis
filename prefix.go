package redis

import (
	"context"
	stderrors "errors"
	"fmt"
	redisclient "github.com/redis/go-redis/v9"
	"strings"
	"time"
)

// ErrPrefixLimit means the complete result could not fit the lookup budget.
// Limit failures return no partial results.
var ErrPrefixLimit = stderrors.New("prefix lookup limit exceeded")

// PrefixLookupLimits bounds a complete literal-prefix lookup. Zero fields use
// defaults: 1,000 distinct keys, 1 MiB of key/value bytes, 1,000 SCAN calls,
// 16 keys per value batch, and a five-second deadline.
type PrefixLookupLimits struct {
	MaxKeys      int
	MaxBytes     int
	MaxScanCalls int
	BatchKeys    int
	Timeout      time.Duration
}

func (limits PrefixLookupLimits) normalized() (PrefixLookupLimits, error) {
	if limits.MaxKeys < 0 || limits.MaxBytes < 0 || limits.MaxScanCalls < 0 || limits.BatchKeys < 0 || limits.Timeout < 0 {
		return limits, fmt.Errorf("prefix lookup budgets must not be negative")
	}
	if limits.MaxKeys == 0 {
		limits.MaxKeys = 1000
	}
	if limits.MaxBytes == 0 {
		limits.MaxBytes = 1 << 20
	}
	if limits.MaxScanCalls == 0 {
		limits.MaxScanCalls = 1000
	}
	if limits.BatchKeys == 0 {
		limits.BatchKeys = 16
	}
	if limits.Timeout == 0 {
		limits.Timeout = 5 * time.Second
	}
	if limits.BatchKeys > 64 || limits.MaxBytes > 64<<20 {
		return limits, fmt.Errorf("prefix value batches exceed supported safety limits")
	}
	if limits.BatchKeys > limits.MaxKeys {
		limits.BatchKeys = limits.MaxKeys
	}
	return limits, nil
}

func literalRedisPrefix(prefix string) string {
	var pattern strings.Builder
	for i := 0; i < len(prefix); i++ {
		switch prefix[i] {
		case '*', '?', '[', ']', 92:
			pattern.WriteByte(92)
		}
		pattern.WriteByte(prefix[i])
	}
	pattern.WriteByte('*')
	return pattern.String()
}

var boundedPrefixValues = redisclient.NewScript(`local remaining = tonumber(ARGV[1])
local result = {}
for _, key in ipairs(KEYS) do
  if redis.call('TYPE', key).ok == 'string' then
    local value = redis.call('GETRANGE', key, 0, remaining)
    if string.len(value) > remaining then
      return redis.error_reply('PREFIX_BYTE_LIMIT')
    end
    remaining = remaining - string.len(value)
    result[#result + 1] = value
  else
    result[#result + 1] = false
  end
end
return result
`)

// GetItemWithPrefixLimited reads only keys that start with the exact literal
// prefix. SCAN's COUNT remains a Redis hint; limits bound retained results,
// total scan calls and value responses, not the Redis server's key-scan internals.
// Namespace indexing/ACLs and embedding-level rate limits remain caller duties.
func (u *Utils) GetItemWithPrefixLimited(ctx context.Context, prefix string, limits PrefixLookupLimits) (map[string]string, error) {
	if prefix == "" {
		return nil, fmt.Errorf("do not scan all keys")
	}
	limits, err := limits.normalized()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, limits.Timeout)
	defer cancel()
	items := make(map[string]string)
	seen := make(map[string]struct{})
	usedBytes := 0
	batch := make([]string, 0, limits.BatchKeys)
	readBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		raw, err := boundedPrefixValues.Run(ctx, u.Client, batch, limits.MaxBytes-usedBytes).Slice()
		if err != nil {
			if strings.Contains(err.Error(), "PREFIX_BYTE_LIMIT") {
				return fmt.Errorf("%w: bytes", ErrPrefixLimit)
			}
			return fmt.Errorf("read prefix batch: %w", err)
		}
		if len(raw) != len(batch) {
			return fmt.Errorf("invalid prefix batch response")
		}
		for i, value := range raw {
			if value == nil {
				continue
			}
			text, ok := value.(string)
			if !ok {
				return fmt.Errorf("invalid prefix value type")
			}
			if len(text) > limits.MaxBytes-usedBytes {
				return fmt.Errorf("%w: bytes", ErrPrefixLimit)
			}
			usedBytes += len(text)
			items[batch[i]] = text
		}
		batch = batch[:0]
		return nil
	}
	var cursor uint64
	for scans := 0; ; scans++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if scans == limits.MaxScanCalls {
			return nil, fmt.Errorf("%w: scan work", ErrPrefixLimit)
		}
		keys, next, err := u.Client.Scan(ctx, cursor, literalRedisPrefix(prefix), ScanCount).Result()
		if err != nil {
			return nil, fmt.Errorf("scan prefix: %w", err)
		}
		for _, key := range keys {
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			if _, exists := seen[key]; exists {
				continue
			}
			if len(seen) == limits.MaxKeys {
				return nil, fmt.Errorf("%w: keys", ErrPrefixLimit)
			}
			if len(key) > limits.MaxBytes-usedBytes {
				return nil, fmt.Errorf("%w: bytes", ErrPrefixLimit)
			}
			seen[key] = struct{}{}
			usedBytes += len(key)
			batch = append(batch, key)
			if len(batch) == limits.BatchKeys {
				if err := readBatch(); err != nil {
					return nil, err
				}
			}
		}
		if err := readBatch(); err != nil {
			return nil, err
		}
		cursor = next
		if cursor == 0 {
			return items, nil
		}
	}
}
