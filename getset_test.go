package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	gutils "github.com/Laisky/go-utils/v5"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestGetSet(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{})
	rtils := NewRedisUtils(rdb)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dbkey := gutils.RandomStringWithLength(20)

	if _, err := rtils.GetItem(ctx, dbkey); err != nil {
		if !IsNil(err) {
			require.NoError(t, err)
		}
	}

	val := gutils.RandomStringWithLength(64)
	err := rtils.SetItem(ctx, dbkey, val, KeyExpImmortal)
	require.NoError(t, err)

	gotVal, err := rtils.GetItem(ctx, dbkey)
	require.NoError(t, err)
	require.Equal(t, val, gotVal)

	time.Sleep(1 * time.Second)
	if _, err := rtils.GetItem(ctx, dbkey); err != nil {
		if !IsNil(err) {
			require.NoError(t, err)
		}
	}
}

// get item and delelte
func TestUtils_GetItemBlockingWithDelete(t *testing.T) {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{})
	rtils := NewRedisUtils(rdb)

	dbkey := "/TestUtils_GetItemBlockingWithDelete"
	val := gutils.RandomStringWithLength(64)

	err := rtils.Set(ctx, dbkey, val, KeyExpImmortal).Err()
	require.NoError(t, err)

	gotVal, err := rtils.GetItemBlocking(ctx, dbkey)
	require.NoError(t, err)
	require.Equal(t, val, gotVal)

	gotVal, err = rtils.Get(ctx, dbkey).Result()
	require.NoError(t, err)
	require.Equal(t, val, gotVal)

	t.Run("get and delete", func(t *testing.T) {
		gotVal, err := rtils.GetItemBlocking(ctx, dbkey)
		require.NoError(t, err)
		require.Equal(t, val, gotVal)

		gotVal, err = rtils.GetItemBlocking(ctx, dbkey, rtils.WithDel())
		require.NoError(t, err)
		require.Equal(t, val, gotVal)

		gotVal, err = rtils.Get(ctx, dbkey).Result()
		require.ErrorContains(t, err, "redis: nil")
	})
}

func TestUtils_GetItemWithPrefix(t *testing.T) {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{})
	rtils := NewRedisUtils(rdb)

	// Generate a unique prefix for this test run
	prefix := "/test-prefix-" + gutils.RandomStringWithLength(8)

	// Create test data - multiple keys with the same prefix
	testData := make(map[string]string)
	for i := 0; i < 5; i++ {
		key := prefix + "/" + gutils.RandomStringWithLength(8)
		val := gutils.RandomStringWithLength(16)
		testData[key] = val

		err := rtils.SetItem(ctx, key, val, KeyExpImmortal)
		require.NoError(t, err)
	}

	// Also create some keys that don't match the prefix
	for i := 0; i < 3; i++ {
		key := "/different-prefix-" + gutils.RandomStringWithLength(8)
		val := gutils.RandomStringWithLength(16)

		err := rtils.SetItem(ctx, key, val, KeyExpImmortal)
		require.NoError(t, err)
	}

	// Call the GetItemWithPrefix function
	results, err := rtils.GetItemWithPrefix(ctx, prefix)
	require.NoError(t, err)

	// Verify results
	require.Equal(t, len(testData), len(results), "Should get exactly the same number of items we set")

	// Check each expected key is present with the correct value
	for key, expectedVal := range testData {
		actualVal, exists := results[key]
		require.True(t, exists, "Key %s should exist in results", key)
		require.Equal(t, expectedVal, actualVal, "Value for key %s should match", key)
	}

	// Test empty prefix case
	_, err = rtils.GetItemWithPrefix(ctx, "")
	require.Error(t, err, "Empty prefix should return an error")
	require.Contains(t, err.Error(), "do not scan all keys")

	// Clean up
	for key := range testData {
		_ = rtils.Client.Del(ctx, key).Err()
	}
}

func TestUtils_RPush_Truncation(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{})
	rtils := NewRedisUtils(rdb)

	// Generate random key for this test
	key := "/test-rpush-truncate-" + gutils.RandomStringWithLength(8)

	// Clean up before and after test
	defer func() {
		_ = rdb.Del(ctx, key).Err()
	}()

	// Add 30 items
	for i := 0; i < 30; i++ {
		val := fmt.Sprintf("val-%d", i)
		err := rtils.RPush(ctx, key, []interface{}{val})
		require.NoError(t, err)
	}

	// Create custom settings for this test - max length 20, trim to last 5 items
	err := rtils.RPush(ctx, key, []interface{}{"final-item"},
		rtils.WithMaxLength(20),
		rtils.WithTrimSize(5),
		rtils.WithForceCheck())
	require.NoError(t, err)

	// Verify the list was truncated (should contain only last 5 items plus the new one)
	length, err := rdb.LLen(ctx, key).Result()
	require.NoError(t, err)
	require.LessOrEqual(t, length, int64(6), "List should be truncated")

	// Check the items are the most recent ones
	items, err := rdb.LRange(ctx, key, 0, -1).Result()
	require.NoError(t, err)

	// Should have the 5 most recent items plus our new item
	require.Equal(t, 6, len(items), "Should have trimSize (5) + 1 new item")

	// Last item should be our final item
	require.Equal(t, "final-item", items[len(items)-1])

	// First item should be val-25 (items 0-24 were trimmed, 25-29 remain)
	require.Equal(t, "val-25", items[0])
}

func TestPopPush(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{})
	rtils := NewRedisUtils(rdb)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dbkey := gutils.RandomStringWithLength(20)

	val := gutils.RandomStringWithLength(64)
	err := rtils.RPush(ctx, dbkey, []interface{}{val})
	require.NoError(t, err)

	// Rest of the test remains the same
	k, gotVal, err := rtils.LPopKeysBlocking(ctx, dbkey)
	require.NoError(t, err)
	require.Equal(t, dbkey, k)
	require.Equal(t, val, gotVal)

	// Rest of the function...
}

func TestUtils_LPopKeysBlocking_MultipleKeys(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{})
	rtils := NewRedisUtils(rdb)

	// Generate random keys
	key1 := "/test-lpop-1-" + gutils.RandomStringWithLength(8)
	key2 := "/test-lpop-2-" + gutils.RandomStringWithLength(8)
	key3 := "/test-lpop-3-" + gutils.RandomStringWithLength(8)

	// Clean up before/after test
	defer func() {
		_ = rdb.Del(ctx, key1, key2, key3).Err()
	}()

	// Test with second key having value
	val2 := "value-in-key2"
	err := rdb.RPush(ctx, key2, val2).Err()
	require.NoError(t, err)

	// This should pop from key2 since key1 is empty
	key, val, err := rtils.LPopKeysBlocking(ctx, key1, key2, key3)
	require.NoError(t, err)
	require.Equal(t, key2, key)
	require.Equal(t, val2, val)

	// Test with only third key having value
	val3 := "value-in-key3"
	err = rdb.RPush(ctx, key3, val3).Err()
	require.NoError(t, err)

	// This should pop from key3 since key1 and key2 are empty
	key, val, err = rtils.LPopKeysBlocking(ctx, key1, key2, key3)
	require.NoError(t, err)
	require.Equal(t, key3, key)
	require.Equal(t, val3, val)
}

func TestUtils_ContextCancellation(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{})
	rtils := NewRedisUtils(rdb)

	t.Run("GetItemBlocking respects context", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		// Try to get a non-existent key - should timeout
		key := "/non-existent-key-" + gutils.RandomStringWithLength(8)
		_, err := rtils.GetItemBlocking(ctx, key)
		require.Error(t, err)
		require.Contains(t, err.Error(), context.DeadlineExceeded.Error())
	})

	t.Run("LPopKeysBlocking respects context", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		// Try to pop from empty keys - should timeout
		key := "/non-existent-key-" + gutils.RandomStringWithLength(8)
		_, _, err := rtils.LPopKeysBlocking(ctx, key)
		require.Error(t, err)
		require.Contains(t, err.Error(), context.DeadlineExceeded.Error())
	})
}

func TestUtils_GetItemWithPrefix_EdgeCases(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{})
	rtils := NewRedisUtils(rdb)

	t.Run("handle different value types", func(t *testing.T) {
		// Generate prefix
		prefix := "/test-types-" + gutils.RandomStringWithLength(8)

		// Create keys with different value types
		key1 := prefix + "/string"
		key2 := prefix + "/int"

		// Set different types
		err := rtils.Client.Set(ctx, key1, "string-value", 0).Err()
		require.NoError(t, err)

		err = rtils.Client.Set(ctx, key2, 12345, 0).Err()
		require.NoError(t, err)

		// Get items with prefix
		results, err := rtils.GetItemWithPrefix(ctx, prefix)
		require.NoError(t, err)
		require.Equal(t, 2, len(results))

		// Check string values
		require.Equal(t, "string-value", results[key1])
		require.Equal(t, "12345", results[key2])

		// Clean up
		_ = rtils.Client.Del(ctx, key1, key2).Err()
	})

	t.Run("handle empty result", func(t *testing.T) {
		// Generate unique prefix that shouldn't match anything
		prefix := "/no-such-prefix-" + gutils.RandomStringWithLength(16)

		results, err := rtils.GetItemWithPrefix(ctx, prefix)
		require.NoError(t, err)
		require.Empty(t, results, "Should return empty map when no keys match")
	})
}

func TestUtils_GetItemBlocking_Error(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{})
	rtils := NewRedisUtils(rdb)

	t.Run("transaction failure recovery", func(t *testing.T) {
		// Set up a key that will be used in the test
		key := "/test-transaction-" + gutils.RandomStringWithLength(8)
		value := "test-value"

		err := rtils.SetItem(ctx, key, value, KeyExpImmortal)
		require.NoError(t, err)

		// Create a goroutine that will modify the key while we're watching it
		go func() {
			time.Sleep(10 * time.Millisecond) // Short delay
			rdb.Set(ctx, key, "modified-value", 0)
		}()

		// Try to get and delete the key - should succeed despite the concurrent modification
		gotVal, err := rtils.GetItemBlocking(ctx, key, rtils.WithDel())
		require.NoError(t, err)
		require.Contains(t, []string{value, "modified-value"}, gotVal)

		// Verify key is deleted
		exists, err := rdb.Exists(ctx, key).Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), exists)
	})
}
