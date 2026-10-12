package redis

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"sync"
	"testing"
)

func TestRPushHardLimit(t *testing.T) {
	for _, tc := range []struct {
		name     string
		existing int
		payload  int
		trim     int64
		reject   bool
	}{
		{"oversized batch", 0, 6, 2, true},
		{"crossing cap", 3, 2, 2, false},
		{"at cap", 4, 1, 2, false},
		{"oversized existing", 6, 1, 2, false},
		{"invalid trim", 0, 1, 5, true},
		{"empty payload compacts oversized list", 6, 0, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			utils, client, ctx, _ := prefixTestClient(t)
			key := "security-list/" + uuid.NewString()
			t.Cleanup(func() { client.Del(context.Background(), key) })
			for i := 0; i < tc.existing; i++ {
				if err := client.RPush(ctx, key, fmt.Sprintf("old-%d", i)).Err(); err != nil {
					t.Fatal(err)
				}
			}
			before, err := client.LRange(ctx, key, 0, -1).Result()
			if err != nil {
				t.Fatal(err)
			}
			payload := make([]interface{}, tc.payload)
			for i := range payload {
				payload[i] = fmt.Sprintf("new-%d", i)
			}
			err = utils.RPush(ctx, key, payload, utils.WithMaxLength(4), utils.WithTrimSize(tc.trim), utils.WithForceCheck())
			after, readErr := client.LRange(ctx, key, 0, -1).Result()
			if readErr != nil {
				t.Fatal(readErr)
			}
			if tc.reject {
				if err == nil || fmt.Sprint(after) != fmt.Sprint(before) {
					t.Fatalf("rejected append mutated list: before=%v after=%v err=%v", before, after, err)
				}
				return
			}
			if err != nil || len(after) > 4 {
				t.Fatalf("completed append exceeds cap: length=%d err=%v", len(after), err)
			}
			if tc.payload > 0 && after[len(after)-1] != fmt.Sprintf("new-%d", tc.payload-1) {
				t.Fatal("newest payload lost")
			}
		})
	}
}
func TestRPushConcurrentHardLimit(t *testing.T) {
	utils, client, ctx, _ := prefixTestClient(t)
	key := "security-list/" + uuid.NewString()
	t.Cleanup(func() { client.Del(context.Background(), key) })
	start := make(chan struct{})
	problems := make(chan error, 8)
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for i := 0; i < 8; i++ {
				err := utils.RPush(ctx, key, []interface{}{fmt.Sprintf("%d/%d", worker, i)}, utils.WithMaxLength(4), utils.WithTrimSize(2), utils.WithForceCheck())
				if err != nil {
					problems <- err
					return
				}
				length, err := client.LLen(ctx, key).Result()
				if err != nil {
					problems <- err
					return
				}
				if length > 4 {
					problems <- fmt.Errorf("completed operation observed length %d", length)
					return
				}
			}
		}(worker)
	}
	close(start)
	wg.Wait()
	close(problems)
	for err := range problems {
		t.Error(err)
	}
}
func TestRPushWrongTypeDoesNotMutate(t *testing.T) {
	utils, client, ctx, _ := prefixTestClient(t)
	key := "security-list/" + uuid.NewString()
	t.Cleanup(func() { client.Del(context.Background(), key) })
	if err := client.Set(ctx, key, "sentinel", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := utils.RPush(ctx, key, []interface{}{"value"}, utils.WithForceCheck()); err == nil {
		t.Fatal("wrong type accepted")
	}
	if value, err := client.Get(ctx, key).Result(); err != nil || value != "sentinel" {
		t.Fatal("wrong-type key mutated")
	}
}
