package redis

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	redisclient "github.com/redis/go-redis/v9"
	"strings"
	"sync"
	"testing"
	"time"
)

type prefixCommandProbe struct {
	mu              sync.Mutex
	scans           int
	largestMGet     int
	finalCursor     uint64
	cancelAfterScan context.CancelFunc
	failReads       bool
}

func (h *prefixCommandProbe) DialHook(next redisclient.DialHook) redisclient.DialHook { return next }
func (h *prefixCommandProbe) ProcessPipelineHook(next redisclient.ProcessPipelineHook) redisclient.ProcessPipelineHook {
	return next
}
func (h *prefixCommandProbe) ProcessHook(next redisclient.ProcessHook) redisclient.ProcessHook {
	return func(ctx context.Context, cmd redisclient.Cmder) error {
		if h.failReads && (cmd.Name() == "mget" || cmd.Name() == "eval" || cmd.Name() == "evalsha") {
			err := fmt.Errorf("synthetic read failure")
			cmd.SetErr(err)
			return err
		}
		err := next(ctx, cmd)
		h.mu.Lock()
		defer h.mu.Unlock()
		if cmd.Name() == "mget" && len(cmd.Args())-1 > h.largestMGet {
			h.largestMGet = len(cmd.Args()) - 1
		}
		if scan, ok := cmd.(*redisclient.ScanCmd); ok {
			_, cursor, _ := scan.Result()
			h.scans++
			h.finalCursor = cursor
			if h.cancelAfterScan != nil {
				h.cancelAfterScan()
				h.cancelAfterScan = nil
			}
		}
		return err
	}
}
func prefixTestClient(t *testing.T) (*Utils, *redisclient.Client, context.Context, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	client := redisclient.NewClient(testRedisOptions())
	t.Cleanup(func() { client.Close() })
	return NewRedisUtils(client), client, ctx, "security-prefix/" + uuid.NewString() + "/"
}
func TestPrefixLookupLiteralBoundary(t *testing.T) {
	for _, suffix := range []string{"*", "tenant*/", "tenant?/", "tenant[a]/", "tenant]/", "tenant\\/"} {
		t.Run(suffix, func(t *testing.T) {
			utils, client, ctx, root := prefixTestClient(t)
			prefix := root + suffix
			if suffix == "*" {
				prefix = "*"
			}
			allowed := prefix + root + "allowed"
			sibling := root + "tenantx/sibling"
			classSibling := root + "tenanta/sibling"
			plainSibling := root + "tenant/sibling"
			keys := []string{allowed, sibling, classSibling, plainSibling}
			for _, key := range keys {
				if err := client.Set(ctx, key, "synthetic", 0).Err(); err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(func() { client.Del(context.Background(), keys...) })
			got, err := utils.GetItemWithPrefix(ctx, prefix)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 || got[allowed] != "synthetic" {
				t.Fatalf("literal prefix crossed boundary: got %d items, expected one literal match", len(got))
			}
			for key := range got {
				if !strings.HasPrefix(key, prefix) {
					t.Fatal("non-literal key returned")
				}
			}
		})
	}
}
func TestPrefixLookupDefaultBudgets(t *testing.T) {
	t.Run("key cap", func(t *testing.T) {
		utils, client, ctx, prefix := prefixTestClient(t)
		keys := make([]string, 1001)
		for i := range keys {
			keys[i] = fmt.Sprintf("%s%04d", prefix, i)
			if err := client.Set(ctx, keys[i], "v", 0).Err(); err != nil {
				t.Fatal(err)
			}
		}
		t.Cleanup(func() { client.Del(context.Background(), keys...) })
		probe := &prefixCommandProbe{}
		client.AddHook(probe)
		got, err := utils.GetItemWithPrefix(ctx, prefix)
		probe.mu.Lock()
		defer probe.mu.Unlock()
		t.Logf("scan calls=%d final cursor=%d largest MGET=%d returned=%d", probe.scans, probe.finalCursor, probe.largestMGet, len(got))
		if err == nil || got != nil {
			t.Fatalf("over-cap lookup must fail without returning partial data; got=%d err=%v", len(got), err)
		}
	})
	t.Run("byte cap", func(t *testing.T) {
		utils, client, ctx, prefix := prefixTestClient(t)
		key := prefix + "large"
		if err := client.Set(ctx, key, strings.Repeat("x", (1<<20)+1), 0).Err(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { client.Del(context.Background(), key) })
		got, err := utils.GetItemWithPrefix(ctx, prefix)
		if err == nil || got != nil {
			t.Fatalf("over-byte-budget lookup returned %d items without error", len(got))
		}
	})
	t.Run("read errors do not expose keys", func(t *testing.T) {
		utils, client, ctx, prefix := prefixTestClient(t)
		key := prefix + "private-synthetic-key"
		if err := client.Set(ctx, key, "v", 0).Err(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { client.Del(context.Background(), key) })
		client.AddHook(&prefixCommandProbe{failReads: true})
		_, err := utils.GetItemWithPrefix(ctx, prefix)
		if err == nil {
			t.Fatal("expected injected read error")
		}
		if strings.Contains(err.Error(), key) {
			t.Fatal("read error disclosed matched key")
		}
	})
}
