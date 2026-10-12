package redis

import (
	"context"
	"errors"
	"fmt"
	redisclient "github.com/redis/go-redis/v9"
	"strconv"
	"strings"
	"testing"
)

type prefixBatchProbe struct {
	prefixCommandProbe
	maxBatch int
}

func (h *prefixBatchProbe) ProcessHook(next redisclient.ProcessHook) redisclient.ProcessHook {
	return h.prefixCommandProbe.ProcessHook(func(ctx context.Context, cmd redisclient.Cmder) error {
		if cmd.Name() == "eval" || cmd.Name() == "evalsha" {
			n, _ := strconv.Atoi(fmt.Sprint(cmd.Args()[2]))
			h.mu.Lock()
			if n > h.maxBatch {
				h.maxBatch = n
			}
			h.mu.Unlock()
		}
		return next(ctx, cmd)
	})
}
func TestPrefixLookupLimitedBudgets(t *testing.T) {
	utils, client, ctx, prefix := prefixTestClient(t)
	keys := make([]string, 64)
	for i := range keys {
		keys[i] = fmt.Sprintf("%s%03d", prefix, i)
		if err := client.Set(ctx, keys[i], "v", 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { client.Del(context.Background(), keys...) })
	probe := &prefixBatchProbe{}
	client.AddHook(probe)
	got, err := utils.GetItemWithPrefixLimited(ctx, prefix, PrefixLookupLimits{MaxKeys: 64, BatchKeys: 2})
	if err != nil || len(got) != 64 {
		t.Fatalf("bounded normal lookup=%d %v", len(got), err)
	}
	probe.mu.Lock()
	batch, mget := probe.maxBatch, probe.largestMGet
	probe.mu.Unlock()
	if batch > 2 || mget != 0 {
		t.Fatalf("unbounded value command: Lua keys=%d MGET=%d", batch, mget)
	}
	got, err = utils.GetItemWithPrefixLimited(ctx, prefix, PrefixLookupLimits{MaxKeys: 3})
	if !errors.Is(err, ErrPrefixLimit) || got != nil {
		t.Fatalf("key cap returned partial data or wrong error: %v", err)
	}
	got, err = utils.GetItemWithPrefixLimited(ctx, prefix+"missing/", PrefixLookupLimits{MaxScanCalls: 1})
	if !errors.Is(err, ErrPrefixLimit) || got != nil {
		t.Fatalf("scan-work cap not enforced: %v", err)
	}
	got, err = utils.GetItemWithPrefixLimited(ctx, prefix, PrefixLookupLimits{MaxBytes: 1})
	if !errors.Is(err, ErrPrefixLimit) || got != nil {
		t.Fatalf("key-byte budget not enforced: %v", err)
	}
	for _, limits := range []PrefixLookupLimits{{MaxKeys: -1}, {MaxBytes: -1}, {MaxScanCalls: -1}, {BatchKeys: 65}, {Timeout: -1}} {
		if _, err := utils.GetItemWithPrefixLimited(ctx, prefix, limits); err == nil {
			t.Fatal("invalid limits accepted")
		}
	}
}
func TestPrefixLookupCancellationBetweenScans(t *testing.T) {
	utils, client, ctx, prefix := prefixTestClient(t)
	keys := make([]string, 64)
	for i := range keys {
		keys[i] = fmt.Sprintf("%s%d", prefix, i)
		if err := client.Set(ctx, keys[i], "v", 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { client.Del(context.Background(), keys...) })
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	probe := &prefixCommandProbe{cancelAfterScan: cancel}
	client.AddHook(probe)
	got, err := utils.GetItemWithPrefix(ctx, prefix)
	if !errors.Is(err, context.Canceled) || got != nil {
		t.Fatalf("cancelled lookup returned partial data: %v", err)
	}
	probe.mu.Lock()
	defer probe.mu.Unlock()
	if probe.scans != 1 || probe.largestMGet != 0 {
		t.Fatalf("work continued after cancellation: scans=%d", probe.scans)
	}
}
func TestPrefixLookupStringCompatibility(t *testing.T) {
	utils, client, ctx, prefix := prefixTestClient(t)
	keys := []string{prefix + "empty", prefix + "integer", prefix + "list"}
	if err := client.Set(ctx, keys[0], "", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, keys[1], 12345, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.RPush(ctx, keys[2], "list-item").Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Del(context.Background(), keys...) })
	got, err := utils.GetItemWithPrefix(ctx, prefix)
	if err != nil || len(got) != 2 || got[keys[0]] != "" || got[keys[1]] != "12345" {
		t.Fatalf("string semantics changed: %+v %v", got, err)
	}
	if _, ok := got[keys[2]]; ok {
		t.Fatal("non-string included")
	}
	if _, err := utils.GetItemWithPrefix(ctx, ""); err == nil {
		t.Fatal("empty prefix accepted")
	}
	got, err = utils.GetItemWithPrefix(ctx, prefix+"absent/")
	if err != nil || len(got) != 0 {
		t.Fatal("empty result changed")
	}
}
func TestPrefixLookupGrowingValueRemainsBounded(t *testing.T) {
	utils, client, ctx, prefix := prefixTestClient(t)
	key := prefix + "grow"
	if err := client.Set(ctx, key, strings.Repeat("v", 1024), 0).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Del(context.Background(), key) })
	got, err := utils.GetItemWithPrefixLimited(ctx, prefix, PrefixLookupLimits{MaxBytes: 128})
	if !errors.Is(err, ErrPrefixLimit) || got != nil {
		t.Fatalf("value response budget not enforced: %v", err)
	}
}
