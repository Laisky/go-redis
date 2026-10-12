package redis

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	redisclient "github.com/redis/go-redis/v9"
	"sync"
	"testing"
	"time"
)

type admissionGapHook struct {
	once     sync.Once
	reserved chan struct{}
	proceed  chan struct{}
}

func (h *admissionGapHook) DialHook(next redisclient.DialHook) redisclient.DialHook { return next }
func (h *admissionGapHook) ProcessHook(next redisclient.ProcessHook) redisclient.ProcessHook {
	return next
}
func (h *admissionGapHook) ProcessPipelineHook(next redisclient.ProcessPipelineHook) redisclient.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redisclient.Cmder) error {
		err := next(ctx, cmds)
		if err != nil {
			return err
		}
		for _, cmd := range cmds {
			if cmd.Name() == "incr" {
				h.once.Do(func() {
					close(h.reserved)
					select {
					case <-h.proceed:
					case <-ctx.Done():
					}
				})
				break
			}
		}
		return nil
	}
}
func TestLeaseSemaphoreAdmissionAtomic(t *testing.T) {
	utils, client, ctx, _ := prefixTestClient(t)
	name := "admission-" + uuid.NewString()
	aClient := redisclient.NewClient(testRedisOptions())
	defer aClient.Close()
	hook := &admissionGapHook{reserved: make(chan struct{}), proceed: make(chan struct{})}
	aClient.AddHook(hook)
	a, err := NewRedisUtils(aClient).NewSemaphore(name, 1, WithSemaphoreBlockingLock(false))
	if err != nil {
		t.Fatal(err)
	}
	b, err := utils.NewSemaphore(name, 1, WithSemaphoreBlockingLock(false))
	if err != nil {
		t.Fatal(err)
	}
	s := b.(*semaphore)
	t.Cleanup(func() { client.Del(context.Background(), s.cids, s.owners, s.counter) })
	type result struct {
		locked bool
		ctx    context.Context
		err    error
	}
	aResult := make(chan result, 1)
	go func() { ok, lctx, err := a.Lock(ctx); aResult <- result{ok, lctx, err} }()
	var ar result
	forced := false
	select {
	case <-hook.reserved:
		forced = true
	case ar = <-aResult:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	bok, bctx, err := b.Lock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	close(hook.proceed)
	if forced {
		ar = <-aResult
	}
	if ar.err != nil {
		t.Fatal(ar.err)
	}
	active := 0
	if ar.locked {
		active++
		defer a.Unlock(ar.ctx)
	}
	if bok {
		active++
		defer b.Unlock(bctx)
	}
	t.Logf("forced split-admission gap=%v simultaneous successful holders=%d", forced, active)
	if active > 1 {
		t.Fatal("semaphore admitted overlapping holders")
	}
}
func TestLeaseSemaphoreMissingMembershipCancels(t *testing.T) {
	utils, client, ctx, _ := prefixTestClient(t)
	name := "renew-" + uuid.NewString()
	a, err := utils.NewSemaphore(name, 1, WithSemaphoreRefreshInterval(20*time.Millisecond), WithSemaphoreTTL(time.Second), WithSemaphoreBlockingLock(false))
	if err != nil {
		t.Fatal(err)
	}
	s := a.(*semaphore)
	t.Cleanup(func() { client.Del(context.Background(), s.cids, s.owners, s.counter) })
	ok, oldCtx, err := a.Lock(ctx)
	if err != nil || !ok {
		t.Fatalf("initial lock: %v %v", ok, err)
	}
	if err := client.Del(ctx, s.cids).Err(); err != nil {
		t.Fatal(err)
	}
	b, err := utils.NewSemaphore(name, 1, WithSemaphoreBlockingLock(false))
	if err != nil {
		t.Fatal(err)
	}
	ok, newCtx, err := b.Lock(ctx)
	if err != nil || !ok {
		t.Fatalf("new holder: %v %v", ok, err)
	}
	defer b.Unlock(newCtx)
	select {
	case <-oldCtx.Done():
	case <-time.After(250 * time.Millisecond):
		t.Fatal("lost semaphore lease context remains live")
	}
}
func TestLeaseMutexSharedObjectExcludesOperations(t *testing.T) {
	utils, client, ctx, _ := prefixTestClient(t)
	mu, err := utils.NewMutex("shared-"+uuid.NewString(), WithMutexBlockingLock(false))
	if err != nil {
		t.Fatal(err)
	}
	m := mu.(*mutex)
	t.Cleanup(func() { client.Del(context.Background(), m.name) })
	ok, firstCtx, err := mu.Lock(ctx)
	if err != nil || !ok {
		t.Fatal("initial lock failed")
	}
	defer mu.Unlock(firstCtx)
	// The first operation remains in its critical section while another logical
	// operation attempts the same object with the original parent context.
	second, secondCtx, err := mu.Lock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if second {
		defer mu.Unlock(secondCtx)
		t.Fatal("two logical operations entered one shared mutex")
	}
	if firstCtx.Err() != nil {
		t.Fatal("contender canceled the existing holder")
	}
}
func TestLeaseSemaphoreSharedObjectConsumesDistinctPermits(t *testing.T) {
	utils, client, ctx, _ := prefixTestClient(t)
	sema, err := utils.NewSemaphore("shared-"+uuid.NewString(), 2, WithSemaphoreBlockingLock(false))
	if err != nil {
		t.Fatal(err)
	}
	s := sema.(*semaphore)
	t.Cleanup(func() { client.Del(context.Background(), s.cids, s.owners, s.counter) })
	ok, a, err := sema.Lock(ctx)
	if err != nil || !ok {
		t.Fatal("first permit failed")
	}
	defer sema.Unlock(a)
	ok, b, err := sema.Lock(ctx)
	if err != nil || !ok {
		t.Fatal("second permit failed")
	}
	defer sema.Unlock(b)
	count, err := client.ZCard(ctx, s.owners).Result()
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 || a.Err() != nil {
		t.Fatalf("shared acquisitions collapsed: owners=%d first context=%v", count, a.Err())
	}
	if ok, _, err := sema.Lock(ctx); err != nil || ok {
		t.Fatalf("third permit: %v %v", ok, err)
	}
}
func TestLeaseMutexStaleReleaseCannotTouchReacquisition(t *testing.T) {
	utils, client, ctx, _ := prefixTestClient(t)
	mu, err := utils.NewMutex("stale-"+uuid.NewString(), WithMutexBlockingLock(false))
	if err != nil {
		t.Fatal(err)
	}
	m := mu.(*mutex)
	t.Cleanup(func() { client.Del(context.Background(), m.name) })
	ok, oldCtx, err := mu.Lock(ctx)
	if err != nil || !ok {
		t.Fatal("first acquire failed")
	}
	if err := client.Del(ctx, m.name).Err(); err != nil {
		t.Fatal(err)
	}
	ok, newCtx, err := mu.Lock(ctx)
	if err != nil || !ok {
		t.Fatal("reacquire failed")
	}
	defer mu.Unlock(newCtx)
	before, err := client.Get(ctx, m.name).Result()
	if err != nil {
		t.Fatal(err)
	}
	_ = mu.Unlock(oldCtx)
	after, err := client.Get(ctx, m.name).Result()
	if err != nil || after != before || newCtx.Err() != nil {
		t.Fatalf("stale cleanup released/canceled newer lease: %v %v", err, newCtx.Err())
	}
}
func TestLeaseNormalRenewalRemainsLive(t *testing.T) {
	utils, client, ctx, _ := prefixTestClient(t)
	sema, err := utils.NewSemaphore("valid-"+uuid.NewString(), 1, WithSemaphoreRefreshInterval(20*time.Millisecond), WithSemaphoreTTL(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	s := sema.(*semaphore)
	t.Cleanup(func() { client.Del(context.Background(), s.cids, s.owners, s.counter) })
	ok, lctx, err := sema.Lock(ctx)
	if err != nil || !ok {
		t.Fatal("acquire failed")
	}
	defer sema.Unlock(lctx)
	time.Sleep(80 * time.Millisecond)
	if lctx.Err() != nil {
		t.Fatal("valid renewal canceled holder")
	}
	// ZADD XX reports zero even for a valid existing member without CH, so
	// checking its integer result alone cannot distinguish missing ownership.
	members, err := client.ZRange(ctx, s.cids, 0, -1).Result()
	if err != nil || len(members) != 1 {
		t.Fatal("membership lost")
	}
	result, err := client.ZAddXX(ctx, s.cids, redisclient.Z{Member: members[0], Score: float64(time.Now().Unix())}).Result()
	if err != nil || result != 0 {
		t.Fatalf("ZADD XX semantics changed: %d %v", result, err)
	}
	t.Log(fmt.Sprintf("valid existing ZADD XX result=%d", result))
}

func TestLeaseSemaphoreBalancedRecursion(t *testing.T) {
	utils, client, ctx, _ := prefixTestClient(t)
	name := "balanced-" + uuid.NewString()
	sema, err := utils.NewSemaphore(name, 1, WithSemaphoreBlockingLock(false))
	if err != nil {
		t.Fatal(err)
	}
	impl := sema.(*semaphore)
	t.Cleanup(func() { client.Del(context.Background(), impl.cids, impl.owners, impl.counter) })
	ok, outer, err := sema.Lock(ctx)
	if err != nil || !ok {
		t.Fatal("outer acquire failed")
	}
	ok, inner, err := sema.Lock(outer)
	if err != nil || !ok {
		t.Fatal("explicit recursive acquire failed")
	}
	if count, err := client.ZCard(ctx, impl.owners).Result(); err != nil || count != 1 {
		t.Fatal("recursion consumed another permit")
	}
	if err := sema.Unlock(outer); err != nil {
		t.Fatal(err)
	}
	if inner.Err() != nil {
		t.Fatal("partial balanced release canceled lease")
	}
	if _, _, err := sema.Lock(outer); err == nil {
		t.Fatal("released reference authorized new recursion")
	}
	contender, err := utils.NewSemaphore(name, 1, WithSemaphoreBlockingLock(false))
	if err != nil {
		t.Fatal(err)
	}
	if ok, _, err := contender.Lock(ctx); err != nil || ok {
		t.Fatal("partial release freed permit")
	}
	if err := sema.Unlock(inner); err != nil {
		t.Fatal(err)
	}
	if inner.Err() == nil {
		t.Fatal("final release left lease live")
	}
	ok, next, err := contender.Lock(ctx)
	if err != nil || !ok {
		t.Fatal("balanced release did not free permit")
	}
	defer contender.Unlock(next)
}
func TestLeaseSemaphoreRenewalFailsClosed(t *testing.T) {
	for _, mode := range []string{"expired score", "missing owner", "Redis error"} {
		t.Run(mode, func(t *testing.T) {
			utils, client, ctx, _ := prefixTestClient(t)
			sema, err := utils.NewSemaphore("renew-control-"+uuid.NewString(), 1, WithSemaphoreRefreshInterval(20*time.Millisecond), WithSemaphoreTTL(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			impl := sema.(*semaphore)
			ok, lctx, err := sema.Lock(ctx)
			if err != nil || !ok {
				t.Fatal("initial acquire failed")
			}
			members, err := client.ZRange(ctx, impl.cids, 0, -1).Result()
			if err != nil || len(members) != 1 {
				t.Fatal("missing membership")
			}
			switch mode {
			case "expired score":
				if err := client.ZAdd(ctx, impl.cids, redisclient.Z{Member: members[0], Score: float64(time.Now().Add(-2 * time.Second).Unix())}).Err(); err != nil {
					t.Fatal(err)
				}
			case "missing owner":
				if err := client.Del(ctx, impl.owners).Err(); err != nil {
					t.Fatal(err)
				}
			case "Redis error":
				client.Close()
			}
			select {
			case <-lctx.Done():
			case <-time.After(250 * time.Millisecond):
				t.Fatal("invalid renewal left lease live")
			}
			if mode != "Redis error" {
				client.Del(context.Background(), impl.cids, impl.owners, impl.counter)
			}
		})
	}
}
func TestLeaseCancellationAndUnboundRelease(t *testing.T) {
	utils, client, ctx, _ := prefixTestClient(t)
	mu, err := utils.NewMutex("context-"+uuid.NewString(), WithMutexBlockingLock(false))
	if err != nil {
		t.Fatal(err)
	}
	impl := mu.(*mutex)
	t.Cleanup(func() { client.Del(context.Background(), impl.name) })
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if ok, _, err := mu.Lock(canceled); err != context.Canceled || ok {
		t.Fatal("canceled acquisition succeeded")
	}
	ok, lctx, err := mu.Lock(ctx)
	if err != nil || !ok {
		t.Fatal("initial acquire failed")
	}
	if err := mu.Unlock(ctx); err != ErrLockContextRequired {
		t.Fatal("unbound parent context could release active lease")
	}
	if lctx.Err() != nil {
		t.Fatal("unbound release canceled holder")
	}
	if err := mu.Unlock(lctx); err != nil {
		t.Fatal(err)
	}
	if err := mu.Unlock(lctx); err != nil {
		t.Fatal("duplicate release was not idempotent")
	}
	for _, opt := range []MutexOptionFunc{WithMutexTTL(0), WithMutexRefreshInterval(0), WithMutexSpinInterval(0), WithMutexTTL(time.Second), WithMutexRefreshInterval(5 * time.Second)} {
		if _, err := utils.NewMutex("invalid-"+uuid.NewString(), opt); err == nil {
			t.Fatal("unsafe lock options accepted")
		}
	}
	if _, err := utils.NewSemaphore("invalid-"+uuid.NewString(), 0); err == nil {
		t.Fatal("zero limit accepted")
	}
}
