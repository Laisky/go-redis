# Redis Tools

Some redis utils depend on <https://github.com/go-redis/redis>

```go
import (
    gredis "github.com/Laisky/go-redis/v2"
    "github.com/go-redis/redis"
)

func main() {
    rtils := NewRedisUtils(redis.NewClient(&redis.Options{}))
}
```

## Features

- `getset.go`: common utils of get/set
- `sync.go`: distributed locks


### Lock ownership and bounded helper changes

Keep the acquisition context returned by Lock and pass that exact context to
Unlock. A shared object may be used concurrently; logical acquisitions receive
different tokens. Recursive calls must use a live returned lock context and
balance every successful call. Parent-only release of an active lease fails
closed. Upgrade every client sharing a lock name before relying on the repaired
protocol. Lease contexts do not replace datastore fencing for noncooperative work.

Prefix lookup treats metacharacters literally and has key, byte, work and time
budgets; a limit failure returns no partial map. GetItemWithPrefixLimited permits
explicit trusted limits. RPush enforces its maximum atomically and rejects an
oversized batch. See [security remediation](docs/security-remediation.md).
