# Redis Tools

Some redis utils depend on <https://github.com/go-redis/redis>

```go
import (
    gredis "github.com/Laisky/go-redis"
    "github.com/go-redis/redis"
)

func main() {
    rtils := NewRedisUtils(redis.NewClient(&redis.Options{}))
}
```

## Features

- `getset.go`: common utils of get/set
- `sync.go`: distributed locks
