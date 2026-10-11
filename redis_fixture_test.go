package redis

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"

	redisclient "github.com/redis/go-redis/v9"
)

func TestMain(m *testing.M) {
	address := os.Getenv("GO_REDIS_TEST_ADDR")
	host, port, err := net.SplitHostPort(address)
	number, portErr := strconv.Atoi(port)
	ip := net.ParseIP(host)
	if err != nil || portErr != nil || number < 1 || number > 65535 || ip == nil || !ip.IsLoopback() {
		fmt.Fprintln(os.Stderr, "set GO_REDIS_TEST_ADDR to an explicit loopback address of a disposable Redis fixture")
		os.Exit(2)
	}
	os.Exit(m.Run())
}

func testRedisOptions() *redisclient.Options {
	return &redisclient.Options{Addr: os.Getenv("GO_REDIS_TEST_ADDR")}
}
