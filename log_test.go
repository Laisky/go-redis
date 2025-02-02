package redis

import (
	"testing"

	glog "github.com/Laisky/go-utils/v5/log"
)

func TestSetLogger(t *testing.T) {
	logger := glog.Shared.Named("test")
	SetLogger(logger)
}
