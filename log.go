package redis

import (
	"sync"

	glog "github.com/Laisky/go-utils/v5/log"
	"github.com/Laisky/zap"
)

var (
	logMux sync.RWMutex
	logger glog.Logger
)

func init() {
	var err error
	if logger, err = glog.NewConsoleWithName("go-redis", glog.LevelInfo); err != nil {
		glog.Shared.Panic("new logger", zap.Error(err))
	}
}

// SetLogger set go-redis logger
func SetLogger(log glog.Logger) {
	logMux.Lock()
	logger = log
	logMux.Unlock()
}
