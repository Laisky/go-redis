package redis

import (
	"sync"

	gutils "github.com/Laisky/go-utils"
	"github.com/Laisky/zap"
)

var (
	logMux sync.RWMutex
	logger *gutils.LoggerType
)

func init() {
	var err error
	if logger, err = gutils.NewConsoleLoggerWithName("go-redis", gutils.LoggerLevelInfo); err != nil {
		gutils.Logger.Panic("new logger", zap.Error(err))
	}
}

// SetLogger set go-redis logger
func SetLogger(log *gutils.LoggerType) {
	logMux.Lock()
	logger = log
	logMux.Unlock()
}
