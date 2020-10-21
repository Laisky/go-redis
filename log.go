package redis

import (
	"sync"

	gutils "github.com/Laisky/go-utils"
	"github.com/Laisky/zap"
)

var (
	logMux      sync.RWMutex
	logInternal *gutils.LoggerType
)

func init() {
	var err error
	if logInternal, err = gutils.NewConsoleLoggerWithName("go-redis", gutils.LoggerLevelInfo); err != nil {
		gutils.Logger.Panic("new logger", zap.Error(err))
	}
}

// SetLogger set go-redis logger
func SetLogger(logger *gutils.LoggerType) {
	logMux.Lock()
	logInternal = logger
	logMux.Unlock()
}

// GetLogger get go-redis logger
func GetLogger() (logger *gutils.LoggerType) {
	logMux.RLock()
	logger = logInternal
	logMux.RUnlock()
	return
}
