package redis

import (
	"testing"

	gutils "github.com/Laisky/go-utils"
)

func TestSetLogger(t *testing.T) {
	logger := gutils.Logger.Named("test")
	SetLogger(logger)
}
