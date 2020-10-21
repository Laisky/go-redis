package redis

import (
	"math/rand"
	"testing"

	gutils "github.com/Laisky/go-utils"
	"golang.org/x/sync/errgroup"
)

func TestSetLogger(t *testing.T) {
	GetLogger().Info("yo")
	logger := gutils.Logger.Named("test")
	SetLogger(logger)
	GetLogger().Info("666")

	var pool errgroup.Group
	for i := 0; i < 50; i++ {
		pool.Go(func() error {
			if rand.Intn(2) == 0 {
				SetLogger(logger)
			} else {
				GetLogger().Info("yo")
			}

			return nil
		})
	}

	_ = pool.Wait()
	// t.Error()
}
