package redis

import (
	"time"
)

// Redis Key expirations
const (
	// KeyExpImmortal nolimit
	KeyExpImmortal = 0
	// KeyExp1Day 1day
	KeyExp1Day = 24 * time.Hour
	// ScanCount how many items return by each scan
	ScanCount = 10
)

//Pop time
const (
	WaitDBKeyDuration = 3 * time.Second
)
