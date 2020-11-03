package redis

const (
	// DefaultKey default prefix of key in redis
	DefaultKey = "/rtils/"
)

// sync

const (
	// DefaultKeySync default key prefix of sync
	DefaultKeySync = DefaultKey + "sync/"
	// DefaultKeySyncMutex default key prefix of sync mutex
	//   `/rtils/sync/mutex/<lock_name>`
	DefaultKeySyncMutex = DefaultKeySync + "mutex/%s"
)
