package redis

const (
	// DefaultKeyPrefix default prefix of key in redis
	DefaultKeyPrefix = "/rtils/"
)

// rank
const (
	// defaultKeyRank default key prefix of score rank
	//   `/rtils/sync/rank/<rank_name>`
	defaultKeyRank = DefaultKeyPrefix + "rank/%s/"

	// defaultKeyRankMeta meta data
	//   `/rtils/sync/rank/<rank_name>/meta`
	// defaultKeyRankMeta = defaultKeyRank + "meta"
	// defaultKeyRankData ranking list
	//   `/rtils/sync/rank/<rank_name>/data/`
	defaultKeyRankData = defaultKeyRank + "data/"
)

// sync
const (
	// defaultKeySync default key prefix of sync
	defaultKeySync = DefaultKeyPrefix + "sync/"
	// defaultKeySyncMutex default key prefix of sync mutex
	//   `/rtils/sync/mutex/<lock_name>`
	defaultKeySyncMutex = defaultKeySync + "mutex/%s"

	// defaultKeySyncSemaphore default key prefix of sync semaphore
	//   `/rtils/sync/sema/<lock_name>`
	defaultKeySyncSemaphore = defaultKeySync + "sema/%s"
	// defaultKeySyncSemaphoreLocks all semaphore locks
	//   `/rtils/sync/sema/<lock_name>/ids/`
	defaultKeySyncSemaphoreLocks = defaultKeySyncSemaphore + "/ids/"
	// defaultKeySyncSemaphoreOwners default key prefix of sync semaphore
	//   `/rtils/sync/sema/<lock_name>/owners/`
	defaultKeySyncSemaphoreOwners = defaultKeySyncSemaphore + "/owners/"
	// defaultKeySyncSemaphoreCounter default key prefix of sync semaphore
	//   `/rtils/sync/sema/<lock_name>/counter`
	defaultKeySyncSemaphoreCounter = defaultKeySyncSemaphore + "/counter"
)
