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

	// defaultKeySyncSemaphore default key prefix of sync semaphore
	//   `/rtils/sync/sema/<lock_name>`
	defaultKeySyncSemaphore = DefaultKeySync + "sema/%s"
	// DefaultKeySyncSemaphoreLocks all semaphore locks
	//   `/rtils/sync/sema/<lock_name>/ids/`
	DefaultKeySyncSemaphoreLocks = defaultKeySyncSemaphore + "/ids/"
	// DefaultKeySyncSemaphoreOwners default key prefix of sync semaphore
	//   `/rtils/sync/sema/<lock_name>/owners/`
	DefaultKeySyncSemaphoreOwners = defaultKeySyncSemaphore + "/owners/"
	// DefaultKeySyncSemaphoreCounter default key prefix of sync semaphore
	//   `/rtils/sync/sema/<lock_name>/counter`
	DefaultKeySyncSemaphoreCounter = defaultKeySyncSemaphore + "/counter"
)
