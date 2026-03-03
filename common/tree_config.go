package common

var (
	UseVerkle        = false
	UseCacheTrie     = true
	VerkleLayerCount = 1
	DebugFlag        = false
	Parallelism      = 5
	ReadSet          = false

	// CacheTrie statistics
	CacheAccountHit           int64
	CacheAccountMissExists    int64
	CacheAccountMissNotExists int64
	CacheStorageHit           int64
	CacheStorageMissExists    int64
	CacheStorageMissNotExists int64
	CacheAccountReadSize      int64
	CacheStorageReadSize      int64
	CacheAccountWriteSize     int64
	CacheStorageWriteSize     int64

	// Total state access statistics (Aggregated)
	TotalAccountReads   int64
	TotalStorageReads   int64
	TotalAccountUpdates int64
	TotalStorageUpdates int64

	TotalReads   int64
	TotalUpdates int64
)
