package common

import "sync"

var (
	BinaryStatsMu sync.Mutex

	UseVerkle        = false
	UseCacheTrie     = true
	VerkleLayerCount = 1
	DebugFlag        = false
	Parallelism      = 5
	ReadSet          = true

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

	// Binary Trie statistics
	BinaryHitCount             int64
	BinaryMissNonExistentCount int64
	BinaryMissExistentCount    int64
	BinaryCycleFPCount         int64
	BinaryMaxFPInSingleBlock   int64
	BinaryTrieFPInBlock        int64
	BinaryFPDistribution       []int64 // Record bucket size (0-100+) when FP occurs (Needs Mu)
	BinaryTrieWriteCount       int64   // Total writes/deletes to trigger CSV flush

	// Binary Trie proof metrics
	BinaryProofVerifTime    int64 // Nanoseconds spent on verifying ECMH buckets
	BinaryProofVerifTimeMax int64 // Max nanoseconds spent on verifying a single bucket
	BinaryProofGenTime      int64 // Nanoseconds spent on searching/generating proofs (including stub hits)
	BinaryProofGenTimeMax   int64 // Max nanoseconds spent on searching/generating proofs

	BinaryPruneTime    int64 // Total nanoseconds spent on Sharded-Pruning
	BinaryPruneTimeMax int64 // Max nanoseconds spent on a single Sharded-Pruning

	BinaryItemProofSizeMin  int64   // Min size of a single item proof
	BinaryItemProofSizeMax  int64   // Max size of a single item proof
	BinaryItemProofSizes    []int64 // All individual proof sizes for five-number summary (Needs Mu)
	BinaryBlockProofSize    int64   // Total proof size in current block (reset per block)
	BinaryBlockProofSizeMax int64   // Max total proof size in a single block
	BinaryTotalProofSize    int64   // Total proof size across the epoch
)
