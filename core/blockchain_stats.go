// Copyright 2025 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"encoding/json"
	"time"

	"github.com/ethereum/go-ethereum/cachetrie"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
)

// ExecuteStats includes all the statistics of a block execution in details.
type ExecuteStats struct {
	// State read times
	AccountReads   time.Duration // Time spent on the account reads
	StorageReads   time.Duration // Time spent on the storage reads
	AccountHashes  time.Duration // Time spent on the account trie hash
	AccountUpdates time.Duration // Time spent on the account trie update
	AccountCommits time.Duration // Time spent on the account trie commit
	StorageUpdates time.Duration // Time spent on the storage trie update
	StorageCommits time.Duration // Time spent on the storage trie commit
	CodeReads      time.Duration // Time spent on the contract code read

	AccountLoaded   int // Number of accounts loaded
	AccountUpdated  int // Number of accounts updated
	AccountDeleted  int // Number of accounts deleted
	StorageLoaded   int // Number of storage slots loaded
	StorageUpdated  int // Number of storage slots updated
	StorageDeleted  int // Number of storage slots deleted
	CodeLoaded      int // Number of contract code loaded
	CodeLoadBytes   int // Number of bytes read from contract code
	CodeUpdated     int // Number of contract code written (CREATE/CREATE2 + EIP-7702)
	CodeUpdateBytes int // Total bytes of code written

	Execution       time.Duration // Time spent on the EVM execution
	Processing      time.Duration // Time spent in the block processor, including state reads
	Validation      time.Duration // Time spent on the block validation
	CrossValidation time.Duration // Optional, time spent on the block cross validation
	DatabaseCommit  time.Duration // Time spent on database commit
	BlockWrite      time.Duration // Time spent on block write
	TotalTime       time.Duration // The total time spent on block execution
	MgasPerSecond   float64       // The million gas processed per second

	// Cache hit rates
	StateReadCacheStats     state.ReaderStats
	StatePrefetchCacheStats state.ReaderStats

	CacheTrie *CacheTrieBlockStats
}

// CacheTrieBlockStats is the per-block SWMT/cachetrie delta plus the live
// pipeline state after the block has been processed.
type CacheTrieBlockStats struct {
	Enabled            bool
	DualRootExperiment bool
	HeaderStateRoot    common.Hash
	GlobalRoot         common.Hash
	SWMTRoot           common.Hash

	Accounts      int
	Storages      int
	LowWatermark  int
	HighWatermark int
	CurrentBit    uint8
	StartBit      uint8
	Pipeline      bool
	PendingBits   uint32
	PendingInputs int

	AccountHits   uint64
	AccountMisses uint64
	StorageHits   uint64
	StorageMisses uint64
	Updates       uint64
	Deletes       uint64

	PruneCount uint64
	PruneItems uint64
	PruneTime  time.Duration

	MergeCount  uint64
	MergeInputs uint64
	MergeTime   time.Duration
	MergeErrors uint64

	WriteWaitCount uint64
	WriteWaitTime  time.Duration

	RootCount uint64
	RootTime  time.Duration

	PublishCount uint64
	PublishTime  time.Duration
}

// reportMetrics uploads execution statistics to the metrics system.
func (s *ExecuteStats) reportMetrics() {
	if s.AccountLoaded != 0 {
		accountReadTimer.Update(s.AccountReads)
		accountReadSingleTimer.Update(s.AccountReads / time.Duration(s.AccountLoaded))
	}
	if s.StorageLoaded != 0 {
		storageReadTimer.Update(s.StorageReads)
		storageReadSingleTimer.Update(s.StorageReads / time.Duration(s.StorageLoaded))
	}
	if s.CodeLoaded != 0 {
		codeReadTimer.Update(s.CodeReads)
		codeReadSingleTimer.Update(s.CodeReads / time.Duration(s.CodeLoaded))
		codeReadBytesTimer.Update(time.Duration(s.CodeLoadBytes))
	}
	accountUpdateTimer.Update(s.AccountUpdates) // Account updates are complete(in validation)
	storageUpdateTimer.Update(s.StorageUpdates) // Storage updates are complete(in validation)
	accountHashTimer.Update(s.AccountHashes)    // Account hashes are complete(in validation)
	accountCommitTimer.Update(s.AccountCommits) // Account commits are complete, we can mark them
	storageCommitTimer.Update(s.StorageCommits) // Storage commits are complete, we can mark them

	blockExecutionTimer.Update(s.Execution)                 // The time spent on EVM processing
	blockValidationTimer.Update(s.Validation)               // The time spent on block validation
	blockCrossValidationTimer.Update(s.CrossValidation)     // The time spent on stateless cross validation
	triedbCommitTimer.Update(s.DatabaseCommit)              // Trie database commits are complete, we can mark them
	blockWriteTimer.Update(s.BlockWrite)                    // The time spent on block write
	blockInsertTimer.Update(s.TotalTime)                    // The total time spent on block execution
	chainMgaspsMeter.Update(time.Duration(s.MgasPerSecond)) // TODO(rjl493456442) generalize the ResettingTimer

	// Cache hit rates
	accountCacheHitPrefetchMeter.Mark(s.StatePrefetchCacheStats.StateStats.AccountCacheHit)
	accountCacheMissPrefetchMeter.Mark(s.StatePrefetchCacheStats.StateStats.AccountCacheMiss)
	storageCacheHitPrefetchMeter.Mark(s.StatePrefetchCacheStats.StateStats.StorageCacheHit)
	storageCacheMissPrefetchMeter.Mark(s.StatePrefetchCacheStats.StateStats.StorageCacheMiss)

	accountCacheHitMeter.Mark(s.StateReadCacheStats.StateStats.AccountCacheHit)
	accountCacheMissMeter.Mark(s.StateReadCacheStats.StateStats.AccountCacheMiss)
	storageCacheHitMeter.Mark(s.StateReadCacheStats.StateStats.StorageCacheHit)
	storageCacheMissMeter.Mark(s.StateReadCacheStats.StateStats.StorageCacheMiss)
}

// slowBlockLog represents the JSON structure for slow block logging.
// This format is designed for cross-client compatibility with other
// Ethereum execution clients (reth, Besu, Nethermind).
type slowBlockLog struct {
	Level       string             `json:"level"`
	Msg         string             `json:"msg"`
	Block       slowBlockInfo      `json:"block"`
	Timing      slowBlockTime      `json:"timing"`
	Throughput  slowBlockThru      `json:"throughput"`
	StateReads  slowBlockReads     `json:"state_reads"`
	StateWrites slowBlockWrites    `json:"state_writes"`
	Cache       slowBlockCache     `json:"cache"`
	CacheTrie   slowBlockCacheTrie `json:"cachetrie"`
}

type slowBlockInfo struct {
	Number  uint64      `json:"number"`
	Hash    common.Hash `json:"hash"`
	GasUsed uint64      `json:"gas_used"`
	TxCount int         `json:"tx_count"`
}

type slowBlockTime struct {
	ExecutionMs               float64 `json:"execution_ms"`
	ProcessWallMs             float64 `json:"process_wall_ms"`
	StateReadMs               float64 `json:"state_read_ms"`
	AccountReadMs             float64 `json:"account_read_ms"`
	StorageReadMs             float64 `json:"storage_read_ms"`
	CodeReadMs                float64 `json:"code_read_ms"`
	StateHashMs               float64 `json:"state_hash_ms"`
	AccountHashMs             float64 `json:"account_hash_ms"`
	AccountUpdateMs           float64 `json:"account_update_ms"`
	StorageUpdateMs           float64 `json:"storage_update_ms"`
	CommitMs                  float64 `json:"commit_ms"`
	AccountCommitMs           float64 `json:"account_commit_ms"`
	StorageCommitMs           float64 `json:"storage_commit_ms"`
	AccountStorageCommitMaxMs float64 `json:"account_storage_commit_max_ms"`
	DBCommitMs                float64 `json:"db_commit_ms"`
	BlockWriteMs              float64 `json:"block_write_ms"`
	TotalMs                   float64 `json:"total_ms"`
}

type slowBlockThru struct {
	MgasPerSec float64 `json:"mgas_per_sec"`
}

type slowBlockReads struct {
	Accounts     int `json:"accounts"`
	StorageSlots int `json:"storage_slots"`
	Code         int `json:"code"`
	CodeBytes    int `json:"code_bytes"`
}

type slowBlockWrites struct {
	Accounts            int `json:"accounts"`
	AccountsDeleted     int `json:"accounts_deleted"`
	StorageSlots        int `json:"storage_slots"`
	StorageSlotsDeleted int `json:"storage_slots_deleted"`
	Code                int `json:"code"`
	CodeBytes           int `json:"code_bytes"`
}

// slowBlockCache represents cache hit/miss statistics for cross-client analysis.
type slowBlockCache struct {
	Account slowBlockCacheEntry     `json:"account"`
	Storage slowBlockCacheEntry     `json:"storage"`
	Code    slowBlockCodeCacheEntry `json:"code"`
}

// slowBlockCacheEntry represents cache statistics for account/storage caches.
type slowBlockCacheEntry struct {
	Hits    int64   `json:"hits"`
	Misses  int64   `json:"misses"`
	HitRate float64 `json:"hit_rate"`
}

// slowBlockCodeCacheEntry represents cache statistics for code cache with byte-level granularity.
type slowBlockCodeCacheEntry struct {
	Hits      int64   `json:"hits"`
	Misses    int64   `json:"misses"`
	HitRate   float64 `json:"hit_rate"`
	HitBytes  int64   `json:"hit_bytes"`
	MissBytes int64   `json:"miss_bytes"`
}

type slowBlockCacheTrie struct {
	Enabled            bool        `json:"enabled"`
	DualRootExperiment bool        `json:"dualroot_experiment"`
	HeaderStateRoot    common.Hash `json:"header_state_root"`
	GlobalRoot         common.Hash `json:"global_root"`
	SWMTRoot           common.Hash `json:"swmt_root"`

	Accounts      int    `json:"accounts"`
	Storages      int    `json:"storages"`
	LowWatermark  int    `json:"low_watermark"`
	HighWatermark int    `json:"high_watermark"`
	CurrentBit    uint8  `json:"current_bit"`
	StartBit      uint8  `json:"start_bit"`
	Pipeline      bool   `json:"pipeline_started"`
	PendingBits   uint32 `json:"pending_bits"`
	PendingInputs int    `json:"pending_inputs"`

	AccountHits   uint64 `json:"account_hits"`
	AccountMisses uint64 `json:"account_misses"`
	StorageHits   uint64 `json:"storage_hits"`
	StorageMisses uint64 `json:"storage_misses"`
	Updates       uint64 `json:"updates"`
	Deletes       uint64 `json:"deletes"`

	MergeCount  uint64  `json:"merge_count"`
	MergeInputs uint64  `json:"merge_inputs"`
	MergeMs     float64 `json:"merge_ms"`
	MergeErrors uint64  `json:"merge_errors"`
	PruneCount  uint64  `json:"prune_count"`
	PruneItems  uint64  `json:"prune_items"`
	PruneMs     float64 `json:"prune_ms"`

	WriteWaitCount uint64  `json:"write_wait_count"`
	WriteWaitMs    float64 `json:"write_wait_ms"`

	SWMTRootCount uint64  `json:"swmt_root_count"`
	SWMTRootMs    float64 `json:"swmt_root_ms"`
	PublishCount  uint64  `json:"publish_count"`
	PublishMs     float64 `json:"publish_ms"`
}

func newCacheTrieBlockStats(enabled bool, dualRootExperiment bool, headerRoot common.Hash, before cachetrie.Stats, after cachetrie.Stats) *CacheTrieBlockStats {
	stats := &CacheTrieBlockStats{
		Enabled:            enabled,
		DualRootExperiment: dualRootExperiment,
		HeaderStateRoot:    headerRoot,
	}
	if !enabled {
		return stats
	}
	stats.GlobalRoot = after.Root
	stats.SWMTRoot = after.SWMTRoot
	stats.Accounts = after.Accounts
	stats.Storages = after.Storages
	stats.LowWatermark = after.LowWatermark
	stats.HighWatermark = after.HighWatermark
	stats.CurrentBit = after.CurrentBit
	stats.StartBit = after.StartBit
	stats.Pipeline = after.Pipeline
	stats.PendingBits = after.PendingBits
	stats.PendingInputs = after.PendingInputs
	stats.AccountHits = deltaUint64(after.AccountHits, before.AccountHits)
	stats.AccountMisses = deltaUint64(after.AccountMisses, before.AccountMisses)
	stats.StorageHits = deltaUint64(after.StorageHits, before.StorageHits)
	stats.StorageMisses = deltaUint64(after.StorageMisses, before.StorageMisses)
	stats.Updates = deltaUint64(after.Updates, before.Updates)
	stats.Deletes = deltaUint64(after.Deletes, before.Deletes)
	stats.PruneCount = deltaUint64(after.CleanupCount, before.CleanupCount)
	stats.PruneItems = deltaUint64(after.CleanupItems, before.CleanupItems)
	stats.PruneTime = deltaDuration(after.CleanupElapsed, before.CleanupElapsed)
	stats.MergeCount = deltaUint64(after.MergeCount, before.MergeCount)
	stats.MergeInputs = deltaUint64(after.MergeInputs, before.MergeInputs)
	stats.MergeTime = deltaDuration(after.MergeElapsed, before.MergeElapsed)
	stats.MergeErrors = deltaUint64(after.MergeErrors, before.MergeErrors)
	stats.WriteWaitCount = deltaUint64(after.WriteWaitCount, before.WriteWaitCount)
	stats.WriteWaitTime = deltaDuration(after.WriteWaitElapsed, before.WriteWaitElapsed)
	stats.RootCount = deltaUint64(after.RootCount, before.RootCount)
	stats.RootTime = deltaDuration(after.RootElapsed, before.RootElapsed)
	stats.PublishCount = deltaUint64(after.PublishCount, before.PublishCount)
	stats.PublishTime = deltaDuration(after.PublishElapsed, before.PublishElapsed)
	return stats
}

func (s *CacheTrieBlockStats) slowBlockEntry() slowBlockCacheTrie {
	if s == nil {
		return slowBlockCacheTrie{}
	}
	return slowBlockCacheTrie{
		Enabled:            s.Enabled,
		DualRootExperiment: s.DualRootExperiment,
		HeaderStateRoot:    s.HeaderStateRoot,
		GlobalRoot:         s.GlobalRoot,
		SWMTRoot:           s.SWMTRoot,
		Accounts:           s.Accounts,
		Storages:           s.Storages,
		LowWatermark:       s.LowWatermark,
		HighWatermark:      s.HighWatermark,
		CurrentBit:         s.CurrentBit,
		StartBit:           s.StartBit,
		Pipeline:           s.Pipeline,
		PendingBits:        s.PendingBits,
		PendingInputs:      s.PendingInputs,
		AccountHits:        s.AccountHits,
		AccountMisses:      s.AccountMisses,
		StorageHits:        s.StorageHits,
		StorageMisses:      s.StorageMisses,
		Updates:            s.Updates,
		Deletes:            s.Deletes,
		MergeCount:         s.MergeCount,
		MergeInputs:        s.MergeInputs,
		MergeMs:            durationToMs(s.MergeTime),
		MergeErrors:        s.MergeErrors,
		PruneCount:         s.PruneCount,
		PruneItems:         s.PruneItems,
		PruneMs:            durationToMs(s.PruneTime),
		WriteWaitCount:     s.WriteWaitCount,
		WriteWaitMs:        durationToMs(s.WriteWaitTime),
		SWMTRootCount:      s.RootCount,
		SWMTRootMs:         durationToMs(s.RootTime),
		PublishCount:       s.PublishCount,
		PublishMs:          durationToMs(s.PublishTime),
	}
}

func deltaUint64(after uint64, before uint64) uint64 {
	if after < before {
		return after
	}
	return after - before
}

func deltaDuration(after time.Duration, before time.Duration) time.Duration {
	if after < before {
		return after
	}
	return after - before
}

// durationToMs converts a time.Duration to milliseconds as a float64
// with sub-millisecond precision for accurate cross-client metrics.
func durationToMs(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / 1e6
}

// logSlow prints the detailed execution statistics in JSON format if the block
// is regarded as slow. The JSON format is designed for cross-client compatibility
// with other Ethereum execution clients.
func (s *ExecuteStats) logSlow(block *types.Block, slowBlockThreshold time.Duration) {
	// Negative threshold means disabled (default when flag not set)
	if slowBlockThreshold < 0 {
		return
	}
	// Threshold of 0 logs all blocks; positive threshold filters
	if slowBlockThreshold > 0 && s.TotalTime < slowBlockThreshold {
		return
	}
	logEntry := slowBlockLog{
		Level: "warn",
		Msg:   "Slow block",
		Block: slowBlockInfo{
			Number:  block.NumberU64(),
			Hash:    block.Hash(),
			GasUsed: block.GasUsed(),
			TxCount: len(block.Transactions()),
		},
		Timing: slowBlockTime{
			ExecutionMs:               durationToMs(s.Execution),
			ProcessWallMs:             durationToMs(s.Processing),
			StateReadMs:               durationToMs(s.AccountReads + s.StorageReads + s.CodeReads),
			AccountReadMs:             durationToMs(s.AccountReads),
			StorageReadMs:             durationToMs(s.StorageReads),
			CodeReadMs:                durationToMs(s.CodeReads),
			StateHashMs:               durationToMs(s.AccountHashes + s.AccountUpdates + s.StorageUpdates),
			AccountHashMs:             durationToMs(s.AccountHashes),
			AccountUpdateMs:           durationToMs(s.AccountUpdates),
			StorageUpdateMs:           durationToMs(s.StorageUpdates),
			CommitMs:                  durationToMs(max(s.AccountCommits, s.StorageCommits) + s.DatabaseCommit + s.BlockWrite),
			AccountCommitMs:           durationToMs(s.AccountCommits),
			StorageCommitMs:           durationToMs(s.StorageCommits),
			AccountStorageCommitMaxMs: durationToMs(max(s.AccountCommits, s.StorageCommits)),
			DBCommitMs:                durationToMs(s.DatabaseCommit),
			BlockWriteMs:              durationToMs(s.BlockWrite),
			TotalMs:                   durationToMs(s.TotalTime),
		},
		Throughput: slowBlockThru{
			MgasPerSec: s.MgasPerSecond,
		},
		StateReads: slowBlockReads{
			Accounts:     s.AccountLoaded,
			StorageSlots: s.StorageLoaded,
			Code:         s.CodeLoaded,
			CodeBytes:    s.CodeLoadBytes,
		},
		StateWrites: slowBlockWrites{
			Accounts:            s.AccountUpdated,
			AccountsDeleted:     s.AccountDeleted,
			StorageSlots:        s.StorageUpdated,
			StorageSlotsDeleted: s.StorageDeleted,
			Code:                s.CodeUpdated,
			CodeBytes:           s.CodeUpdateBytes,
		},
		Cache: slowBlockCache{
			Account: slowBlockCacheEntry{
				Hits:    s.StateReadCacheStats.StateStats.AccountCacheHit,
				Misses:  s.StateReadCacheStats.StateStats.AccountCacheMiss,
				HitRate: s.StateReadCacheStats.StateStats.AccountCacheHitRate(),
			},
			Storage: slowBlockCacheEntry{
				Hits:    s.StateReadCacheStats.StateStats.StorageCacheHit,
				Misses:  s.StateReadCacheStats.StateStats.StorageCacheMiss,
				HitRate: s.StateReadCacheStats.StateStats.StorageCacheHitRate(),
			},
			Code: slowBlockCodeCacheEntry{
				Hits:      s.StateReadCacheStats.CodeStats.CacheHit,
				Misses:    s.StateReadCacheStats.CodeStats.CacheMiss,
				HitRate:   s.StateReadCacheStats.CodeStats.HitRate(),
				HitBytes:  s.StateReadCacheStats.CodeStats.CacheHitBytes,
				MissBytes: s.StateReadCacheStats.CodeStats.CacheMissBytes,
			},
		},
		CacheTrie: s.CacheTrie.slowBlockEntry(),
	}
	jsonBytes, err := json.Marshal(logEntry)
	if err != nil {
		log.Error("Failed to marshal slow block log", "error", err)
		return
	}
	log.Warn(string(jsonBytes))
}
