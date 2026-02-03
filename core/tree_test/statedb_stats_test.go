package tree

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/hashdb"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
	"github.com/holiman/uint256"
)

// TestStateDBStats 测试StateDB统计功能
func TestStateDBStats(t *testing.T) {
	// 创建临时数据库
	dbDir := filepath.Join(os.TempDir(), "statedb_stats_test")
	defer os.RemoveAll(dbDir)

	ldb, err := leveldb.New(dbDir, 1024, 1024, "statedb-stats-test", false)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer ldb.Close()

	db := rawdb.NewDatabase(ldb)
	hashdb := hashdb.Defaults
	pathdb := pathdb.Defaults
	if common.UseVerkle {
		hashdb = nil
	} else {
		pathdb = nil
	}
	trieDB := triedb.NewDatabase(db, &triedb.Config{
		Preimages: false,
		IsVerkle:  common.UseVerkle,
		HashDB:    hashdb,
		PathDB:    pathdb,
	})

	// 创建StateDB
	stateDB, err := state.New(common.Hash{}, state.NewDatabase(trieDB, nil))
	if err != nil {
		t.Fatalf("Failed to create StateDB: %v", err)
	}

	// 累计统计变量
	var cumulativeAccountCommitsDuration time.Duration
	var cumulativeStorageUpdatesDuration time.Duration
	var cumulativeAccountUpdatesDuration time.Duration
	var cumulativeAccountHashesDuration time.Duration
	var cumulativeSnapshotCommitsDuration time.Duration
	var cumulativeTrieDBCommitsDuration time.Duration

	// 模拟处理多个区块
	for blockNum := uint64(1); blockNum <= 20000; blockNum++ {
		// 模拟一些状态变更
		addr := common.HexToAddress("0x1234567890123456789012345678901234567890")
		stateDB.SetBalance(addr, uint256.NewInt(1), tracing.BalanceChangeUnspecified)
		stateDB.SetNonce(addr, blockNum, tracing.NonceChangeUnspecified)

		// 记录统计前的值
		preAccountCommits := stateDB.AccountCommits
		preStorageUpdates := stateDB.StorageUpdates
		preAccountUpdates := stateDB.AccountUpdates
		preAccountHashes := stateDB.AccountHashes
		preSnapshotCommits := stateDB.SnapshotCommits
		preTrieDBCommits := stateDB.TrieDBCommits

		// 执行Commit操作
		_, err := stateDB.Commit(blockNum, false, false)
		if err != nil {
			t.Fatalf("Failed to commit StateDB at block %d: %v", blockNum, err)
		}

		// 累计统计数据
		cumulativeAccountCommitsDuration += stateDB.AccountCommits - preAccountCommits
		cumulativeStorageUpdatesDuration += stateDB.StorageUpdates - preStorageUpdates
		cumulativeAccountUpdatesDuration += stateDB.AccountUpdates - preAccountUpdates
		cumulativeAccountHashesDuration += stateDB.AccountHashes - preAccountHashes
		cumulativeSnapshotCommitsDuration += stateDB.SnapshotCommits - preSnapshotCommits
		cumulativeTrieDBCommitsDuration += stateDB.TrieDBCommits - preTrieDBCommits

		// 每10000个区块打印一次统计信息
		if blockNum%10000 == 0 {
			t.Logf("区块 %d - StateDB 7个统计区域累计统计:", blockNum)
			t.Logf("      └─ 统计1 - AccountCommits (Finalise): %.3fms", float64(cumulativeAccountCommitsDuration.Microseconds()/1e3))
			t.Logf("      └─ 统计2&3 - StorageUpdates (并发处理存储): %.3fms", float64(cumulativeStorageUpdatesDuration.Microseconds()/1e3))
			t.Logf("      └─ 统计4 - AccountUpdates (更新删除状态对象): %.3fms", float64(cumulativeAccountUpdatesDuration.Microseconds()/1e3))
			t.Logf("      └─ 统计5 - AccountHashes (计算trie哈希): %.3fms", float64(cumulativeAccountHashesDuration.Microseconds()/1e3))
			t.Logf("      └─ 统计6 - SnapshotCommits (更新快照树): %.3fms", float64(cumulativeSnapshotCommitsDuration.Microseconds()/1e3))
			t.Logf("      └─ 统计7 - TrieDBCommits (更新TrieDB): %.3fms", float64(cumulativeTrieDBCommitsDuration.Microseconds()/1e3))

			// 重置累计统计
			cumulativeAccountCommitsDuration = 0
			cumulativeStorageUpdatesDuration = 0
			cumulativeAccountUpdatesDuration = 0
			cumulativeAccountHashesDuration = 0
			cumulativeSnapshotCommitsDuration = 0
			cumulativeTrieDBCommitsDuration = 0
		}
	}

	t.Logf("StateDB统计功能测试完成")
}
