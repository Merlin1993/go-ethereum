package state

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/holiman/uint256"
)

func TestKVDatabaseCommitAndAccessStats(t *testing.T) {
	db := NewKVDatabase(rawdb.NewMemoryDatabase())
	addr := common.HexToAddress("0x1000000000000000000000000000000000000001")
	slot := common.HexToHash("0x01")
	value := common.HexToHash("0x02")

	db.SetBlockNum(10)
	statedb, err := New(common.Hash{}, db)
	if err != nil {
		t.Fatalf("failed to open kv state: %v", err)
	}
	statedb.AddBalance(addr, uint256.NewInt(7), 0)
	statedb.SetState(addr, slot, value)
	if _, err := statedb.Commit(10, false, false); err != nil {
		t.Fatalf("failed to commit kv state: %v", err)
	}
	stats := db.KVBlockStats()
	if stats.Writes == 0 || stats.WriteNonExistent == 0 {
		t.Fatalf("expected initial writes to be counted as nonexistent, stats=%+v", stats)
	}

	db.SetBlockNum(10 + kvBlocksPerThree)
	statedb, err = New(common.Hash{}, db)
	if err != nil {
		t.Fatalf("failed to reopen kv state: %v", err)
	}
	if balance := statedb.GetBalance(addr); balance.Uint64() != 7 {
		t.Fatalf("unexpected balance: have %d, want 7", balance.Uint64())
	}
	if got := statedb.GetState(addr, slot); got != value {
		t.Fatalf("unexpected storage: have %x, want %x", got, value)
	}
	statedb.GetBalance(common.HexToAddress("0x2000000000000000000000000000000000000002"))
	statedb.GetState(addr, common.HexToHash("0xdead"))
	statedb.SetState(addr, slot, common.HexToHash("0x03"))
	stats = db.KVBlockStats()
	if stats.Read3M == 0 || stats.Read6M == 0 || stats.Read1Y == 0 {
		t.Fatalf("expected reads to hit all recency windows, stats=%+v", stats)
	}
	if stats.Write3M == 0 || stats.Write6M == 0 || stats.Write1Y == 0 {
		t.Fatalf("expected writes to hit all recency windows, stats=%+v", stats)
	}
	if stats.ReadNonExistent == 0 {
		t.Fatalf("expected second block reads to count nonexistent keys, stats=%+v", stats)
	}
	if stats.WriteNonExistent != 0 {
		t.Fatalf("expected second block write to touch an existing key, stats=%+v", stats)
	}
}
