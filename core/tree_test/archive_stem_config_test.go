// Copyright 2026 The go-ethereum Authors
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

package tree

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/holiman/uint256"
)

func TestProcessorHostEnablesStemArchiveMode(t *testing.T) {
	host, err := NewProcessorHost(&ProcessorConfig{
		UseBinaryTrie:     true,
		UseMemory:         true,
		BinaryStemArchive: true,
		ShardDepth:        4,
		ArchiveBucketSize: 30,
		CuckooBuckets:     8,
		CuckooSlots:       4,
		BinaryNodeStorage: "path",
	})
	if err != nil {
		t.Fatalf("new processor host: %v", err)
	}
	defer host.Close()
	stateTrie, err := host.sdb.OpenTrie(common.Hash{})
	if err != nil {
		t.Fatalf("open state trie: %v", err)
	}
	archiveTrie, ok := stateTrie.(*trie.ArchiveTrie)
	if !ok {
		t.Fatalf("unexpected state trie type %T", stateTrie)
	}
	if !archiveTrie.StemMode() {
		t.Fatal("binaryStemArchive did not enable ArchiveTrie stem mode")
	}
}

func TestProcessorHostStemArchiveStateRoundTrip(t *testing.T) {
	host, err := NewProcessorHost(&ProcessorConfig{
		UseBinaryTrie:     true,
		UseMemory:         true,
		BinaryStemArchive: true,
		ShardDepth:        4,
		ArchiveBucketSize: 30,
		CuckooBuckets:     8,
		CuckooSlots:       4,
		BinaryNodeStorage: "path",
	})
	if err != nil {
		t.Fatalf("new processor host: %v", err)
	}
	defer host.Close()

	statedb, err := state.New(common.Hash{}, host.sdb)
	if err != nil {
		t.Fatalf("new state: %v", err)
	}
	addr := common.HexToAddress("0x123456")
	slot := common.HexToHash("0x03")
	slot2 := common.HexToHash("0x04")
	storageValue := common.HexToHash("0x4455")
	storageValue2 := common.HexToHash("0x6677")
	code := []byte{0x60, 0x01, 0x60, 0x02, 0x01}
	statedb.SetBalance(addr, uint256.NewInt(1234), tracing.BalanceChangeUnspecified)
	statedb.SetNonce(addr, 9, tracing.NonceChangeUnspecified)
	statedb.SetState(addr, slot, storageValue)
	statedb.SetState(addr, slot2, storageValue2)
	statedb.SetCode(addr, code)
	statedb.Finalise(false)
	root, err := statedb.Commit(1, false, false)
	if err != nil {
		t.Fatalf("commit state: %v", err)
	}
	if err := host.trieDB.Commit(root, false); err != nil {
		t.Fatalf("persist root: %v", err)
	}

	reloaded, err := state.New(root, host.sdb)
	if err != nil {
		t.Fatalf("reload state: %v", err)
	}
	if got := reloaded.GetBalance(addr); got.Cmp(uint256.NewInt(1234)) != 0 {
		t.Fatalf("balance mismatch: got %s", got)
	}
	if got := reloaded.GetNonce(addr); got != 9 {
		t.Fatalf("nonce mismatch: got %d", got)
	}
	if got := reloaded.GetState(addr, slot); got != storageValue {
		t.Fatalf("storage mismatch: got %x want %x", got, storageValue)
	}
	if got := reloaded.GetState(addr, slot2); got != storageValue2 {
		t.Fatalf("second storage mismatch: got %x want %x", got, storageValue2)
	}
	if got := reloaded.GetCode(addr); !bytes.Equal(got, code) {
		t.Fatalf("code mismatch: got %x want %x", got, code)
	}

	storageValue3 := common.HexToHash("0x8899")
	reloaded.SetState(addr, slot, common.Hash{})
	reloaded.SetState(addr, slot2, storageValue3)
	reloaded.Finalise(false)
	root2, err := reloaded.Commit(2, false, false)
	if err != nil {
		t.Fatalf("commit mixed storage batch: %v", err)
	}
	if err := host.trieDB.Commit(root2, false); err != nil {
		t.Fatalf("persist second root: %v", err)
	}
	finalState, err := state.New(root2, host.sdb)
	if err != nil {
		t.Fatalf("reload second state: %v", err)
	}
	if got := finalState.GetState(addr, slot); got != (common.Hash{}) {
		t.Fatalf("deleted storage returned %x", got)
	}
	if got := finalState.GetState(addr, slot2); got != storageValue3 {
		t.Fatalf("updated second storage mismatch: got %x want %x", got, storageValue3)
	}
}

func TestProcessorHostStemArchiveSelfDestructWipesIndexedState(t *testing.T) {
	host, err := NewProcessorHost(&ProcessorConfig{
		UseBinaryTrie:     true,
		UseMemory:         true,
		BinaryStemArchive: true,
		ShardDepth:        8,
		ArchiveBucketSize: 30,
		CuckooBuckets:     8,
		CuckooSlots:       4,
		BinaryNodeStorage: "path",
	})
	if err != nil {
		t.Fatalf("new processor host: %v", err)
	}
	defer host.Close()

	addr := common.HexToAddress("0xdead")
	other := common.HexToAddress("0xbeef")
	slot0 := common.HexToHash("0x01")
	slot1 := common.HexToHash("0x0100")
	otherSlot := common.HexToHash("0x02")
	state1, err := state.New(common.Hash{}, host.sdb)
	if err != nil {
		t.Fatal(err)
	}
	state1.SetBalance(addr, uint256.NewInt(1), tracing.BalanceChangeUnspecified)
	state1.SetState(addr, slot0, common.HexToHash("0x11"))
	state1.SetState(addr, slot1, common.HexToHash("0x22"))
	state1.SetCode(addr, bytes.Repeat([]byte{0x60}, 40))
	state1.SetBalance(other, uint256.NewInt(1), tracing.BalanceChangeUnspecified)
	state1.SetState(other, otherSlot, common.HexToHash("0x33"))
	root1, err := state1.Commit(1, false, false)
	if err != nil {
		t.Fatalf("commit initial state: %v", err)
	}
	if err := host.trieDB.Commit(root1, false); err != nil {
		t.Fatalf("persist initial root: %v", err)
	}

	state2, err := state.New(root1, host.sdb)
	if err != nil {
		t.Fatal(err)
	}
	// The generic state prefetcher models separate account/storage tries. Binary
	// mode must not replace the already-mutated unified ArchiveTrie with a second
	// pre-state instance during commit.
	state2.StartPrefetcher("archive-stem-destruction", nil)
	state2.SelfDestruct(addr)
	root2, err := state2.Commit(2, false, false)
	if err != nil {
		t.Fatalf("commit destruction: %v", err)
	}
	if state2.CommitWipedAccounts != 1 || state2.CommitWipedStorageSlots != 2 || state2.CommitWipedCodeChunks != 2 {
		t.Fatalf("wipe diagnostics mismatch: accounts=%d slots=%d code=%d",
			state2.CommitWipedAccounts, state2.CommitWipedStorageSlots, state2.CommitWipedCodeChunks)
	}
	if err := host.trieDB.Commit(root2, false); err != nil {
		t.Fatalf("persist destruction root: %v", err)
	}

	deleted, err := state.New(root2, host.sdb)
	if err != nil {
		t.Fatal(err)
	}
	if deleted.Exist(addr) {
		t.Fatal("destroyed account still exists")
	}
	if got := deleted.GetState(addr, slot0); got != (common.Hash{}) {
		t.Fatalf("destroyed slot 0 resurfaced: %x", got)
	}
	if got := deleted.GetState(addr, slot1); got != (common.Hash{}) {
		t.Fatalf("destroyed slot 1 resurfaced: %x", got)
	}
	if got := deleted.GetState(other, otherSlot); got != common.HexToHash("0x33") {
		t.Fatalf("unrelated storage changed: %x", got)
	}

	deleted.SetBalance(addr, uint256.NewInt(2), tracing.BalanceChangeUnspecified)
	deleted.SetState(addr, slot0, common.HexToHash("0x44"))
	root3, err := deleted.Commit(3, false, false)
	if err != nil {
		t.Fatalf("commit recreated account: %v", err)
	}
	if err := host.trieDB.Commit(root3, false); err != nil {
		t.Fatalf("persist recreated root: %v", err)
	}
	recreated, err := state.New(root3, host.sdb)
	if err != nil {
		t.Fatal(err)
	}
	if got := recreated.GetState(addr, slot0); got != common.HexToHash("0x44") {
		t.Fatalf("recreated slot mismatch: %x", got)
	}
	if got := recreated.GetState(addr, slot1); got != (common.Hash{}) {
		t.Fatalf("old unmodified slot resurfaced after recreation: %x", got)
	}
}

func TestProcessorHostStemArchiveSelfDestructRecreateSameBlock(t *testing.T) {
	host, err := NewProcessorHost(&ProcessorConfig{
		UseBinaryTrie:     true,
		UseMemory:         true,
		BinaryStemArchive: true,
		ShardDepth:        8,
		ArchiveBucketSize: 30,
		CuckooBuckets:     8,
		CuckooSlots:       4,
		BinaryNodeStorage: "path",
	})
	if err != nil {
		t.Fatalf("new processor host: %v", err)
	}
	defer host.Close()

	addr := common.HexToAddress("0xcafe")
	oldSlot := common.HexToHash("0x01")
	newSlot := common.HexToHash("0x02")
	initial, err := state.New(common.Hash{}, host.sdb)
	if err != nil {
		t.Fatal(err)
	}
	initial.SetBalance(addr, uint256.NewInt(1), tracing.BalanceChangeUnspecified)
	initial.SetState(addr, oldSlot, common.HexToHash("0x11"))
	initial.SetCode(addr, bytes.Repeat([]byte{0x60}, 40))
	root1, err := initial.Commit(1, false, false)
	if err != nil {
		t.Fatalf("commit initial state: %v", err)
	}
	if err := host.trieDB.Commit(root1, false); err != nil {
		t.Fatalf("persist initial root: %v", err)
	}

	transition, err := state.New(root1, host.sdb)
	if err != nil {
		t.Fatal(err)
	}
	transition.SelfDestruct(addr)
	transition.Finalise(false) // transaction boundary inside the same block
	transition.SetBalance(addr, uint256.NewInt(2), tracing.BalanceChangeUnspecified)
	transition.SetState(addr, newSlot, common.HexToHash("0x22"))
	transition.SetCode(addr, []byte{0x60, 0x01})
	root2, err := transition.Commit(2, false, false)
	if err != nil {
		t.Fatalf("commit same-block recreation: %v", err)
	}
	if transition.CommitWipedAccounts != 1 || transition.CommitWipedStorageSlots != 1 || transition.CommitWipedCodeChunks != 2 {
		t.Fatalf("wipe diagnostics mismatch: accounts=%d slots=%d code=%d",
			transition.CommitWipedAccounts, transition.CommitWipedStorageSlots, transition.CommitWipedCodeChunks)
	}
	if err := host.trieDB.Commit(root2, false); err != nil {
		t.Fatalf("persist recreated root: %v", err)
	}

	reloaded, err := state.New(root2, host.sdb)
	if err != nil {
		t.Fatal(err)
	}
	if got := reloaded.GetBalance(addr); got.Cmp(uint256.NewInt(2)) != 0 {
		t.Fatalf("recreated balance mismatch: %s", got)
	}
	if got := reloaded.GetState(addr, oldSlot); got != (common.Hash{}) {
		t.Fatalf("old slot resurfaced after same-block recreation: %x", got)
	}
	if got := reloaded.GetState(addr, newSlot); got != common.HexToHash("0x22") {
		t.Fatalf("new slot missing after same-block recreation: %x", got)
	}
	if got := reloaded.GetCode(addr); !bytes.Equal(got, []byte{0x60, 0x01}) {
		t.Fatalf("new code mismatch: %x", got)
	}
}

func TestProcessorHostStemArchiveWipesCreatedAccountAfterIntermediateRoot(t *testing.T) {
	host, err := NewProcessorHost(&ProcessorConfig{
		UseBinaryTrie:     true,
		UseMemory:         true,
		BinaryStemArchive: true,
		ShardDepth:        8,
		ArchiveBucketSize: 30,
		CuckooBuckets:     8,
		CuckooSlots:       4,
		BinaryNodeStorage: "path",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	transition, err := state.New(common.Hash{}, host.sdb)
	if err != nil {
		t.Fatal(err)
	}
	addr := common.HexToAddress("0xc001")
	slot := common.HexToHash("0x01")
	transition.SetBalance(addr, uint256.NewInt(1), tracing.BalanceChangeUnspecified)
	transition.SetState(addr, slot, common.HexToHash("0x11"))
	transition.SetCode(addr, []byte{0x60, 0x01})
	transition.IntermediateRoot(false) // pre-Byzantium transaction boundary

	// The account did not exist at block start, but its unified-tree leaves now
	// do exist and must still be removed by a later self-destruct.
	transition.SelfDestruct(addr)
	transition.IntermediateRoot(false)
	root, err := transition.Commit(1, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.trieDB.Commit(root, false); err != nil {
		t.Fatal(err)
	}
	reloaded, err := state.New(root, host.sdb)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.Exist(addr) {
		t.Fatal("created-then-destroyed account survived")
	}
	if got := reloaded.GetState(addr, slot); got != (common.Hash{}) {
		t.Fatalf("created-then-destroyed storage survived: %x", got)
	}
	if got := reloaded.GetCode(addr); len(got) != 0 {
		t.Fatalf("created-then-destroyed code survived: %x", got)
	}
}

func TestProcessorHostStemArchiveWipesRepeatedDestructionAcrossIntermediateRoots(t *testing.T) {
	host, err := NewProcessorHost(&ProcessorConfig{
		UseBinaryTrie:     true,
		UseMemory:         true,
		BinaryStemArchive: true,
		ShardDepth:        8,
		ArchiveBucketSize: 30,
		CuckooBuckets:     8,
		CuckooSlots:       4,
		BinaryNodeStorage: "path",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	addr := common.HexToAddress("0xc002")
	oldSlot := common.HexToHash("0x01")
	newSlot := common.HexToHash("0x02")
	initial, err := state.New(common.Hash{}, host.sdb)
	if err != nil {
		t.Fatal(err)
	}
	initial.SetBalance(addr, uint256.NewInt(1), tracing.BalanceChangeUnspecified)
	initial.SetState(addr, oldSlot, common.HexToHash("0x11"))
	initial.SetCode(addr, []byte{0x60, 0x01})
	root, err := initial.Commit(1, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.trieDB.Commit(root, false); err != nil {
		t.Fatal(err)
	}

	transition, err := state.New(root, host.sdb)
	if err != nil {
		t.Fatal(err)
	}
	transition.SelfDestruct(addr)
	transition.IntermediateRoot(false)
	transition.SetBalance(addr, uint256.NewInt(2), tracing.BalanceChangeUnspecified)
	transition.SetState(addr, newSlot, common.HexToHash("0x22"))
	transition.SetCode(addr, []byte{0x60, 0x02})
	transition.IntermediateRoot(false)
	if got := transition.GetState(addr, newSlot); got != common.HexToHash("0x22") {
		t.Fatalf("recreated storage missing before second destruction: %x", got)
	}
	transition.SelfDestruct(addr)
	transition.IntermediateRoot(false)
	root, err = transition.Commit(2, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.trieDB.Commit(root, false); err != nil {
		t.Fatal(err)
	}
	reloaded, err := state.New(root, host.sdb)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.Exist(addr) {
		t.Fatal("twice-destroyed account survived")
	}
	if got := reloaded.GetState(addr, oldSlot); got != (common.Hash{}) {
		t.Fatalf("old storage survived: %x", got)
	}
	if got := reloaded.GetState(addr, newSlot); got != (common.Hash{}) {
		t.Fatalf("recreated storage survived second destruction: %x", got)
	}
	if got := reloaded.GetCode(addr); len(got) != 0 {
		t.Fatalf("recreated code survived second destruction: %x", got)
	}
}

func BenchmarkProcessorHostStemArchiveSelfDestruct100K(b *testing.B) {
	const slots = 100_000
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		host, err := NewProcessorHost(&ProcessorConfig{
			UseBinaryTrie:       true,
			UseMemory:           true,
			BinaryStemArchive:   true,
			ShardDepth:          20,
			ArchiveBucketSize:   100,
			CuckooBuckets:       16,
			CuckooSlots:         4,
			BinaryNodeStorage:   "path",
			BinaryCommitWorkers: 16,
		})
		if err != nil {
			b.Fatal(err)
		}
		addr := common.HexToAddress("0x5370183")
		initial, err := state.New(common.Hash{}, host.sdb)
		if err != nil {
			host.Close()
			b.Fatal(err)
		}
		initial.SetBalance(addr, uint256.NewInt(1), tracing.BalanceChangeUnspecified)
		for i := 0; i < slots; i++ {
			var slot, value common.Hash
			binary.BigEndian.PutUint64(slot[common.HashLength-8:], uint64(i))
			binary.BigEndian.PutUint64(value[common.HashLength-8:], uint64(i+1))
			initial.SetState(addr, slot, value)
		}
		root, err := initial.Commit(1, false, false)
		if err != nil {
			host.Close()
			b.Fatal(err)
		}
		if err := host.trieDB.Commit(root, false); err != nil {
			host.Close()
			b.Fatal(err)
		}
		transition, err := state.New(root, host.sdb)
		if err != nil {
			host.Close()
			b.Fatal(err)
		}
		transition.SelfDestruct(addr)

		b.StartTimer()
		preStart := time.Now()
		if _, _, err := transition.PreCommit(false); err != nil {
			b.Fatal(err)
		}
		preDuration := time.Since(preStart)
		postStart := time.Now()
		root, err = transition.PostCommit(2, false, false)
		if err != nil {
			b.Fatal(err)
		}
		postDuration := time.Since(postStart)
		b.StopTimer()

		if transition.CommitWipedStorageSlots != slots {
			host.Close()
			b.Fatalf("wiped %d slots, want %d", transition.CommitWipedStorageSlots, slots)
		}
		b.ReportMetric(float64(preDuration)/float64(time.Millisecond), "precommit-ms/op")
		b.ReportMetric(float64(postDuration)/float64(time.Millisecond), "postcommit-ms/op")
		b.ReportMetric(float64(transition.CommitAccountStateWipe)/float64(time.Millisecond), "wipe-ms/op")
		b.ReportMetric(float64(transition.CommitWipedStemRecords), "stems/op")
		if err := host.trieDB.Commit(root, false); err != nil {
			host.Close()
			b.Fatal(err)
		}
		deleted, err := state.New(root, host.sdb)
		if err != nil {
			host.Close()
			b.Fatal(err)
		}
		if deleted.Exist(addr) {
			host.Close()
			b.Fatal("destroyed 100k-slot account still exists")
		}
		var lastSlot common.Hash
		binary.BigEndian.PutUint64(lastSlot[common.HashLength-8:], slots-1)
		if value := deleted.GetState(addr, lastSlot); value != (common.Hash{}) {
			host.Close()
			b.Fatalf("destroyed slot survived: %x", value)
		}
		host.Close()
	}
}
