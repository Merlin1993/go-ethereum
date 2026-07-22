// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.

package state

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

func TestBuildUnifiedAccountWipeUsesBlockOrigins(t *testing.T) {
	slot0 := common.HexToHash("0x01")
	slot1 := common.HexToHash("0x02")
	slot2 := common.HexToHash("0x03")
	origin0 := common.HexToHash("0xaa")
	origin1 := common.HexToHash("0xbb")
	result := trie.AccountStateWipeResult{Storage: []trie.StorageWipeItem{
		{Key: slot0[:], Value: []byte{0xcc}}, // current intermediate-root value
	}}
	wipe, err := buildUnifiedAccountWipe(result, Storage{slot0: origin0, slot1: origin1, slot2: common.Hash{}})
	if err != nil {
		t.Fatal(err)
	}
	if len(wipe.storages) != 2 || len(wipe.storageOrigins) != 2 {
		t.Fatalf("wipe size mismatch: storage=%d origins=%d", len(wipe.storages), len(wipe.storageOrigins))
	}
	for slot, origin := range map[common.Hash]common.Hash{slot0: origin0, slot1: origin1} {
		hash := crypto.Keccak256Hash(slot[:])
		want, err := rlp.EncodeToBytes(common.TrimLeftZeroes(origin[:]))
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := wipe.storages[hash]; !ok {
			t.Fatalf("slot %x missing from deletion set", slot)
		}
		if got := wipe.storageOrigins[hash]; !bytes.Equal(got, want) {
			t.Fatalf("slot %x origin mismatch: got %x want %x", slot, got, want)
		}
	}
	if hash := crypto.Keccak256Hash(slot2[:]); wipe.storages[hash] != nil {
		t.Fatal("block-origin empty slot was retained in deletion history")
	} else if _, ok := wipe.storages[hash]; ok {
		t.Fatal("block-origin empty slot was retained in deletion set")
	}
}

func BenchmarkBuildUnifiedAccountWipe100K(b *testing.B) {
	const slots = 100_000
	result := trie.AccountStateWipeResult{Storage: make([]trie.StorageWipeItem, slots)}
	for i := range result.Storage {
		key := make([]byte, 32)
		binary.BigEndian.PutUint64(key[24:], uint64(i))
		result.Storage[i] = trie.StorageWipeItem{Key: key, Value: []byte{byte(i)}}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wipe, err := buildUnifiedAccountWipe(result, nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(wipe.storages) != slots || len(wipe.storageOrigins) != slots {
			b.Fatalf("built %d/%d storage records", len(wipe.storages), len(wipe.storageOrigins))
		}
	}
}
