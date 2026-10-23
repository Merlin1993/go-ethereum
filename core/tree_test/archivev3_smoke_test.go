// Copyright 2024 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.

package tree_test

import (
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/trie/archivev3"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
)

// TestArchiveV3Smoke 端到端冒烟：E1 AMT 臂的 v3 接线（triedb → state 层 →
// v3 wrapper → mpt 树）走一遍写、commit、重开、读回、删除、裁剪调度。
// 不依赖主网数据，全部走内存 KV，几十毫秒级。
func TestArchiveV3Smoke(t *testing.T) {
	ldb := memorydb.New()
	defer ldb.Close()
	db := rawdb.NewDatabase(ldb)

	tdb := triedb.NewFixedDatabase(db, &triedb.Config{
		IsVerkle:     false,
		IsBinary:     false,
		AMTv3:        true,
		AMTv3KVDir:   "", // 空 = v3 树内存 KV（AMTv3KVStore 自建）
		AMTv3Depth:   8,
		AMTv3Backend: "hashdb",
	})
	defer tdb.CloseAMTv3KVStore()
	sdb := state.NewDatabase(tdb, nil)

	addr := common.HexToAddress("0xdeadbeef")
	slot := common.HexToHash("0x01")
	code := []byte("smoke-code")

	// 第一块：写入账户、存储、代码，commit。
	st, err := state.New(common.Hash{}, sdb)
	if err != nil {
		t.Fatalf("state.New: %v", err)
	}
	if err := st.SetCode(addr, code); err != nil {
		t.Fatalf("SetCode: %v", err)
	}
	st.SetState(addr, slot, common.HexToHash("0x42"))
	st.SetBalance(addr, uint256.NewInt(7), tracing.BalanceChangeUnspecified)
	root1, err := st.Commit(0, false, false)
	if err != nil {
		t.Fatalf("commit #1: %v", err)
	}
	fmt.Printf("smoke: root1=%s\n", root1.Hex())

	// 第二块：重开（OpenTrie 应命中 triedb 槽位拿回同一棵 v3 树），读回
	// 全部三类值，再删除存储槽，commit。
	st2, err := state.New(root1, sdb)
	if err != nil {
		t.Fatalf("state.New(root1): %v", err)
	}
	if gotCode := st2.GetCode(addr); string(gotCode) != string(code) {
		t.Fatalf("code mismatch: got %q want %q", gotCode, code)
	}
	if gotSlot := st2.GetState(addr, slot); gotSlot != common.HexToHash("0x42") {
		t.Fatalf("storage mismatch: got %s want 0x42", gotSlot.Hex())
	}
	if gotBal := st2.GetBalance(addr); gotBal.Uint64() != 7 {
		t.Fatalf("balance mismatch: got %d want 7", gotBal.Uint64())
	}
	st2.SetState(addr, slot, common.Hash{})
	root2, err := st2.Commit(0, false, false)
	if err != nil {
		t.Fatalf("commit #2: %v", err)
	}
	fmt.Printf("smoke: root2=%s\n", root2.Hex())

	// 第三块：读回删除后的槽，确认 v3 键域下删除生效。
	st3, err := state.New(root2, sdb)
	if err != nil {
		t.Fatalf("state.New(root2): %v", err)
	}
	if got := st3.GetState(addr, slot); got != (common.Hash{}) {
		t.Fatalf("deleted storage slot resurrected: %s", got.Hex())
	}

	// 裁剪调度：StateDB.PruneNextShard 应转发到 v3 树（2^8 个分片的轮换
	// 推进一格，不报错即认为调度存在）。
	if err := st3.PruneNextShard(); err != nil {
		t.Fatalf("PruneNextShard: %v", err)
	}
	// 归档桶字节数指标可通过底层 wrapper 读到（此时应为 0 或小值，无碍）。
	if wt := tdb.GetArchiveTrie(); wt == nil {
		t.Fatal("triedb archive slot is empty after commits")
	} else if _, ok := wt.(*archivev3.ArchiveV3Trie); !ok {
		t.Fatalf("archive slot holds %T, want *archivev3.ArchiveV3Trie", wt)
	}
}
