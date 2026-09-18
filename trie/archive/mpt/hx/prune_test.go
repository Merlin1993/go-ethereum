// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
// General Public License for more details <https://www.gnu.org/licenses/>.

package hx

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/triedb/database"
)

// countingNodeDB wraps a memNodeDB and counts node reads, so the CanSkip
// acceptance test can prove skipped subtrees are never touched.
type countingNodeDB struct {
	*memNodeDB
	reads atomic.Int64
}

type countingNodeReader struct {
	database.NodeReader
	reads *atomic.Int64
}

func (db *countingNodeDB) NodeReader(owner common.Hash) (database.NodeReader, error) {
	r, err := db.memNodeDB.NodeReader(owner)
	if err != nil {
		return nil, err
	}
	return &countingNodeReader{NodeReader: r, reads: &db.reads}, nil
}

func (r *countingNodeReader) Node(owner common.Hash, path []byte, hash common.Hash) ([]byte, error) {
	r.reads.Add(1)
	return r.NodeReader.Node(owner, path, hash)
}

// pruneTestKey builds a 32-byte key in the given one-nibble domain.
func pruneTestKey(domain, seq int) []byte {
	key := make([]byte, 32)
	key[0] = byte(domain) << 4
	key[29] = byte(seq >> 8)
	key[30] = byte(seq)
	key[31] = byte(seq*7 + 3)
	return key
}

// TestHXExtractDomainEpochSelective verifies extraction removes exactly the
// leaves with the evict epoch, leaves everything else readable, and keeps the
// aggregate indicators truthful afterwards.
func TestHXExtractDomainEpochSelective(t *testing.T) {
	db := newTestDB()
	tr := NewEmpty(db)
	// Domain 5 gets mixed epochs; domain 7 is uniformly fresh.
	tr.SetEpochPolicy(func(key []byte) byte {
		if key[0]>>4 == 7 {
			return 1
		}
		return key[30] & 1 // even seq -> 0, odd seq -> 1
	})
	want := make(map[string]string)
	for i := 0; i < 64; i++ {
		key := pruneTestKey(5, i)
		val := []byte(fmt.Sprintf("d5-%d", i))
		if err := tr.Update(key, val); err != nil {
			t.Fatal(err)
		}
		if i%2 == 1 { // odd seq have epoch 1 and must survive evict=0
			want[string(key)] = string(val)
		}
	}
	for i := 0; i < 32; i++ {
		key := pruneTestKey(7, i)
		val := []byte(fmt.Sprintf("d7-%d", i))
		if err := tr.Update(key, val); err != nil {
			t.Fatal(err)
		}
		want[string(key)] = string(val)
	}

	extracted, err := tr.ExtractDomain([]byte{5}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(extracted) != 32 {
		t.Fatalf("extracted %d entries, want 32 (even-seq epoch-0 leaves)", len(extracted))
	}
	for _, e := range extracted {
		if e.Key[0]>>4 != 5 {
			t.Fatalf("extracted key outside domain: %x", e.Key)
		}
		if e.Key[30]&1 != 0 {
			t.Fatalf("extracted fresh leaf %x (odd seq has epoch 1)", e.Key)
		}
	}
	// Survivors readable, extracted gone, aggregates truthful.
	for i := 0; i < 64; i++ {
		key := pruneTestKey(5, i)
		val, err := tr.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if i%2 == 1 {
			if string(val) != fmt.Sprintf("d5-%d", i) {
				t.Fatalf("survivor %d: %q", i, val)
			}
		} else if val != nil {
			t.Fatalf("evicted leaf %d still present", i)
		}
	}
	if err := tr.VerifyAggregates(); err != nil {
		t.Fatalf("aggregates after extract: %v", err)
	}
	// The surviving tree must still commit and reload correctly.
	tdb := newTestDB()
	tdb.nodes = db.nodes
	tdb.commit(t, tr)
}

// TestHXCanSkipSubtreeDoesNotTouch is the paper's phase-1 acceptance test:
// pruning a domain whose leaves all carry the non-evict epoch must not read a
// single node below the domain's navigation path, while an extraction that
// really has targets does touch the subtree.
func TestHXCanSkipSubtreeDoesNotTouch(t *testing.T) {
	db := &countingNodeDB{memNodeDB: newTestDB()}
	tr := NewEmpty(db)
	tr.SetEpochPolicy(func(key []byte) byte { return 1 }) // all fresh
	for i := 0; i < 64; i++ {
		if err := tr.Update(pruneTestKey(3, i), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 16; i++ {
		if err := tr.Update(pruneTestKey(9, i), []byte(fmt.Sprintf("w%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	root, nodes := tr.Commit()
	if nodes != nil {
		for _, n := range nodes.Nodes {
			if n.IsDeleted() {
				delete(db.nodes, n.Hash)
				continue
			}
			db.nodes[n.Hash] = n.Blob
		}
	}

	// Cold reopen, prune domain 3 with evict=0 (nothing expired).
	db.reads.Store(0)
	cold, err := New(root, common.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}
	extracted, err := cold.ExtractDomain([]byte{3}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(extracted) != 0 {
		t.Fatalf("extracted %d fresh leaves, want 0", len(extracted))
	}
	navReads := db.reads.Load()
	// Navigation resolves only the path down to the domain boundary: the root
	// plus at most a couple of compressed nodes. Any descent into the domain
	// subtree would read dozens of nodes for 64 leaves.
	if navReads > 3 {
		t.Fatalf("CanSkip violated: %d node reads for a fully-skipped domain, want <= 3", navReads)
	}

	// Same domain, evict=1 (everything expired): the subtree IS touched.
	db.reads.Store(0)
	cold2, err := New(root, common.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}
	extracted, err = cold2.ExtractDomain([]byte{3}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(extracted) != 64 {
		t.Fatalf("extracted %d, want 64", len(extracted))
	}
	if db.reads.Load() <= navReads {
		t.Fatalf("extraction read %d nodes, want strictly more than the %d navigation reads", db.reads.Load(), navReads)
	}
	if err := cold2.VerifyAggregates(); err != nil {
		t.Fatal(err)
	}
}

// TestHXExtractDomainAbsent covers the empty-tree and absent-domain edges.
func TestHXExtractDomainAbsent(t *testing.T) {
	db := newTestDB()
	tr := NewEmpty(db)
	out, err := tr.ExtractDomain([]byte{4}, 0)
	if err != nil || len(out) != 0 {
		t.Fatalf("empty trie: %d entries, err %v", len(out), err)
	}
	tr.Update(pruneTestKey(1, 1), []byte("x"))
	out, err = tr.ExtractDomain([]byte{4}, 0)
	if err != nil || len(out) != 0 {
		t.Fatalf("absent domain: %d entries, err %v", len(out), err)
	}
	// Domain prefix cutting through a compressed segment: single-leaf tree.
	tr2 := NewEmpty(db)
	tr2.SetEpochPolicy(func(key []byte) byte { return 0 })
	only := pruneTestKey(4, 77)
	tr2.Update(only, []byte("only"))
	out, err = tr2.ExtractDomain([]byte{4}, 0)
	if err != nil || len(out) != 1 || string(out[0].Value) != "only" {
		t.Fatalf("single-leaf domain extract: %d entries, err %v", len(out), err)
	}
	if val, _ := tr2.Get(only); val != nil {
		t.Fatal("extracted leaf still present")
	}
}
