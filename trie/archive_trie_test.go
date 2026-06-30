package trie

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	archivetrie "github.com/ethereum/go-ethereum/trie/archive"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

func TestNodeSetBatcherPersistsRawFlatValues(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	adapter := &archiveDBAdapter{disk: disk}
	nodes := trienode.NewNodeSet(common.Hash{})
	batch := disk.NewBatch()
	writer := &nodeSetBatcher{
		adapter:  adapter,
		nodes:    nodes,
		rawBatch: batch,
	}

	key := []byte("BFV1-account-key")
	value := []byte("flat-value")
	if err := writer.Put(key, value); err != nil {
		t.Fatalf("put flat value: %v", err)
	}
	if len(nodes.Nodes) != 0 {
		t.Fatalf("flat value was incorrectly added to trie node set")
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("write flat value batch: %v", err)
	}
	got, err := disk.Get(key)
	if err != nil {
		t.Fatalf("read persisted flat value: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("flat value mismatch: got %q want %q", got, value)
	}

	deleteBatch := disk.NewBatch()
	writer.rawBatch = deleteBatch
	if err := writer.Delete(key); err != nil {
		t.Fatalf("delete flat value: %v", err)
	}
	if err := deleteBatch.Write(); err != nil {
		t.Fatalf("write flat value delete batch: %v", err)
	}
	has, err := disk.Has(key)
	if err != nil {
		t.Fatalf("check deleted flat value: %v", err)
	}
	if has {
		t.Fatalf("flat value still exists after batched delete")
	}
}

func TestNodeSetBatcherTreatsThirtyTwoBytePathKeyAsRaw(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	adapter := &archiveDBAdapter{disk: disk}
	nodes := trienode.NewNodeSet(common.Hash{})
	batch := disk.NewBatch()
	writer := &nodeSetBatcher{
		adapter:  adapter,
		nodes:    nodes,
		rawBatch: batch,
	}

	key := make([]byte, common.HashLength)
	copy(key, []byte("BPN1"))
	if !archivetrie.IsPathStorageKey(key) {
		t.Fatalf("test key was not recognized as path storage")
	}
	value := []byte("path-node")
	if err := writer.Put(key, value); err != nil {
		t.Fatal(err)
	}
	if len(nodes.Nodes) != 0 {
		t.Fatalf("32-byte path key was incorrectly added to trie node set")
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	got, err := disk.Get(key)
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("raw path node mismatch: got %x err %v", got, err)
	}

	deleteBatch := disk.NewBatch()
	writer.rawBatch = deleteBatch
	if err := writer.Delete(key); err != nil {
		t.Fatal(err)
	}
	if err := deleteBatch.Write(); err != nil {
		t.Fatal(err)
	}
	if has, err := disk.Has(key); err != nil || has {
		t.Fatalf("raw path node delete failed: has=%t err=%v", has, err)
	}
}

func TestRawBatchBufferReplaysInOrder(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	buffer := new(rawBatchBuffer)
	key := []byte("path-key")

	if err := buffer.Put(key, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := buffer.Delete(key); err != nil {
		t.Fatal(err)
	}
	if err := buffer.Put(key, []byte("last")); err != nil {
		t.Fatal(err)
	}
	batch := disk.NewBatch()
	if err := buffer.replay(batch); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	got, err := disk.Get(key)
	if err != nil || !bytes.Equal(got, []byte("last")) {
		t.Fatalf("replayed value mismatch: got %q err %v", got, err)
	}
}
