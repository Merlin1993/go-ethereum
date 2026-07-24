package archive

import (
	"bytes"
	"errors"
	"testing"
)

type laggingFlatReader struct {
	values map[string][]byte
}

func (r *laggingFlatReader) GetFlatValue(key []byte) ([]byte, error) {
	if value, ok := r.values[string(key)]; ok {
		return bytes.Clone(value), nil
	}
	return nil, ErrNodeNotFound
}

func TestCommittedFlatDeleteMasksLaggingReader(t *testing.T) {
	db := NewMemoryDBAdapter()
	reader := &laggingFlatReader{values: make(map[string][]byte)}
	config := DefaultConfig()
	config.ShardDepth = 0
	config.FlatReader = reader
	trie := NewTrie(nil, db, NewPooledKeccakHasher(), config, false)
	key := bytes.Repeat([]byte{0x7a}, 32)
	value := []byte("old")

	if err := trie.Put(key, value); err != nil {
		t.Fatal(err)
	}
	batch := db.NewBatch()
	if _, err := trie.CommitToBatch(batch, false); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	// Simulate a snapshot reader that has observed the put but not the next
	// deletion yet.
	reader.values[string(key)] = bytes.Clone(value)

	if err := trie.Delete(key); err != nil {
		t.Fatal(err)
	}
	batch = db.NewBatch()
	if _, err := trie.CommitToBatch(batch, false); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	if _, err := trie.GetFlatValue(key); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("committed delete exposed lagging value: %v", err)
	}
}
