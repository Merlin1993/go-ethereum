package archive

import (
	"bytes"
	"testing"
)

func TestPrefixKeys(t *testing.T) {
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()
	config := DefaultConfig()
	trie := NewTrie(nil, db, hasher, config, false)

	// Domain prefix: 0x00 for account, 0x01 for storage
	keyA := append([]byte{0x00}, []byte{0xaa, 0xbb, 0xcc}...)
	valA := []byte{0x01}

	keyAB := append([]byte{0x01}, []byte{0xaa, 0xbb, 0xcc, 0xdd}...) // Storage slot
	valAB := []byte{0x02}

	t.Logf("Putting keyA")
	trie.Put(keyA, valA)

	t.Logf("Putting keyAB")
	trie.Put(keyAB, valAB)

	v, err := trie.Get(keyA)
	if err != nil {
		t.Errorf("Get(keyA) error: %v", err)
	}
	if !bytes.Equal(v, valA) {
		t.Errorf("Get(keyA) mismatch: got %x, want %x", v, valA)
	}

	v, err = trie.Get(keyAB)
	if err != nil {
		t.Errorf("Get(keyAB) error: %v", err)
	}
	if !bytes.Equal(v, valAB) {
		t.Errorf("Get(keyAB) mismatch: got %x, want %x", v, valAB)
	}
}
