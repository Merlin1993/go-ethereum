package archive

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
)

// traceSplitArchiveStore keeps MPTA payload records in a separate LevelDB.
// All other records, including MPTB metadata and pathdb nodes, stay in the
// full-node database. This adapter is enabled only by the E2 replay flag.
type traceSplitArchiveStore struct {
	ethdb.KeyValueStore
	archive ethdb.KeyValueStore
}

var traceArchivePayloadPrefix = []byte{'M', 'P', 'T', 'A', 1}

func (s *traceSplitArchiveStore) target(key []byte) ethdb.KeyValueStore {
	if bytes.HasPrefix(key, traceArchivePayloadPrefix) {
		return s.archive
	}
	return s.KeyValueStore
}

func (s *traceSplitArchiveStore) Has(key []byte) (bool, error) {
	return s.target(key).Has(key)
}

func (s *traceSplitArchiveStore) Get(key []byte) ([]byte, error) {
	return s.target(key).Get(key)
}

func (s *traceSplitArchiveStore) Put(key, value []byte) error {
	return s.target(key).Put(key, value)
}

func (s *traceSplitArchiveStore) Delete(key []byte) error {
	return s.target(key).Delete(key)
}

func (s *traceSplitArchiveStore) NewBatch() ethdb.Batch {
	return &traceSplitArchiveBatch{full: s.KeyValueStore.NewBatch(), archive: s.archive.NewBatch()}
}

func (s *traceSplitArchiveStore) NewBatchWithSize(size int) ethdb.Batch {
	// pathdb's large size hint belongs to the full-node batch. Allocating it
	// again for the archive would double the transient memory requirement.
	return &traceSplitArchiveBatch{full: s.KeyValueStore.NewBatchWithSize(size), archive: s.archive.NewBatch()}
}

func (s *traceSplitArchiveStore) Compact(start, limit []byte) error {
	if err := s.KeyValueStore.Compact(start, limit); err != nil {
		return err
	}
	return s.archive.Compact(start, limit)
}

func (s *traceSplitArchiveStore) NewIterator(prefix, start []byte) ethdb.Iterator {
	if len(prefix) == 0 {
		return &traceSplitErrorIterator{err: errors.New("split archive store requires an explicit iterator prefix")}
	}
	return s.target(prefix).NewIterator(prefix, start)
}

type traceSplitErrorIterator struct{ err error }

func (it *traceSplitErrorIterator) Next() bool    { return false }
func (it *traceSplitErrorIterator) Error() error  { return it.err }
func (it *traceSplitErrorIterator) Key() []byte   { return nil }
func (it *traceSplitErrorIterator) Value() []byte { return nil }
func (it *traceSplitErrorIterator) Release()      {}

type traceSplitArchiveBatch struct {
	full, archive ethdb.Batch
}

func (b *traceSplitArchiveBatch) target(key []byte) ethdb.Batch {
	if bytes.HasPrefix(key, traceArchivePayloadPrefix) {
		return b.archive
	}
	return b.full
}

func (b *traceSplitArchiveBatch) Put(key, value []byte) error {
	return b.target(key).Put(key, value)
}

func (b *traceSplitArchiveBatch) Delete(key []byte) error {
	return b.target(key).Delete(key)
}

func (b *traceSplitArchiveBatch) ValueSize() int {
	return b.full.ValueSize() + b.archive.ValueSize()
}

func (b *traceSplitArchiveBatch) Write() error {
	// Archive first: if the second write fails, the full-node database cannot
	// contain a new reference to an absent payload. Cross-DB atomicity is not
	// provided; a failed replay must not be resumed from these two directories.
	if err := b.archive.Write(); err != nil {
		return err
	}
	return b.full.Write()
}

func (b *traceSplitArchiveBatch) Reset() {
	b.full.Reset()
	b.archive.Reset()
}

func (b *traceSplitArchiveBatch) Replay(w ethdb.KeyValueWriter) error {
	if err := b.archive.Replay(w); err != nil {
		return err
	}
	return b.full.Replay(w)
}

func TestTraceSplitArchiveStoreRouting(t *testing.T) {
	full, err := leveldb.New(filepath.Join(t.TempDir(), "full"), 16, 16, "full", false)
	if err != nil {
		t.Fatal(err)
	}
	defer full.Close()
	archive, err := leveldb.New(filepath.Join(t.TempDir(), "archive"), 16, 16, "archive", false)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	store := &traceSplitArchiveStore{KeyValueStore: full, archive: archive}
	archiveKey := append(bytes.Clone(traceArchivePayloadPrefix), 0x42)
	metaKey := []byte{'M', 'P', 'T', 'B', 1, 0x42}
	nodeKey := []byte("AMTP/node")
	batch := store.NewBatchWithSize(1024)
	for _, key := range [][]byte{archiveKey, metaKey, nodeKey} {
		if err := batch.Put(key, []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	if err := store.Compact(nil, nil); err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		db      ethdb.KeyValueStore
		key     []byte
		present bool
	}{
		{archive, archiveKey, true}, {full, archiveKey, false},
		{full, metaKey, true}, {archive, metaKey, false},
		{full, nodeKey, true}, {archive, nodeKey, false},
	} {
		got, err := check.db.Has(check.key)
		if err != nil || got != check.present {
			t.Fatalf("key %x: present=%v, want %v, err=%v", check.key, got, check.present, err)
		}
	}
}
