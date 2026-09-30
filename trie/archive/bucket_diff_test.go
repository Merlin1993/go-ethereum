package archive

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"sort"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/leveldb"
)

// TestArchiveCompareBuckets dumps every archive bucket record (prefix
// "MPTA\x01") from two state_db directories and diffs them: path sets must be
// identical and payloads (magic|version|count|sorted entries — deterministic
// by construction) must be byte-equal. Used to localize a final-root
// divergence between two full-length stress runs without re-running them.
var (
	bucketDiffA = flag.String("bucketDiffA", "", "state_db dir of run A")
	bucketDiffB = flag.String("bucketDiffB", "", "state_db dir of run B")
)

var bucketRecordPrefix = []byte{'M', 'P', 'T', 'A', 1}

type bucketDigest struct {
	sum   [32]byte
	count uint32
	size  int
}

func dumpBuckets(t *testing.T, dir string) map[string]bucketDigest {
	t.Helper()
	db, err := leveldb.New(dir, 256, 64, "bucket-diff", true)
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	defer db.Close()
	out := make(map[string]bucketDigest)
	it := db.NewIterator(bucketRecordPrefix, nil)
	defer it.Release()
	for it.Next() {
		key := it.Key()
		val := it.Value()
		var d bucketDigest
		d.sum = sha256.Sum256(val)
		d.size = len(val)
		if len(val) >= 9 {
			d.count = binary.BigEndian.Uint32(val[5:9])
		}
		out[string(bytes.Clone(key))] = d
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterate %s: %v", dir, err)
	}
	return out
}

func TestArchiveCompareBuckets(t *testing.T) {
	if *bucketDiffA == "" || *bucketDiffB == "" {
		t.Skip("need -bucketDiffA and -bucketDiffB")
	}
	a := dumpBuckets(t, *bucketDiffA)
	b := dumpBuckets(t, *bucketDiffB)
	t.Logf("buckets: A=%d B=%d", len(a), len(b))

	var onlyA, onlyB, differ []string
	var entriesA, entriesB int64
	for k, da := range a {
		entriesA += int64(da.count)
		db, ok := b[k]
		if !ok {
			onlyA = append(onlyA, k)
			continue
		}
		if da != db {
			differ = append(differ, k)
		}
	}
	for k, db := range b {
		entriesB += int64(db.count)
		if _, ok := a[k]; !ok {
			onlyB = append(onlyB, k)
		}
	}
	t.Logf("entries: A=%d B=%d (delta=%d)", entriesA, entriesB, entriesB-entriesA)
	t.Logf("paths only in A: %d, only in B: %d, payload differs: %d", len(onlyA), len(onlyB), len(differ))

	show := func(label string, paths []string) {
		sort.Strings(paths)
		for i, p := range paths {
			if i >= 8 {
				t.Logf("%s: ... and %d more", label, len(paths)-8)
				break
			}
			da, oka := a[p]
			dbb, okb := b[p]
			t.Logf("%s path=%x A(count=%d,size=%d,present=%v) B(count=%d,size=%d,present=%v)",
				label, []byte(p), da.count, da.size, oka, dbb.count, dbb.size, okb)
		}
	}
	show("onlyA", onlyA)
	show("onlyB", onlyB)
	show("differ", differ)
}
