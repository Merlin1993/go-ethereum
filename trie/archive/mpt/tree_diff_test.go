package mpt

import (
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	gethtrie "github.com/ethereum/go-ethereum/trie/archive/mpt/hx"
)

// TestMPTTreeDiff opens two state_db directories (path backend) at their
// reported final roots and forensically compares the hot trees:
//  1. hx.New must resolve the reported root (catches a root that was never
//     persisted by the final forced checkpoint),
//  2. VerifyAggregates must hold (catches a stale aggregate sealed by the
//     split-commit pattern),
//  3. the ordered leaf streams are hashed and compared (catches genuine
//     content divergence).
var (
	treeDiffA     = flag.String("treeDiffA", "", "state_db dir of run A")
	treeDiffARoot = flag.String("treeDiffARoot", "", "final root of run A")
	treeDiffB     = flag.String("treeDiffB", "", "state_db dir of run B")
	treeDiffBRoot = flag.String("treeDiffBRoot", "", "final root of run B")
)

func openTreeAtRoot(t *testing.T, dir, rootHex string) (*gethtrie.Trie, func()) {
	t.Helper()
	ldb, err := leveldb.New(dir, 256, 64, "tree-diff", true)
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	tdb, _, err := openPathDatabase(&levelStore{ldb}, 256<<20, 256<<20)
	if err != nil {
		ldb.Close()
		t.Fatalf("open pathdb %s: %v", dir, err)
	}
	tr, err := gethtrie.New(common.HexToHash(rootHex), common.Hash{}, tdb)
	if err != nil {
		tdb.Close()
		ldb.Close()
		t.Fatalf("resolve root %s in %s: %v", rootHex, dir, err)
	}
	return tr, func() { tdb.Close(); ldb.Close() }
}

func TestMPTTreeDiff(t *testing.T) {
	if *treeDiffA == "" || *treeDiffB == "" {
		t.Skip("need -treeDiffA/-treeDiffARoot/-treeDiffB/-treeDiffBRoot")
	}
	trA, closeA := openTreeAtRoot(t, *treeDiffA, *treeDiffARoot)
	defer closeA()
	trB, closeB := openTreeAtRoot(t, *treeDiffB, *treeDiffBRoot)
	defer closeB()

	if err := trA.VerifyAggregates(); err != nil {
		t.Logf("A VerifyAggregates FAIL: %v", err)
	} else {
		t.Logf("A VerifyAggregates OK")
	}
	if err := trB.VerifyAggregates(); err != nil {
		t.Logf("B VerifyAggregates FAIL: %v", err)
	} else {
		t.Logf("B VerifyAggregates OK")
	}

	leafDigest := func(name string, tr *gethtrie.Trie) ([32]byte, int64) {
		h := sha256.New()
		var n int64
		if err := tr.CollectLeaves(func(key, value []byte) bool {
			var l [4]byte
			binary.BigEndian.PutUint32(l[:], uint32(len(value)))
			h.Write(key)
			h.Write(l[:])
			h.Write(value)
			n++
			return true
		}); err != nil {
			t.Fatalf("%s CollectLeaves: %v", name, err)
		}
		var sum [32]byte
		copy(sum[:], h.Sum(nil))
		return sum, n
	}
	sumA, nA := leafDigest("A", trA)
	sumB, nB := leafDigest("B", trB)
	t.Logf("A leaves=%d digest=%x", nA, sumA)
	t.Logf("B leaves=%d digest=%x", nB, sumB)
	if sumA != sumB || nA != nB {
		t.Logf("LEAF CONTENT DIVERGES")
	} else {
		t.Logf("LEAF CONTENT IDENTICAL")
	}
}
