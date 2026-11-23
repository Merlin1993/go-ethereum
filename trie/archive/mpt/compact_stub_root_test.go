package mpt

import (
	"encoding/hex"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

func TestCompactStubRootCompatibility(t *testing.T) {
	for _, backend := range []string{BackendHash, BackendPath} {
		t.Run(backend, func(t *testing.T) {
			tr := newTestTrieWithBackend(t, &testStore{memorydb.New()}, backend, true)
			keys := [][]byte{{0x00, 0x11}, {0x00, 0x12}, {0x00, 0x13}, {0x00, 0x14}, {0x00, 0x15}}
			for i, key := range keys {
				if err := tr.Put(key, []byte{byte(i + 1)}); err != nil {
					t.Fatal(err)
				}
			}
			var roots [3]string
			root, err := tr.Commit()
			if err != nil {
				t.Fatal(err)
			}
			roots[0] = hex.EncodeToString(root)
			expirePrune(t, tr, 0)
			root, err = tr.Commit()
			if err != nil {
				t.Fatal(err)
			}
			roots[1] = hex.EncodeToString(root)
			for _, key := range keys[:2] {
				if _, err := tr.Get(key); err != nil {
					t.Fatal(err)
				}
			}
			root, err = tr.Commit()
			if err != nil {
				t.Fatal(err)
			}
			roots[2] = hex.EncodeToString(root)
			want := [3]string{
				"a6af1eef99cd9e5fb32e5c1d4fa7c0e0c9a6c780edff5848cd1a03f1a9fdd143",
				"c77f5565a116113cd21189edb3eb52de68d658a3a62732b17e06d8096c0b0d19",
				"b928c68542dba2db8521b72d6aa4bb06a21bc42ab0335542260f4d5b09081df5",
			}
			if roots != want {
				t.Fatalf("root encoding changed: got %v, want %v", roots, want)
			}
		})
	}
}
