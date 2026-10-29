package mpt

import (
	"encoding/hex"
	"testing"
)

func TestMPTBucketItemSHA256(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	got := bucketItem(key, []byte("payload"))
	const wantValueHash = "239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5"
	if len(got) != 64 || hex.EncodeToString(got[32:]) != wantValueHash {
		t.Fatalf("unexpected bucket item: %x", got)
	}
	for i := range key {
		if got[i] != key[i] {
			t.Fatalf("key byte %d changed", i)
		}
	}
}
