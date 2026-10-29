package archive

import (
	"encoding/hex"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// Fixed values independently calculated with SHA-256 for the key-bound value
// reference and the 32-byte ECMH message. This catches accidental reversion
// to Keccak-256 even if producer and verifier are changed together.
func TestArchiveECMHSHA256Input(t *testing.T) {
	key := []byte{0x12, 0x34, 0x56, 0x78}
	value := []byte("payload")
	const wantRef = "6450a635a3f4fcb535f519a13e91db48fbe37d1be9bb0c03fbdb0118ace0d809"
	const wantEntry = "185d0e83bf7717a01761a8912c7bb269316efc886161767a5593505e94f97058"
	ref := valueRefForKeyValue(key, value)
	if hex.EncodeToString(ref) != wantRef {
		t.Fatalf("value reference mismatch: %x", ref)
	}
	bucket := &ArchiveBucketNode{}
	entry := ArchivedKey{Suffix: key, SuffixBits: len(key) * 8, ValueRef: ref}
	got, _, _ := (&Shard{}).archivePointHash(bucket, entry, ref, nil, nil)
	if got != common.HexToHash(wantEntry) {
		t.Fatalf("ECMH entry digest mismatch: %x", got)
	}
}
