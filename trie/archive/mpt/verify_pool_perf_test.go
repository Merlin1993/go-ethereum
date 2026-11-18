package mpt

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/trie/archive/cuckoo"
	"github.com/ethereum/go-ethereum/trie/archive/mpt/ecmh"
)

// BenchmarkAuditPending models the distinct pre-redemption bucket snapshots
// verified at one block's commit. The common case has one or two buckets.
func BenchmarkAuditPending(b *testing.B) {
	for _, bucketCount := range []int{1, 2, 8} {
		b.Run(fmt.Sprintf("buckets=%d", bucketCount), func(b *testing.B) {
			audits := make([]bucketAudit, bucketCount)
			for i := range audits {
				entries := make([]ecmh.Entry, 64)
				items := make([][]byte, len(entries))
				filter := cuckoo.New(32, 4)
				for j := range entries {
					key := make([]byte, 32)
					binary.BigEndian.PutUint32(key[:4], uint32(i))
					binary.BigEndian.PutUint32(key[28:], uint32(j))
					value := sha256.Sum256(key)
					entries[j] = ecmh.Entry{Key: key, Value: value[:]}
					items[j] = bucketItem(key, value[:])
					if err := filter.Insert(key); err != nil {
						b.Fatal(err)
					}
				}
				bucket := &bucket{
					path:       []byte{byte(i)},
					filter:     filter,
					commitment: ecmh.Create(items),
					count:      len(entries),
				}
				bucket.stub()
				audits[i] = bucketAudit{
					path:       bucket.path,
					commitment: bucket.commitment,
					metaHash:   bucket.metaHash,
					metaBytes:  bucket.metaBytes,
					entries:    entries,
				}
			}
			tr := &Trie{config: Config{VerifyBucketWorkers: 8, VerifyItemWorkers: 2}, pendingAudits: audits}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := tr.auditPendingLocked(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
