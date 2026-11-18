package ecmh

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"
)

func TestCreateEntriesParallelMatchesSerial(t *testing.T) {
	for _, count := range []int{1, 2, 7, 31, 100} {
		entries := make([]Entry, count)
		items := make([][]byte, count)
		for i := range entries {
			key := make([]byte, 32)
			binary.BigEndian.PutUint64(key[24:], uint64(i))
			value := []byte{byte(i), byte(i >> 8), 0x91}
			entries[i] = Entry{Key: key, Value: value}
			hash := sha256.Sum256(value)
			items[i] = append(append([]byte{}, key...), hash[:]...)
		}
		want := Create(items)
		if got := CreateEntriesParallel(entries, 2); got != want {
			t.Fatalf("%d entries: parallel commitment differs", count)
		}
	}
}

func TestCreateEntryBatchesParallelMatchesSerial(t *testing.T) {
	for _, sizes := range [][]int{{}, {0}, {1}, {7, 64}, {64}, {64, 64}, {3, 64, 12, 0, 31}} {
		batches := make([][]Entry, len(sizes))
		want := make([]Commitment, len(sizes))
		for bucket, size := range sizes {
			batches[bucket] = make([]Entry, size)
			for i := range batches[bucket] {
				key := make([]byte, 32)
				binary.BigEndian.PutUint32(key[:4], uint32(bucket))
				binary.BigEndian.PutUint32(key[28:], uint32(i))
				batches[bucket][i] = Entry{Key: key, Value: []byte{byte(i), byte(bucket)}}
			}
			want[bucket] = CreateEntriesParallel(batches[bucket], 1)
		}
		for _, workers := range []int{0, 1, 2, 4, 8, 16} {
			got := CreateEntryBatchesParallel(batches, workers)
			if len(got) != len(want) {
				t.Fatalf("sizes=%v workers=%d: results=%d, want %d", sizes, workers, len(got), len(want))
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("sizes=%v workers=%d bucket=%d: got %x, want %x", sizes, workers, i, got[i], want[i])
				}
			}
		}
	}
}

func BenchmarkCreateEntriesParallel(b *testing.B) {
	for _, count := range []int{20, 100} {
		entries := make([]Entry, count)
		for i := range entries {
			key := make([]byte, 32)
			binary.BigEndian.PutUint64(key[24:], uint64(i))
			value := sha256.Sum256(key)
			entries[i] = Entry{Key: key, Value: value[:]}
		}
		for _, workers := range []int{1, 2, 4, 8, 16} {
			b.Run(fmt.Sprintf("entries=%d/workers=%d", count, workers), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					benchCommit = CreateEntriesParallel(entries, workers)
				}
			})
		}
	}
}
