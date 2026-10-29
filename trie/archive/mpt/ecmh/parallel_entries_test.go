package ecmh

import (
	"crypto/sha256"
	"encoding/binary"
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
