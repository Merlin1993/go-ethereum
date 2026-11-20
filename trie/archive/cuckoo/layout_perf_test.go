package cuckoo

import (
	"encoding/hex"
	"testing"
)

var filterLayoutSink *Filter
var filterLookupSink bool

func BenchmarkFilterNew(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		filterLayoutSink = New(32, 4)
	}
}

func BenchmarkFilterLookup(b *testing.B) {
	f := New(32, 4)
	for i := 0; i < 64; i++ {
		if err := f.Insert([]byte{byte(i)}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filterLookupSink = f.Lookup([]byte{byte(i)})
	}
}

func TestFilterWireFixture(t *testing.T) {
	f := New(32, 4)
	for _, item := range []string{"apple", "banana", "cherry", "date"} {
		if err := f.Insert([]byte(item)); err != nil {
			t.Fatal(err)
		}
	}
	const want = "000000200000000400800000000000080080000800000000d10b7026121b3b5b"
	if got := hex.EncodeToString(f.Encode()); got != want {
		t.Fatalf("filter encoding changed: got %s, want %s", got, want)
	}
	oldWire, err := hex.DecodeString(want)
	if err != nil {
		t.Fatal(err)
	}
	reloaded := New(0, 0)
	if err := reloaded.Decode(oldWire, 0, 0); err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(reloaded.Encode()); got != want {
		t.Fatalf("old filter encoding did not round-trip: got %s", got)
	}
	for _, item := range []string{"apple", "banana", "cherry", "date"} {
		if !reloaded.Lookup([]byte(item)) {
			t.Fatalf("old filter encoding lost %q", item)
		}
	}
}
