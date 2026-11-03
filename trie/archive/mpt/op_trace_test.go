package mpt

import (
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

func TestOpTraceScopesForegroundRedemption(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, true)
	key := []byte{0x00, 0x11}
	if err := tr.Put(key, []byte("value")); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	tr.buckets = make(map[string]*bucket)
	tr.archiveLRU.Init()
	tr.residentEntries = 0
	tr.opTrace = true
	opTraceActive.Store(false)
	defer opTraceActive.Store(false)
	before := opTraceSnapshot()
	if _, archived, err := tr.GetValueRef(key); err != nil || !archived {
		t.Fatalf("classification: archived=%t err=%v", archived, err)
	}
	for name, value := range opTraceSnapshot() {
		if value != before[name] {
			t.Fatalf("inactive classification changed %s: %d -> %d", name, before[name], value)
		}
	}
	// Force a payload reload by dropping the classification read's resident map.
	tr.buckets = make(map[string]*bucket)
	tr.archiveLRU.Init()
	tr.residentEntries = 0
	opTraceActive.Store(true)
	if _, err := tr.Get(key); err != nil {
		t.Fatal(err)
	}
	opTraceActive.Store(false)
	after := opTraceSnapshot()
	delta := func(name string) int64 { return after[name] - before[name] }
	for name, want := range map[string]int64{"ops": 1, "hot_misses": 1, "probes": 1, "loads": 1, "removes": 1} {
		if got := delta(name); got != want {
			t.Errorf("%s = %d, want %d", name, got, want)
		}
	}
	for _, name := range []string{"hot_ns", "probe_ns", "filter_ns", "load_ns", "promotion_ns", "remove_ns", "promotion_hot_ns"} {
		if delta(name) < 0 {
			t.Errorf("%s = %d, want nonnegative", name, delta(name))
		}
	}
	if delta("filter_ns")+delta("load_ns") > delta("probe_ns") {
		t.Error("nested probe timings exceed probe total")
	}
	if delta("remove_ns")+delta("promotion_hot_ns") > delta("promotion_ns") {
		t.Error("nested promotion timings exceed promotion total")
	}
}
