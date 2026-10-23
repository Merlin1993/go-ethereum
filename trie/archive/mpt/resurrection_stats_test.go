package mpt

import (
	"bytes"
	"sort"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	archivetrie "github.com/ethereum/go-ethereum/trie/archive"
)

// TestResurrectionProofHistogram 验证复活证明直方图的语义：归档后读触发复活，
// TakeMPTRedemptionProofHistogram 返回的 size 与该次复活实际递送的证明字节
// 一致（差分对账 MPTRedemptionProofStats 的 bytes 增量），latency 非负、
// 计数与复活次数一致；drain 后再次 Take 为空（不抽样、不清零即丢口径）。
func TestResurrectionProofHistogram(t *testing.T) {
	db := &testStore{memorydb.New()}
	// activate=true：读时复活
	tr := newTestTrie(t, db, true)

	key := []byte{0x00, 'r'}
	value := []byte("resurrect-me")
	if err := tr.Put(key, value); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	// 归档（一个完整裁剪轮换周期）
	expirePrune(t, tr, 0)
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}

	// 复活前清空直方图残留（drain-to-empty：直方图是进程级全局遥测，
	// 同包其它测试的复活操作会留下事件——drain 是唯一干净起点）
	archivetrie.TakeMPTRedemptionProofHistogram()
	sizes, latencies, _, _, _ := archivetrie.TakeMPTRedemptionProofHistogram()
	if len(sizes) != 0 || len(latencies) != 0 {
		t.Fatalf("histogram not empty after drain: sizes=%d latencies=%d", len(sizes), len(latencies))
	}

	// 读触发一次复活
	beforeCount, beforeBytes, _, _ := archivetrie.MPTRedemptionProofStats()
	got, err := tr.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("resurrected %q; want %q", got, value)
	}
	afterCount, afterBytes, _, _ := archivetrie.MPTRedemptionProofStats()

	sizes, latencies, maxSz, maxNs, _ := archivetrie.TakeMPTRedemptionProofHistogram()
	if len(sizes) != 1 || len(latencies) != 1 {
		t.Fatalf("histogram recorded %d/%d events; want 1/1 (no sampling)", len(sizes), len(latencies))
	}
	if delta := afterBytes - beforeBytes; sizes[0] != delta {
		t.Fatalf("histogram size %d != proof-bytes delta %d", sizes[0], delta)
	}
	if afterCount-beforeCount != 1 {
		t.Fatalf("resurrections delta %d; want 1", afterCount-beforeCount)
	}
	if sizes[0] <= 0 {
		t.Fatalf("proof size %d must be positive (stub meta + payload)", sizes[0])
	}
	// maxSize/maxNs 是 lifetime 峰值（跨 drain 不清零，与既有全局遥测口径
	// 一致）——只断言它们不低于本次观测值
	if maxSz < sizes[0] {
		t.Fatalf("lifetime maxSize %d < observed size %d", maxSz, sizes[0])
	}
	if latencies[0] < 0 || maxNs < latencies[0] {
		t.Fatalf("latency %d / lifetime max %d must satisfy max >= latency >= 0", latencies[0], maxNs)
	}
	// drain 后再 Take 为空
	sizes, _, _, _, _ = archivetrie.TakeMPTRedemptionProofHistogram()
	if len(sizes) != 0 {
		t.Fatalf("histogram not drained: %d events remain", len(sizes))
	}
}

// TestBucketOccupancySnapshot 验证归档桶占用分布快照的语义：每桶条目数、
// 空桶计数与桶注册表对账一致，且桶总数与 LiveBuckets 同源。
func TestBucketOccupancySnapshot(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, true)

	// 写三个域的键再归档，让桶有不同占用
	put := func(k string) {
		if err := tr.Put([]byte{0x00, 'a', 'v', k[0]}, []byte("v-"+k)); err != nil {
			t.Fatal(err)
		}
	}
	put("1")
	put("2")
	put("3")
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}

	counts, empty := tr.BucketOccupancy()
	if len(counts) == 0 {
		t.Fatal("no buckets reported")
	}
	// 对账 1：与桶注册表逐桶比对
	regCounts, regEmpty := 0, int64(0)
	for _, b := range tr.buckets {
		n := int64(len(b.entries))
		regCounts++
		if n == 0 {
			regEmpty++
		}
	}
	if int64(len(counts)) != int64(regCounts) {
		t.Fatalf("snapshot bucket count %d != registry %d", len(counts), regCounts)
	}
	if empty != regEmpty {
		t.Fatalf("snapshot empty buckets %d != registry %d", empty, regEmpty)
	}
	// 对账 2：快照桶总数与 LiveBuckets 同源
	if int64(len(counts)) != tr.LiveBuckets() {
		t.Fatalf("snapshot buckets %d != LiveBuckets %d", len(counts), tr.LiveBuckets())
	}
	// 占用值合理性：全部在 [0, bucket capacity]
	sorted := append([]int64(nil), counts...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	for _, c := range sorted {
		if c < 0 || c > 100 {
			t.Fatalf("bucket occupancy %d out of [0,100]", c)
		}
	}
	// 事件键已复活场景外——这里没有 Get，桶应保有全部三键或部分（裁剪轮换语义），
	// 但快照必须自洽：sum(counts) == sum(registry entries)
	sumSnap := int64(0)
	for _, c := range counts {
		sumSnap += c
	}
	sumReg := int64(0)
	for _, b := range tr.buckets {
		sumReg += int64(len(b.entries))
	}
	if sumSnap != sumReg {
		t.Fatalf("snapshot occupancy sum %d != registry sum %d", sumSnap, sumReg)
	}
}
