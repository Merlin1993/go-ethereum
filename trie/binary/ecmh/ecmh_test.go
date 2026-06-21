// Copyright 2024 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package ecmh

import (
	"math/rand"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// randomHash 生成一个随机的 common.Hash。
func randomHash() common.Hash {
	var h common.Hash
	rand.Read(h[:])
	return h
}

// TestECMHBasic 测试 ECMH 的基本添加和删除功能。
func TestECMHBasic(t *testing.T) {
	committer := New()
	hashes := []common.Hash{
		randomHash(),
		randomHash(),
		randomHash(),
	}

	// 1. 测试从零开始添加
	commitment, err := committer.Add(nil, hashes)
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if len(commitment) != 33 {
		t.Errorf("Commitment length mismatch: got %d, want 33", len(commitment))
	}

	// 2. 测试验证
	if !committer.Verify(hashes, commitment) {
		t.Error("Verify failed for valid hashes")
	}

	// 3. 测试删除后的还原
	afterDelete, err := committer.Delete(commitment, hashes)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	// 删除所有哈希后应该回到无穷远点（空字节）
	if len(afterDelete) != 0 {
		t.Errorf("Commitment after deleting all should be empty, got %v", afterDelete)
	}
}

// TestECMHCommutative 测试 ECMH 的交换律（添加顺序无关性）。
func TestECMHCommutative(t *testing.T) {
	committer := New()
	h1, h2, h3 := randomHash(), randomHash(), randomHash()

	// 顺序 1: h1, h2, h3
	c1, _ := committer.Add(nil, []common.Hash{h1, h2, h3})

	// 顺序 2: h3, h1, h2
	c2, _ := committer.Add(nil, []common.Hash{h3, h1, h2})

	if !reflect.DeepEqual(c1, c2) {
		t.Errorf("Order independence failed: order1 %x, order2 %x", c1, c2)
	}
}

// TestECMHIncremental 测试增量更新。
func TestECMHIncremental(t *testing.T) {
	committer := New()
	h1, h2 := randomHash(), randomHash()

	// 先添加 h1
	c1, _ := committer.Add(nil, []common.Hash{h1})
	// 再在 c1 基础上添加 h2
	c1_2, _ := committer.Add(c1, []common.Hash{h2})

	// 直接添加 h1 和 h2
	cDirect, _ := committer.Add(nil, []common.Hash{h1, h2})

	if !reflect.DeepEqual(c1_2, cDirect) {
		t.Errorf("Incremental add failed: incremental %x, direct %x", c1_2, cDirect)
	}
}

// TestECMHVerifyFailure 测试 Verify 的失败情况。
func TestECMHMerge(t *testing.T) {
	committer := New()
	left := []common.Hash{randomHash(), randomHash()}
	right := []common.Hash{randomHash(), randomHash(), randomHash()}

	leftCommitment, err := committer.Add(nil, left)
	if err != nil {
		t.Fatalf("left Add failed: %v", err)
	}
	rightCommitment, err := committer.Add(nil, right)
	if err != nil {
		t.Fatalf("right Add failed: %v", err)
	}
	merged, err := committer.Merge(leftCommitment, rightCommitment)
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	all := append(append([]common.Hash{}, left...), right...)
	direct, err := committer.Add(nil, all)
	if err != nil {
		t.Fatalf("direct Add failed: %v", err)
	}
	if !reflect.DeepEqual(merged, direct) {
		t.Fatalf("merged commitment mismatch: merged %x direct %x", merged, direct)
	}
}

func TestECMHMergePoints(t *testing.T) {
	committer := New()
	left := []common.Hash{randomHash(), randomHash()}
	right := []common.Hash{randomHash(), randomHash(), randomHash()}

	leftCommitment, err := committer.Add(nil, left)
	if err != nil {
		t.Fatalf("left Add failed: %v", err)
	}
	rightCommitment, err := committer.Add(nil, right)
	if err != nil {
		t.Fatalf("right Add failed: %v", err)
	}
	leftPoint, err := committer.DecodePoint(leftCommitment)
	if err != nil {
		t.Fatalf("decode left failed: %v", err)
	}
	rightPoint, err := committer.DecodePoint(rightCommitment)
	if err != nil {
		t.Fatalf("decode right failed: %v", err)
	}

	mergedPoints, cachedPoint, err := committer.MergePoints(leftPoint, rightPoint)
	if err != nil {
		t.Fatalf("MergePoints failed: %v", err)
	}
	mergedAgain, _, err := committer.MergePoints(cachedPoint)
	if err != nil {
		t.Fatalf("MergePoints cached point failed: %v", err)
	}
	if !reflect.DeepEqual(mergedPoints, mergedAgain) {
		t.Fatalf("cached point commitment mismatch: merged %x cached %x", mergedPoints, mergedAgain)
	}

	merged, err := committer.Merge(leftCommitment, rightCommitment)
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
	if !reflect.DeepEqual(mergedPoints, merged) {
		t.Fatalf("point merge mismatch: points %x merge %x", mergedPoints, merged)
	}
}

func TestECMHVerifyFailure(t *testing.T) {
	committer := New()
	hashes := []common.Hash{randomHash(), randomHash()}
	commitment, _ := committer.Add(nil, hashes)

	// 情况 1: 哈希集合缺失
	if committer.Verify(hashes[:1], commitment) {
		t.Error("Verify should fail for missing hash")
	}

	// 情况 2: 哈希集合多了一个
	if committer.Verify(append(hashes, randomHash()), commitment) {
		t.Error("Verify should fail for extra hash")
	}

	// 情况 3: 承诺被篡改
	tampered := make([]byte, len(commitment))
	copy(tampered, commitment)
	tampered[10] ^= 0xff
	if committer.Verify(hashes, tampered) {
		t.Error("Verify should fail for tampered commitment")
	}
}

// BenchmarkHashToPoint 测量哈希映射到曲线上点的时间。
func BenchmarkHashToPoint(b *testing.B) {
	committer := New()
	h := randomHash()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		committer.hashToPoint(h)
	}
}

// BenchmarkECMHAdd 测量批量添加哈希的时间。
func BenchmarkECMHAdd(b *testing.B) {
	committer := New()
	hashes := make([]common.Hash, 100)
	for i := range hashes {
		hashes[i] = randomHash()
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = committer.Add(nil, hashes)
	}
}

// BenchmarkECMHUpdate 测量在现有承诺上更新一个哈希的时间。
func BenchmarkECMHUpdate(b *testing.B) {
	committer := New()
	h1 := randomHash()
	h2 := randomHash()
	c, _ := committer.Add(nil, []common.Hash{h1})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = committer.Add(c, []common.Hash{h2})
	}
}

func TestHashToPointFastMatchesBigInt(t *testing.T) {
	committer := New()
	for i := 0; i < 128; i++ {
		h := randomHash()
		fastX, fastY := committer.hashToPoint(h)
		refX, refY := committer.hashToPointBigInt(h)
		if fastX.Cmp(refX) != 0 || fastY.Cmp(refY) != 0 {
			t.Fatalf("hashToPoint mismatch at %d: fast=(%x,%x), ref=(%x,%x)", i, fastX, fastY, refX, refY)
		}
	}
}
