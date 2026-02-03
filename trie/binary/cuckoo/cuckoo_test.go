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

package cuckoo

import (
	"fmt"
	"testing"
)

func TestFilterBasic(t *testing.T) {
	f := New()
	data := []byte("hello")

	if f.Lookup(data) {
		t.Fatal("未插入的数据查询结果应为 false")
	}

	if err := f.Insert(data); err != nil {
		t.Fatalf("插入失败: %v", err)
	}

	if !f.Lookup(data) {
		t.Fatal("插入的数据查询结果应为 true")
	}

	if !f.Delete(data) {
		t.Fatal("删除已存在的数据应返回 true")
	}

	if f.Lookup(data) {
		t.Fatal("删除后的数据查询结果应为 false")
	}
}

func TestFilterCapacity(t *testing.T) {
	f := New()
	count := 0
	for i := 0; i < 100; i++ {
		data := []byte(fmt.Sprintf("data-%d", i))
		if err := f.Insert(data); err != nil {
			t.Logf("在存满前共插入了 %d 个元素", i)
			break
		}
		count++
	}

	if count < 80 {
		t.Fatalf("过滤器应至少支持 80 个元素，但仅支持 %d 个", count)
	}

	for i := 0; i < count; i++ {
		data := []byte(fmt.Sprintf("data-%d", i))
		if !f.Lookup(data) {
			t.Fatalf("查询已插入元素 %d 失败", i)
		}
	}
}

func TestFalsePositiveRate(t *testing.T) {
	f := New()
	numInserted := 100
	for i := 0; i < numInserted; i++ {
		data := []byte(fmt.Sprintf("data-%d", i))
		_ = f.Insert(data)
	}

	fpCount := 0
	numTested := 10000
	for i := 0; i < numTested; i++ {
		data := []byte(fmt.Sprintf("test-%d", i))
		if f.Lookup(data) {
			fpCount++
		}
	}

	fpRate := float64(fpCount) / float64(numTested)
	t.Logf("假阳性次数: %d, 概率: %f", fpCount, fpRate)

	if fpRate > 0.001 {
		t.Fatalf("假阳性率过高: %f (预期 < 0.001)", fpRate)
	}
}

func TestReset(t *testing.T) {
	f := New()
	f.Insert([]byte("a"))
	f.Insert([]byte("b"))
	f.Reset()

	if f.Count() != 0 {
		t.Fatal("重置后计数应为 0")
	}
	if f.Lookup([]byte("a")) || f.Lookup([]byte("b")) {
		t.Fatal("重置后查询应返回 false")
	}
}

func TestEncodeDecode(t *testing.T) {
	f1 := New()
	elements := []string{"apple", "banana", "cherry", "date"}
	for _, el := range elements {
		f1.Insert([]byte(el))
	}

	data := f1.Encode()
	// 预期大小: 16 (位图) + 4 (元素) * 2 (字节/uint16) = 24 字节
	expectedSize := 16 + len(elements)*2
	if len(data) != expectedSize {
		t.Fatalf("编码后的数据长度应为 %d, 实际为 %d", expectedSize, len(data))
	}

	f2 := New()
	if err := f2.Decode(data); err != nil {
		t.Fatalf("解码失败: %v", err)
	}

	if f2.Count() != f1.Count() {
		t.Fatalf("计数不匹配: 实际 %d, 预期 %d", f2.Count(), f1.Count())
	}

	for _, el := range elements {
		if !f2.Lookup([]byte(el)) {
			t.Errorf("解码后的过滤器查询元素 %s 失败", el)
		}
	}

	if f2.Lookup([]byte("grape")) {
		t.Error("未插入元素的查询结果应为 false")
	}

	// 测试空过滤器编码
	f3 := New()
	emptyData := f3.Encode()
	if len(emptyData) != 16 {
		t.Fatalf("空过滤器的编码长度应为 16, 实际为 %d", len(emptyData))
	}
}
