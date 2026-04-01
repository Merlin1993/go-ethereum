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
	"encoding/binary"
	"errors"

	"github.com/ethereum/go-ethereum/crypto"
)

const (
	numBuckets     = 32
	slotsPerBucket = 4
	maxKicks       = 500
)

var (
	ErrFull = errors.New("cuckoo filter is full")
)

// Filter 是一个紧凑的布谷鸟过滤器。
type Filter struct {
	buckets        [][]uint16
	numBuckets     int
	slotsPerBucket int
	count          int
}

// New 创建一个新的布谷鸟过滤器。
func New(buckets, slots int) *Filter {
	if buckets == 0 {
		buckets = 32
	}
	if slots == 0 {
		slots = 4
	}
	f := &Filter{
		numBuckets:     buckets,
		slotsPerBucket: slots,
		buckets:        make([][]uint16, buckets),
	}
	for i := range f.buckets {
		f.buckets[i] = make([]uint16, slots)
	}
	return f
}

// hash 返回数据的原始哈希值和指纹。
func (f *Filter) hash(data []byte) (uint, uint16) {
	h := crypto.Keccak256(data)
	// 使用前 4 个字节计算桶索引
	i1 := uint(binary.BigEndian.Uint32(h[0:4])) & uint(f.numBuckets-1)
	// 使用接下来的 2 个字节作为指纹
	fp := uint16(binary.BigEndian.Uint16(h[4:6]))
	if fp == 0 {
		fp = 1 // 0 保留给空槽位
	}
	return i1, fp
}

// alternateIndex 返回给定的索引和指纹的备选索引。
// 这实现了 Partial-Key Cuckoo Hashing: i2 = i1 ^ hash(fp)
func (f *Filter) alternateIndex(i uint, fp uint16) uint {
	fpBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(fpBytes, fp)
	h := crypto.Keccak256(fpBytes)
	// 异或操作确保我们可以从 i1 计算出 i2，反之亦然。
	// 由于 numBuckets 是 2 的幂，使用位与操作将其限制在范围内。
	return (i ^ uint(binary.BigEndian.Uint32(h[0:4]))) & uint(f.numBuckets-1)
}

// Insert 将数据添加到过滤器中。
func (f *Filter) Insert(data []byte) error {
	i1, fp := f.hash(data)

	// 尝试存入主桶
	for s := 0; s < f.slotsPerBucket; s++ {
		if f.buckets[i1][s] == 0 {
			f.buckets[i1][s] = fp
			f.count++
			return nil
		}
	}

	// 尝试存入备选桶
	i2 := f.alternateIndex(i1, fp)
	for s := 0; s < f.slotsPerBucket; s++ {
		if f.buckets[i2][s] == 0 {
			f.buckets[i2][s] = fp
			f.count++
			return nil
		}
	}

	// 两边都满了，开始踢出（kick-out）逻辑
	// 确定性地从 i1 开始踢出（原逻辑是随机选择 i1 或 i2）
	i := i1

	for n := 0; n < maxKicks; n++ {
		// 确定性地选择槽位（原逻辑是 rand.Intn(f.slotsPerBucket)）
		slot := n % f.slotsPerBucket
		f.buckets[i][slot], fp = fp, f.buckets[i][slot]
		i = f.alternateIndex(i, fp)

		for s := 0; s < f.slotsPerBucket; s++ {
			if f.buckets[i][s] == 0 {
				f.buckets[i][s] = fp
				f.count++
				return nil
			}
		}
	}

	return ErrFull
}

// Lookup 检查数据是否在过滤器中。
func (f *Filter) Lookup(data []byte) bool {
	i1, fp := f.hash(data)
	for s := 0; s < f.slotsPerBucket; s++ {
		if f.buckets[i1][s] == fp {
			return true
		}
	}

	i2 := f.alternateIndex(i1, fp)
	for s := 0; s < f.slotsPerBucket; s++ {
		if f.buckets[i2][s] == fp {
			return true
		}
	}

	return false
}

// Delete 从过滤器中移除数据。
func (f *Filter) Delete(data []byte) bool {
	i1, fp := f.hash(data)
	for s := 0; s < f.slotsPerBucket; s++ {
		if f.buckets[i1][s] == fp {
			f.buckets[i1][s] = 0
			f.count--
			return true
		}
	}

	i2 := f.alternateIndex(i1, fp)
	for s := 0; s < f.slotsPerBucket; s++ {
		if f.buckets[i2][s] == fp {
			f.buckets[i2][s] = 0
			f.count--
			return true
		}
	}

	return false
}

// Count 返回过滤器中元素的数量。
func (f *Filter) Count() int {
	return f.count
}

// Reset 清空过滤器。
func (f *Filter) Reset() {
	for i := 0; i < f.numBuckets; i++ {
		for s := 0; s < f.slotsPerBucket; s++ {
			f.buckets[i][s] = 0
		}
	}
	f.count = 0
}

// Encode 返回过滤器的二进制表示形式。
// 格式如下：
// - 4 字节：numBuckets (BigEndian)
// - 4 字节：slotsPerBucket (BigEndian)
// - (numBuckets * slotsPerBucket / 8) 字节：位图
// - N*2 字节：仅存储已占用槽位的原始 uint16 指纹（大端序）
func (f *Filter) Encode() []byte {
	totalSlots := f.numBuckets * f.slotsPerBucket
	bitmaskSize := (totalSlots + 7) / 8
	bitmask := make([]byte, bitmaskSize)
	var fingerprints []byte

	for i := 0; i < f.numBuckets; i++ {
		for s := 0; s < f.slotsPerBucket; s++ {
			slotIdx := i*f.slotsPerBucket + s
			if f.buckets[i][s] != 0 {
				// 在位图中设置对应位
				bitmask[slotIdx/8] |= (1 << (7 - (slotIdx % 8)))
				// 追加指纹数据
				fpBytes := make([]byte, 2)
				binary.BigEndian.PutUint16(fpBytes, f.buckets[i][s])
				fingerprints = append(fingerprints, fpBytes...)
			}
		}
	}

	res := make([]byte, 8+bitmaskSize+len(fingerprints))
	binary.BigEndian.PutUint32(res[0:4], uint32(f.numBuckets))
	binary.BigEndian.PutUint32(res[4:8], uint32(f.slotsPerBucket))
	copy(res[8:8+bitmaskSize], bitmask)
	copy(res[8+bitmaskSize:], fingerprints)
	return res
}

// Decode 从二进制数据中加载过滤器。
func (f *Filter) Decode(data []byte, buckets, slots int) error {
	if len(data) < 8 {
		return errors.New("data too short for header")
	}

	encodedBuckets := int(binary.BigEndian.Uint32(data[0:4]))
	encodedSlots := int(binary.BigEndian.Uint32(data[4:8]))

	// Use provided buckets/slots if they are set (for compatibility or override)
	// But usually we should trust the encoded header
	if buckets == 0 {
		buckets = encodedBuckets
	}
	if slots == 0 {
		slots = encodedSlots
	}

	f.numBuckets = buckets
	f.slotsPerBucket = slots
	f.buckets = make([][]uint16, buckets)
	for i := range f.buckets {
		f.buckets[i] = make([]uint16, slots)
	}
	f.count = 0

	totalSlots := encodedBuckets * encodedSlots
	bitmaskSize := (totalSlots + 7) / 8
	if len(data) < 8+bitmaskSize {
		return errors.New("data too short for bitmask")
	}

	bitmask := data[8 : 8+bitmaskSize]
	fingerprints := data[8+bitmaskSize:]

	fpIdx := 0
	for i := 0; i < encodedBuckets; i++ {
		for s := 0; s < encodedSlots; s++ {
			slotIdx := i*encodedSlots + s
			if (bitmask[slotIdx/8] & (1 << (7 - (slotIdx % 8)))) != 0 {
				if fpIdx+2 > len(fingerprints) {
					return errors.New("missing fingerprint data")
				}
				fp := binary.BigEndian.Uint16(fingerprints[fpIdx : fpIdx+2])
				fpIdx += 2

				// Only store if within current (potentially different) dimensions
				if i < f.numBuckets && s < f.slotsPerBucket {
					f.buckets[i][s] = fp
					f.count++
				}
			}
		}
	}
	return nil
}
