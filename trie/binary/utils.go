package binary

func (s *Shard) setBitInBytes(data []byte, bitIdx int, val byte) {
	byteIdx := bitIdx / 8
	bitOffset := 7 - (bitIdx % 8)
	if byteIdx >= len(data) {
		return
	}
	if val == 1 {
		data[byteIdx] |= (1 << bitOffset)
	} else {
		data[byteIdx] &= ^(1 << bitOffset)
	}
}

// 工具方法

func getBit(key []byte, depth int) byte {
	byteIdx := depth / 8
	bitIdx := 7 - (depth % 8)
	if byteIdx >= len(key) {
		return 0
	}
	if (key[byteIdx] & (1 << bitIdx)) != 0 {
		return 1
	}
	return 0
}

func (s *Shard) getBit(key []byte, depth int) byte {
	return getBit(key, depth)
}

func (s *Shard) getBitFromBytes(data []byte, bitIndex int) byte {
	byteIdx := bitIndex / 8
	bitIdx := 7 - (bitIndex % 8)
	if byteIdx >= len(data) {
		return 0
	}
	if (data[byteIdx] & (1 << bitIdx)) != 0 {
		return 1
	}
	return 0
}

func (s *Shard) commonPrefixLen(a []byte, aBits int, b []byte, bStartBit int) int {
	// a 为路径后缀（按位），b 为完整 key（从 bStartBit 位开始对齐比较）
	// [OPTIMIZED] 优先使用字节比较提速
	matched := 0
	bTotalBits := len(b) * 8
	if bStartBit >= bTotalBits {
		return 0
	}

	// 1. 尝试按字节对齐比较 (如果 bStartBit 是 8 的倍数且 aBits >= 8)
	if bStartBit%8 == 0 && aBits >= 8 {
		bIdx := bStartBit / 8
		for matched+8 <= aBits && bIdx < len(b) {
			if a[matched/8] != b[bIdx] {
				break
			}
			matched += 8
			bIdx++
		}
	}

	// 2. 剩余位逐位比较
	for matched < aBits && (bStartBit+matched) < bTotalBits {
		bitA := s.getBitFromBytes(a, matched)
		bitB := s.getBit(b, bStartBit+matched)
		if bitA != bitB {
			break
		}
		matched++
	}
	return matched
}

func (s *Shard) getSuffix(key []byte, depth int, buf []byte) []byte {
	// 提取从 depth 位开始的 key 剩余位，并按高位在前压缩为字节数组返回
	totalBits := len(key)*8 - depth
	if totalBits <= 0 {
		return nil
	}

	// 位移压缩（逐位处理，原型实现即可）
	size := (totalBits + 7) / 8
	var res []byte
	if cap(buf) >= size {
		res = buf[:size]
		for i := range res {
			res[i] = 0
		}
	} else {
		res = make([]byte, size)
	}

	for i := 0; i < totalBits; i++ {
		if s.getBit(key, depth+i) == 1 {
			byteIdx := i / 8
			bitIdx := 7 - (i % 8)
			res[byteIdx] |= (1 << bitIdx)
		}
	}
	return res
}

func (s *Shard) shiftBits(data []byte, bits int, shift int, buf []byte) []byte {
	// 跳过前 shift 位，返回剩余位的字节压缩表示
	newBits := bits - shift
	if newBits <= 0 {
		return nil
	}
	size := (newBits + 7) / 8
	var res []byte
	if cap(buf) >= size {
		res = buf[:size]
		for i := range res {
			res[i] = 0
		}
	} else {
		res = make([]byte, size)
	}

	for i := 0; i < newBits; i++ {
		// 原位下标 = shift + i
		if s.getBitFromBytes(data, shift+i) == 1 {
			byteIdx := i / 8
			bitIdx := 7 - (i % 8)
			res[byteIdx] |= (1 << bitIdx)
		}
	}
	return res
}

func (s *Shard) prefixBits(data []byte, bits int, buf []byte) []byte {
	if bits <= 0 {
		return nil
	}
	size := (bits + 7) / 8
	var res []byte
	if cap(buf) >= size {
		res = buf[:size]
		for i := range res {
			res[i] = 0
		}
	} else {
		res = make([]byte, size)
	}

	for i := 0; i < bits; i++ {
		if s.getBitFromBytes(data, i) == 1 {
			byteIdx := i / 8
			bitIdx := 7 - (i % 8)
			res[byteIdx] |= (1 << bitIdx)
		}
	}
	return res
}

// prependBit 在位序列前部增加一位。用于在树收缩或路径调整时重新计算路径。
func (s *Shard) prependBit(data []byte, bits int, bit byte, buf []byte) []byte {
	newBits := bits + 1
	size := (newBits + 7) / 8
	var res []byte
	if cap(buf) >= size {
		res = buf[:size]
		for i := range res {
			res[i] = 0
		}
	} else {
		res = make([]byte, size)
	}

	if bit == 1 {
		res[0] |= 0x80
	}
	for i := 0; i < bits; i++ {
		if s.getBitFromBytes(data, i) == 1 {
			byteIdx := (i + 1) / 8
			bitIdx := 7 - ((i + 1) % 8)
			res[byteIdx] |= (1 << bitIdx)
		}
	}
	return res
}

func (s *Shard) appendBit(data []byte, bits int, bit byte) ([]byte, int) {
	newBits := bits + 1
	size := (newBits + 7) / 8
	res := make([]byte, size)
	copy(res, data)
	if bit == 1 {
		byteIdx := bits / 8
		if byteIdx < len(res) {
			bitOffset := 7 - (bits % 8)
			res[byteIdx] |= (1 << bitOffset)
		}
	}
	return res, newBits
}

func (s *Shard) copyBits(dst []byte, dstStart int, src []byte, srcBits int) {
	for i := 0; i < srcBits; i++ {
		if s.getBitFromBytes(src, i) == 1 {
			bitPos := dstStart + i
			byteIdx := bitPos / 8
			if byteIdx < len(dst) {
				bitIdx := 7 - (bitPos % 8)
				dst[byteIdx] |= (1 << bitIdx)
			}
		}
	}
}

func (s *Shard) prependPath(base []byte, baseBits int, prefix []byte, prefixBits int) []byte {
	res := make([]byte, (baseBits+prefixBits+7)/8)
	s.copyBits(res, 0, prefix, prefixBits)
	s.copyBits(res, prefixBits, base, baseBits)
	return res
}

func (s *Shard) concatPath(path1 []byte, bits1 int, bit byte, path2 []byte, bits2 int) []byte {
	resBits := bits1 + 1 + bits2
	res := make([]byte, (resBits+7)/8)
	s.copyBits(res, 0, path1, bits1)
	if bit == 1 {
		byteIdx := bits1 / 8
		bitOffset := 7 - (bits1 % 8)
		res[byteIdx] |= (1 << bitOffset)
	}
	s.copyBits(res, bits1+1, path2, bits2)
	return res
}

func (s *Shard) suffixMatches(key []byte, depth int, path []byte, pathBits int) bool {
	if pathBits == 0 {
		return true
	}
	if depth+pathBits > len(key)*8 {
		return false
	}
	for i := 0; i < pathBits; i++ {
		if s.getBit(key, depth+i) != s.getBitFromBytes(path, i) {
			return false
		}
	}
	return true
}
