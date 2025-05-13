// Copyright 2023 The go-ethereum Authors
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

package cacheTrie

// Trie键的编码方式:
// - 在完整节点的边缘上，使用原始的十六进制字符（例如键'abc'成为['a', 'b', 'c']）
// - 对于短节点，使用压缩格式，其中终端标志位(用于知道路径是否结束)和奇数标志位一起存储

// hexToCompact 将十六进制格式的键转换为压缩格式
func hexToCompact(hex []byte) []byte {
	terminator := byte(0)
	if hasTerm(hex) {
		terminator = 1
		hex = hex[:len(hex)-1]
	}
	buf := make([]byte, len(hex)/2+1)
	buf[0] = terminator << 5 // 终端标志位放在第一个字节的高3位
	if len(hex)&1 == 1 {
		buf[0] |= 1 << 4 // 奇数标志位
		buf[0] |= hex[0] // 第一个字符
		hex = hex[1:]
	}
	decodeNibbles(hex, buf[1:])
	return buf
}

// compactToHex 将压缩格式的键转换为十六进制格式
func compactToHex(compact []byte) []byte {
	if len(compact) == 0 {
		return nil
	}
	base := keybytesToHex(compact)
	// 删除第一个字节的标志位
	if base[0] < 2 {
		// 如果没有奇数标志位，删除第一个字符
		base = base[1:]
	}
	// 加上可能的终端标志位
	if base[len(base)-1] == 16 {
		base = base[:len(base)-1]
	}
	if len(base)&1 == 1 {
		// 确保长度为偶数
		base = append(base, 0)
	}
	return base
}

// 判断十六进制键是否有终端标志
func hasTerm(s []byte) bool {
	return len(s) > 0 && s[len(s)-1] == 16
}

// keybytesToHex 将二进制键转换为十六进制格式
func keybytesToHex(str []byte) []byte {
	l := len(str)*2 + 1
	var nibbles = make([]byte, l)
	for i, b := range str {
		nibbles[i*2] = b / 16
		nibbles[i*2+1] = b % 16
	}
	nibbles[l-1] = 16 // 添加终端标志
	return nibbles
}

// 将十六进制格式解码回二进制数据
func decodeNibbles(nibbles []byte, bytes []byte) {
	for bi, ni := 0, 0; ni < len(nibbles); bi, ni = bi+1, ni+2 {
		bytes[bi] = nibbles[ni]<<4 | nibbles[ni+1]
	}
}

// prefixLen 返回a和b公共前缀的长度
func prefixLen(a, b []byte) int {
	var i, length = 0, len(a)
	if len(b) < length {
		length = len(b)
	}
	for ; i < length; i++ {
		if a[i] != b[i] {
			break
		}
	}
	return i
}
