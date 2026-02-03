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

package binary

import (
	"crypto/ecdsa"
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

var (
	errInvalidCommitment = errors.New("invalid commitment encoding")
)

// Committer 提供 ECMH (Elliptic Curve Multiset Hash) 承诺功能。
// 它可以将一组哈希值映射到椭圆曲线上的点并进行累加，结果与添加顺序无关。
type Committer struct {
	curve crypto.EllipticCurve
}

// New 创建一个新的 ECMH 承诺器。
func New() *Committer {
	return &Committer{
		curve: crypto.S256(),
	}
}

// Add 为给定的承诺增加多个哈希。如果旧承诺为空，则从零点开始。
// 返回更新后的承诺。
func (c *Committer) Add(commitment []byte, hashes []common.Hash) ([]byte, error) {
	currX, currY, err := c.decode(commitment)
	if err != nil {
		return nil, err
	}

	for _, h := range hashes {
		pkX, pkY := c.hashToPoint(h)
		currX, currY = c.curve.Add(currX, currY, pkX, pkY)
	}

	return c.encode(currX, currY), nil
}

// Delete 从给定的承诺中减去多个哈希处理。
// 返回更新后的承诺。
func (c *Committer) Delete(commitment []byte, hashes []common.Hash) ([]byte, error) {
	currX, currY, err := c.decode(commitment)
	if err != nil {
		return nil, err
	}

	p := c.curve.Params().P
	for _, h := range hashes {
		pkX, pkY := c.hashToPoint(h)
		// 减法等同于加上点的反元素 (x, -y % p)
		negY := new(big.Int).Mod(new(big.Int).Neg(pkY), p)
		currX, currY = c.curve.Add(currX, currY, pkX, negY)
	}

	return c.encode(currX, currY), nil
}

// Verify 验证给定的哈希集合是否对应于给定的承诺。
func (c *Committer) Verify(hashes []common.Hash, commitment []byte) bool {
	res, err := c.Add(nil, hashes)
	if err != nil {
		return false
	}
	if len(res) != len(commitment) {
		return false
	}
	for i := range res {
		if res[i] != commitment[i] {
			return false
		}
	}
	return true
}

// hashToPoint 使用 "Try-and-increment" 方法将哈希映射到 secp256k1 曲线上的点。
func (c *Committer) hashToPoint(h common.Hash) (x, y *big.Int) {
	p := c.curve.Params().P
	data := h[:]
	for {
		x = new(big.Int).SetBytes(data)
		if x.Cmp(p) < 0 {
			// 计算 y^2 = (x^3 + 7) % p
			x3 := new(big.Int).Mul(x, x)
			x3.Mul(x3, x)
			x3.Add(x3, big.NewInt(7))
			x3.Mod(x3, p)

			y = new(big.Int).ModSqrt(x3, p)
			if y != nil {
				// 确定性地选择 y。选择偶数 y 以保证唯一性。
				if y.Bit(0) != 0 {
					y.Sub(p, y)
				}
				return x, y
			}
		}
		// 如果不满足条件，则对哈希再次取哈希，尝试下一个 x
		data = crypto.Keccak256(data)
	}
}

// encode 将曲线上的点编码为压缩格式（33 字节）。
// 如果点是无穷远点（0, 0），则返回空字节。
func (c *Committer) encode(x, y *big.Int) []byte {
	if x.Sign() == 0 && y.Sign() == 0 {
		return nil
	}
	pk := &ecdsa.PublicKey{
		Curve: c.curve,
		X:     x,
		Y:     y,
	}
	return crypto.CompressPubkey(pk)
}

// decode 将压缩格式的承诺解码为曲线上的点。
// 如果承诺为空，则返回无穷远点（0, 0）。
func (c *Committer) decode(commitment []byte) (x, y *big.Int, err error) {
	if len(commitment) == 0 {
		return new(big.Int), new(big.Int), nil
	}
	pk, err := crypto.DecompressPubkey(commitment)
	if err != nil {
		return nil, nil, errInvalidCommitment
	}
	return pk.X, pk.Y, nil
}
