// Copyright 2026 The go-ethereum Authors
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

// Package ecmh implements the ECMH (Elliptic Curve Multiset Hash) commitment
// primitive used by the ASCT archive stub buckets (paper P5b).
//
// A bucket commitment is the secp256k1 point sum
//
//	C_ECMH = Σ H_c(k ‖ hash(v))
//
// over all items in the bucket, where each item is the 64-byte concatenation
// of a 32-byte key k and the 32-byte value hash hash(v). Because elliptic
// curve point addition is commutative and associative, the commitment is
// order-independent (multiset homomorphism) and fully deterministic: there
// are no random sources anywhere in this package.
//
// Point at infinity representation: secp256k1's compressed encoding has no
// canonical form for the point at infinity, so this package represents it as
// the all-zero 33-byte Commitment. IsZero reports this case. All exported
// functions treat the all-zero commitment as the identity element.
//
// Performance: BlindAppend and Delete are O(1) — they perform a single
// hash-to-curve of the affected item plus one point addition/subtraction
// against the running commitment. They never touch any other preimage in
// the bucket, which is exactly what makes blind (preimage-free) bucket
// updates possible.
package ecmh

import (
	"math/big"

	secp256k1 "github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/ethereum/go-ethereum/crypto"
)

// Commitment is an ECMH multiset commitment: a 33-byte compressed secp256k1
// point. The point at infinity (empty multiset) is represented by the
// all-zero 33-byte array, since compressed secp256k1 encoding has no
// canonical infinity representation.
type Commitment [33]byte

// curveP is the secp256k1 field prime p = 2^256 - 2^32 - 977.
var curveP = secp256k1.S256().Params().P

// HashToCurve deterministically maps arbitrary data to a point on secp256k1
// using try-and-increment: x starts at keccak256(data) and is incremented
// (mod p) until x³+7 is a quadratic residue, i.e. until 0x02‖x is a valid
// compressed public key. The even-y root is always selected (0x02 prefix),
// fixing the sign ambiguity deterministically. Returns the 33-byte compressed
// encoding of the point.
func HashToCurve(data []byte) (pointBytes33 []byte) {
	x := new(big.Int).SetBytes(crypto.Keccak256(data))
	x.Mod(x, curveP)
	var compressed [33]byte
	compressed[0] = 0x02 // even-y compressed key: deterministic sign choice.
	for {
		xBytes := x.Bytes()
		copy(compressed[1+32-len(xBytes):], xBytes)
		// Clear the tail in case a previous iteration left longer bytes.
		if len(xBytes) < 32 {
			for i := 1; i < 1+32-len(xBytes); i++ {
				compressed[i] = 0
			}
		}
		if _, err := secp256k1.ParsePubKey(compressed[:]); err == nil {
			out := make([]byte, 33)
			copy(out, compressed[:])
			return out
		}
		x.Add(x, big.NewInt(1))
		x.Mod(x, curveP)
	}
}

// itemPoint hashes one item to a Jacobian curve point. The item is expected
// to be the 64-byte k ‖ hash(v) concatenation, but any byte string is
// accepted (the caller is responsible for the preimage format).
func itemPoint(item []byte) secp256k1.JacobianPoint {
	pub, err := secp256k1.ParsePubKey(HashToCurve(item))
	if err != nil {
		panic("ecmh: HashToCurve returned an unparseable point")
	}
	var p secp256k1.JacobianPoint
	pub.AsJacobian(&p)
	return p
}

// decodePoint decodes a Commitment into a Jacobian point. The all-zero
// commitment decodes to the point at infinity (Jacobian zero value).
func decodePoint(c Commitment) secp256k1.JacobianPoint {
	var p secp256k1.JacobianPoint
	if c.IsZero() {
		return p
	}
	pub, err := secp256k1.ParsePubKey(c[:])
	if err != nil {
		panic("ecmh: invalid commitment encoding")
	}
	pub.AsJacobian(&p)
	return p
}

// encodePoint serializes a Jacobian point as a Commitment. The point at
// infinity encodes as the all-zero Commitment.
func encodePoint(p *secp256k1.JacobianPoint) Commitment {
	var c Commitment
	if p.Z.IsZero() || (p.X.IsZero() && p.Y.IsZero()) {
		return c // infinity -> all-zero encoding.
	}
	p.ToAffine()
	pub := secp256k1.NewPublicKey(&p.X, &p.Y)
	copy(c[:], pub.SerializeCompressed())
	return c
}

// Create computes the ECMH commitment of a multiset of items:
//
//	Create(items) = Σ H_c(item)
//
// where each item is the 64-byte k ‖ hash(v) concatenation. The result is
// independent of the order of items (multiset commutativity). An empty
// multiset yields the point at infinity (all-zero Commitment).
func Create(items [][]byte) Commitment {
	var sum secp256k1.JacobianPoint
	for _, item := range items {
		p := itemPoint(item)
		secp256k1.AddNonConst(&sum, &p, &sum)
	}
	return encodePoint(&sum)
}

// BlindAppend returns the commitment with one item added: c + H_c(item).
// It is O(1): a single hash-to-curve plus one point addition, without
// touching any other preimage in the bucket. Adding to the zero commitment
// (empty bucket) is supported.
func BlindAppend(c Commitment, item []byte) Commitment {
	sum := decodePoint(c)
	p := itemPoint(item)
	secp256k1.AddNonConst(&sum, &p, &sum)
	return encodePoint(&sum)
}

// Delete returns the commitment with one occurrence of item removed:
// c - H_c(item), i.e. c + (-H_c(item)) where the negation flips y to p-y.
// It is O(1): a single hash-to-curve plus one point addition against the
// negated item point, without touching any other preimage in the bucket.
// Removing the last item yields the all-zero (infinity) commitment.
func Delete(c Commitment, item []byte) Commitment {
	sum := decodePoint(c)
	p := itemPoint(item)
	// Negate the item point: (x, y) -> (x, -y mod p).
	p.Y.Negate(1).Normalize()
	secp256k1.AddNonConst(&sum, &p, &sum)
	return encodePoint(&sum)
}

// Verify reports whether the commitment equals the ECMH sum of the given
// items: Σ H_c(item) == c. Verification is order-independent.
func Verify(c Commitment, items [][]byte) bool {
	return Create(items) == c
}

// IsZero reports whether the commitment is the point at infinity (empty
// multiset), represented as the all-zero 33-byte array.
func (c Commitment) IsZero() bool {
	return c == Commitment{}
}
