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
//	C_ECMH = Σ H_c(SHA-256(k ‖ SHA-256(v)))
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
	"crypto/sha256"
	"sync"

	bytemaresecp256k1 "github.com/bytemare/secp256k1"

	secp256k1 "github.com/decred/dcrd/dcrec/secp256k1/v4"
)

// Commitment is an ECMH multiset commitment: a 33-byte compressed secp256k1
// point. The point at infinity (empty multiset) is represented by the
// all-zero 33-byte array, since compressed secp256k1 encoding has no
// canonical infinity representation.
type Commitment [33]byte

// Entry is an archived key and its raw value. The value hash is calculated
// inside the verification worker, alongside the curve mapping.
type Entry struct {
	Key, Value []byte
}

// mptRFC9380DST separates this MPT archive format from other hash-to-curve uses.
// Changing it invalidates every nonempty MPT archive bucket commitment.
const mptRFC9380DST = "AMT-MPT-ECMH-v1-secp256k1_XMD:SHA-256_SSWU_RO_"

// HashToCurve hashes the fixed-format item to 32 bytes and applies the RFC 9380
// secp256k1 random-oracle suite. Ordinary points retain 33-byte compression.
func HashToCurve(data []byte) (pointBytes33 []byte) {
	digest := sha256.Sum256(data)
	point, err := bytemaresecp256k1.HashToGroup(digest[:], []byte(mptRFC9380DST))
	if err != nil {
		panic(err) // The fixed, nonempty DST makes its only documented error impossible.
	}
	return point.Encode()
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

// CreateEntriesParallel covers value hashing as well as hash-to-curve work.
func CreateEntriesParallel(entries []Entry, workers int) Commitment {
	if len(entries) == 0 {
		return Commitment{}
	}
	workers = max(1, min(workers, len(entries)))
	partials := make([]secp256k1.JacobianPoint, workers)
	addRange := func(worker int) {
		for _, entry := range entries[worker*len(entries)/workers : (worker+1)*len(entries)/workers] {
			valueHash := sha256.Sum256(entry.Value)
			item := make([]byte, 0, len(entry.Key)+len(valueHash))
			item = append(item, entry.Key...)
			item = append(item, valueHash[:]...)
			point := itemPoint(item)
			secp256k1.AddNonConst(&partials[worker], &point, &partials[worker])
		}
	}
	var wg sync.WaitGroup
	for worker := 1; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() { defer wg.Done(); addRange(worker) }()
	}
	addRange(0)
	wg.Wait()
	var sum secp256k1.JacobianPoint
	for i := range partials {
		secp256k1.AddNonConst(&sum, &partials[i], &sum)
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
