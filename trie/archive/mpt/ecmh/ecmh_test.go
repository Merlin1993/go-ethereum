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

package ecmh

import (
	"math/rand"
	"testing"

	secp256k1 "github.com/decred/dcrd/dcrec/secp256k1/v4"
)

// makeItem builds a deterministic 64-byte k ‖ hash(v) item from a seed.
func makeItem(r *rand.Rand) []byte {
	item := make([]byte, 64)
	r.Read(item)
	return item
}

func makeItems(r *rand.Rand, n int) [][]byte {
	items := make([][]byte, n)
	for i := range items {
		items[i] = makeItem(r)
	}
	return items
}

// TestCreateEmpty verifies that the empty multiset maps to the point at
// infinity (all-zero commitment).
func TestCreateEmpty(t *testing.T) {
	c := Create(nil)
	if !c.IsZero() {
		t.Fatalf("Create(nil) = %x, want zero (infinity)", c)
	}
	if got := Create([][]byte{}); !got.IsZero() {
		t.Fatalf("Create(empty) = %x, want zero (infinity)", got)
	}
}

// TestHashToCurve verifies determinism, 33-byte output, and that the result
// is a valid compressed secp256k1 public key.
func TestHashToCurve(t *testing.T) {
	inputs := [][]byte{
		[]byte(""),
		[]byte("a"),
		[]byte("hello ecmh"),
		make([]byte, 64),
	}
	for i, in := range inputs {
		p1 := HashToCurve(in)
		if len(p1) != 33 {
			t.Fatalf("case %d: HashToCurve length = %d, want 33", i, len(p1))
		}
		if _, err := secp256k1.ParsePubKey(p1); err != nil {
			t.Fatalf("case %d: ParsePubKey failed: %v", i, err)
		}
		p2 := HashToCurve(in)
		if string(p1) != string(p2) {
			t.Fatalf("case %d: HashToCurve not deterministic", i)
		}
	}
	// Distinct inputs must (overwhelmingly likely) map to distinct points.
	if string(HashToCurve([]byte("x"))) == string(HashToCurve([]byte("y"))) {
		t.Fatal("HashToCurve collided for distinct inputs")
	}
}

// TestRoundTrip verifies A+B-B = A: appending an item then deleting it
// restores the original commitment.
func TestRoundTrip(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	items := makeItems(r, 5)
	a := Create(items)

	extra := makeItem(r)
	b := BlindAppend(a, extra)
	if b == a {
		t.Fatal("BlindAppend did not change the commitment")
	}
	if got := Delete(b, extra); got != a {
		t.Fatalf("Delete(BlindAppend(A, x), x) != A\n got: %x\nwant: %x", got, a)
	}
	// Round-trip from the empty commitment must return to infinity.
	c := BlindAppend(Commitment{}, extra)
	if c.IsZero() {
		t.Fatal("BlindAppend(zero, x) is still zero")
	}
	if got := Delete(c, extra); !got.IsZero() {
		t.Fatalf("Delete(Append(zero, x), x) = %x, want zero (infinity)", got)
	}
}

// TestAppendEqualsCreate verifies blind updates match a fresh Create over
// the full multiset.
func TestAppendEqualsCreate(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	items := makeItems(r, 4)

	c := Commitment{}
	for _, it := range items {
		c = BlindAppend(c, it)
	}
	if want := Create(items); c != want {
		t.Fatalf("incremental BlindAppend != Create\n got: %x\nwant: %x", c, want)
	}
	// Delete one element and compare against Create of the remainder.
	c2 := Delete(c, items[1])
	if want := Create([][]byte{items[0], items[2], items[3]}); c2 != want {
		t.Fatalf("Delete mismatch\n got: %x\nwant: %x", c2, want)
	}
}

// TestCommutative verifies multiset commutativity: any permutation of the
// same items yields the same commitment, as do interleaved append/delete
// orderings.
func TestCommutative(t *testing.T) {
	r := rand.New(rand.NewSource(99))
	items := makeItems(r, 10)
	base := Create(items)

	perm := rand.New(rand.NewSource(1)).Perm(len(items))
	shuffled := make([][]byte, len(items))
	for i, j := range perm {
		shuffled[i] = items[j]
	}
	if got := Create(shuffled); got != base {
		t.Fatalf("Create not commutative\n got: %x\nwant: %x", got, base)
	}

	// Duplicates count with multiplicity: {a, a} != {a}.
	dup := Create([][]byte{items[0], items[0]})
	if dup == Create([][]byte{items[0]}) {
		t.Fatal("multiset multiplicity ignored: {a,a} == {a}")
	}
	// Multiset equality: {a, a, b} == {b, a, a}.
	x := Create([][]byte{items[0], items[0], items[1]})
	y := Create([][]byte{items[1], items[0], items[0]})
	if x != y {
		t.Fatal("multiset with duplicates not order-independent")
	}
}

// TestVerify verifies positive and negative verification.
func TestVerify(t *testing.T) {
	r := rand.New(rand.NewSource(2024))
	items := makeItems(r, 8)
	c := Create(items)

	if !Verify(c, items) {
		t.Fatal("Verify failed for genuine commitment")
	}
	// Empty set verifies against the zero commitment.
	if !Verify(Commitment{}, nil) {
		t.Fatal("Verify(zero, nil) failed")
	}
	if Verify(c, nil) {
		t.Fatal("Verify(c, nil) succeeded for non-zero c")
	}
	// Dropping an item must fail.
	if Verify(c, items[:len(items)-1]) {
		t.Fatal("Verify succeeded with an item dropped")
	}
}

// TestVerifyTamper flips each byte of one item in turn and checks that
// verification fails every time.
func TestVerifyTamper(t *testing.T) {
	r := rand.New(rand.NewSource(31337))
	items := makeItems(r, 3)
	c := Create(items)

	for pos := 0; pos < 64; pos++ {
		tampered := make([][]byte, len(items))
		for i, it := range items {
			cp := make([]byte, len(it))
			copy(cp, it)
			tampered[i] = cp
		}
		tampered[1][pos] ^= 0x01
		if Verify(c, tampered) {
			t.Fatalf("Verify succeeded with byte %d tampered", pos)
		}
	}
	// Tampering the commitment itself must also fail.
	for pos := 0; pos < 33; pos++ {
		bad := c
		bad[pos] ^= 0x01
		if Verify(bad, items) {
			t.Fatalf("Verify succeeded with commitment byte %d tampered", pos)
		}
	}
}

// TestVerify100 runs the fixed-seed 100-item verification.
func TestVerify100(t *testing.T) {
	r := rand.New(rand.NewSource(100))
	items := makeItems(r, 100)
	c := Create(items)
	if c.IsZero() {
		t.Fatal("commitment of 100 items is zero (astronomically unlikely)")
	}
	if !Verify(c, items) {
		t.Fatal("Verify failed for 100 fixed-seed items")
	}
	// Incremental construction must agree with the one-shot Create.
	inc := Commitment{}
	for _, it := range items {
		inc = BlindAppend(inc, it)
	}
	if inc != c {
		t.Fatal("incremental construction over 100 items != Create")
	}
	// Remove all items one by one; the bucket must drain back to infinity.
	for _, it := range items {
		inc = Delete(inc, it)
	}
	if !inc.IsZero() {
		t.Fatal("deleting all 100 items did not return to infinity")
	}
}
