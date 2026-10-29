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

package ecmh

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"runtime"
	"strings"
	"testing"

	bytemaresecp256k1 "github.com/bytemare/secp256k1"
	"github.com/ethereum/go-ethereum/common"
)

// RFC 9380 Appendix J.8.1: these messages, DST, and affine coordinates test
// the raw suite. AMT's 32-byte entry digest and its own DST are tested below.
func TestRFC9380OfficialVectors(t *testing.T) {
	const dst = "QUUX-V01-CS02-with-secp256k1_XMD:SHA-256_SSWU_RO_"
	vectors := []struct{ msg, x, y string }{
		{"", "c1cae290e291aee617ebaef1be6d73861479c48b841eaba9b7b5852ddfeb1346", "64fa678e07ae116126f08b022a94af6de15985c996c3a91b64c406a960e51067"},
		{"abc", "3377e01eab42db296b512293120c6cee72b6ecf9f9205760bd9ff11fb3cb2c4b", "7f95890f33efebd1044d382a01b1bee0900fb6116f94688d487c6c7b9c8371f6"},
		{"abcdef0123456789", "bac54083f293f1fe08e4a70137260aa90783a5cb84d3f35848b324d0674b0e3a", "4436476085d4c3c4508b60fcf4389c40176adce756b398bdee27bca19758d828"},
		{"q128_" + strings.Repeat("q", 128), "e2167bc785333a37aa562f021f1e881defb853839babf52a7f72b102e41890e9", "f2401dd95cc35867ffed4f367cd564763719fbc6a53e969fb8496a1e6685d873"},
		{"a512_" + strings.Repeat("a", 512), "e3c8d35aaaf0b9b647e88a0a0a7ee5d5bed5ad38238152e4e6fd8c1f8cb7c998", "8446eeb6181bf12f56a9d24e262221cc2f0c4725c7e3803024b5888ee5823aa6"},
	}
	for i, v := range vectors {
		point, err := bytemaresecp256k1.HashToGroup([]byte(v.msg), []byte(dst))
		if err != nil {
			t.Fatalf("vector %d: %v", i, err)
		}
		want, err := hex.DecodeString(v.x + v.y)
		if err != nil {
			t.Fatal(err)
		}
		got := point.EncodeUncompressed()
		if len(got) != 65 || got[0] != 4 || !bytes.Equal(got[1:], want) {
			t.Fatalf("vector %d mismatch: got %x, want 04%x", i, got, want)
		}
	}
}

func rfcTestHashes(n int) []common.Hash {
	hashes := make([]common.Hash, n)
	var input [8]byte
	for i := range hashes {
		binary.BigEndian.PutUint64(input[:], uint64(i))
		hashes[i] = sha256.Sum256(input[:])
	}
	return hashes
}

func TestRFC9380ECMH(t *testing.T) {
	c := New()
	hashes := rfcTestHashes(100)
	first := c.hashToJacobianPoint(hashes[0])
	second := c.hashToJacobianPoint(hashes[0])
	if !bytes.Equal(c.encodeJacobian(&first), c.encodeJacobian(&second)) {
		t.Fatal("mapping is not deterministic")
	}
	direct, err := bytemaresecp256k1.HashToGroup(hashes[0][:], []byte(amtRFC9380DST))
	if err != nil || !bytes.Equal(c.encodeJacobian(&second), direct.Encode()) {
		t.Fatal("Jacobian bridge differs from RFC point", err)
	}
	all, err := c.Add(nil, hashes)
	if err != nil || !c.Verify(hashes, all) || len(all) != 33 {
		t.Fatalf("invalid 100-item commitment: %x, %v", all, err)
	}
	reversed := append([]common.Hash(nil), hashes...)
	for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
		reversed[i], reversed[j] = reversed[j], reversed[i]
	}
	reordered, err := c.Add(nil, reversed)
	if err != nil || !bytes.Equal(all, reordered) {
		t.Fatal("insertion order changed commitment")
	}
	left, err := c.Add(nil, hashes[:40])
	if err != nil {
		t.Fatal(err)
	}
	right, err := c.Add(nil, hashes[40:])
	if err != nil {
		t.Fatal(err)
	}
	merged, err := c.Merge(left, right)
	if err != nil || !bytes.Equal(merged, all) {
		t.Fatal("Merge differs from recomputation", err)
	}
	leftPoint, err := c.DecodePoint(left)
	if err != nil {
		t.Fatal(err)
	}
	rightPoint, err := c.DecodePoint(right)
	if err != nil {
		t.Fatal(err)
	}
	mergedPoints, _, err := c.MergePoints(leftPoint, rightPoint)
	if err != nil || !bytes.Equal(mergedPoints, all) {
		t.Fatal("MergePoints differs from recomputation", err)
	}
	added, err := c.Add(left, hashes[40:])
	if err != nil || !bytes.Equal(added, all) {
		t.Fatal("incremental Add differs from recomputation", err)
	}
	deleted, err := c.Delete(all, hashes[40:])
	if err != nil || !bytes.Equal(deleted, left) {
		t.Fatal("Delete is not Add inverse", err)
	}
	empty, err := c.Delete(all, hashes)
	if err != nil || len(empty) != 0 || !c.Verify(nil, empty) {
		t.Fatal("identity handling failed", err)
	}
	duplicate, err := c.Add(nil, []common.Hash{hashes[0], hashes[0]})
	if err != nil {
		t.Fatal(err)
	}
	single, err := c.Delete(duplicate, hashes[:1])
	if err != nil || !c.Verify(hashes[:1], single) {
		t.Fatal("duplicate multiplicity failed", err)
	}
	if c.Verify(hashes[:99], all) || c.Verify(hashes, append([]byte(nil), all[:32]...)) {
		t.Fatal("Verify accepted tampering")
	}
	if _, err := c.Add([]byte{2, 1}, hashes[:1]); err == nil {
		t.Fatal("invalid compressed point accepted")
	}
	if _, err := c.DecodePoint([]byte{0}); err == nil {
		t.Fatal("noncanonical identity accepted")
	}
	oldProcs := runtime.GOMAXPROCS(1)
	serial, err := c.Add(nil, hashes)
	runtime.GOMAXPROCS(oldProcs)
	if err != nil || !bytes.Equal(serial, all) {
		t.Fatal("serial and parallel results differ", err)
	}
}

var benchmarkCommitment []byte

func BenchmarkECMHMappingRFC9380(b *testing.B) {
	c := New()
	h := rfcTestHashes(1)[0]
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		point := c.hashToJacobianPoint(h)
		if point.Z.IsZero() {
			b.Fatal("unexpected identity")
		}
	}
}

func BenchmarkECMHOperationsRFC9380(b *testing.B) {
	hashes := rfcTestHashes(100)
	c := New()
	base, err := c.Add(nil, hashes)
	if err != nil {
		b.Fatal(err)
	}
	for _, op := range []string{"bucket100", "add1", "delete1", "verify100"} {
		b.Run(op, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				switch op {
				case "bucket100":
					benchmarkCommitment, err = c.Add(nil, hashes)
				case "add1":
					benchmarkCommitment, err = c.Add(base, hashes[:1])
				case "delete1":
					benchmarkCommitment, err = c.Delete(base, hashes[:1])
				case "verify100":
					if !c.Verify(hashes, base) {
						b.Fatal("Verify failed")
					}
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
