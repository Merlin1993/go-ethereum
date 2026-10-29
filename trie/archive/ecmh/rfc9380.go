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
	bytemaresecp256k1 "github.com/bytemare/secp256k1"
	secp256k1 "github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/ethereum/go-ethereum/common"
)

// amtRFC9380DST separates AMT entry points from every other use of this suite.
// Changing it changes every nonempty bucket commitment.
const amtRFC9380DST = "AMT-ECMH-v1-secp256k1_XMD:SHA-256_SSWU_RO_"

func hashToRFC9380JacobianPoint(h common.Hash) secp256k1.JacobianPoint {
	// HashToGroup implements the RFC 9380 random-oracle suite, including
	// expand_message_xmd(SHA-256), two field elements, SSWU, and the 3-isogeny.
	// The fixed nonempty DST makes its only documented error impossible.
	element, err := bytemaresecp256k1.HashToGroup(h[:], []byte(amtRFC9380DST))
	if err != nil {
		panic(err)
	}
	encoded := element.Encode()
	var point secp256k1.JacobianPoint
	if len(encoded) == 1 && encoded[0] == 0 {
		return point
	}
	pubKey, err := secp256k1.ParsePubKey(encoded)
	if err != nil {
		panic(err) // A valid secp256k1 point must parse in both libraries.
	}
	pubKey.AsJacobian(&point)
	return point
}
