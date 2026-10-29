package ecmh

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	bytemaresecp256k1 "github.com/bytemare/secp256k1"
)

func TestRFC9380ItemBridge(t *testing.T) {
	item := make([]byte, 64)
	for i := range item {
		item[i] = byte(i)
	}
	digest := sha256.Sum256(item)
	p, err := bytemaresecp256k1.HashToGroup(digest[:], []byte(mptRFC9380DST))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(HashToCurve(item), p.Encode()) {
		t.Fatal("RFC 9380 bridge mismatch")
	}
	if bytes.Equal(HashToCurve(item), HashToCurve(item[:63])) {
		t.Fatal("distinct input collision")
	}
}

func TestRFC9380SuiteOfficialVector(t *testing.T) {
	const dst = "QUUX-V01-CS02-with-secp256k1_XMD:SHA-256_SSWU_RO_"
	p, err := bytemaresecp256k1.HashToGroup(nil, []byte(dst))
	if err != nil {
		t.Fatal(err)
	}
	want, _ := hex.DecodeString("04c1cae290e291aee617ebaef1be6d73861479c48b841eaba9b7b5852ddfeb134664fa678e07ae116126f08b022a94af6de15985c996c3a91b64c406a960e51067")
	if !bytes.Equal(p.EncodeUncompressed(), want) {
		t.Fatalf("official vector mismatch: %x", p.EncodeUncompressed())
	}
}

func BenchmarkHashToCurveRFC9380(b *testing.B) {
	item := make([]byte, 64)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = HashToCurve(item)
	}
}
