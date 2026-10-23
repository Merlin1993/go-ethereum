package mpt

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

// TestForEachPrefixSemantics 验证前缀限定遍历的语义：命中集与全树遍历
// 过滤后一致、跨域键不泄漏、单账户存储域扫描只返回该域。
func TestForEachPrefixSemantics(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, false)

	addrA := bytes.Repeat([]byte{0x0a}, 20) // 存储域 A
	addrB := bytes.Repeat([]byte{0x0b}, 20) // 存储域 B
	keyA := append(append([]byte{}, addrA...), bytes.Repeat([]byte{0x01}, 32)...)
	keyA2 := append(append([]byte{}, addrA...), bytes.Repeat([]byte{0x02}, 32)...)
	keyB := append(append([]byte{}, addrB...), bytes.Repeat([]byte{0x03}, 32)...)
	for _, kv := range [][2][]byte{
		{addrA, []byte("accountA")}, // 账户键也以 addrA 开头——同一前缀空间
		{keyA, []byte("slotA1")},
		{keyA2, []byte("slotA2")},
		{keyB, []byte("slotB1")},
	} {
		if err := tr.Put(kv[0], kv[1]); err != nil {
			t.Fatal(err)
		}
		if _, err := tr.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	// 存储域前缀（首字节 xor 0x01）与老 wrapper 口径一致。
	wipePrefix := func(addr []byte) []byte {
		p := append([]byte{}, addr...)
		p[0] ^= 0x01
		return p
	}
	pA := addrA // 账户键+存储键都以 addrA 开头，域非空
	gotA := map[string][]byte{}
	tr.ForEachPrefix(pA, len(pA)*8, func(key, value []byte) bool {
		gotA[string(key)] = value
		return true
	})
	gotAll := map[string][]byte{}
	tr.ForEach(func(key, value []byte) bool {
		gotAll[string(key)] = value
		return true
	})
	// 全树遍历过滤 pA 前缀的期望集。
	expect := map[string][]byte{}
	for k, v := range gotAll {
		if bytes.HasPrefix([]byte(k), pA) {
			expect[k] = v
		}
	}
	if len(gotA) != len(expect) {
		t.Fatalf("prefix scan got %d keys, expect %d: got=%v expect=%v", len(gotA), len(expect), keysOf(gotA), keysOf(expect))
	}
	for k, v := range expect {
		if !bytes.Equal(gotA[k], v) {
			t.Fatalf("prefix scan value mismatch for %x: got %s expect %s", k, gotA[k], v)
		}
	}
	// A 域扫到的键必须全部以 pA 开头，且不含 B 域键。
	for k := range gotA {
		if !bytes.HasPrefix([]byte(k), pA) {
			t.Fatalf("prefix scan leaked key %x (not under prefix %x)", k, pA)
		}
		if bytes.HasPrefix([]byte(k), wipePrefix(addrB)) {
			t.Fatalf("prefix scan leaked cross-domain key %x", k)
		}
	}
	fmt.Printf("forEachPrefix: A-domain=%d keys, full-tree=%d keys\n", len(gotA), len(gotAll))
}

func keysOf(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, fmt.Sprintf("%x", k))
	}
	return out
}
