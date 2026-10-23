package mpt

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

// TestAllStubsWithinSemantics 验证前缀限定 stub 枚举的语义：命中的挂载点
// 集合与全树枚举过滤后一致（既不多也不少），跨域挂载点不泄漏。挂载点可
// 能比前缀短（stub 挂在前缀路径上方）也可能长（桶挂在前缀子树深处），
// 两种位置都必须命中。
func TestAllStubsWithinSemantics(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, false)

	addrA := bytes.Repeat([]byte{0x0a}, 20)
	addrB := bytes.Repeat([]byte{0x0b}, 20)
	// 每域放足量键使 hx 归档切出 stub（触发桶挂载）。
	mk := func(addr []byte, n int) {
		for i := 0; i < n; i++ {
			slot := bytes.Repeat([]byte{byte(i + 1)}, 32)
			key := append(append([]byte{}, addr...), slot...)
			if err := tr.Put(key, []byte(fmt.Sprintf("v%d", i))); err != nil {
				t.Fatal(err)
			}
			if _, err := tr.Commit(); err != nil {
				t.Fatal(err)
			}
		}
	}
	mk(addrA, 6)
	mk(addrB, 6)

	// 通过公开面收集：全树 vs 前缀限定（都走 mpt.Trie 的封装）。
	// 全树：ForEach(nil) 会加载所有桶——间接证明挂载点集合。
	fullKeys := map[string]bool{}
	tr.ForEach(func(key, value []byte) bool {
		fullKeys[string(key)] = true
		return true
	})
	prefixKeys := map[string]bool{}
	tr.ForEachPrefix(addrA, len(addrA)*8, func(key, value []byte) bool {
		prefixKeys[string(key)] = true
		return true
	})
	// 期望：前缀扫描命中 = 全树过滤 addrA。
	expect := map[string]bool{}
	for k := range fullKeys {
		if bytes.HasPrefix([]byte(k), addrA) {
			expect[k] = true
		}
	}
	if len(prefixKeys) != len(expect) {
		t.Fatalf("prefix scan got %d keys, expect %d (all=%d)", len(prefixKeys), len(expect), len(fullKeys))
	}
	for k := range expect {
		if !prefixKeys[k] {
			t.Fatalf("prefix scan missed key %x", []byte(k))
		}
	}
	// 跨域：B 域键不得出现在 A 前缀扫描里（挂载点剪枝没把 B 域桶拉进来出错）。
	for k := range prefixKeys {
		if bytes.HasPrefix([]byte(k), addrB) {
			t.Fatalf("prefix scan leaked B-domain key %x", []byte(k))
		}
	}
	fmt.Printf("allStubsWithin: A-domain=%d keys, full-tree=%d keys\n", len(prefixKeys), len(fullKeys))
}
