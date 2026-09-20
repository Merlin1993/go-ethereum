package hx

// DebugCheckDangling walks the in-memory tree collecting every child path
// (hashNode children included — their paths are known without resolving) and
// reports any path that is simultaneously reachable and pending deletion.
// Used by the mpt dangling-reference regression test; resolve-free so the
// check itself never perturbs trie state. Not part of the public API.
func (t *Trie) DebugCheckDangling() [][]byte {
	deleted := make(map[string]struct{})
	for _, p := range t.tracer.deletedNodes() {
		deleted[string(p)] = struct{}{}
	}
	var bad [][]byte
	var walk func(n node, path []byte)
	walk = func(n node, path []byte) {
		switch n := n.(type) {
		case *shortNode:
			childPath := concat(path, n.Key...)
			if _, ok := deleted[string(childPath)]; ok {
				bad = append(bad, childPath)
			}
			if _, isVal := n.Val.(valueNode); !isVal {
				walk(n.Val, childPath)
			}
		case *fullNode:
			for i := 0; i < 16; i++ {
				if n.Children[i] == nil {
					continue
				}
				childPath := concat(path, byte(i))
				if _, ok := deleted[string(childPath)]; ok {
					bad = append(bad, childPath)
				}
				if _, isHash := n.Children[i].(hashNode); !isHash {
					walk(n.Children[i], childPath)
				}
			}
		}
	}
	walk(t.root, nil)
	return bad
}
