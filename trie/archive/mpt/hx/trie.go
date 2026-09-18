// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License as
// published by the Free Software Foundation, version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package hx

import (
	"bytes"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb/database"
)

// Trie is the AMT hexary state tree: geth's MPT operation semantics plus the
// paper's epoch bitmap and stubList node fields. Not safe for concurrent use;
// the mpt layer serializes with its own mutex.
//
// Epoch policy: writes (insert/update, including same-value rewrites) stamp
// the leaf with epochPolicy(key); plain reads never touch the epoch. This
// matches the paper's dynamic-assignment rule, which speaks of "newly written
// data" — a hot read does not extend lifetime, an archived read resurrects via
// a real write (the redemption path re-inserts through Update).
type Trie struct {
	root  node
	owner common.Hash

	committed   bool
	unhashed    int
	uncommitted int

	reader *trieReader
	tracer *tracer

	// epochPolicy assigns the lifecycle bit for newly written leaves. Nil
	// means "always 0" (used before the mpt layer wires the domain schedule).
	epochPolicy func(key []byte) byte
}

// New opens the trie at the given root. owner is the pathdb owner hash
// (zero for the single account-less state tree, as the mpt layer uses).
func New(root common.Hash, owner common.Hash, db database.NodeDatabase) (*Trie, error) {
	reader, err := newTrieReader(root, owner, db)
	if err != nil {
		return nil, err
	}
	trie := &Trie{
		owner:  owner,
		reader: reader,
		tracer: newTracer(),
	}
	if root != (common.Hash{}) && root != types.EmptyRootHash {
		rootnode, err := trie.resolveAndTrack(root[:], nil)
		if err != nil {
			return nil, err
		}
		trie.root = rootnode
	}
	return trie, nil
}

// NewEmpty creates an empty tree.
func NewEmpty(db database.NodeDatabase) *Trie {
	tr, _ := New(types.EmptyRootHash, common.Hash{}, db)
	return tr
}

// SetEpochPolicy installs the leaf-epoch assignment callback (P4b).
func (t *Trie) SetEpochPolicy(fn func(key []byte) byte) {
	t.epochPolicy = fn
}

func (t *Trie) epochFor(key []byte) byte {
	if t.epochPolicy == nil {
		return 0
	}
	return t.epochPolicy(key) & 1
}

func (t *Trie) newFlag() nodeFlag {
	return nodeFlag{dirty: true}
}

// Get returns the value for key stored in the trie, or nil when absent.
func (t *Trie) Get(key []byte) ([]byte, error) {
	if t.committed {
		return nil, ErrCommitted
	}
	value, newroot, didResolve, err := t.get(t.root, keybytesToHex(key), 0)
	if err == nil && didResolve {
		t.root = newroot
	}
	return value, err
}

func (t *Trie) get(origNode node, key []byte, pos int) (value []byte, newnode node, didResolve bool, err error) {
	switch n := (origNode).(type) {
	case nil:
		return nil, nil, false, nil
	case valueNode:
		return n, n, false, nil
	case *shortNode:
		if !bytes.HasPrefix(key[pos:], n.Key) {
			return nil, n, false, nil
		}
		value, newnode, didResolve, err = t.get(n.Val, key, pos+len(n.Key))
		if err == nil && didResolve {
			n = n.copy()
			n.Val = newnode
		}
		return value, n, didResolve, err
	case *fullNode:
		value, newnode, didResolve, err = t.get(n.Children[key[pos]], key, pos+1)
		if err == nil && didResolve {
			n = n.copy()
			n.Children[key[pos]] = newnode
		}
		return value, n, didResolve, err
	case hashNode:
		child, err := t.resolveAndTrack(n, key[:pos])
		if err != nil {
			return nil, n, true, err
		}
		value, newnode, _, err := t.get(child, key, pos)
		return value, newnode, true, err
	default:
		panic("hx: invalid node type")
	}
}

// Update associates key with value in the trie. Empty value deletes.
func (t *Trie) Update(key, value []byte) error {
	if t.committed {
		return ErrCommitted
	}
	t.unhashed++
	t.uncommitted++
	k := keybytesToHex(key)
	if len(value) != 0 {
		epoch := t.epochFor(key)
		_, n, err := t.insert(t.root, nil, k, valueNode(value), epoch)
		if err != nil {
			return err
		}
		t.root = n
	} else {
		_, n, err := t.delete(t.root, nil, k)
		if err != nil {
			return err
		}
		t.root = n
	}
	return nil
}

func (t *Trie) insert(n node, prefix, key []byte, value node, epoch byte) (bool, node, error) {
	if len(key) == 0 {
		if v, ok := n.(valueNode); ok {
			return !bytes.Equal(v, value.(valueNode)), value, nil
		}
		return true, value, nil
	}
	switch n := n.(type) {
	case *shortNode:
		matchlen := prefixLen(key, n.Key)
		if matchlen == len(n.Key) {
			dirty, nn, err := t.insert(n.Val, append(prefix, key[:matchlen]...), key[matchlen:], value, epoch)
			if err != nil {
				return false, n, err
			}
			if !dirty {
				// Same-value rewrite of an existing leaf still refreshes the
				// epoch (paper: epoch marks the most recent update).
				if _, isVal := n.Val.(valueNode); isVal && n.Epoch != epoch {
					return true, &shortNode{Key: n.Key, Val: n.Val, Epoch: epoch, Stubs: n.Stubs, flags: t.newFlag()}, nil
				}
				return false, n, nil
			}
			fresh := &shortNode{Key: n.Key, Val: nn, Stubs: n.Stubs, flags: t.newFlag()}
			if _, isVal := nn.(valueNode); isVal {
				fresh.Epoch = epoch
			} else {
				fresh.Agg = subtreeAggOf(nn)
			}
			return true, fresh, nil
		}
		// Otherwise branch out at the index where they differ. The existing
		// node is remounted under its diverging nibble with its epoch
		// metadata PRESERVED — reinserting it through insert() would stamp
		// it with the new write's epoch and silently resurrect stale data.
		// Stubs ride the HIGHEST replacement node (the extension above the
		// branch, or the branch itself), so a mounted bucket always stays on
		// the route from the root to its keys.
		branch := &fullNode{flags: t.newFlag()}
		var err error
		oldSuffix := n.Key[matchlen+1:]
		if len(oldSuffix) == 0 {
			branch.Children[n.Key[matchlen]] = n.Val
		} else {
			t.tracer.onInsert(append(prefix, n.Key[:matchlen+1]...))
			branch.Children[n.Key[matchlen]] = &shortNode{Key: oldSuffix, Val: n.Val, Epoch: n.Epoch, Agg: n.Agg, flags: t.newFlag()}
		}
		_, branch.Children[key[matchlen]], err = t.insert(nil, append(prefix, key[:matchlen+1]...), key[matchlen+1:], value, epoch)
		if err != nil {
			return false, nil, err
		}
		branch.refreshAgg(int(n.Key[matchlen]))
		branch.refreshAgg(int(key[matchlen]))
		if matchlen == 0 {
			branch.Stubs = n.Stubs
			return true, branch, nil
		}
		t.tracer.onInsert(append(prefix, key[:matchlen]...))
		ext := &shortNode{Key: key[:matchlen], Val: branch, Stubs: n.Stubs, flags: t.newFlag()}
		ext.Agg = subtreeAggOf(branch)
		return true, ext, nil

	case *fullNode:
		dirty, nn, err := t.insert(n.Children[key[0]], append(prefix, key[0]), key[1:], value, epoch)
		if !dirty || err != nil {
			return false, n, err
		}
		n = n.copy()
		n.flags = t.newFlag()
		n.Children[key[0]] = nn
		n.refreshAgg(int(key[0]))
		return true, n, nil

	case nil:
		t.tracer.onInsert(prefix)
		return true, &shortNode{Key: key, Val: value, Epoch: epoch, flags: t.newFlag()}, nil

	case hashNode:
		rn, err := t.resolveAndTrack(n, prefix)
		if err != nil {
			return false, nil, err
		}
		dirty, nn, err := t.insert(rn, prefix, key, value, epoch)
		if !dirty || err != nil {
			return false, rn, err
		}
		return true, nn, nil

	default:
		panic("hx: invalid node type")
	}
}

// Delete removes any existing value for key.
func (t *Trie) Delete(key []byte) error {
	if t.committed {
		return ErrCommitted
	}
	t.uncommitted++
	t.unhashed++
	k := keybytesToHex(key)
	_, n, err := t.delete(t.root, nil, k)
	if err != nil {
		return err
	}
	t.root = n
	return nil
}

// delete returns the new root of the trie with key deleted, reducing the trie
// to minimal form on the way up. Surviving nodes keep their epoch metadata:
// deletion of a sibling must not rewrite the survivor's lifecycle bit.
func (t *Trie) delete(n node, prefix, key []byte) (bool, node, error) {
	switch n := n.(type) {
	case *shortNode:
		matchlen := prefixLen(key, n.Key)
		if matchlen < len(n.Key) {
			return false, n, nil
		}
		if matchlen == len(key) {
			t.tracer.onDelete(prefix)
			return true, nil, nil
		}
		dirty, child, err := t.delete(n.Val, append(prefix, key[:len(n.Key)]...), key[len(n.Key):])
		if !dirty || err != nil {
			return false, n, err
		}
		switch child := child.(type) {
		case *shortNode:
			// Merging into a leaf would land n's stubs on a leaf, which the
			// mounting rules forbid — keep the extension above it instead.
			if _, isVal := child.Val.(valueNode); isVal && len(n.Stubs) > 0 {
				fresh := &shortNode{Key: n.Key, Val: child, Stubs: n.Stubs, flags: t.newFlag()}
				fresh.Agg = subtreeAggOf(child)
				return true, fresh, nil
			}
			t.tracer.onDelete(append(prefix, n.Key...))
			merged := &shortNode{Key: concat(n.Key, child.Key...), Val: child.Val, Epoch: child.Epoch, Agg: child.Agg, Stubs: mergeStubs(n.Stubs, child.Stubs), flags: t.newFlag()}
			return true, merged, nil
		default:
			fresh := &shortNode{Key: n.Key, Val: child, Stubs: n.Stubs, flags: t.newFlag()}
			fresh.Agg = subtreeAggOf(child)
			return true, fresh, nil
		}

	case *fullNode:
		dirty, nn, err := t.delete(n.Children[key[0]], append(prefix, key[0]), key[1:])
		if !dirty || err != nil {
			return false, n, err
		}
		n = n.copy()
		n.flags = t.newFlag()
		n.Children[key[0]] = nn

		if nn != nil {
			n.refreshAgg(int(key[0]))
			return true, n, nil
		}
		// Reduction: collapse to a short node when a single child remains.
		n.setAgg(int(key[0]), aggEmpty)
		pos := -1
		for i, cld := range &n.Children {
			if cld != nil {
				if pos == -1 {
					pos = i
				} else {
					pos = -2
					break
				}
			}
		}
		if pos >= 0 {
			if pos != 16 {
				cnode, err := t.resolve(n.Children[pos], append(prefix, byte(pos)))
				if err != nil {
					return false, nil, err
				}
				if cnode, ok := cnode.(*shortNode); ok {
					// As above: never land stubs on a leaf through a merge.
					if _, isVal := cnode.Val.(valueNode); isVal && len(n.Stubs) > 0 {
						fresh := &shortNode{Key: []byte{byte(pos)}, Val: cnode, Stubs: n.Stubs, flags: t.newFlag()}
						fresh.Agg = subtreeAggOf(cnode)
						return true, fresh, nil
					}
					t.tracer.onDelete(append(prefix, byte(pos)))
					k := append([]byte{byte(pos)}, cnode.Key...)
					return true, &shortNode{Key: k, Val: cnode.Val, Epoch: cnode.Epoch, Agg: cnode.Agg, Stubs: mergeStubs(n.Stubs, cnode.Stubs), flags: t.newFlag()}, nil
				}
				// Child is a branch: the one-nibble extension inherits its aggregate.
				fresh := &shortNode{Key: []byte{byte(pos)}, Val: n.Children[pos], Stubs: n.Stubs, flags: t.newFlag()}
				fresh.Agg = n.getAgg(pos)
				return true, fresh, nil
			}
			// Branch value (slot 16): unreachable for fixed-width state keys.
			return true, &shortNode{Key: []byte{byte(pos)}, Val: n.Children[pos], Stubs: n.Stubs, flags: t.newFlag()}, nil
		}
		return true, n, nil

	case valueNode:
		return true, nil, nil

	case nil:
		return false, nil, nil

	case hashNode:
		rn, err := t.resolveAndTrack(n, prefix)
		if err != nil {
			return false, nil, err
		}
		dirty, nn, err := t.delete(rn, prefix, key)
		if !dirty || err != nil {
			return false, rn, err
		}
		return true, nn, nil

	default:
		panic("hx: invalid node type")
	}
}

func concat(s1 []byte, s2 ...byte) []byte {
	r := make([]byte, len(s1)+len(s2))
	copy(r, s1)
	copy(r[len(s1):], s2)
	return r
}

func (t *Trie) resolve(n node, prefix []byte) (node, error) {
	if n, ok := n.(hashNode); ok {
		return t.resolveAndTrack(n, prefix)
	}
	return n, nil
}

// resolveAndTrack loads a node from the store and records the read blob in the
// tracer so the committer can tell pre-existing nodes from fresh ones.
func (t *Trie) resolveAndTrack(n hashNode, prefix []byte) (node, error) {
	blob, err := t.reader.node(prefix, common.BytesToHash(n))
	if err != nil {
		return nil, err
	}
	t.tracer.onRead(prefix, blob)
	decoded, err := decodeNode(n, blob)
	if err != nil {
		return nil, err
	}
	return decoded, nil
}

// Hash returns the root hash without writing anything.
func (t *Trie) Hash() common.Hash {
	hash, cached := t.hashRoot()
	t.root = cached
	return common.BytesToHash(hash.(hashNode))
}

// Commit collects all dirty nodes and returns the new root plus the nodeset.
// The trie is single-use after Commit, matching the geth contract the mpt
// layer is built around.
func (t *Trie) Commit() (common.Hash, *trienode.NodeSet) {
	defer func() {
		t.committed = true
	}()
	if t.root == nil {
		paths := t.tracer.deletedNodes()
		if len(paths) == 0 {
			return types.EmptyRootHash, nil
		}
		nodes := trienode.NewNodeSet(t.owner)
		for _, path := range paths {
			nodes.AddNode([]byte(path), trienode.NewDeleted())
		}
		return types.EmptyRootHash, nodes
	}
	rootHash := t.Hash()
	if hashedNode, dirty := t.root.cache(); !dirty {
		t.root = hashedNode
		return rootHash, nil
	}
	nodes := trienode.NewNodeSet(t.owner)
	for _, path := range t.tracer.deletedNodes() {
		nodes.AddNode([]byte(path), trienode.NewDeleted())
	}
	t.root = newCommitter(nodes, t.tracer).Commit(t.root, t.uncommitted > 100)
	t.uncommitted = 0
	return rootHash, nodes
}

func (t *Trie) hashRoot() (node, node) {
	if t.root == nil {
		return hashNode(types.EmptyRootHash.Bytes()), nil
	}
	h := newHasher(t.unhashed >= 100)
	defer func() {
		returnHasherToPool(h)
		t.unhashed = 0
	}()
	hashed, cached := h.hash(t.root, true)
	return hashed, cached
}

// LeafEpoch reports the lifecycle bit of the leaf at key. Diagnostics and
// tests only; the prune path reads epochs from the nodes it visits directly.
func (t *Trie) LeafEpoch(key []byte) (byte, bool, error) {
	if t.committed {
		return 0, false, ErrCommitted
	}
	n := t.root
	pos := 0
	hexKey := keybytesToHex(key)
	for {
		switch nn := n.(type) {
		case nil:
			return 0, false, nil
		case *shortNode:
			if !bytes.HasPrefix(hexKey[pos:], nn.Key) {
				return 0, false, nil
			}
			pos += len(nn.Key)
			if _, ok := nn.Val.(valueNode); ok {
				return nn.Epoch, true, nil
			}
			n = nn.Val
		case *fullNode:
			n = nn.Children[hexKey[pos]]
			pos++
		case hashNode:
			resolved, err := t.resolveAndTrack(nn, hexKey[:pos])
			if err != nil {
				return 0, false, err
			}
			n = resolved
		case valueNode:
			return 0, false, nil // branch value: no epoch carrier
		default:
			panic("hx: invalid node type")
		}
	}
}

// VerifyAggregates walks the whole tree and checks every aggregate indicator
// against the actual subtree contents. O(tree size) — tests and audits only.
func (t *Trie) VerifyAggregates() error {
	if t.committed {
		return ErrCommitted
	}
	_, err := t.verifyNode(t.root, nil)
	return err
}

func (t *Trie) verifyNode(n node, prefix []byte) (uint8, error) {
	switch n := n.(type) {
	case nil:
		return aggEmpty, nil
	case valueNode:
		return aggEmpty, nil
	case *shortNode:
		if _, isVal := n.Val.(valueNode); isVal {
			return epochToAgg(n.Epoch), nil
		}
		childAgg, err := t.verifyNode(n.Val, concat(prefix, n.Key...))
		if err != nil {
			return aggEmpty, err
		}
		if n.Agg != childAgg {
			return aggEmpty, fmt.Errorf("hx: ext at %x agg %d, subtree %d", prefix, n.Agg, childAgg)
		}
		return n.Agg, nil
	case *fullNode:
		agg, seen := uint8(0), false
		for i := 0; i < 16; i++ {
			childAgg, err := t.verifyNode(n.Children[i], concat(prefix, byte(i)))
			if err != nil {
				return aggEmpty, err
			}
			if got := n.getAgg(i); got != childAgg {
				return aggEmpty, fmt.Errorf("hx: branch at %x slot %d agg %d, subtree %d", prefix, i, got, childAgg)
			}
			agg, seen = combineAgg(agg, seen, childAgg)
		}
		if !seen {
			return aggEmpty, nil
		}
		return agg, nil
	case hashNode:
		resolved, err := t.resolveAndTrack(n, prefix)
		if err != nil {
			return aggEmpty, err
		}
		return t.verifyNode(resolved, prefix)
	default:
		return aggEmpty, fmt.Errorf("hx: verifyNode on %T", n)
	}
}

// CollectLeaves walks the whole tree (resolving from disk as needed) and
// invokes fn for every stored key/value in lexicographic-path order. fn
// returning false stops the walk. Used by the mpt layer's ForEach and by the
// pre-P4c domain scanner; the epoch-based pruner does not need it.
func (t *Trie) CollectLeaves(fn func(key, value []byte) bool) error {
	if t.committed {
		return ErrCommitted
	}
	_, err := t.collect(t.root, nil, fn)
	return err
}

func (t *Trie) collect(n node, prefix []byte, fn func(key, value []byte) bool) (bool, error) {
	switch n := n.(type) {
	case nil:
		return true, nil
	case *shortNode:
		if val, ok := n.Val.(valueNode); ok {
			full := concat(prefix, n.Key...)
			return fn(hexToKeybytes(full), val), nil
		}
		return t.collect(n.Val, concat(prefix, n.Key...), fn)
	case *fullNode:
		for i := 0; i < 16; i++ {
			if n.Children[i] == nil {
				continue
			}
			cont, err := t.collect(n.Children[i], concat(prefix, byte(i)), fn)
			if err != nil || !cont {
				return cont, err
			}
		}
		if val, ok := n.Children[16].(valueNode); ok {
			full := concat(prefix, 16)
			return fn(hexToKeybytes(full), val), nil
		}
		return true, nil
	case hashNode:
		resolved, err := t.resolveAndTrack(n, prefix)
		if err != nil {
			return false, err
		}
		return t.collect(resolved, prefix, fn)
	case valueNode:
		return fn(hexToKeybytes(concat(prefix, 16)), n), nil
	default:
		panic("hx: invalid node type")
	}
}
