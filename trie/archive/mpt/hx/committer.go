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
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

// committer collects dirty nodes into a trienode.NodeSet. Adapted from geth's
// trie/committer.go (leaf collection dropped: the archive layer never needs
// leaf preimages in the nodeset).
type committer struct {
	nodes  *trienode.NodeSet
	tracer *tracer
}

func newCommitter(nodeset *trienode.NodeSet, tracer *tracer) *committer {
	return &committer{nodes: nodeset, tracer: tracer}
}

// Commit collapses a node down into a hash node.
func (c *committer) Commit(n node, parallel bool) hashNode {
	return c.commit(nil, n, parallel).(hashNode)
}

// commit collapses a node down into a hash node and returns it.
func (c *committer) commit(path []byte, n node, parallel bool) node {
	hash, dirty := n.cache()
	if hash != nil && !dirty {
		return hash
	}
	switch cn := n.(type) {
	case *shortNode:
		collapsed := cn.copy()
		if _, ok := cn.Val.(*fullNode); ok {
			collapsed.Val = c.commit(append(path, cn.Key...), cn.Val, false)
		}
		collapsed.Key = hexToCompact(cn.Key)
		hashedNode := c.store(path, collapsed)
		if hn, ok := hashedNode.(hashNode); ok {
			return hn
		}
		return collapsed
	case *fullNode:
		hashedKids := c.commitChildren(path, cn, parallel)
		collapsed := cn.copy()
		collapsed.Children = hashedKids
		hashedNode := c.store(path, collapsed)
		if hn, ok := hashedNode.(hashNode); ok {
			return hn
		}
		return collapsed
	case hashNode:
		return cn
	default:
		panic(fmt.Sprintf("%T: invalid node: %v", n, n))
	}
}

func (c *committer) commitChildren(path []byte, n *fullNode, parallel bool) [17]node {
	var (
		wg       sync.WaitGroup
		nodesMu  sync.Mutex
		children [17]node
	)
	for i := 0; i < 16; i++ {
		child := n.Children[i]
		if child == nil {
			continue
		}
		if hn, ok := child.(hashNode); ok {
			children[i] = hn
			continue
		}
		if !parallel {
			children[i] = c.commit(append(path, byte(i)), child, false)
		} else {
			wg.Add(1)
			go func(index int) {
				p := append(path, byte(index))
				childSet := trienode.NewNodeSet(c.nodes.Owner)
				childCommitter := newCommitter(childSet, c.tracer)
				children[index] = childCommitter.commit(p, child, false)
				nodesMu.Lock()
				c.nodes.MergeSet(childSet)
				nodesMu.Unlock()
				wg.Done()
			}(i)
		}
	}
	if parallel {
		wg.Wait()
	}
	if n.Children[16] != nil {
		children[16] = n.Children[16]
	}
	return children
}

// store hashes the node n and adds it to the modified nodeset.
func (c *committer) store(path []byte, n node) node {
	var hash, _ = n.cache()
	if hash == nil {
		// Embedded in parent; mark deleted only if it existed on disk before.
		_, ok := c.tracer.accessList[string(path)]
		if ok {
			c.nodes.AddNode(path, trienode.NewDeleted())
		}
		return n
	}
	nhash := common.BytesToHash(hash)
	c.nodes.AddNode(path, trienode.New(nhash, nodeToBytes(n)))
	return hash
}
