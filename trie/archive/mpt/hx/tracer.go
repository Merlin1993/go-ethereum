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
	"maps"
)

// tracer tracks inserted/deleted node paths so the committer can emit
// deletion markers (mandatory for the path backend: without them pathdb would
// serve stale nodes at paths the new root no longer contains). Copied from
// geth's trie/tracer.go, minus the copy-on-read bookkeeping we don't need.
type tracer struct {
	inserts    map[string]struct{}
	deletes    map[string]struct{}
	accessList map[string][]byte
}

func newTracer() *tracer {
	return &tracer{
		inserts:    make(map[string]struct{}),
		deletes:    make(map[string]struct{}),
		accessList: make(map[string][]byte),
	}
}

func (t *tracer) onRead(path []byte, val []byte) {
	t.accessList[string(path)] = val
}

func (t *tracer) onInsert(path []byte) {
	if _, present := t.deletes[string(path)]; present {
		delete(t.deletes, string(path))
		return
	}
	t.inserts[string(path)] = struct{}{}
}

func (t *tracer) onDelete(path []byte) {
	if _, present := t.inserts[string(path)]; present {
		delete(t.inserts, string(path))
		return
	}
	t.deletes[string(path)] = struct{}{}
}

func (t *tracer) reset() {
	t.inserts = make(map[string]struct{})
	t.deletes = make(map[string]struct{})
	t.accessList = make(map[string][]byte)
}

func (t *tracer) copy() *tracer {
	return &tracer{
		inserts:    maps.Clone(t.inserts),
		deletes:    maps.Clone(t.deletes),
		accessList: maps.Clone(t.accessList),
	}
}

// deletedNodes returns the paths of nodes deleted from the trie that were
// previously loaded from disk (embedded-only nodes never hit the database).
func (t *tracer) deletedNodes() []string {
	var paths []string
	for path := range t.deletes {
		if _, ok := t.accessList[path]; !ok {
			continue
		}
		paths = append(paths, path)
	}
	return paths
}
