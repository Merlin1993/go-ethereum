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

package trie

import (
	"maps"
	"sync"
)

// PrevalueTracer tracks the original values of resolved trie nodes.
type PrevalueTracer struct {
	data map[string][]byte
	lock sync.RWMutex
}

// NewPrevalueTracer initializes the tracer for capturing resolved trie nodes.
func NewPrevalueTracer() *PrevalueTracer {
	return &PrevalueTracer{
		data: make(map[string][]byte),
	}
}

// Put tracks the newly loaded trie node and caches its encoded blob.
func (t *PrevalueTracer) Put(path []byte, val []byte) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.data[string(path)] = val
}

// Get returns the cached trie node value.
func (t *PrevalueTracer) Get(path []byte) []byte {
	t.lock.RLock()
	defer t.lock.RUnlock()

	return t.data[string(path)]
}

// HasList returns flags indicating whether the corresponding trie nodes exist.
func (t *PrevalueTracer) HasList(list [][]byte) []bool {
	t.lock.RLock()
	defer t.lock.RUnlock()

	exists := make([]bool, 0, len(list))
	for _, path := range list {
		_, ok := t.data[string(path)]
		exists = append(exists, ok)
	}
	return exists
}

// Values returns a shallow copy of the cached trie node values.
func (t *PrevalueTracer) Values() map[string][]byte {
	t.lock.RLock()
	defer t.lock.RUnlock()

	return maps.Clone(t.data)
}

// Reset resets the cached content.
func (t *PrevalueTracer) Reset() {
	t.lock.Lock()
	defer t.lock.Unlock()

	clear(t.data)
}

// Copy returns a copied prevalue tracer instance.
func (t *PrevalueTracer) Copy() *PrevalueTracer {
	t.lock.RLock()
	defer t.lock.RUnlock()

	return &PrevalueTracer{
		data: maps.Clone(t.data),
	}
}
