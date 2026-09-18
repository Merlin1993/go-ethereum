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
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func mkKey(i int) []byte {
	b := make([]byte, 32)
	b[0] = byte(i)
	b[1] = byte(i >> 8)
	b[31] = byte(i * 31)
	return b
}

func stubFor(pathNib byte, count uint32) *Stub {
	s := &Stub{Path: []byte{pathNib, 0x0f}, Count: count}
	s.Filter = []byte{0x01, pathNib}
	s.Commitment[0] = 0x02
	s.Commitment[32] = pathNib
	return s
}

// Stubs must be covered by the node hash: mounting changes the root, and
// stubs survive a commit/reload round trip.
func TestHXStubMountHashAndReload(t *testing.T) {
	db := newTestDB()
	tr := NewEmpty(db)
	for i := 0; i < 40; i++ {
		k := mkKey(i)
		if err := tr.Update(k, k[:8]); err != nil {
			t.Fatal(err)
		}
	}
	rootBefore := tr.Hash()
	if _, err := tr.MountStub([]byte{3}, stubFor(3, 7)); err != nil {
		t.Fatal(err)
	}
	if rootAfter := tr.Hash(); rootAfter == rootBefore {
		t.Fatal("root unchanged after mounting a stub: stub not covered by hash")
	}
	root := db.commit(t, tr)

	reopened, err := New(root, common.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}
	var found []*Stub
	if err := reopened.StubsOnPath(mkKey(0), func(_ []byte, s *Stub) bool {
		found = append(found, s)
		return false
	}); err != nil {
		t.Fatal(err)
	}
	if len(found) == 0 {
		t.Fatal("stub lost across reload")
	}
	// mkKey(0) starts with nibble 0, so the domain-3 stub must NOT be on its
	// path; probe a key inside domain 3 instead.
	found = found[:0]
	k3 := mkKey(0)
	k3[0] = 0x30
	if err := reopened.StubsOnPath(k3, func(_ []byte, s *Stub) bool {
		found = append(found, s)
		return false
	}); err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 || found[0].Count != 7 || !bytes.Equal(found[0].Path, []byte{3, 0x0f}) {
		t.Fatalf("stub after reload = %+v", found)
	}
	if err := reopened.VerifyAggregates(); err != nil {
		t.Fatal(err)
	}
}

// ReplaceStub updates in place (count/commitment churn from redemptions) and
// removes on nil (bucket destruction at count zero).
func TestHXStubReplaceAndRemove(t *testing.T) {
	db := newTestDB()
	tr := NewEmpty(db)
	for i := 0; i < 30; i++ {
		if err := tr.Update(mkKey(i), mkKey(i)[:4]); err != nil {
			t.Fatal(err)
		}
	}
	mp, err := tr.MountStub([]byte{1}, stubFor(1, 3))
	if err != nil {
		t.Fatal(err)
	}
	if mp == nil {
		t.Fatal("mount path nil on non-empty tree")
	}
	repl := stubFor(1, 2) // same Path, new count
	repl.Commitment[1] = 0x99
	if err := tr.ReplaceStub(mp, []byte{1, 0x0f}, repl); err != nil {
		t.Fatal(err)
	}
	root := db.commit(t, tr)
	reopened, _ := New(root, common.Hash{}, db)
	var seen []*Stub
	if err := reopened.AllStubs(func(_ []byte, s *Stub) { seen = append(seen, s) }); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 || seen[0].Count != 2 || seen[0].Commitment[1] != 0x99 {
		t.Fatalf("stub after replace = %+v", seen)
	}
	// Destroy the bucket: stub leaves the tree, root changes.
	before := reopened.Hash()
	if err := reopened.ReplaceStub(mp, []byte{1, 0x0f}, nil); err != nil {
		t.Fatal(err)
	}
	if reopened.Hash() == before {
		t.Fatal("root unchanged after stub removal")
	}
	seen = seen[:0]
	if err := reopened.AllStubs(func(_ []byte, s *Stub) { seen = append(seen, s) }); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 0 {
		t.Fatalf("stub survived removal: %+v", seen)
	}
}

// Root catch-all: pruning may shrink the tree below every mount point; the
// residual bucket hangs on the (possibly stub-only) root.
func TestHXStubRootCatchAll(t *testing.T) {
	db := newTestDB()
	tr := NewEmpty(db)
	if _, err := tr.MountStub([]byte{9}, stubFor(9, 1)); err != nil {
		t.Fatal(err)
	}
	if tr.root == nil {
		t.Fatal("root nil after catch-all mount")
	}
	root := db.commit(t, tr)
	reopened, err := New(root, common.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}
	var seen int
	if err := reopened.AllStubs(func(_ []byte, s *Stub) { seen++ }); err != nil {
		t.Fatal(err)
	}
	if seen != 1 {
		t.Fatalf("catch-all stub lost across reload (seen %d)", seen)
	}
	// The stub-only root must not break data operations.
	if err := reopened.Update(mkKey(1), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if v, err := reopened.Get(mkKey(1)); err != nil || !bytes.Equal(v, []byte("v")) {
		t.Fatalf("get on stub-rooted tree: %q %v", v, err)
	}
	if err := reopened.VerifyAggregates(); err != nil {
		t.Fatal(err)
	}
}

// MountPoint must land on the deepest surviving node on the route.
func TestHXStubMountPointDeepest(t *testing.T) {
	db := newTestDB()
	tr := NewEmpty(db)
	for i := 0; i < 60; i++ {
		if err := tr.Update(mkKey(i), mkKey(i)[:4]); err != nil {
			t.Fatal(err)
		}
	}
	mp, err := tr.MountPoint([]byte{3})
	if err != nil {
		t.Fatal(err)
	}
	if len(mp) > 1 {
		t.Fatalf("mount point %x deeper than the prefix", mp)
	}
	got, err := tr.MountStub([]byte{3}, stubFor(3, 1))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, mp) {
		t.Fatalf("MountStub landed at %x, MountPoint promised %x", got, mp)
	}
}
