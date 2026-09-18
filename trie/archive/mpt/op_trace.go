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
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
// General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package mpt

import (
	"os"
	"sync/atomic"
	"time"

	archivetrie "github.com/ethereum/go-ethereum/trie/archive"
)

// Op-level timing breakdown (plan item T4), gated by the MPT_OP_TRACE
// environment variable. The driver's Operations_ms bucket mixes several
// mechanisms; these counters attribute it to the hot trie op, the archive
// probe, bucket disk reloads, and the write-path removal of archived copies.
//
// Overhead model: with the gate closed every instrumented section costs one
// struct-field bool check and nothing else — no timers, no atomics. With the
// gate open each section pays two time.Now() calls and one atomic add.
//
// The driver cannot import this package (import cycle), so the snapshot hook
// is registered into the archive package, mirroring the TraceHotTrieNew hook.

var opTraceGate atomic.Bool

// opTraceCounters holds cumulative counters; the driver diffs snapshots per
// window, so nothing here ever resets.
var opTrace struct {
	ops         atomic.Int64 // instrumented Get/Put/Delete/GetValueRef calls
	hotNanos    atomic.Int64 // geth hexary trie Get/Update/Delete
	probeNanos  atomic.Int64 // archiveLookupLocked: index + filter + map confirm
	loads       atomic.Int64 // bucket entry-map (re)loads from disk
	loadNanos   atomic.Int64 // optionalGet + decodeArchive for those loads
	removes     atomic.Int64 // write-path removals of archived copies
	removeNanos atomic.Int64
	evictions   atomic.Int64 // resident-budget evictions
}

func init() {
	enabled := os.Getenv("MPT_OP_TRACE") != ""
	opTraceGate.Store(enabled)
	if enabled {
		archivetrie.OpTraceProbe = opTraceSnapshot
	}
}

// opTraceEnabled reports whether this trie should time its sections. The gate
// is process-wide; storing it on the trie turns the hot path into a single
// field load.
func (t *Trie) opTraceEnabled() bool { return t.opTrace }

// opTraceSnapshot reports cumulative counters in fixed units (nanoseconds for
// durations). Keys are stable: the driver writes them as CSV columns.
func opTraceSnapshot() map[string]int64 {
	return map[string]int64{
		"ops":       opTrace.ops.Load(),
		"hot_ns":    opTrace.hotNanos.Load(),
		"probe_ns":  opTrace.probeNanos.Load(),
		"loads":     opTrace.loads.Load(),
		"load_ns":   opTrace.loadNanos.Load(),
		"removes":   opTrace.removes.Load(),
		"remove_ns": opTrace.removeNanos.Load(),
		"evictions": opTrace.evictions.Load(),
	}
}

// opTraceSince returns the zero time with the gate closed, so callers can do
// `start := t.opTraceStart()` unconditionally and only pay when enabled.
func (t *Trie) opTraceStart() time.Time {
	if !t.opTrace {
		return time.Time{}
	}
	return time.Now()
}

// opTraceAdd records one section; a zero start (gate closed) makes it a no-op.
func opTraceAdd(counter *atomic.Int64, start time.Time) {
	if start.IsZero() {
		return
	}
	counter.Add(int64(time.Since(start)))
}
