# Binary stem mode

The complete logical design is documented in
[`doc/ARCHIVE_LOGIC_STEM.md`](doc/ARCHIVE_LOGIC_STEM.md).

`StemTrie` is an explicit adapter over the existing ASCT. It accepts a
32-byte tree key and splits it into:

- bytes `[0:31]`: the stem routed through the outer binary ASCT;
- byte `[31]`: one of the stem's 256 suffixes.

The outer ASCT stores one leaf per stem. Its value is the stem's 32-byte
`ValuesRoot`. A 40-byte metadata record stores the version and presence bitmap,
and each populated suffix value is stored separately under its complete
32-byte key. Consequently, the existing ASCT epoch, pruning, archive bucket,
and activation logic still operate on a complete stem. An archive bucket may
contain several stem records; updating or activating one stem removes only
that stem record from the bucket.

Inside the encoded stem, 256 value hashes form an eight-level binary tree.
`Stem.ValuesRoot` is its root, and `Stem.Prove` returns exactly eight sibling
hashes for a suffix. The current implementation uses the ASCT Keccak hasher
with separate domains for empty leaves, populated leaves, and branches.

When one suffix is updated, `StemTrie.Put` recomputes `ValuesRoot` and writes
only the changed suffix value. The 40-byte bitmap is rewritten only when a
suffix is added or removed. If the stem was archived, the whole stem becomes
hot again, but its unchanged suffix records are not rewritten. No old root is
kept in the live trie.

`trie.ArchiveTrie` enables this adapter when `database.BinaryConfig.StemMode`
is true. It uses the same SHA-256 state-key derivation as `trie/bintrie`:

- account RLP is stored at suffix 0 of the account header stem;
- storage slots 0..63 use suffixes 64..127 of that stem;
- code chunks 0..127 use suffixes 128..255 of that stem;
- later storage slots and code chunks are mapped into additional 256-value
  stems by the binary-tree key encoder.

The replay test exposes this as `-binaryStemArchive=true`; the default remains
false. The separate top-tree container has also been removed: shard prefixes
now form ordinary binary paths to the global root. This root encoding and Stem
mode both require a fresh database. Stem and per-key modes still use different
flat-value layouts, so the flag must not be switched while reopening a root.

Replay CSV output keeps the existing outer-leaf/archive-record columns and adds
`Active_Logical_Values` and `Archived_Logical_Values`. In stem mode, the former
columns count stems while the new logical-value columns count the populated
suffixes inside them, so storage cost can be compared per real value as well as
per stem.

Storage values include their original address and slot in a small envelope.
The binary key is one-way, so this metadata is needed to keep storage iteration
and deletion tooling correct. It is removed before values are returned to the
state layer.

Account destruction uses persistent address-to-slot and address-to-code-chunk
indexes. Storage and code updates maintain these indexes in the same commit as
the trie changes. A wipe therefore reads and deletes only the destroyed
account's own entries instead of filtering a global stem scan. Indexed keys are
grouped by stem before deletion, so each affected stem is loaded, decoded, and
written at most once; index removals are staged under one lock per batch. The
wipe runs before same-block recreation updates are applied, so old slots and
code chunks are removed without deleting the new incarnation. Replay
diagnostics report the slot/stem counts and the index scan, stem deletion,
index staging, and StateDB history-building times separately. An index schema
marker rejects a pre-index stem database instead of silently performing an
incomplete wipe; replay must start from a fresh database.

Pre-commit root calculation hashes independent dirty shards in parallel, using
the configured commit-worker cap. The later persistence pass reuses hashes
already computed by pre-commit. Any node mutation or path-storage relocation
invalidates the cached hash, and a changed child hash also invalidates its
parent, so this reuse does not change the resulting root. Replay metrics expose
the shard wall/work time, hashing and serialization time, worker count, and
slowest root calculation.

The current implementation retains the bitmap and suffix records in ASCT's
flat value store after archiving. A stateless/full-node mode that drops these
records will need a separate availability protocol. Because activation is
stem-granular, the current activation interface would require the complete
current set of suffix values; a single suffix and its proof are enough to
verify that suffix, but not to reconstruct and activate the complete stem.
