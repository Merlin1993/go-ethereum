## Go Ethereum

Golang execution layer implementation of the Ethereum protocol.

[![API Reference](
https://pkg.go.dev/badge/github.com/ethereum/go-ethereum
)](https://pkg.go.dev/github.com/ethereum/go-ethereum?tab=doc)
[![Go Report Card](https://goreportcard.com/badge/github.com/ethereum/go-ethereum)](https://goreportcard.com/report/github.com/ethereum/go-ethereum)
[![Travis](https://app.travis-ci.com/ethereum/go-ethereum.svg?branch=master)](https://app.travis-ci.com/github/ethereum/go-ethereum)
[![Discord](https://img.shields.io/badge/discord-join%20chat-blue.svg)](https://discord.gg/nthXNEv)
[![Twitter](https://img.shields.io/twitter/follow/go_ethereum)](https://x.com/go_ethereum)

Automated builds are available for stable releases and the unstable master branch. Binary
archives are published at https://geth.ethereum.org/downloads/.

## Building the source

For prerequisites and detailed build instructions please read the [Installation Instructions](https://geth.ethereum.org/docs/getting-started/installing-geth).

Building `geth` requires both a Go (version 1.23 or later) and a C compiler. You can install
them using your favourite package manager. Once the dependencies are installed, run

```shell
make geth
```

or, to build the full suite of utilities:

```shell
make all
```

## SWMT / cachetrie dual-root experiment

This branch contains an experimental SWMT-backed cachetrie path for replaying
mainnet blocks on top of go-ethereum 1.17. The goal is to keep recent global
state writes in a live in-memory SWMT overlay, merge them into the backing MPT
asynchronously, and disclose two roots during the experiment:

* `Root`: the currently disclosed backing MPT global state root.
* `SWMTRoot`: the live SWMT overlay commitment.

This is an experiment mode. Normal geth execution remains single-root and keeps
validating legacy block headers exactly as before.

### What changed

The cachetrie implementation now maintains a live SWMT overlay with low/high
watermarks:

* `--cachetrie.maxitems` is the high watermark.
* `--cachetrie.lowwatermark` is the low watermark. A value of `0` means 80% of
  the high watermark.
* The low watermark starts the first pending merge only once.
* After startup, the high watermark or a full window ring triggers the normal
  pipeline step: disclose the completed merge root, prune the already merged
  window bits from live SWMT, then select the next pending merge input set.

The live SWMT entries are not removed when a merge starts. They continue serving
reads and continue participating in `SWMTRoot` until the later disclose/prune
step. If a key is rewritten after a pending merge is formed, the new leaf moves
to the current window bit and is not pruned by the old pending bit set.

The state read path now tries SWMT before the backing MPT. Account metadata can
come from SWMT, while the backing MPT storage root is preserved so old storage
tries are not polluted by SWMT logical roots. Storage values are shadowed by
SWMT when present.

The state commit path has a fast async mode used by the dual-root experiment.
In this mode `StateDB` does not compute an MPT `IntermediateRoot`, does not
update the foreground account/storage tries, and does not commit MPT trie nodes
for the current block. Instead, it finalizes the block write set and publishes
account/storage/code changes into SWMT. Contract code blobs are still written to
the code database.

The backing MPT receives SWMT writes through the asynchronous merge worker. The
merge worker applies retained merge inputs to a no-cache `StateDB`, preserving
backing storage roots for accounts and materializing dependency accounts when a
storage input needs an owner account.

The block validation path has two modes:

* Normal mode: unchanged legacy validation, including `header.Root`.
* Dual-root experiment mode: validates gas, receipts, requests and execution
  results, but skips the legacy mainnet `header.Root` equality check because
  the local root cursor follows dual-root semantics. If a header contains
  `SWMTRoot`, it is checked against the local SWMT commitment.

### Flags

Enable the experiment with:

```shell
geth import \
  --cachetrie \
  --cachetrie.experiment.dualroot \
  --cachetrie.maxitems 1000000 \
  --cachetrie.lowwatermark 0 \
  --debug.logslowblock=0 \
  <block-files.rlp>
```

Important flags:

* `--cachetrie`: enables the cachetrie/SWMT facility.
* `--cachetrie.experiment.dualroot`: enables dual-root replay semantics.
* `--cachetrie.maxitems`: high watermark for live SWMT entries.
* `--cachetrie.lowwatermark`: low watermark; `0` selects the default 80% of
  high watermark.
* `--debug.logslowblock=0`: emits one slowlog JSON record per block, useful for
  replay analysis.

`--cachetrie.experiment.dualroot` requires `--cachetrie`.

### Slowlog fields

Each slowlog JSON record contains the usual block timing fields plus a
`cachetrie` object. The main fields to inspect are:

* `header_state_root`: the original block header state root.
* `global_root`: the local disclosed backing MPT root.
* `swmt_root`: the live SWMT commitment.
* `accounts`, `storages`: live SWMT size.
* `low_watermark`, `high_watermark`, `current_bit`, `start_bit`.
* `pipeline_started`, `pending_bits`, `pending_inputs`.
* `account_hits`, `account_misses`, `storage_hits`, `storage_misses`.
* `updates`, `deletes`.
* `merge_count`, `merge_inputs`, `merge_ms`, `merge_errors`.
* `prune_count`, `prune_items`, `prune_ms`.
* `write_wait_count`, `write_wait_ms`.
* `swmt_root_count`, `swmt_root_ms`, `publish_count`, `publish_ms`.

For the main performance question, use:

* transaction processing time: `timing.process_wall_ms`
* state read time: `timing.state_read_ms`
* root/hash time: `timing.state_hash_ms`
* state maintenance/commit time: `timing.commit_ms`

`cachetrie.merge_ms` is background pipeline work. It is reported for pipeline
health, but it should not be counted as foreground block execution time.
`cachetrie.write_wait_ms` is replay pacing/backpressure; when comparing replay
wall time, subtract only this wait from `timing.total_ms`.

### Testing

Run the focused test set:

```shell
go test ./cachetrie -count=1
go test ./core/state -run CacheTrie -count=1
go test ./core -run 'CacheTrie|DualRoot|Slow' -count=1
go build ./cmd/geth
```

For a wider check after touching state commit or validation code:

```shell
go test ./core/state -count=1
go test ./core -count=1
go test ./cmd/utils ./cmd/geth -count=1
```

The important tests cover:

* low watermark starts the first pending merge only once.
* high watermark or ring-full discloses and prunes only after the previous
  merge is complete.
* rewriting a key after pending merge formation prevents the old pending bit
  prune from deleting the new leaf.
* async commit publishes writes to SWMT without foreground MPT root/trie commit.
* dual-root validation skips legacy `header.Root` in experiment mode but keeps
  normal single-root validation unchanged.
* cachetrie reads preserve backing MPT storage roots.

### Verification checklist

For ordinary mode:

1. Import without `--cachetrie.experiment.dualroot`.
2. Confirm legacy `header.Root` validation still runs.
3. Confirm no `cachetrie.dualroot_experiment` slowlog records are emitted.

For dual-root experiment mode:

1. Import with `--cachetrie --cachetrie.experiment.dualroot`.
2. Confirm there are no `invalid block`, `nonce too low`, `invalid gas used`,
   or `Unexpected trie node` errors.
3. Confirm every slowlog row has a `cachetrie` field.
4. Confirm `global_root` and `swmt_root` are recorded.
5. Confirm `timing.state_hash_ms` is zero or near zero in the async path.
6. Confirm foreground account/storage trie commit timers stay zero in the async
   path.
7. Confirm merge/prune counters advance only around watermark pipeline events.

The final replay root is not a correctness target for this experiment. The run
does not do a final drain of all live SWMT entries into the backing MPT.

### Replay with an overlay datadir

Never replay directly on a shared snapshot datadir. Use an overlay where the
snapshot is the read-only lowerdir and all writes go into a run directory.

Example:

```shell
run=/root/snz/runs/cachetrie_dualroot_example
base=/root/snz/geth-chain
geth=/root/snz/tools/geth-cachetrie/geth

mkdir -p "$run/upper" "$run/work" "$run/merged" "$run/logs"
mount -t overlay overlay \
  -o "lowerdir=$base,upperdir=$run/upper,workdir=$run/work" \
  "$run/merged"

"$geth" --datadir "$run/merged" import \
  --cachetrie \
  --cachetrie.experiment.dualroot \
  --cachetrie.maxitems 1000000 \
  --cachetrie.lowwatermark 0 \
  --nocompaction \
  --debug.logslowblock=0 \
  /path/to/block_*.rlp \
  > "$run/logs/import.log" 2>&1

umount "$run/merged"
```

In the `/root/snz` experiment environment, the existing replay helper can be
used when installed:

```shell
SPIKE_RUN_ID=cachetrie_dualroot_spike_001 \
CACHETRIE_MAXITEMS=1000000 \
CACHETRIE_LOWWATERMARK=0 \
/root/snz/scripts/run_cachetrie_dualroot_replay_param.sh spike

FULL_RUN_ID=cachetrie_dualroot_31100_001 \
CACHETRIE_MAXITEMS=1000000 \
CACHETRIE_LOWWATERMARK=0 \
/root/snz/scripts/run_cachetrie_dualroot_replay_param.sh full
```

Suggested replay flow:

1. Run a short probe first, for example two blocks or a small window around a
   previous correctness blocker.
2. Run the targeted spike window.
3. Run the full 31,100 block replay only after the probe and spike are clean.
4. Compare `process_wall_ms`, `state_read_ms`, `state_hash_ms`, and `commit_ms`
   against the baseline. Treat `merge_ms` as background pipeline time and
   inspect it separately.

## Executables

The go-ethereum project comes with several wrappers/executables found in the `cmd`
directory.

|  Command   | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| :--------: | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`geth`** | Our main Ethereum CLI client. It is the entry point into the Ethereum network (main-, test- or private net), capable of running as a full node (default), archive node (retaining all historical state) or a light node (retrieving data live). It can be used by other processes as a gateway into the Ethereum network via JSON RPC endpoints exposed on top of HTTP, WebSocket and/or IPC transports. `geth --help` and the [CLI page](https://geth.ethereum.org/docs/fundamentals/command-line-options) for command line options. |
|   `clef`   | Stand-alone signing tool, which can be used as a backend signer for `geth`.                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|  `devp2p`  | Utilities to interact with nodes on the networking layer, without running a full blockchain.                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|  `abigen`  | Source code generator to convert Ethereum contract definitions into easy-to-use, compile-time type-safe Go packages. It operates on plain [Ethereum contract ABIs](https://docs.soliditylang.org/en/develop/abi-spec.html) with expanded functionality if the contract bytecode is also available. However, it also accepts Solidity source files, making development much more streamlined. Please see our [Native DApps](https://geth.ethereum.org/docs/developers/dapp-developer/native-bindings) page for details.                                  |
|   `evm`    | Developer utility version of the EVM (Ethereum Virtual Machine) that is capable of running bytecode snippets within a configurable environment and execution mode. Its purpose is to allow isolated, fine-grained debugging of EVM opcodes (e.g. `evm --code 60ff60ff --debug run`).                                                                                                                                                                                                                                               |
| `rlpdump`  | Developer utility tool to convert binary RLP ([Recursive Length Prefix](https://ethereum.org/en/developers/docs/data-structures-and-encoding/rlp)) dumps (data encoding used by the Ethereum protocol both network as well as consensus wise) to user-friendlier hierarchical representation (e.g. `rlpdump --hex CE0183FFFFFFC4C304050583616263`).                                                                                                                                                                                |

## Running `geth`

Going through all the possible command line flags is out of scope here (please consult our
[CLI Wiki page](https://geth.ethereum.org/docs/fundamentals/command-line-options)),
but we've enumerated a few common parameter combos to get you up to speed quickly
on how you can run your own `geth` instance.

### Hardware Requirements

Minimum:

* CPU with 4+ cores
* 8GB RAM
* 1TB free storage space to sync the Mainnet
* 8 MBit/sec download Internet service

Recommended:

* Fast CPU with 8+ cores
* 16GB+ RAM
* High-performance SSD with at least 1TB of free space
* 25+ MBit/sec download Internet service

### Full node on the main Ethereum network

By far the most common scenario is people wanting to simply interact with the Ethereum
network: create accounts; transfer funds; deploy and interact with contracts. For this
particular use case, the user doesn't care about years-old historical data, so we can
sync quickly to the current state of the network. To do so:

```shell
$ geth console
```

This command will:
 * Start `geth` in snap sync mode (default, can be changed with the `--syncmode` flag),
   causing it to download more data in exchange for avoiding processing the entire history
   of the Ethereum network, which is very CPU intensive.
 * Start the built-in interactive [JavaScript console](https://geth.ethereum.org/docs/interacting-with-geth/javascript-console),
   (via the trailing `console` subcommand) through which you can interact using [`web3` methods](https://github.com/ChainSafe/web3.js/blob/0.20.7/DOCUMENTATION.md) 
   (note: the `web3` version bundled within `geth` is very old, and not up to date with official docs),
   as well as `geth`'s own [management APIs](https://geth.ethereum.org/docs/interacting-with-geth/rpc).
   This tool is optional and if you leave it out you can always attach it to an already running
   `geth` instance with `geth attach`.

### A Full node on the Holesky test network

Transitioning towards developers, if you'd like to play around with creating Ethereum
contracts, you almost certainly would like to do that without any real money involved until
you get the hang of the entire system. In other words, instead of attaching to the main
network, you want to join the **test** network with your node, which is fully equivalent to
the main network, but with play-Ether only.

```shell
$ geth --holesky console
```

The `console` subcommand has the same meaning as above and is equally
useful on the testnet too.

Specifying the `--holesky` flag, however, will reconfigure your `geth` instance a bit:

 * Instead of connecting to the main Ethereum network, the client will connect to the Holesky 
   test network, which uses different P2P bootnodes, different network IDs and genesis
   states.
 * Instead of using the default data directory (`~/.ethereum` on Linux for example), `geth`
   will nest itself one level deeper into a `holesky` subfolder (`~/.ethereum/holesky` on
   Linux). Note, on OSX and Linux this also means that attaching to a running testnet node
   requires the use of a custom endpoint since `geth attach` will try to attach to a
   production node endpoint by default, e.g.,
   `geth attach <datadir>/holesky/geth.ipc`. Windows users are not affected by
   this.

*Note: Although some internal protective measures prevent transactions from
crossing over between the main network and test network, you should always
use separate accounts for play and real money. Unless you manually move
accounts, `geth` will by default correctly separate the two networks and will not make any
accounts available between them.*

### Configuration

As an alternative to passing the numerous flags to the `geth` binary, you can also pass a
configuration file via:

```shell
$ geth --config /path/to/your_config.toml
```

To get an idea of how the file should look like you can use the `dumpconfig` subcommand to
export your existing configuration:

```shell
$ geth --your-favourite-flags dumpconfig
```

#### Docker quick start

One of the quickest ways to get Ethereum up and running on your machine is by using
Docker:

```shell
docker run -d --name ethereum-node -v /Users/alice/ethereum:/root \
           -p 8545:8545 -p 30303:30303 \
           ethereum/client-go
```

This will start `geth` in snap-sync mode with a DB memory allowance of 1GB, as the
above command does.  It will also create a persistent volume in your home directory for
saving your blockchain as well as map the default ports. There is also an `alpine` tag
available for a slim version of the image.

Do not forget `--http.addr 0.0.0.0`, if you want to access RPC from other containers
and/or hosts. By default, `geth` binds to the local interface and RPC endpoints are not
accessible from the outside.

### Programmatically interfacing `geth` nodes

As a developer, sooner rather than later you'll want to start interacting with `geth` and the
Ethereum network via your own programs and not manually through the console. To aid
this, `geth` has built-in support for a JSON-RPC based APIs ([standard APIs](https://ethereum.org/en/developers/docs/apis/json-rpc/)
and [`geth` specific APIs](https://geth.ethereum.org/docs/interacting-with-geth/rpc)).
These can be exposed via HTTP, WebSockets and IPC (UNIX sockets on UNIX based
platforms, and named pipes on Windows).

The IPC interface is enabled by default and exposes all the APIs supported by `geth`,
whereas the HTTP and WS interfaces need to manually be enabled and only expose a
subset of APIs due to security reasons. These can be turned on/off and configured as
you'd expect.

HTTP based JSON-RPC API options:

  * `--http` Enable the HTTP-RPC server
  * `--http.addr` HTTP-RPC server listening interface (default: `localhost`)
  * `--http.port` HTTP-RPC server listening port (default: `8545`)
  * `--http.api` API's offered over the HTTP-RPC interface (default: `eth,net,web3`)
  * `--http.corsdomain` Comma separated list of domains from which to accept cross-origin requests (browser enforced)
  * `--ws` Enable the WS-RPC server
  * `--ws.addr` WS-RPC server listening interface (default: `localhost`)
  * `--ws.port` WS-RPC server listening port (default: `8546`)
  * `--ws.api` API's offered over the WS-RPC interface (default: `eth,net,web3`)
  * `--ws.origins` Origins from which to accept WebSocket requests
  * `--ipcdisable` Disable the IPC-RPC server
  * `--ipcpath` Filename for IPC socket/pipe within the datadir (explicit paths escape it)

You'll need to use your own programming environments' capabilities (libraries, tools, etc) to
connect via HTTP, WS or IPC to a `geth` node configured with the above flags and you'll
need to speak [JSON-RPC](https://www.jsonrpc.org/specification) on all transports. You
can reuse the same connection for multiple requests!

**Note: Please understand the security implications of opening up an HTTP/WS based
transport before doing so! Hackers on the internet are actively trying to subvert
Ethereum nodes with exposed APIs! Further, all browser tabs can access locally
running web servers, so malicious web pages could try to subvert locally available
APIs!**

### Operating a private network

Maintaining your own private network is more involved as a lot of configurations taken for
granted in the official networks need to be manually set up.

Unfortunately since [the Merge](https://ethereum.org/en/roadmap/merge/) it is no longer possible
to easily set up a network of geth nodes without also setting up a corresponding beacon chain.

There are three different solutions depending on your use case:

  * If you are looking for a simple way to test smart contracts from go in your CI, you can use the [Simulated Backend](https://geth.ethereum.org/docs/developers/dapp-developer/native-bindings#blockchain-simulator).
  * If you want a convenient single node environment for testing, you can use our [Dev Mode](https://geth.ethereum.org/docs/developers/dapp-developer/dev-mode).
  * If you are looking for a multiple node test network, you can set one up quite easily with [Kurtosis](https://geth.ethereum.org/docs/fundamentals/kurtosis).

## Contribution

Thank you for considering helping out with the source code! We welcome contributions
from anyone on the internet, and are grateful for even the smallest of fixes!

If you'd like to contribute to go-ethereum, please fork, fix, commit and send a pull request
for the maintainers to review and merge into the main code base. If you wish to submit
more complex changes though, please check up with the core devs first on [our Discord Server](https://discord.gg/invite/nthXNEv)
to ensure those changes are in line with the general philosophy of the project and/or get
some early feedback which can make both your efforts much lighter as well as our review
and merge procedures quick and simple.

Please make sure your contributions adhere to our coding guidelines:

 * Code must adhere to the official Go [formatting](https://golang.org/doc/effective_go.html#formatting)
   guidelines (i.e. uses [gofmt](https://golang.org/cmd/gofmt/)).
 * Code must be documented adhering to the official Go [commentary](https://golang.org/doc/effective_go.html#commentary)
   guidelines.
 * Pull requests need to be based on and opened against the `master` branch.
 * Commit messages should be prefixed with the package(s) they modify.
   * E.g. "eth, rpc: make trace configs optional"

Please see the [Developers' Guide](https://geth.ethereum.org/docs/developers/geth-developer/dev-guide)
for more details on configuring your environment, managing project dependencies, and
testing procedures.

### Contributing to geth.ethereum.org

For contributions to the [go-ethereum website](https://geth.ethereum.org), please checkout and raise pull requests against the `website` branch.
For more detailed instructions please see the `website` branch [README](https://github.com/ethereum/go-ethereum/tree/website#readme) or the 
[contributing](https://geth.ethereum.org/docs/developers/geth-developer/contributing) page of the website.

## License

The go-ethereum library (i.e. all code outside of the `cmd` directory) is licensed under the
[GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html),
also included in our repository in the `COPYING.LESSER` file.

The go-ethereum binaries (i.e. all code inside of the `cmd` directory) are licensed under the
[GNU General Public License v3.0](https://www.gnu.org/licenses/gpl-3.0.en.html), also
included in our repository in the `COPYING` file.
