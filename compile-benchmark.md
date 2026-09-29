# Compile time: smol/isahc vs tokio/reqwest

Measurements for [tontinton/maki#1100](https://github.com/tontinton/maki/issues/1100), 2026-09-29.
They compare today's `main` with a full move to tokio, plus the halfway step
where only the HTTP layer moves.

## TL;DR

Moving maki completely from smol + isahc to tokio + reqwest makes clean dev
builds **about a third cheaper** and leaves incremental rebuilds unchanged. The
HTTP switch is responsible for all of the gain: going from "HTTP only" to "full
tokio" costs nothing in dev builds.

| vs. `main` | clean dev build (CPU) | clean dev build (wall) | clean release build (CPU) | incremental rebuild | stripped binary | crates |
|---|---|---|---|---|---|---|
| **full tokio** | **−33%** | **−40%** | **−6%** | same | **−2.2 MB** | 496 vs 500 |
| HTTP layer only | −33% | −39% | −11% | same | −2.4 MB | 512 vs 500 |

The one open question is release builds: full tokio came out 6% above the
HTTP-only variant in a single run each (see [Caveats](#caveats)).

## What was measured

| Variant | Branch | Async runtime | HTTP / TLS |
|---|---|---|---|
| `isahc-smol` | `main` @ `49fda0e5` | smol | isahc → libcurl (C) + OpenSSL built from source |
| `reqwest-smol` | HTTP-only prototype | smol, plus a private tokio runtime for HTTP | reqwest 0.13 + rustls/ring |
| `reqwest-tokio` | `proto/tokio-full` @ `ed9ce990` | tokio, `current_thread` | reqwest 0.13 + rustls/ring |

`proto/tokio-full` removes smol, async-io, async-process, async-lock,
event-listener and futures-lite from maki's own code and from the macOS
dependency graph. A small `maki-rt` crate owns one current-thread tokio runtime
on its own thread, the same shape smol's global executor had.

Each round runs every variant in turn:

- **clean-dev**: `cargo build` into an empty target dir (the `maki` binary, dev profile)
- **incr-providers**: `touch maki-providers/src/lib.rs && cargo build`, a
  typical edit-rebuild cycle that recompiles `maki-providers` and everything
  that depends on it, then relinks

Two rounds of that, then one **clean-release** build (`cargo build --release`)
per variant.

Setup: Apple M3 Pro (12 cores), macOS, stable Rust 1.98.1 via `cargo +stable`,
`RUSTFLAGS=""`, a separate target dir per variant, dependencies pre-fetched
(`--offline`). **CPU** is user + sys time from `/usr/bin/time`, the total
compile work, and the steadier number on a noisy machine. **Wall** is elapsed
time.

## Results

Each cell shows both rounds (`round 1 / round 2`).

### Clean dev build

| Variant | CPU (s) | Wall (s) | Crates |
|---|---|---|---|
| isahc-smol | 459.9 / 468.4 | 101.5 / 104.3 | 500 |
| reqwest-smol | 313.6 / 312.1 | 63.9 / 62.0 | 512 |
| **reqwest-tokio** | **313.2 / 311.2** | **62.2 / 60.4** | **496** |

### Incremental rebuild (touch `maki-providers`)

| Variant | CPU (s) | Wall (s) |
|---|---|---|
| isahc-smol | 9.9 / 9.8 | 10.2 / 9.1 |
| reqwest-smol | 9.4 / 9.3 | 9.2 / 8.8 |
| reqwest-tokio | 9.7 / 9.5 | 9.9 / 9.0 |

All three recompile the same 6 crates. The differences are within noise.

### Clean release build (1 run each)

| Variant | CPU (s) | Wall (s) | Binary (MB) | Stripped (MB) |
|---|---|---|---|---|
| isahc-smol | 1084.3 | 162.0 | 98.2 | 87.0 |
| reqwest-smol | 964.1 | 122.1 | 96.0 | 84.6 |
| **reqwest-tokio** | **1019.2** | **135.0** | **96.3** | **84.8** |

## Where the difference comes from

An earlier run with `cargo build --timings` (on `main` and the HTTP-only
prototype) shows where the time goes:

- **Removed with isahc:** `openssl-sys` (85 s of compile time when OpenSSL is
  vendored), `curl-sys`, `libnghttp2-sys`, `libz-sys`, `isahc`. That is C code
  compiled from source on every clean build.
- **Added with reqwest + ring:** `ring` (11 s), `tokio` (7 s), `rustls` (5 s),
  `h2`, `hyper`, `reqwest` (2–3 s each) and a tail of small crates.
- **Removed with smol (full tokio only):** smol, async-io, polling, blocking,
  async-executor, async-process, async-lock, async-channel, event-listener,
  futures-lite, piper and a few more. They are small, so they barely register in
  build time, but they take the crate count from 512 down to 496.

The TLS crypto library matters more than the async runtime. reqwest 0.13's
default provider, aws-lc-rs, adds a 77 s C build (`aws-lc-sys`) that cancels
most of the gain. Both branches use this feature set instead:

```toml
reqwest = { version = "0.13", default-features = false, features = ["http2", "rustls-no-provider"] }
rustls  = { version = "0.23", default-features = false, features = ["std", "tls12"] }  # + "ring" in maki-http
tokio   = { version = "1", default-features = false, features = ["rt", "time", "sync", "macros", "process", "net", "fs", "io-util", "io-std"] }
```

## Caveats

- **Noisy machine.** Microsoft Defender's real-time scanning runs on this Mac
  and was measured separately at about +79% CPU on top of a clean build.
  Absolute times are inflated and wall times vary between sessions: `main`'s
  clean build took 101–104 s here and 135–161 s in an earlier run. Variants take
  turns within a run, so each comparison is fair, and CPU time is the more
  reliable column. Quote numbers from a quiet Linux machine or CI.
- **Few samples.** 2 rounds for dev builds, **1 for release**. The ±6% release
  gap between the two reqwest variants is within what a single run can show. It
  could be real, for example from tokio's extra features (process, fs, net,
  macros) being compiled and monomorphised in release. It needs more rounds to
  tell.
- **macOS only.** On Linux, `keyring`'s default secret-service store depends on
  `zbus` with its default async-io runtime, so part of the smol stack stays in
  the Linux build. maki can't change that from its side without choosing a
  different Linux credential store.
- **`current_thread` only.** The runtime is single-threaded on purpose to keep
  the migration simple. A multi-threaded runtime would add `rt-multi-thread` to
  the build.

## Reproduce

```sh
MAIN_SRC=~/code/maki HTTP_SRC=~/code/maki-reqwest TOKIO_SRC=~/code/maki-tokio \
  BENCH_DIR=/tmp/maki-bench ./compile-benchmark.sh      # ROUNDS=2 by default
```

Needs `perl`, `bc` and `/usr/bin/time` (on Debian/Ubuntu, install the `time`
package). Results go to `$BENCH_DIR/results3.csv`, build logs to
`$BENCH_DIR/logs3/`. Expect about 15–20 minutes on a 12-core machine.

## Branch status

`proto/tokio-full` (pushed to `FalkWoldmann/maki`):

- `cargo check --workspace --tests` and clippy are clean apart from warnings
  `main` has too, and `cargo fmt` is clean.
- Full test suite: 6,533 passed, 20 failed. All 20 fail on `main` too or are
  intermittent: `maki-ui` theme-order tests (identical results on `main` when
  run single-threaded), `file_index` timing tests, provider-catalog shared-state
  tests, and one Lua policy test that passed 3 of 3 reruns.
- The dev-profile binary starts, renders the system prompt, and completes an ACP
  `initialize` → `session/new` → `session/prompt` exchange. The prompt makes a
  real HTTP request, which the dummy API key turns into a clean "Authentication
  required".
