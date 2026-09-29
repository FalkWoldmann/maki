# Compile time: smol/isahc vs tokio/reqwest

Numbers for [tontinton/maki#1100](https://github.com/tontinton/maki/issues/1100), measured on 2026-09-29. I compared current `main` against two branches: one where only the HTTP layer moves to reqwest (still on smol), and one where everything moves to tokio.

Short version: a clean dev build of the full tokio branch needs about a third less CPU than `main`, incremental rebuilds don't change, and the binary gets a bit smaller. Nearly all of that comes from no longer compiling curl and OpenSSL. Swapping smol for tokio on top of the HTTP change doesn't cost anything measurable in dev builds.

| vs `main` | clean dev (CPU) | clean dev (wall) | clean release (CPU) | incremental | stripped binary | crates |
|---|---|---|---|---|---|---|
| full tokio | -33% | -40% | -6% | same | -2.2 MB | 496 vs 500 |
| HTTP only | -33% | -39% | -11% | same | -2.4 MB | 512 vs 500 |

## Variants

| Name | Source | Runtime | HTTP / TLS |
|---|---|---|---|
| isahc-smol | `main` at `49fda0e5` | smol | isahc, libcurl and OpenSSL built from source |
| reqwest-smol | HTTP-only prototype | smol, plus a small tokio runtime just for HTTP | reqwest 0.13, rustls with ring |
| reqwest-tokio | `proto/tokio-full` | tokio, current_thread | reqwest 0.13, rustls with ring |

On `proto/tokio-full`, smol, async-io, async-process, async-lock, event-listener and futures-lite are gone from maki's code and from the macOS dependency graph. A small `maki-rt` crate owns a single current-thread tokio runtime on its own thread, which is the same shape smol's global executor had.

## How I measured

Each round builds every variant in turn. First a clean `cargo build` of the `maki` binary into an empty target dir, then `touch maki-providers/src/lib.rs && cargo build` as a typical edit and rebuild. After two rounds, each variant gets one clean `cargo build --release`.

Machine was an Apple M3 Pro with 12 cores on macOS, stable Rust 1.98.1, `RUSTFLAGS=""`, a separate target dir per variant and dependencies fetched beforehand. CPU is user plus sys time from `/usr/bin/time`, so it's the total amount of compile work. Wall is elapsed time. CPU is the more useful column here, because this machine was noisy (more on that below).

## Results

Clean dev build, round 1 / round 2:

| Variant | CPU (s) | Wall (s) | Crates |
|---|---|---|---|
| isahc-smol | 459.9 / 468.4 | 101.5 / 104.3 | 500 |
| reqwest-smol | 313.6 / 312.1 | 63.9 / 62.0 | 512 |
| reqwest-tokio | 313.2 / 311.2 | 62.2 / 60.4 | 496 |

Incremental rebuild after touching `maki-providers`, round 1 / round 2. All three recompile the same 6 crates, and the differences are noise.

| Variant | CPU (s) | Wall (s) |
|---|---|---|
| isahc-smol | 9.9 / 9.8 | 10.2 / 9.1 |
| reqwest-smol | 9.4 / 9.3 | 9.2 / 8.8 |
| reqwest-tokio | 9.7 / 9.5 | 9.9 / 9.0 |

Clean release build, one run each:

| Variant | CPU (s) | Wall (s) | Binary (MB) | Stripped (MB) |
|---|---|---|---|---|
| isahc-smol | 1084.3 | 162.0 | 98.2 | 87.0 |
| reqwest-smol | 964.1 | 122.1 | 96.0 | 84.6 |
| reqwest-tokio | 1019.2 | 135.0 | 96.3 | 84.8 |

## Where the time goes

An earlier run with `cargo build --timings` on `main` and the HTTP-only branch shows it. Dropping isahc removes `openssl-sys` (85 s of compile time when OpenSSL is vendored), `curl-sys`, `libnghttp2-sys`, `libz-sys` and `isahc` itself. That's all C that gets compiled from scratch on every clean build. reqwest with ring adds back `ring` (11 s), `tokio` (7 s), `rustls` (5 s), and 2 to 3 s each for `h2`, `hyper` and `reqwest`, plus a tail of small crates. Removing smol on top takes out smol, async-io, polling, blocking, async-executor, async-process, async-lock, async-channel, event-listener, futures-lite, piper and a few others. They're small enough that you barely see them in build time, but they bring the crate count from 512 down to 496.

The TLS crypto library matters more than the async runtime. reqwest 0.13 defaults to aws-lc-rs, and `aws-lc-sys` alone is a 77 s C build that eats most of the gain. Both branches use this instead:

```toml
reqwest = { version = "0.13", default-features = false, features = ["http2", "rustls-no-provider"] }
rustls  = { version = "0.23", default-features = false, features = ["std", "tls12"] }  # "ring" is enabled in maki-http
tokio   = { version = "1", default-features = false, features = ["rt", "time", "sync", "macros", "process", "net", "fs", "io-util", "io-std"] }
```

## Things to keep in mind

The Mac I measured on runs Microsoft Defender with real-time scanning, which I measured separately at roughly 79% extra CPU on a clean build. So the absolute numbers are inflated and wall times drift between sessions. `main`'s clean build took 101 to 104 s in this run and 135 to 161 s in an earlier one. Within a run the variants take turns, so they all see the same load, and the comparison holds. I'm rerunning it on Linux to get clean absolute numbers.

There are only two rounds for dev builds and a single run for release. The 6% release gap between the two reqwest variants could be noise. It could also be real, for example tokio's extra features (process, fs, net, macros) getting compiled and monomorphised in release. More runs would tell.

On Linux, `keyring`'s default secret-service store depends on `zbus` with default features, which pulls in async-io. So part of the smol stack stays in the Linux build, and maki can't change that without picking a different Linux credential store.

The runtime is current_thread on purpose, to keep the migration simple. Switching to the multi-threaded runtime would add `rt-multi-thread` to the build.

## Reproducing it

The script expects three checkouts: `main`, the HTTP-only prototype and `proto/tokio-full`. It fetches dependencies for each (not timed), then runs the builds and writes `results3.csv` plus a log per build into `BENCH_DIR`.

```sh
MAIN_SRC=~/code/maki HTTP_SRC=~/code/maki-reqwest TOKIO_SRC=~/code/maki-tokio \
  BENCH_DIR=~/maki-bench ./compile-benchmark.sh
```

It needs rustup with the stable toolchain, a C/C++ compiler with `make` and `perl` (for the vendored OpenSSL on `main`), `bc`, and GNU time at `/usr/bin/time`. On Debian or Ubuntu that's `sudo apt install build-essential perl bc time`. Don't run it inside `nix develop`, because the dev shell sets `OPENSSL_NO_VENDOR` and that changes the `main` variant. `ROUNDS=3` gives a third round. On a 12-core machine the whole thing takes 15 to 20 minutes.

## State of the branch

`cargo check --workspace --tests`, clippy and `cargo fmt` are clean, apart from warnings that `main` has too. The full test suite had 6,533 passes and 20 failures, and all 20 also fail on `main` or are flaky: some `maki-ui` tests that depend on test order (same result on `main` when run single-threaded), `file_index` timing tests, provider catalog tests that share state, and one Lua policy test that passed three reruns. The dev binary also works end to end. It renders the system prompt and gets through an ACP `initialize`, `session/new` and `session/prompt`, where the prompt does a real HTTP request that fails cleanly with "Authentication required" because of the dummy API key.
