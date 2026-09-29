#!/usr/bin/env bash
# main vs HTTP-only reqwest vs full tokio: clean dev (+ incremental) builds,
# variants taking turns, then one clean release build each.
set -uo pipefail

S="${BENCH_DIR:-$(mktemp -d)}"
ROUNDS="${ROUNDS:-2}"
OUT="$S/results3.csv"
# Checkouts: main, the HTTP-only prototype, and proto/tokio-full.
VARIANTS=(
  "isahc-smol|${MAIN_SRC:?set MAIN_SRC to a checkout of main}"
  "reqwest-smol|${HTTP_SRC:?set HTTP_SRC to a checkout of the HTTP-only prototype}"
  "reqwest-tokio|${TOKIO_SRC:?set TOKIO_SRC to a checkout of proto/tokio-full}"
)

echo "variant,round,kind,wall_s,cpu_s,units" > "$OUT"
mkdir -p "$S/logs3"

run() {
  local name=$1 src=$2 round=$3 kind=$4; shift 4
  local dir="$S/t3-$name" log="$S/logs3/$name-$round-$kind.log" start end
  start=$(perl -MTime::HiRes=time -e 'printf "%.3f", time')
  (cd "$src" && env RUSTFLAGS="" CARGO_TARGET_DIR="$dir" CARGO_BUILD_BUILD_DIR="$dir" \
    /usr/bin/time -p cargo +stable "$@" --offline) >"$log" 2>&1
  local status=$?
  end=$(perl -MTime::HiRes=time -e 'printf "%.3f", time')
  local cpu units wall
  cpu=$(awk '/^user/{u=$2} /^sys/{s=$2} END{printf "%.1f", u+s}' "$log")
  units=$(grep -c '^ *Compiling ' "$log")
  wall=$(echo "$end - $start" | bc)
  [[ $status -ne 0 ]] && wall=NA
  echo "$name,$round,$kind,$wall,$cpu,$units" >> "$OUT"
  echo "$(date +%H:%M:%S) $name r$round $kind wall=${wall}s cpu=${cpu}s units=$units status=$status"
}

for round in $(seq 1 "$ROUNDS"); do
  for v in "${VARIANTS[@]}"; do
    IFS='|' read -r name src <<< "$v"
    rm -rf "$S/t3-$name"
    run "$name" "$src" "$round" clean-dev build
    touch "$src/maki-providers/src/lib.rs"
    run "$name" "$src" "$round" incr-providers build
  done
done

for v in "${VARIANTS[@]}"; do
  IFS='|' read -r name src <<< "$v"
  run "$name" "$src" 1 clean-release build --release
  echo "$name,1,release-bin-bytes,$(wc -c < "$S/t3-$name/release/maki" | tr -d ' '),," >> "$OUT"
done
echo "done: $OUT"
