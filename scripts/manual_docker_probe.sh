#!/usr/bin/env bash
set -euo pipefail

RUNTIME="${RUNTIME:-docker}"
IMAGE="${IMAGE:-docker.io/pw410/ska-sdp-mock:0.1}"
ROOT="${1:-$PWD/docker-probe}"
DATA="$ROOT/data"
REPORT="$ROOT/report.txt"
mkdir -p "$DATA"
: > "$REPORT"

log(){ printf '%s\n' "$*" | tee -a "$REPORT"; }

run_timed(){
  local label="$1"; shift
  log "===== $label ====="
  local start end rc
  start=$(python3 - <<'PY'
import time; print(time.time())
PY
)
  if "$@" >>"$REPORT" 2>&1; then
    rc=0
  else
    rc=$?
  fi
  end=$(python3 - <<'PY'
import time; print(time.time())
PY
)
  python3 - "$start" "$end" "$rc" >>"$REPORT" <<'PY'
import sys
s,e,rc=float(sys.argv[1]),float(sys.argv[2]),int(sys.argv[3])
print(f"exit_code={rc}")
print(f"wall_seconds={e-s:.3f}")
PY
  return "$rc"
}

memory_to_bytes(){
  python3 - "$1" <<'PY'
import re, sys
s=sys.argv[1].strip()
m=re.fullmatch(r'([0-9.]+)([KMGTP]?i?B)', s, re.I)
if not m:
    print(0); raise SystemExit
n=float(m.group(1)); u=m.group(2).upper()
scale={
    'B':1,'KB':1000,'MB':1000**2,'GB':1000**3,'TB':1000**4,
    'KIB':1024,'MIB':1024**2,'GIB':1024**3,'TIB':1024**4,
}
print(int(n*scale.get(u, 1)))
PY
}

run_profiled_container(){
  local label="$1" name="$2"; shift 2
  log "===== $label ====="
  "$RUNTIME" rm -f "$name" >/dev/null 2>&1 || true
  local start end rc pid usage bytes peak=0
  start=$(python3 - <<'PY'
import time; print(time.time())
PY
)

  "$RUNTIME" run --name "$name" "$@" >>"$REPORT" 2>&1 &
  pid=$!

  while kill -0 "$pid" >/dev/null 2>&1; do
    usage=$($RUNTIME stats --no-stream --format '{{.MemUsage}}' "$name" 2>/dev/null | head -n1 | awk '{print $1}') || true
    if [[ -n "${usage:-}" ]]; then
      bytes=$(memory_to_bytes "$usage")
      if (( bytes > peak )); then peak=$bytes; fi
    fi
    sleep 0.2
  done

  if wait "$pid"; then
    rc=0
  else
    rc=$?
  fi
  end=$(python3 - <<'PY'
import time; print(time.time())
PY
)

  python3 - "$start" "$end" "$rc" "$peak" >>"$REPORT" <<'PY'
import sys
s,e,rc,peak=float(sys.argv[1]),float(sys.argv[2]),int(sys.argv[3]),int(sys.argv[4])
print(f"exit_code={rc}")
print(f"wall_seconds={e-s:.3f}")
print(f"peak_sampled_container_memory_bytes={peak}")
PY
  "$RUNTIME" rm -f "$name" >/dev/null 2>&1 || true
  return "$rc"
}

command -v "$RUNTIME" >/dev/null || { echo "$RUNTIME not found" >&2; exit 127; }
log "runtime=$($RUNTIME --version)"
log "image=$IMAGE"
log "root=$ROOT"
log "memory_sampling=container stats every 0.2s (sampled peak, not exact RSS high-water mark)"

rm -rf "$DATA/out.ms" "$DATA/out"* "$DATA/repeat"* "$DATA/broken"* "$DATA/no-parent"
run_profiled_container "generate visibilities" "sdpctl-probe-generate" \
  -v "$DATA:/data" "$IMAGE" \
  /scripts/generate_visibilities.sh /data/out.ms

log "===== visibility sizes ====="
python3 - "$DATA/out.ms" >>"$REPORT" <<'PY'
from pathlib import Path
import sys
p=Path(sys.argv[1])
print("apparent_bytes=", sum(x.stat().st_size for x in p.rglob('*') if x.is_file()), sep='')
PY
if command -v du >/dev/null; then
  du -sk "$DATA/out.ms" | awk '{print "du_kib=" $1}' >> "$REPORT"
fi

mkdir -p "$DATA/products/run-001"
run_profiled_container "process visibilities" "sdpctl-probe-process" \
  -v "$DATA:/data" "$IMAGE" \
  /scripts/process_visibilities.sh /data/out.ms /data/products/run-001/out

log "===== products ====="
find "$DATA/products/run-001" -maxdepth 1 -type f -print | sort | tee -a "$REPORT"

set +e
run_timed "broken input (expected failure)" \
  "$RUNTIME" run --rm -v "$DATA:/data" "$IMAGE" \
  /scripts/process_visibilities.sh /data/does-not-exist.ms /data/broken/out
BROKEN_RC=$?
set -e
log "broken_input_rc=$BROKEN_RC"

# Test whether parent output directory is required by intentionally omitting it.
rm -rf "$DATA/no-parent"
set +e
run_timed "processing without pre-created parent" \
  "$RUNTIME" run --rm -v "$DATA:/data" "$IMAGE" \
  /scripts/process_visibilities.sh /data/out.ms /data/no-parent/run/out
NO_PARENT_RC=$?
set -e
log "no_parent_rc=$NO_PARENT_RC"

# Test rerun semantics on the exact same prefix.
set +e
run_profiled_container "rerun same prefix" "sdpctl-probe-rerun" \
  -v "$DATA:/data" "$IMAGE" \
  /scripts/process_visibilities.sh /data/out.ms /data/products/run-001/out
RERUN_RC=$?
set -e
log "rerun_same_prefix_rc=$RERUN_RC"

log "===== summary ====="
log "Report written to $REPORT"
log "Inspect wall time, sampled peak container memory, exit codes, sizes, output-parent behaviour, and rerun behaviour above."
