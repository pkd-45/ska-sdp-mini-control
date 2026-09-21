# SKAO SDP Mini Control System

A small, stateful controller for the SKAO second-stage interview assignment.

## Implemented core behaviour

- one observation at a time;
- parallel processing with a configurable cap;
- configurable automatic retry of failed processing runs, with persisted exhaustion and explicit human reprocess/discard recovery;
- reservation-based raw-visibility admission control;
- in-flight size watchdog with `abort` or `warn` policy;
- persisted SQLite state and append-only event timeline;
- separate Observation and ProcessingRun state machines;
- human QA: accept (safe raw deletion) or reprocess (new run attempt), plus explicit discard for an exhausted processing failure;
- FITS -> PNG preview generated best-effort for QA; a preview failure does not invalidate a good FITS product;
- crash-safe rename-before-remove deletion;
- QA accept plus `STORED -> DELETING` committed atomically;
- QA reprocess supersede plus replacement-run creation committed atomically;
- startup reconciliation: remove/await all managed containers first, quarantine partial raw data, delete partial derived products, resume accepted deletions, and mark unexplained missing raw data `INCONSISTENT`;
- quarantine accounting/list/purge and operator resolution for inconsistent or wedged-deletion observations;
- FakeRunner for fast tests and a configurable Docker/Podman-compatible ContainerRunner;
- GitLab CI and Makefile test entry points.

## Assumptions

1. The configured threshold is an admission budget for raw visibility storage.
2. A configured reservation is charged before an observation starts.
3. `actual <= reservation` is a separate completion property. `abort` enforces it by stopping an overrun; `warn` records the violation and allows completion.
4. Quarantine and trash still consume the same disk, so they are also charged against new-observation admission. Derived products are reported separately and do not count toward the logical raw-visibility threshold.
5. "Stop observing" means pause admission and resume when QA/deletion frees enough charged raw storage.
6. Unknown raw data is quarantined, never automatically destroyed or overwritten. Quarantine retention is manual/operator-controlled via `sdpctl quarantine list|purge`.
7. The supplied exercise re-runs the same deterministic processing configuration, so the default is `max_processing_attempts: 1`: a clean application failure goes directly to human action rather than spending time repeating the same WSClean failure. The mechanism remains configurable above 1 for deployments where failures can be transient infrastructure faults. Exhausted failures require an explicit operator reprocess or discard decision.
8. For a finite demo, `max_observations` is configurable.

## Quickstart (fake runner, Conda)

```bash
conda env create -f environment.yml
conda activate ska-sdpctl
make test
sdpctl --config config/default.yaml status
sdpctl --config config/default.yaml run
```

If the environment already exists, activate it with `conda activate ska-sdpctl` rather than creating another Python environment.

In another terminal:

```bash
sdpctl --config config/default.yaml qa list
# inspect the preview path if it exists; the FITS file remains authoritative
sdpctl --config config/default.yaml qa accept <RUN_ID>
# or
sdpctl --config config/default.yaml qa reprocess <RUN_ID>
# exhausted FAILED runs also support:
sdpctl --config config/default.yaml qa discard <RUN_ID>
```

## Real container runner

Set `runner: container` and `container_executable: docker` (or `podman`) in YAML. The command uses the supplied image and scripts, deterministic names, and `sdpctl.managed=true` labels. Reconciliation discovers containers in running, created, and exited states (`ps -aq`) and removes them before touching their files.

The supplied image has now been executed successfully with Docker Desktop on an Apple Silicon Mac. The host emitted an expected `linux/amd64` image versus `linux/arm64/v8` host warning and ran the image through emulation. The real probe verified successful visibility generation and WSClean processing, non-zero failure behaviour, the requirement to pre-create the output parent directory, and successful same-prefix reruns. See `docs/docker-probe-results.md` for the measured values and limitations.

For an end-to-end real-container demo, use `config/real.yaml`. It keeps the conservative `1300MiB` observation reservation, limits processing concurrency to two based on the local generation/processing timing ratio, and uses a small finite campaign for demonstration.

## Real-container demo

With Docker Desktop running:

```bash
conda activate ska-sdpctl
rm -rf workspace-real
sdpctl --config config/real.yaml run
```

In a second terminal:

```bash
conda activate ska-sdpctl
sdpctl --config config/real.yaml status
sdpctl --config config/real.yaml qa list
```

When a run reaches `AWAITING_QA`, inspect the generated `preview.png`, then either accept it or request reprocessing with the QA commands above.

## Storage accounting

`status` reports logical raw used/reserved bytes and tracked physical bytes in quarantine, trash, and products. Admission charges `logical used + logical reserved + quarantine + trash + next reservation` against the configured threshold. Managed raw bytes come from cached DB accounting; the admission hot path walks only quarantine/trash, caches those two sizes, and invalidates the cache on controller-owned mutations. The full observations/products trees are walked only for explicit status reporting. The cache is not a recursive filesystem watcher: out-of-process mutation inside an already-existing quarantine/trash entry can remain stale until an explicit invalidation/status snapshot/restart.

Long-campaign retry scans are also history-bounded: retry exhaustion is persisted on each run, exhausted failures are excluded by an indexed predicate, retry candidate filtering/execution counting happens in one SQL query, and initial-run repair uses one anti-join rather than one query per stored observation.

## Failure/recovery rules

- managed containers are removed and confirmed absent before recovery touches files;
- interrupted raw visibilities are quarantined because they are irreplaceable input;
- interrupted derived products are deleted because they are regenerable;
- a missing `STORED` MS becomes `INCONSISTENT`, never ordinary `DELETED`;
- `DELETING` is the durable deletion-intent marker and reconciliation completes it;
- legacy `STORED + ACCEPTED` crash states are detected and deletion is resumed;
- worker exceptions are converted to persisted `FAILED`/`OBSERVE_FAILED` states rather than leaving `RUNNING` records wedged;
- failed processing runs consume at most `max_processing_attempts`; exhaustion is persisted so terminal history leaves the scheduler hot path, and exhausted failures remain visible in `qa list` for operator reprocess/discard; the default is 1 for this deterministic exercise, while larger values are intended only for potentially transient infrastructure failures;
- after managed containers are confirmed absent, startup recovery is entity-scoped so one wedged record does not block recovery of the rest;
- reservations are released on observation terminal failure paths.

## Tests

The suite includes regressions for:

- guarded transitions and exact admission boundary arithmetic;
- storage-full pause then accept-and-resume;
- reprocessing without freeing raw data;
- accept precondition failure causing zero mutation;
- crash immediately after atomic QA-accept commit and restart recovery;
- legacy `STORED + ACCEPTED` recovery;
- quarantine bytes blocking admission;
- quarantine name collisions preserving both copies;
- preview failure remaining QA-able;
- processing exceptions following a bounded retry budget rather than respawning without limit;
- interrupted observation/run reconciliation;
- missing `STORED` data becoming `INCONSISTENT`;
- reservation overrun abort/quarantine;
- deterministic randomised QA/reconcile sequences with invariants checked after each action;
- exhausted processing failures remaining human-actionable and freeing storage after explicit discard;
- entity-scoped reconciliation continuing past a wedged deletion;
- admission walking only quarantine/trash and reusing its cache;
- randomised observation/processing failures with explicit safety *and liveness* assertions.

Run:

```bash
make test
```
