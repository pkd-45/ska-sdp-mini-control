# Adversarial review fixes

The review rounds documented here were AI-assisted. Claude produced the initial architecture proposal and later adversarial reviews; ChatGPT critiqued the design and implemented the controller and tests from the written specification and agreed design. I reproduced findings before accepting changes and made the final decisions. See [`development-process.md`](development-process.md) for the full workflow.

This revision incorporates failures reproduced against the first working implementation.

1. **QA validation before mutation:** `accept_run` checks run state, observation state, raw-data availability, and the no-other-active-run invariant before changing state. The store revalidates state inside the transaction.
2. **Atomic QA acceptance:** run `AWAITING_QA -> ACCEPTED` and observation `STORED -> DELETING` commit together. Reconciliation also repairs legacy `STORED + ACCEPTED` states.
3. **Physical backpressure:** quarantine and trash bytes now block new observation admission.
4. **Collision-safe quarantine:** quarantining never removes an existing item; `.001`, `.002`, ... suffixes preserve every copy.
5. **Worker exception recovery:** observation/processing exceptions become persisted failure states, orphaned in-process `RUNNING` states are swept, task exceptions are retrieved, and PNG preview generation is best-effort.
6. **Container cleanup:** reconciliation uses `ps -aq` and `rm -f`, then polls until managed containers are absent before touching files.

Additional fixes: NumPy is declared directly, matplotlib uses the Agg backend, SQLite read connections are explicitly closed, observation paths derive from the AUTOINCREMENT id rather than row count, `warn` overruns emit timeline events, reprocess supersede+replacement creation is atomic, the probe uses `python3`, and GitLab CI/Makefile entries are included.

## Second adversarial review

7. **Processing-failure liveness:** failed runs now auto-retry up to persisted `max_processing_attempts`; exhausted failures remain visible in `sdpctl qa list` and can be human-reprocessed or explicitly discarded. Automatic execution count is derived from the append-only event log, so retry budget survives restarts.
8. **Entity-scoped startup recovery:** after the mandatory global container-removal gate, each observation/run recovery is isolated. A wedged delete no longer prevents missing-MS checks or orphan quarantine for unrelated entities. Recovery errors are recorded and `resolve <obs-id> --drop` can explicitly clear wedged `DELETING` states.
9. **Admission hot-path cost:** `can_admit()` no longer calls the full physical snapshot. It uses DB-accounted managed raw bytes plus a cached walk of only quarantine/trash; controller mutations invalidate that cache.
10. **Safety plus liveness tests:** the randomised test now injects observation and processing failures and checks that every `STORED` observation retains a path to progress: runnable work, a human QA item, or a legacy accepted state reconciliation can finish.

Additional fixes: `python -m sdpctl.cli` now invokes `main`, the redundant timeout exception tuple is gone, and the real-container probe samples container memory during generation/processing to inform the processing-concurrency default.

## v4 long-campaign / policy pass

- Persisted `processing_runs.retry_exhausted` and indexed it so exhausted historical failures leave the per-tick retry scan permanently.
- Retry candidate filtering, execution counting, STORED-observation validation, and active-run exclusion are now performed in one SQL query rather than three round trips per historical failure.
- Initial-run repair now uses a single anti-join (`STORED` observations with no run), removing one query per stored observation from every tick.
- Removed the obsolete `resolve_inconsistent_drop`; `resolve_observation_drop` is the single operator recovery path.
- Set the shipped retry policy to one execution for this deterministic mock; values above one are documented as a policy for potentially transient infrastructure failures, not deterministic science-application failures.
- `qa list` reports cumulative processing executions across an observation's reprocess chain.
- Documented the external-only limitation of the quarantine/trash mtime cache.

Development-container microbenchmark (`make benchmark`, 50 warm samples per size) after the hot-path change:

- 20 exhausted failures: ~3.05 ms average tick
- 100: ~3.47 ms
- 300: ~3.59 ms
- 1000: ~3.83 ms

These are not performance guarantees; the benchmark is committed so the scaling shape can be reproduced on the interview machine.

## Real-container validation / Conda pass

- Replaced the README's local `venv` quickstart with a project `environment.yml` and Conda-first instructions.
- Executed the supplied `docker.io/pw410/ska-sdp-mock:0.1` image successfully on Apple Silicon Docker Desktop.
- Confirmed visibility generation and WSClean processing both return zero on valid input.
- Confirmed broken input returns 255.
- Confirmed WSClean fails with 255 when the output parent directory is absent; `ContainerRunner.process()` already creates that directory before launch.
- Confirmed a same-prefix rerun succeeds, while the controller intentionally still creates a new logical run directory for human-requested reprocessing so history remains auditable.
- Added `config/real.yaml` for a finite end-to-end real-container demo and `docs/docker-probe-results.md` with measured local timings, sizes, sampled memory, and the Apple-Silicon emulation caveat.
