# ADR-001: Separate observation and processing-run state

An observation owns the raw visibility bytes. A processing run owns one logical attempt to derive products. Reprocessing therefore creates a new `ProcessingRun` and never overwrites the prior logical attempt. Automatic retries of the same failed run reuse its output prefix only after partial derived products are removed; the persistent event log counts each `QUEUED -> RUNNING` execution.

Normal science QA has two outcomes: `AWAITING_QA -> ACCEPTED` (delete raw input) or `AWAITING_QA -> SUPERSEDED` plus a replacement run. The exercise explicitly says reprocessing uses the same configuration and therefore gives the same result, so a clean deterministic application failure should not be retried blindly: the shipped configuration uses `max_processing_attempts: 1`. The retry mechanism remains configurable above 1 for environments where the same command can fail for transient infrastructure reasons (container/runtime/host faults). Once a run reaches its configured budget, `retry_exhausted` is persisted and the `FAILED` run becomes human-actionable: it can be superseded by a fresh reprocess run or explicitly `DISCARDED`.

A human reprocess creates a fresh logical run and therefore a fresh per-run budget. To keep that cost visible, `qa list` reports both executions of the current run and cumulative executions across the full observation run chain.

The deletion invariant is therefore: raw data is removed only after an explicit human storage-release decision, represented by either an `ACCEPTED` science product or a `DISCARDED` exhausted failure.

## Empirical validation note

The assignment's statement that the same reprocessing configuration gives the
same result is treated as a workflow simplification, not as a byte-identity
guarantee. Real WSClean validation produced numerically equivalent but not
bitwise-identical FITS files across two attempts. See
[`../docker-probe-results.md`](../docker-probe-results.md) for the measured
differences. Preserving each `ProcessingRun` independently therefore remains
the correct audit model.
