# ADR-003: Reconcile desired and observed state conservatively

The controller is a small reconciler rather than a linear shell pipeline. State transitions are guarded updates.

On restart, all managed containers are discovered across running, created, and exited states and removed/awaited before files are touched. Failure of this global writer-safety gate is intentionally fatal: recovery must not mutate paths while a container may still own them.

After that gate, reconciliation is entity-scoped. A failure while recovering one observation or processing run is recorded against that entity and the remaining entities continue to reconcile. This lets the controller start degraded rather than repeatedly refusing to start because of one wedged record. `status` exposes observation recovery errors and the event timeline preserves the exception context.

Partial raw data is quarantined because it is telescope input; partial derived products are removed because they are regenerable. Missing stored data is `INCONSISTENT`, not `DELETED`. Unknown raw filesystem objects are moved to collision-safe quarantine and never overwritten.

QA acceptance and the observation's transition to `DELETING` are one SQLite transaction, eliminating the normal crash window between those states. Reconciliation still recognises legacy/defensive `STORED + ACCEPTED` states and resumes deletion. `DELETING` is the durable intent marker across the rename/remove boundary. If an impossible/ambiguous delete state such as simultaneous live and trash copies is encountered, automatic recovery records the error and continues; an operator may explicitly resolve it with `sdpctl resolve <obs-id> --drop`.
