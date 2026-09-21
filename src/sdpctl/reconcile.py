from __future__ import annotations

from pathlib import Path
import shutil

from .models import ObservationState, RunState
from .storage import (
    finish_observation_deletion,
    force_drop_observation_data,
    quarantine_path,
)
from .store import Store


def _record_observation_recovery_failure(store: Store, oid: int, exc: Exception, context: str) -> None:
    message = f"reconcile failure during {context}: {exc!r}"
    try:
        store.force_observation_fields(oid, error=message)
    finally:
        store.record_event("observation", oid, message)


def _record_run_recovery_failure(store: Store, rid: int, exc: Exception, context: str) -> None:
    store.record_event("run", rid, f"reconcile failure during {context}: {exc!r}")


async def startup_reconcile(store: Store, runner, data_root: Path) -> None:
    # This global safety gate remains intentionally fatal: if managed containers cannot
    # be confirmed absent, recovery must not touch files that could still have live writers.
    await runner.kill_managed()

    # After the writer-safety gate, recovery is entity-scoped. One wedged observation/run
    # is recorded and left degraded while the controller continues reconciling the rest.
    for obs in store.list_observations([ObservationState.OBSERVING]):
        try:
            p = Path(obs.ms_path)
            if p.exists():
                quarantine_path(data_root, p, f"obs-{obs.id:06d}-partial.ms")
            store.transition_observation(
                obs.id,
                ObservationState.OBSERVING,
                ObservationState.OBSERVE_FAILED,
                reason="restart found interrupted observation",
                reserved_bytes=0,
                error="interrupted by previous shutdown",
            )
        except Exception as exc:
            _record_observation_recovery_failure(store, obs.id, exc, "interrupted observation")

    # Interrupted derived products are regenerable, so partial output is removed.
    for run in store.list_runs([RunState.RUNNING]):
        try:
            out_dir = Path(run.output_prefix).parent
            if out_dir.exists():
                shutil.rmtree(out_dir)
            store.transition_run(
                run.id,
                RunState.RUNNING,
                RunState.FAILED,
                reason="restart found interrupted processing",
                exit_code=-1,
            )
        except Exception as exc:
            _record_run_recovery_failure(store, run.id, exc, "interrupted processing")

    # Defensive/legacy recovery: ACCEPTED is durable operator authorisation to delete.
    for obs in store.list_observations([ObservationState.STORED]):
        try:
            if store.has_accepted_run(obs.id) and not store.has_active_runs(obs.id):
                store.transition_observation(
                    obs.id,
                    ObservationState.STORED,
                    ObservationState.DELETING,
                    reason="reconcile resumed accepted QA deletion",
                )
        except Exception as exc:
            _record_observation_recovery_failure(store, obs.id, exc, "accepted deletion repair")

    # DELETING is the durable intent marker. Complete either side of the rename boundary.
    for obs in store.list_observations([ObservationState.DELETING]):
        try:
            finish_observation_deletion(store, data_root, obs.id)
        except Exception as exc:
            _record_observation_recovery_failure(store, obs.id, exc, "deletion completion")

    # STORED without the MS is not a legitimate deletion; it is an inconsistency.
    for obs in store.list_observations([ObservationState.STORED]):
        try:
            if not Path(obs.ms_path).exists():
                store.transition_observation(
                    obs.id,
                    ObservationState.STORED,
                    ObservationState.INCONSISTENT,
                    reason="database says STORED but MS is missing",
                    size_bytes=0,
                    reserved_bytes=0,
                    error="stored MS missing on restart",
                )
        except Exception as exc:
            _record_observation_recovery_failure(store, obs.id, exc, "stored-MS verification")

    # Unknown filesystem objects are quarantined, never destroyed or overwritten.
    known = {Path(o.ms_path).resolve() for o in store.list_observations()}
    obs_root = data_root / "observations"
    if obs_root.exists():
        for child in list(obs_root.iterdir()):
            if child.resolve() in known:
                continue
            try:
                quarantine_path(data_root, child, f"orphan-{child.name}")
            except Exception as exc:
                store.record_event("reconcile", 0, f"orphan {child}: {exc!r}")


def resolve_observation_drop(
    store: Store,
    data_root: Path,
    observation_id: int,
    note: str = "operator acknowledged recovery inconsistency and dropped raw data",
) -> None:
    """Explicit operator escape hatch for INCONSISTENT or wedged DELETING rows."""
    force_drop_observation_data(store, data_root, observation_id, reason=note)
