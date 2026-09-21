from __future__ import annotations

from pathlib import Path

from .models import ObservationState, ProcessingRun, RunState
from .storage import finish_observation_deletion
from .store import Store


def list_qa_items(store: Store) -> list[ProcessingRun]:
    """Every processing item currently intended for human action.

    FAILED runs stay with the automatic retry policy until their persisted exhaustion marker
    is set; only then are they surfaced alongside ordinary science-QA items.
    """
    return store.list_human_actionable_runs()


def accept_run(store: Store, data_root: Path, run_id: int, note: str = "") -> None:
    # Validate *all* externally visible preconditions before the first mutation.
    run = store.get_run(run_id)
    if run.state != RunState.AWAITING_QA:
        raise ValueError("run is not awaiting QA")
    obs = store.get_observation(run.observation_id)
    if obs.state != ObservationState.STORED:
        raise ValueError("observation is not STORED")
    if not Path(obs.ms_path).exists():
        raise ValueError("raw visibility is unavailable")
    if store.has_other_active_runs(obs.id, run.id):
        raise ValueError("observation has another non-terminal processing run")

    # The human verdict and the durable DELETING intent are one DB transaction.
    store.accept_run_and_begin_deletion(run.id, note=note)
    finish_observation_deletion(store, data_root, obs.id)


def reprocess_run(store: Store, data_root: Path, run_id: int, note: str = "") -> int:
    run = store.get_run(run_id)
    if run.state not in {RunState.AWAITING_QA, RunState.FAILED}:
        raise ValueError("run is neither awaiting QA nor FAILED")
    obs = store.get_observation(run.observation_id)
    if obs.state != ObservationState.STORED or not Path(obs.ms_path).exists():
        raise ValueError("raw visibility is unavailable")
    if store.has_other_active_runs(obs.id, run.id):
        raise ValueError("observation already has another non-terminal processing run")
    next_attempt = run.attempt + 1
    prefix = data_root / "products" / f"obs-{obs.id:06d}" / f"run-{next_attempt:03d}" / "product"
    log = data_root / "logs" / f"process-{obs.id:06d}-{next_attempt:03d}.log"
    return store.reprocess_run_and_create(run.id, str(prefix), str(log), note=note)


def discard_failed_run(store: Store, data_root: Path, run_id: int, note: str = "") -> None:
    """Explicit operator resolution when automatic processing attempts are exhausted."""
    run = store.get_run(run_id)
    if run.state != RunState.FAILED:
        raise ValueError("only FAILED runs can be discarded")
    obs = store.get_observation(run.observation_id)
    if obs.state != ObservationState.STORED:
        raise ValueError("observation is not STORED")
    if not Path(obs.ms_path).exists():
        raise ValueError("raw visibility is unavailable")
    if store.has_other_active_runs(obs.id, run.id):
        raise ValueError("observation already has another non-terminal processing run")

    oid = store.discard_failed_run_and_begin_deletion(run.id, note=note)
    finish_observation_deletion(store, data_root, oid)
