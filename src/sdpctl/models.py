from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class ObservationState(StrEnum):
    PLANNED = "PLANNED"
    OBSERVING = "OBSERVING"
    STORED = "STORED"
    DELETING = "DELETING"
    DELETED = "DELETED"
    OBSERVE_FAILED = "OBSERVE_FAILED"
    INCONSISTENT = "INCONSISTENT"


class RunState(StrEnum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    AWAITING_QA = "AWAITING_QA"
    ACCEPTED = "ACCEPTED"
    SUPERSEDED = "SUPERSEDED"
    FAILED = "FAILED"
    DISCARDED = "DISCARDED"


OBS_TRANSITIONS: dict[ObservationState, set[ObservationState]] = {
    ObservationState.PLANNED: {ObservationState.OBSERVING, ObservationState.OBSERVE_FAILED},
    ObservationState.OBSERVING: {ObservationState.STORED, ObservationState.OBSERVE_FAILED},
    ObservationState.STORED: {ObservationState.DELETING, ObservationState.INCONSISTENT},
    ObservationState.DELETING: {ObservationState.DELETED, ObservationState.INCONSISTENT},
    ObservationState.OBSERVE_FAILED: set(),
    ObservationState.INCONSISTENT: {ObservationState.DELETED},
    ObservationState.DELETED: set(),
}

RUN_TRANSITIONS: dict[RunState, set[RunState]] = {
    RunState.QUEUED: {RunState.RUNNING, RunState.FAILED},
    RunState.RUNNING: {RunState.AWAITING_QA, RunState.FAILED},
    RunState.AWAITING_QA: {RunState.ACCEPTED, RunState.SUPERSEDED},
    RunState.FAILED: {RunState.QUEUED, RunState.SUPERSEDED, RunState.DISCARDED},
    RunState.ACCEPTED: set(),
    RunState.SUPERSEDED: set(),
    RunState.DISCARDED: set(),
}


@dataclass(frozen=True)
class Observation:
    id: int
    state: ObservationState
    ms_path: str
    size_bytes: int
    reserved_bytes: int
    error: str | None


@dataclass(frozen=True)
class ProcessingRun:
    id: int
    observation_id: int
    attempt: int
    state: RunState
    output_prefix: str
    log_path: str
    exit_code: int | None
    qa_verdict: str | None
    qa_note: str | None
    retry_exhausted: bool
