from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import sqlite3
from typing import Iterable

from .models import (
    OBS_TRANSITIONS,
    RUN_TRANSITIONS,
    Observation,
    ObservationState,
    ProcessingRun,
    RunState,
)


class TransitionError(RuntimeError):
    pass


_ACTIVE_RUN_STATES = (RunState.QUEUED, RunState.RUNNING, RunState.AWAITING_QA)


class Store:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA foreign_keys=ON")
        return con

    @contextmanager
    def connection(self):
        con = self._connect()
        try:
            yield con
        finally:
            con.close()

    @contextmanager
    def tx(self):
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            yield con
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def _init(self) -> None:
        with self.connection() as con:
            con.executescript(
                """
                CREATE TABLE IF NOT EXISTS observations (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  state TEXT NOT NULL,
                  ms_path TEXT NOT NULL,
                  size_bytes INTEGER NOT NULL DEFAULT 0,
                  reserved_bytes INTEGER NOT NULL DEFAULT 0,
                  error TEXT,
                  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS processing_runs (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  observation_id INTEGER NOT NULL REFERENCES observations(id),
                  attempt INTEGER NOT NULL,
                  state TEXT NOT NULL,
                  output_prefix TEXT NOT NULL,
                  log_path TEXT NOT NULL,
                  exit_code INTEGER,
                  qa_verdict TEXT,
                  qa_note TEXT,
                  retry_exhausted INTEGER NOT NULL DEFAULT 0,
                  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  UNIQUE(observation_id, attempt)
                );
                CREATE TABLE IF NOT EXISTS events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  entity_type TEXT NOT NULL,
                  entity_id INTEGER NOT NULL,
                  at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  from_state TEXT,
                  to_state TEXT,
                  reason TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_observations_state
                  ON observations(state);
                CREATE INDEX IF NOT EXISTS idx_runs_observation
                  ON processing_runs(observation_id);
                CREATE INDEX IF NOT EXISTS idx_events_run_transition
                  ON events(entity_type, entity_id, from_state, to_state);
                """
            )
            # Small forward migration for repositories/databases created by v3.
            cols = {row["name"] for row in con.execute("PRAGMA table_info(processing_runs)").fetchall()}
            if "retry_exhausted" not in cols:
                con.execute(
                    "ALTER TABLE processing_runs ADD COLUMN retry_exhausted INTEGER NOT NULL DEFAULT 0"
                )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_runs_state_exhausted "
                "ON processing_runs(state, retry_exhausted)"
            )

    def _insert_observation(self, con: sqlite3.Connection, ms_path: str, reserved_bytes: int) -> int:
        cur = con.execute(
            "INSERT INTO observations(state, ms_path, reserved_bytes) VALUES (?, ?, ?)",
            (ObservationState.PLANNED.value, ms_path, reserved_bytes),
        )
        oid = int(cur.lastrowid)
        con.execute(
            "INSERT INTO events(entity_type, entity_id, to_state, reason) VALUES ('observation', ?, ?, 'created')",
            (oid, ObservationState.PLANNED.value),
        )
        return oid

    def create_observation(self, ms_path: str, reserved_bytes: int = 0) -> int:
        with self.tx() as con:
            return self._insert_observation(con, ms_path, reserved_bytes)

    def create_observation_auto(self, observations_root: str | Path, reserved_bytes: int = 0) -> int:
        """Create an observation whose directory name derives from its AUTOINCREMENT id."""
        root = Path(observations_root)
        with self.tx() as con:
            oid = self._insert_observation(con, "__pending__", reserved_bytes)
            ms_path = str(root / f"obs-{oid:06d}.ms")
            con.execute("UPDATE observations SET ms_path=? WHERE id=?", (ms_path, oid))
            return oid

    def get_observation(self, oid: int) -> Observation:
        with self.connection() as con:
            row = con.execute("SELECT * FROM observations WHERE id=?", (oid,)).fetchone()
        if not row:
            raise KeyError(oid)
        return self._obs(row)

    def list_observations(self, states: Iterable[ObservationState] | None = None) -> list[Observation]:
        with self.connection() as con:
            if states:
                vals = [s.value for s in states]
                q = f"SELECT * FROM observations WHERE state IN ({','.join('?' for _ in vals)}) ORDER BY id"
                rows = con.execute(q, vals).fetchall()
            else:
                rows = con.execute("SELECT * FROM observations ORDER BY id").fetchall()
        return [self._obs(r) for r in rows]

    def transition_observation(
        self,
        oid: int,
        expected: ObservationState,
        new: ObservationState,
        *,
        reason: str = "",
        size_bytes: int | None = None,
        reserved_bytes: int | None = None,
        error: str | None = None,
    ) -> None:
        if new not in OBS_TRANSITIONS[expected]:
            raise TransitionError(f"illegal observation transition {expected}->{new}")
        sets = ["state=?", "updated_at=CURRENT_TIMESTAMP"]
        params: list[object] = [new.value]
        if size_bytes is not None:
            sets.append("size_bytes=?")
            params.append(size_bytes)
        if reserved_bytes is not None:
            sets.append("reserved_bytes=?")
            params.append(reserved_bytes)
        if error is not None:
            sets.append("error=?")
            params.append(error)
        params += [oid, expected.value]
        with self.tx() as con:
            cur = con.execute(
                f"UPDATE observations SET {', '.join(sets)} WHERE id=? AND state=?",
                params,
            )
            if cur.rowcount != 1:
                raise TransitionError(f"guard failed for observation {oid}: expected {expected}")
            con.execute(
                "INSERT INTO events(entity_type, entity_id, from_state, to_state, reason) VALUES ('observation',?,?,?,?)",
                (oid, expected.value, new.value, reason),
            )

    def force_observation_fields(
        self,
        oid: int,
        *,
        reserved_bytes: int | None = None,
        error: str | None = None,
    ) -> None:
        sets: list[str] = []
        params: list[object] = []
        if reserved_bytes is not None:
            sets.append("reserved_bytes=?")
            params.append(reserved_bytes)
        if error is not None:
            sets.append("error=?")
            params.append(error)
        if not sets:
            return
        params.append(oid)
        with self.tx() as con:
            con.execute(
                f"UPDATE observations SET {', '.join(sets)}, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                params,
            )

    def create_run(self, observation_id: int, output_prefix: str, log_path: str) -> int:
        with self.tx() as con:
            attempt = con.execute(
                "SELECT COALESCE(MAX(attempt),0)+1 FROM processing_runs WHERE observation_id=?",
                (observation_id,),
            ).fetchone()[0]
            cur = con.execute(
                "INSERT INTO processing_runs(observation_id, attempt, state, output_prefix, log_path) VALUES (?,?,?,?,?)",
                (observation_id, attempt, RunState.QUEUED.value, output_prefix, log_path),
            )
            rid = int(cur.lastrowid)
            con.execute(
                "INSERT INTO events(entity_type, entity_id, to_state, reason) VALUES ('run', ?, ?, 'created')",
                (rid, RunState.QUEUED.value),
            )
            return rid

    def get_run(self, rid: int) -> ProcessingRun:
        with self.connection() as con:
            row = con.execute("SELECT * FROM processing_runs WHERE id=?", (rid,)).fetchone()
        if not row:
            raise KeyError(rid)
        return self._run(row)

    def list_runs(
        self,
        states: Iterable[RunState] | None = None,
        observation_id: int | None = None,
    ) -> list[ProcessingRun]:
        clauses: list[str] = []
        params: list[object] = []
        if states:
            vals = [s.value for s in states]
            clauses.append(f"state IN ({','.join('?' for _ in vals)})")
            params.extend(vals)
        if observation_id is not None:
            clauses.append("observation_id=?")
            params.append(observation_id)
        q = "SELECT * FROM processing_runs"
        if clauses:
            q += " WHERE " + " AND ".join(clauses)
        q += " ORDER BY id"
        with self.connection() as con:
            rows = con.execute(q, params).fetchall()
        return [self._run(r) for r in rows]

    def list_human_actionable_runs(self) -> list[ProcessingRun]:
        """Return QA-ready products plus failures whose automatic retry budget is exhausted."""
        with self.connection() as con:
            rows = con.execute(
                "SELECT * FROM processing_runs "
                "WHERE state=? OR (state=? AND retry_exhausted=1) ORDER BY id",
                (RunState.AWAITING_QA.value, RunState.FAILED.value),
            ).fetchall()
        return [self._run(row) for row in rows]

    def transition_run(
        self,
        rid: int,
        expected: RunState,
        new: RunState,
        *,
        reason: str = "",
        exit_code: int | None = None,
        qa_verdict: str | None = None,
        qa_note: str | None = None,
    ) -> None:
        if new not in RUN_TRANSITIONS[expected]:
            raise TransitionError(f"illegal run transition {expected}->{new}")
        sets = ["state=?", "updated_at=CURRENT_TIMESTAMP"]
        params: list[object] = [new.value]
        if exit_code is not None:
            sets.append("exit_code=?")
            params.append(exit_code)
        if qa_verdict is not None:
            sets.append("qa_verdict=?")
            params.append(qa_verdict)
        if qa_note is not None:
            sets.append("qa_note=?")
            params.append(qa_note)
        params += [rid, expected.value]
        with self.tx() as con:
            cur = con.execute(
                f"UPDATE processing_runs SET {', '.join(sets)} WHERE id=? AND state=?",
                params,
            )
            if cur.rowcount != 1:
                raise TransitionError(f"guard failed for run {rid}: expected {expected}")
            con.execute(
                "INSERT INTO events(entity_type, entity_id, from_state, to_state, reason) VALUES ('run',?,?,?,?)",
                (rid, expected.value, new.value, reason),
            )

    def record_event(self, entity_type: str, entity_id: int, reason: str) -> None:
        with self.tx() as con:
            con.execute(
                "INSERT INTO events(entity_type, entity_id, reason) VALUES (?, ?, ?)",
                (entity_type, entity_id, reason),
            )

    def has_other_active_runs(self, observation_id: int, exclude_run_id: int) -> bool:
        vals = [s.value for s in _ACTIVE_RUN_STATES]
        with self.connection() as con:
            row = con.execute(
                f"SELECT 1 FROM processing_runs WHERE observation_id=? AND id<>? "
                f"AND state IN ({','.join('?' for _ in vals)}) LIMIT 1",
                [observation_id, exclude_run_id, *vals],
            ).fetchone()
        return row is not None

    def has_active_runs(self, observation_id: int) -> bool:
        vals = [s.value for s in _ACTIVE_RUN_STATES]
        with self.connection() as con:
            row = con.execute(
                f"SELECT 1 FROM processing_runs WHERE observation_id=? "
                f"AND state IN ({','.join('?' for _ in vals)}) LIMIT 1",
                [observation_id, *vals],
            ).fetchone()
        return row is not None

    def has_accepted_run(self, observation_id: int) -> bool:
        with self.connection() as con:
            row = con.execute(
                "SELECT 1 FROM processing_runs WHERE observation_id=? AND state=? LIMIT 1",
                (observation_id, RunState.ACCEPTED.value),
            ).fetchone()
        return row is not None

    def run_execution_attempts(self, rid: int) -> int:
        """Number of times this run actually entered RUNNING, persisted via events."""
        with self.connection() as con:
            row = con.execute(
                "SELECT COUNT(*) FROM events WHERE entity_type='run' AND entity_id=? "
                "AND from_state=? AND to_state=?",
                (rid, RunState.QUEUED.value, RunState.RUNNING.value),
            ).fetchone()
        return int(row[0])

    def observation_execution_attempts(self, observation_id: int) -> int:
        """Total processing executions across every logical run for one observation."""
        with self.connection() as con:
            row = con.execute(
                "SELECT COUNT(*) FROM events e "
                "JOIN processing_runs r ON r.id=e.entity_id "
                "WHERE e.entity_type='run' AND r.observation_id=? "
                "AND e.from_state=? AND e.to_state=?",
                (observation_id, RunState.QUEUED.value, RunState.RUNNING.value),
            ).fetchone()
        return int(row[0])

    def mark_exhausted_failed_runs(self, max_attempts: int) -> list[int]:
        """Persist retry exhaustion once so terminal failures leave the scheduler hot path.

        The correlated count is evaluated only for FAILED rows whose marker is still zero.
        Once marked, an exhausted run is excluded by the indexed state/marker predicate on
        every later tick. Returns newly-marked run ids.
        """
        with self.tx() as con:
            rows = con.execute(
                """
                UPDATE processing_runs
                   SET retry_exhausted=1, updated_at=CURRENT_TIMESTAMP
                 WHERE state=? AND retry_exhausted=0
                   AND (
                     SELECT COUNT(*) FROM events e
                      WHERE e.entity_type='run'
                        AND e.entity_id=processing_runs.id
                        AND e.from_state=? AND e.to_state=?
                   ) >= ?
                RETURNING id
                """,
                (RunState.FAILED.value, RunState.QUEUED.value, RunState.RUNNING.value, max_attempts),
            ).fetchall()
            ids = [int(row[0]) for row in rows]
            for rid in ids:
                con.execute(
                    "INSERT INTO events(entity_type, entity_id, reason) VALUES ('run', ?, ?)",
                    (rid, f"automatic retry budget exhausted at {max_attempts} execution(s)"),
                )
            return ids

    def mark_run_retry_exhausted(self, rid: int, *, reason: str) -> None:
        """Make a FAILED run operator-actionable when automatic retry cannot proceed."""
        with self.tx() as con:
            cur = con.execute(
                "UPDATE processing_runs SET retry_exhausted=1, updated_at=CURRENT_TIMESTAMP "
                "WHERE id=? AND state=? AND retry_exhausted=0",
                (rid, RunState.FAILED.value),
            )
            if cur.rowcount:
                con.execute(
                    "INSERT INTO events(entity_type, entity_id, reason) VALUES ('run', ?, ?)",
                    (rid, reason),
                )

    def list_retryable_failed_runs(self, max_attempts: int) -> list[tuple[ProcessingRun, int]]:
        """Fetch only failures that can actually consume another automatic execution.

        Filtering, execution counting, observation-state validation, and the other-active-run
        check are all done in one SQL statement. Exhausted rows have a persisted marker and
        therefore do not accumulate work in the scheduler as campaign history grows.
        """
        active = [s.value for s in _ACTIVE_RUN_STATES]
        placeholders = ','.join('?' for _ in active)
        q = f"""
            SELECT * FROM (
                SELECT r.*,
                       (
                         SELECT COUNT(*) FROM events e
                          WHERE e.entity_type='run'
                            AND e.entity_id=r.id
                            AND e.from_state=? AND e.to_state=?
                       ) AS executions
                  FROM processing_runs r
                  JOIN observations o ON o.id=r.observation_id
                 WHERE r.state=?
                   AND r.retry_exhausted=0
                   AND o.state=?
                   AND NOT EXISTS (
                       SELECT 1 FROM processing_runs other
                        WHERE other.observation_id=r.observation_id
                          AND other.id<>r.id
                          AND other.state IN ({placeholders})
                   )
            ) retryable
            WHERE executions < ?
            ORDER BY id
        """
        params = [
            RunState.QUEUED.value, RunState.RUNNING.value,
            RunState.FAILED.value, ObservationState.STORED.value,
            *active, max_attempts,
        ]
        with self.connection() as con:
            rows = con.execute(q, params).fetchall()
        return [(self._run(row), int(row["executions"])) for row in rows]

    def list_stored_observations_without_runs(self) -> list[Observation]:
        """One-query repair scan for STORED observations that never got their initial run."""
        with self.connection() as con:
            rows = con.execute(
                """
                SELECT o.* FROM observations o
                 WHERE o.state=?
                   AND NOT EXISTS (
                       SELECT 1 FROM processing_runs r WHERE r.observation_id=o.id
                   )
                 ORDER BY o.id
                """,
                (ObservationState.STORED.value,),
            ).fetchall()
        return [self._obs(row) for row in rows]

    def accept_run_and_begin_deletion(self, rid: int, *, note: str = "") -> int:
        """Atomically accept a QA run and mark its observation DELETING.

        This closes the crash window where a run could be terminal ACCEPTED while the
        observation remained STORED with no remaining QA action available.
        """
        with self.tx() as con:
            run = con.execute("SELECT * FROM processing_runs WHERE id=?", (rid,)).fetchone()
            if not run:
                raise KeyError(rid)
            if run["state"] != RunState.AWAITING_QA.value:
                raise TransitionError(f"run {rid} is not awaiting QA")

            oid = int(run["observation_id"])
            obs = con.execute("SELECT * FROM observations WHERE id=?", (oid,)).fetchone()
            if not obs:
                raise KeyError(oid)
            if obs["state"] != ObservationState.STORED.value:
                raise TransitionError(f"observation {oid} is not STORED")

            active_vals = [s.value for s in _ACTIVE_RUN_STATES]
            other = con.execute(
                f"SELECT 1 FROM processing_runs WHERE observation_id=? AND id<>? "
                f"AND state IN ({','.join('?' for _ in active_vals)}) LIMIT 1",
                [oid, rid, *active_vals],
            ).fetchone()
            if other:
                raise TransitionError(f"observation {oid} has another active run")

            cur = con.execute(
                "UPDATE processing_runs SET state=?, qa_verdict='accept', qa_note=?, "
                "updated_at=CURRENT_TIMESTAMP WHERE id=? AND state=?",
                (RunState.ACCEPTED.value, note, rid, RunState.AWAITING_QA.value),
            )
            if cur.rowcount != 1:
                raise TransitionError(f"guard failed for run {rid}: expected {RunState.AWAITING_QA}")
            con.execute(
                "INSERT INTO events(entity_type, entity_id, from_state, to_state, reason) "
                "VALUES ('run',?,?,?,?)",
                (rid, RunState.AWAITING_QA.value, RunState.ACCEPTED.value, "human QA accepted"),
            )

            cur = con.execute(
                "UPDATE observations SET state=?, updated_at=CURRENT_TIMESTAMP "
                "WHERE id=? AND state=?",
                (ObservationState.DELETING.value, oid, ObservationState.STORED.value),
            )
            if cur.rowcount != 1:
                raise TransitionError(f"guard failed for observation {oid}: expected {ObservationState.STORED}")
            con.execute(
                "INSERT INTO events(entity_type, entity_id, from_state, to_state, reason) "
                "VALUES ('observation',?,?,?,?)",
                (oid, ObservationState.STORED.value, ObservationState.DELETING.value,
                 "accepted product permits raw deletion"),
            )
            return oid

    def reprocess_run_and_create(
        self,
        rid: int,
        output_prefix: str,
        log_path: str,
        *,
        note: str = "",
    ) -> int:
        """Atomically supersede an awaiting-QA or failed run and create its replacement."""
        with self.tx() as con:
            run = con.execute("SELECT * FROM processing_runs WHERE id=?", (rid,)).fetchone()
            if not run:
                raise KeyError(rid)
            source_state = RunState(run["state"])
            if source_state not in {RunState.AWAITING_QA, RunState.FAILED}:
                raise TransitionError(f"run {rid} is not reprocessable from {source_state}")
            if RunState.SUPERSEDED not in RUN_TRANSITIONS[source_state]:
                raise TransitionError(f"illegal run transition {source_state}->{RunState.SUPERSEDED}")

            oid = int(run["observation_id"])
            obs = con.execute("SELECT * FROM observations WHERE id=?", (oid,)).fetchone()
            if not obs:
                raise KeyError(oid)
            if obs["state"] != ObservationState.STORED.value:
                raise TransitionError(f"observation {oid} is not STORED")

            active_vals = [s.value for s in _ACTIVE_RUN_STATES]
            other = con.execute(
                f"SELECT 1 FROM processing_runs WHERE observation_id=? AND id<>? "
                f"AND state IN ({','.join('?' for _ in active_vals)}) LIMIT 1",
                [oid, rid, *active_vals],
            ).fetchone()
            if other:
                raise TransitionError(f"observation {oid} has another active run")

            cur = con.execute(
                "UPDATE processing_runs SET state=?, qa_verdict='reprocess', qa_note=?, "
                "updated_at=CURRENT_TIMESTAMP WHERE id=? AND state=?",
                (RunState.SUPERSEDED.value, note, rid, source_state.value),
            )
            if cur.rowcount != 1:
                raise TransitionError(f"guard failed for run {rid}: expected {source_state}")
            con.execute(
                "INSERT INTO events(entity_type, entity_id, from_state, to_state, reason) "
                "VALUES ('run',?,?,?,?)",
                (rid, source_state.value, RunState.SUPERSEDED.value,
                 "human requested reprocessing"),
            )

            next_attempt = int(run["attempt"]) + 1
            cur = con.execute(
                "INSERT INTO processing_runs(observation_id, attempt, state, output_prefix, log_path) "
                "VALUES (?,?,?,?,?)",
                (oid, next_attempt, RunState.QUEUED.value, output_prefix, log_path),
            )
            new_rid = int(cur.lastrowid)
            con.execute(
                "INSERT INTO events(entity_type, entity_id, to_state, reason) VALUES ('run', ?, ?, 'created by reprocess')",
                (new_rid, RunState.QUEUED.value),
            )
            return new_rid

    def discard_failed_run_and_begin_deletion(self, rid: int, *, note: str = "") -> int:
        """Atomically record explicit operator discard and begin raw-data deletion."""
        with self.tx() as con:
            run = con.execute("SELECT * FROM processing_runs WHERE id=?", (rid,)).fetchone()
            if not run:
                raise KeyError(rid)
            if run["state"] != RunState.FAILED.value:
                raise TransitionError(f"run {rid} is not FAILED")

            oid = int(run["observation_id"])
            obs = con.execute("SELECT * FROM observations WHERE id=?", (oid,)).fetchone()
            if not obs:
                raise KeyError(oid)
            if obs["state"] != ObservationState.STORED.value:
                raise TransitionError(f"observation {oid} is not STORED")

            active_vals = [s.value for s in _ACTIVE_RUN_STATES]
            other = con.execute(
                f"SELECT 1 FROM processing_runs WHERE observation_id=? AND id<>? "
                f"AND state IN ({','.join('?' for _ in active_vals)}) LIMIT 1",
                [oid, rid, *active_vals],
            ).fetchone()
            if other:
                raise TransitionError(f"observation {oid} has another active run")

            cur = con.execute(
                "UPDATE processing_runs SET state=?, qa_verdict='discard', qa_note=?, "
                "updated_at=CURRENT_TIMESTAMP WHERE id=? AND state=?",
                (RunState.DISCARDED.value, note, rid, RunState.FAILED.value),
            )
            if cur.rowcount != 1:
                raise TransitionError(f"guard failed for run {rid}: expected {RunState.FAILED}")
            con.execute(
                "INSERT INTO events(entity_type, entity_id, from_state, to_state, reason) "
                "VALUES ('run',?,?,?,?)",
                (rid, RunState.FAILED.value, RunState.DISCARDED.value,
                 "operator discarded raw data after exhausted processing failure"),
            )

            cur = con.execute(
                "UPDATE observations SET state=?, updated_at=CURRENT_TIMESTAMP "
                "WHERE id=? AND state=?",
                (ObservationState.DELETING.value, oid, ObservationState.STORED.value),
            )
            if cur.rowcount != 1:
                raise TransitionError(f"guard failed for observation {oid}: expected {ObservationState.STORED}")
            con.execute(
                "INSERT INTO events(entity_type, entity_id, from_state, to_state, reason) "
                "VALUES ('observation',?,?,?,?)",
                (oid, ObservationState.STORED.value, ObservationState.DELETING.value,
                 "operator discard permits raw deletion"),
            )
            return oid

    def logical_storage(self) -> tuple[int, int]:
        with self.connection() as con:
            used = con.execute(
                "SELECT COALESCE(SUM(size_bytes),0) FROM observations WHERE state IN (?, ?, ?)",
                (ObservationState.STORED.value, ObservationState.DELETING.value, ObservationState.INCONSISTENT.value),
            ).fetchone()[0]
            reserved = con.execute(
                "SELECT COALESCE(SUM(reserved_bytes),0) FROM observations WHERE state IN (?, ?)",
                (ObservationState.PLANNED.value, ObservationState.OBSERVING.value),
            ).fetchone()[0]
        return int(used), int(reserved)

    def count_observations(self) -> int:
        with self.connection() as con:
            return int(con.execute("SELECT COUNT(*) FROM observations").fetchone()[0])

    def events_for(self, entity_type: str, entity_id: int):
        with self.connection() as con:
            return con.execute(
                "SELECT * FROM events WHERE entity_type=? AND entity_id=? ORDER BY id",
                (entity_type, entity_id),
            ).fetchall()

    @staticmethod
    def _obs(r: sqlite3.Row) -> Observation:
        return Observation(
            r["id"],
            ObservationState(r["state"]),
            r["ms_path"],
            r["size_bytes"],
            r["reserved_bytes"],
            r["error"],
        )

    @staticmethod
    def _run(r: sqlite3.Row) -> ProcessingRun:
        return ProcessingRun(
            r["id"],
            r["observation_id"],
            r["attempt"],
            RunState(r["state"]),
            r["output_prefix"],
            r["log_path"],
            r["exit_code"],
            r["qa_verdict"],
            r["qa_note"],
            bool(r["retry_exhausted"]),
        )
