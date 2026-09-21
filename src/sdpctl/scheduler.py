from __future__ import annotations

import asyncio
from contextlib import suppress
from pathlib import Path

from .config import Config
from .models import ObservationState, RunState
from .previews import make_preview
from .sizing import apparent_size
from .store import Store, TransitionError
from .storage import auxiliary_charged_storage, quarantine_path


class Scheduler:
    def __init__(self, cfg: Config, store: Store, runner):
        self.cfg = cfg
        self.store = store
        self.runner = runner
        self._stop = asyncio.Event()
        self._obs_tasks: dict[int, asyncio.Task] = {}
        self._run_tasks: dict[int, asyncio.Task] = {}
        self._processing_sem = asyncio.Semaphore(cfg.max_parallel_processing)

    def stop(self):
        self._stop.set()

    def can_admit(self) -> bool:
        used, reserved = self.store.logical_storage()
        quarantine, trash = auxiliary_charged_storage(self.cfg.data_root)
        # Stored raw bytes are DB-accounted; only quarantine/trash need filesystem walks.
        charged = used + reserved + quarantine + trash
        return charged + self.cfg.observation_reservation <= self.cfg.storage_threshold

    def _campaign_has_capacity(self) -> bool:
        return self.cfg.max_observations is None or self.store.count_observations() < self.cfg.max_observations

    def _new_observation(self) -> int:
        return self.store.create_observation_auto(
            self.cfg.data_root / "observations",
            reserved_bytes=self.cfg.observation_reservation,
        )

    @staticmethod
    def _retrieve_task_exception(task: asyncio.Task) -> None:
        # Always retrieve exceptions so a programming error is not emitted later as
        # "Task exception was never retrieved". Worker coroutines also persist failures.
        with suppress(asyncio.CancelledError):
            task.exception()

    def _track(self, mapping: dict[int, asyncio.Task], key: int, coro) -> asyncio.Task:
        task = asyncio.create_task(coro)
        task.add_done_callback(self._retrieve_task_exception)
        mapping[key] = task
        return task

    async def _stop_observation_runner(self, oid: int, task: asyncio.Task | None) -> None:
        with suppress(Exception):
            await self.runner.abort_observation(oid)
        if task is None or task.done():
            return
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=2.0)
        except Exception:
            if not task.done():
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

    async def _fail_observation(self, oid: int, reason: str, error: str) -> None:
        try:
            obs = self.store.get_observation(oid)
        except KeyError:
            return

        if obs.state == ObservationState.OBSERVING:
            p = Path(obs.ms_path)
            if p.exists():
                with suppress(Exception):
                    quarantine_path(self.cfg.data_root, p, f"obs-{oid:06d}-exception.ms")
            with suppress(TransitionError):
                self.store.transition_observation(
                    oid,
                    ObservationState.OBSERVING,
                    ObservationState.OBSERVE_FAILED,
                    reason=reason,
                    reserved_bytes=0,
                    error=error,
                )
        elif obs.state == ObservationState.PLANNED:
            with suppress(TransitionError):
                self.store.transition_observation(
                    oid,
                    ObservationState.PLANNED,
                    ObservationState.OBSERVE_FAILED,
                    reason=reason,
                    reserved_bytes=0,
                    error=error,
                )
        else:
            self.store.record_event("observation", oid, f"worker exception after state {obs.state}: {error}")

    async def _observe(self, oid: int) -> None:
        runner_task: asyncio.Task | None = None
        try:
            obs = self.store.get_observation(oid)
            try:
                self.store.transition_observation(
                    oid,
                    ObservationState.PLANNED,
                    ObservationState.OBSERVING,
                    reason="admitted under storage reservation",
                )
            except TransitionError:
                return

            # Refresh after transition so exception handling sees current data.
            obs = self.store.get_observation(oid)
            log = self.cfg.data_root / "logs" / f"observe-{oid:06d}.log"
            runner_task = asyncio.create_task(self.runner.observe(oid, Path(obs.ms_path), log))
            overrun_warned = False

            while not runner_task.done():
                await asyncio.sleep(min(self.cfg.watchdog_interval, 0.1))
                size = apparent_size(obs.ms_path)
                if size <= self.cfg.observation_reservation:
                    continue

                if self.cfg.reservation_overrun_action == "abort":
                    await self._stop_observation_runner(oid, runner_task)
                    p = Path(obs.ms_path)
                    if p.exists():
                        quarantine_path(self.cfg.data_root, p, f"obs-{oid:06d}-reservation-overrun.ms")
                    self.store.transition_observation(
                        oid,
                        ObservationState.OBSERVING,
                        ObservationState.OBSERVE_FAILED,
                        reason="in-flight size exceeded reservation",
                        reserved_bytes=0,
                        error=f"reservation exceeded at {size} bytes",
                    )
                    return

                if not overrun_warned:
                    self.store.record_event(
                        "observation",
                        oid,
                        f"WARNING: in-flight size {size} exceeded reservation {self.cfg.observation_reservation}",
                    )
                    overrun_warned = True

            rc = await runner_task
            if rc != 0:
                p = Path(obs.ms_path)
                if p.exists():
                    quarantine_path(self.cfg.data_root, p, f"obs-{oid:06d}-failed.ms")
                self.store.transition_observation(
                    oid,
                    ObservationState.OBSERVING,
                    ObservationState.OBSERVE_FAILED,
                    reason="runner exited non-zero",
                    reserved_bytes=0,
                    error=f"exit code {rc}",
                )
                return

            actual = apparent_size(obs.ms_path)
            if actual > self.cfg.observation_reservation:
                if self.cfg.reservation_overrun_action == "abort":
                    p = Path(obs.ms_path)
                    if p.exists():
                        quarantine_path(self.cfg.data_root, p, f"obs-{oid:06d}-reservation-overrun.ms")
                    self.store.transition_observation(
                        oid,
                        ObservationState.OBSERVING,
                        ObservationState.OBSERVE_FAILED,
                        reason="post-completion reservation exceeded",
                        reserved_bytes=0,
                        error=f"actual {actual} > reserved {self.cfg.observation_reservation}",
                    )
                    return
                self.store.record_event(
                    "observation",
                    oid,
                    f"WARNING: completed size {actual} exceeded reservation {self.cfg.observation_reservation}",
                )

            self.store.transition_observation(
                oid,
                ObservationState.OBSERVING,
                ObservationState.STORED,
                reason="observation completed",
                size_bytes=actual,
                reserved_bytes=0,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._stop_observation_runner(oid, runner_task)
            await self._fail_observation(oid, "observation worker raised exception", repr(exc))

    async def _process(self, rid: int) -> None:
        try:
            async with self._processing_sem:
                run = self.store.get_run(rid)
                # Claim the run before doing any fallible setup. This makes every worker
                # execution consume retry budget, including failures before the runner starts.
                try:
                    self.store.transition_run(
                        rid,
                        RunState.QUEUED,
                        RunState.RUNNING,
                        reason="processing slot available",
                    )
                except TransitionError:
                    return
                obs = self.store.get_observation(run.observation_id)

                rc = await self.runner.process(
                    obs.id,
                    rid,
                    Path(obs.ms_path),
                    Path(run.output_prefix),
                    Path(run.log_path),
                )
                if rc != 0:
                    self.store.transition_run(
                        rid,
                        RunState.RUNNING,
                        RunState.FAILED,
                        reason="processing exited non-zero",
                        exit_code=rc,
                    )
                    return

                image = Path(run.output_prefix + "-image.fits")
                if not image.exists():
                    self.store.transition_run(
                        rid,
                        RunState.RUNNING,
                        RunState.FAILED,
                        reason="expected restored image missing",
                        exit_code=0,
                    )
                    return

                preview_reason = "processing completed; preview available"
                try:
                    make_preview(image, Path(run.output_prefix).parent / "preview.png")
                except Exception as exc:
                    # The FITS product is the science product; a failed convenience PNG must
                    # not strand an otherwise valid run in RUNNING.
                    self.store.record_event("run", rid, f"preview unavailable: {exc!r}")
                    preview_reason = "processing completed; preview unavailable"

                self.store.transition_run(
                    rid,
                    RunState.RUNNING,
                    RunState.AWAITING_QA,
                    reason=preview_reason,
                    exit_code=0,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            try:
                run = self.store.get_run(rid)
            except KeyError:
                return
            if run.state == RunState.QUEUED:
                with suppress(TransitionError):
                    self.store.transition_run(
                        rid,
                        RunState.QUEUED,
                        RunState.FAILED,
                        reason="processing worker raised before start",
                        exit_code=-1,
                    )
            elif run.state == RunState.RUNNING:
                with suppress(TransitionError):
                    self.store.transition_run(
                        rid,
                        RunState.RUNNING,
                        RunState.FAILED,
                        reason=f"processing worker exception: {exc!r}",
                        exit_code=-1,
                    )
            else:
                self.store.record_event("run", rid, f"worker exception after state {run.state}: {exc!r}")

    def _retry_failed_runs(self) -> None:
        """Retry only failures that still have automatic budget.

        Exhaustion is persisted in the run row, so terminal historical failures leave the
        scheduler hot path permanently. Candidate filtering and execution counting happen in
        SQL rather than as per-run round trips.
        """
        import shutil

        self.store.mark_exhausted_failed_runs(self.cfg.max_processing_attempts)
        for run, executions in self.store.list_retryable_failed_runs(
            self.cfg.max_processing_attempts
        ):
            out_dir = Path(run.output_prefix).parent
            try:
                if out_dir.exists():
                    shutil.rmtree(out_dir)
            except Exception as exc:
                # Automatic progress is impossible. Persist that fact once and hand the item
                # to the operator rather than retrying this cleanup on every tick forever.
                self.store.mark_run_retry_exhausted(
                    run.id,
                    reason=f"automatic retry blocked by product cleanup failure: {exc!r}",
                )
                continue

            try:
                self.store.transition_run(
                    run.id,
                    RunState.FAILED,
                    RunState.QUEUED,
                    reason=(
                        f"automatic retry {executions + 1}/{self.cfg.max_processing_attempts}"
                    ),
                )
            except TransitionError:
                # Another actor may have reprocessed/discarded the failure concurrently.
                continue

    def _create_initial_run(self, observation_id: int) -> None:
        prefix = self.cfg.data_root / "products" / f"obs-{observation_id:06d}" / "run-001" / "product"
        plog = self.cfg.data_root / "logs" / f"process-{observation_id:06d}-001.log"
        self.store.create_run(observation_id, str(prefix), str(plog))

    async def _sweep_orphaned_inprocess_states(self) -> None:
        for obs in self.store.list_observations([ObservationState.OBSERVING]):
            task = self._obs_tasks.get(obs.id)
            if task is None or task.done():
                p = Path(obs.ms_path)
                if p.exists():
                    with suppress(Exception):
                        quarantine_path(self.cfg.data_root, p, f"obs-{obs.id:06d}-orphaned-task.ms")
                with suppress(TransitionError):
                    self.store.transition_observation(
                        obs.id,
                        ObservationState.OBSERVING,
                        ObservationState.OBSERVE_FAILED,
                        reason="OBSERVING state had no live in-process task",
                        reserved_bytes=0,
                        error="orphaned in-process observation task",
                    )

        for run in self.store.list_runs([RunState.RUNNING]):
            task = self._run_tasks.get(run.id)
            if task is None or task.done():
                with suppress(TransitionError):
                    self.store.transition_run(
                        run.id,
                        RunState.RUNNING,
                        RunState.FAILED,
                        reason="RUNNING state had no live in-process task",
                        exit_code=-1,
                    )

    async def tick(self) -> None:
        await self._sweep_orphaned_inprocess_states()
        self._retry_failed_runs()

        # Resume a PLANNED record whose worker never actually started.
        for obs in self.store.list_observations([ObservationState.PLANNED]):
            task = self._obs_tasks.get(obs.id)
            if task is None or task.done():
                self._track(self._obs_tasks, obs.id, self._observe(obs.id))

        active_obs = self.store.list_observations([ObservationState.OBSERVING, ObservationState.PLANNED])
        if not active_obs and self._campaign_has_capacity() and self.can_admit():
            oid = self._new_observation()
            self._track(self._obs_tasks, oid, self._observe(oid))

        # A crash after OBSERVING->STORED but before run creation must not strand data.
        # The anti-join is one SQL query, so this does not add one query per historical STORED row.
        for obs in self.store.list_stored_observations_without_runs():
            self._create_initial_run(obs.id)

        for run in self.store.list_runs([RunState.QUEUED]):
            task = self._run_tasks.get(run.id)
            if task is None or task.done():
                self._track(self._run_tasks, run.id, self._process(run.id))

    async def run(self) -> None:
        while not self._stop.is_set():
            await self.tick()
            await asyncio.sleep(self.cfg.tick_interval)

    async def run_until_idle(self, timeout: float = 10.0) -> None:
        async def done():
            while True:
                await self.tick()
                tasks = [
                    t
                    for t in [*self._obs_tasks.values(), *self._run_tasks.values()]
                    if not t.done()
                ]
                # idle means no runnable work; awaiting QA is intentionally idle/backpressured.
                queued = self.store.list_runs([RunState.QUEUED, RunState.RUNNING])
                observing = self.store.list_observations([ObservationState.PLANNED, ObservationState.OBSERVING])
                if not tasks and not queued and not observing:
                    return
                await asyncio.sleep(0.02)

        await asyncio.wait_for(done(), timeout=timeout)
