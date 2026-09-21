#!/usr/bin/env python3
"""Reproduce scheduler hot-path timing with exhausted historical failures.

Run from the repository root:
    PYTHONPATH=src python3 scripts/benchmark_tick_history.py

This is a development benchmark, not a hard performance test: absolute timings depend on
hardware/filesystem. The useful signal is whether tick cost grows materially with terminal
history after retry exhaustion has been persisted.
"""
from __future__ import annotations

import asyncio
from pathlib import Path
import tempfile
import time

from sdpctl.config import Config
from sdpctl.models import ObservationState, RunState
from sdpctl.runners.fake import FakeRunner
from sdpctl.scheduler import Scheduler
from sdpctl.storage import ensure_layout
from sdpctl.store import Store


async def benchmark(n: int, samples: int = 50) -> tuple[float, float, float]:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        ensure_layout(root)
        cfg = Config(
            data_root=root,
            storage_threshold=10**12,
            observation_reservation=1,
            max_parallel_processing=2,
            max_processing_attempts=1,
            tick_interval=0.5,
            max_observations=n,
            fake_observation_size=1,
            fake_observation_delay=0,
            fake_processing_delay=0,
            watchdog_interval=0.01,
        )
        store = Store(root / "control.db")
        for i in range(n):
            oid = store.create_observation(str(root / "observations" / f"obs-{i+1:06d}.ms"), 0)
            store.transition_observation(oid, ObservationState.PLANNED, ObservationState.OBSERVING)
            store.transition_observation(
                oid,
                ObservationState.OBSERVING,
                ObservationState.STORED,
                size_bytes=1,
                reserved_bytes=0,
            )
            rid = store.create_run(
                oid,
                str(root / "products" / f"run-{i+1:06d}" / "product"),
                str(root / "logs" / f"run-{i+1:06d}.log"),
            )
            store.transition_run(rid, RunState.QUEUED, RunState.RUNNING)
            store.transition_run(rid, RunState.RUNNING, RunState.FAILED, exit_code=1)

        store.mark_exhausted_failed_runs(1)
        scheduler = Scheduler(cfg, store, FakeRunner(1, 0, 0))
        await scheduler.tick()  # warm cache / DB pages

        timings = []
        for _ in range(samples):
            start = time.perf_counter()
            await scheduler.tick()
            timings.append((time.perf_counter() - start) * 1000)
        timings.sort()
        return sum(timings) / len(timings), timings[len(timings) // 2], timings[-1]


async def main() -> None:
    for n in (20, 100, 300, 1000):
        avg, median, worst = await benchmark(n)
        print(f"{n:4d} exhausted failures: avg={avg:7.3f} ms  median={median:7.3f} ms  max={worst:7.3f} ms")


if __name__ == "__main__":
    asyncio.run(main())
