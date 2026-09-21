from __future__ import annotations

import asyncio
from pathlib import Path

import numpy as np


class FakeRunner:
    def __init__(self, observation_size: int, observation_delay: float = 0.05, processing_delay: float = 0.05):
        self.observation_size = observation_size
        self.observation_delay = observation_delay
        self.processing_delay = processing_delay
        self.fail_observations: set[int] = set()
        self.fail_runs: set[int] = set()
        self.fail_run_times: dict[int, int] = {}
        self._process_calls: dict[int, int] = {}

    async def observe(self, observation_id: int, ms_path: Path, log_path: Path) -> int:
        await asyncio.sleep(self.observation_delay)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        if observation_id in self.fail_observations:
            log_path.write_text("fake observation failure\n")
            return 2
        ms_path.mkdir(parents=True, exist_ok=True)
        # Real bytes, not sparse: avoids coupling tests to apparent-vs-block-size accounting.
        marker = b"Measurement Set v2 fake fixture\n"
        remaining = max(0, self.observation_size - len(marker))
        chunk = b"\0" * min(1024 * 1024, max(1, remaining))
        with (ms_path / "DATA").open("wb") as fh:
            while remaining:
                n = min(len(chunk), remaining)
                fh.write(chunk[:n])
                remaining -= n
        # Tiny marker standing in for MS subtables.
        (ms_path / "table.info").write_bytes(marker)
        log_path.write_text("fake observation success\n")
        return 0

    async def process(self, observation_id: int, run_id: int, ms_path: Path, output_prefix: Path, log_path: Path) -> int:
        await asyncio.sleep(self.processing_delay)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        self._process_calls[run_id] = self._process_calls.get(run_id, 0) + 1
        fail_n = self.fail_run_times.get(run_id, 0)
        if run_id in self.fail_runs or self._process_calls[run_id] <= fail_n:
            log_path.write_text("fake processing failure\n")
            return 3
        output_prefix.parent.mkdir(parents=True, exist_ok=True)
        y, x = np.mgrid[-32:32, -32:32]
        data = np.exp(-(x*x+y*y)/(2*6.0**2)).astype("float32")
        # Store NumPy arrays in files named like FITS products. The preview loader
        # understands this test fixture format; the real container path produces real FITS.
        for suffix, arr in (("-image.fits", data), ("-model.fits", data * 0.9), ("-residual.fits", data * 0.1)):
            with open(str(output_prefix) + suffix, "wb") as fh:
                np.save(fh, arr)
        log_path.write_text("fake processing success\n")
        return 0

    async def abort_observation(self, observation_id: int) -> None:
        return None

    async def kill_managed(self) -> None:
        return None
