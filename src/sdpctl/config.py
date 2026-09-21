from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import yaml

_UNITS = {
    "B": 1,
    "KB": 1000,
    "MB": 1000**2,
    "GB": 1000**3,
    "TB": 1000**4,
    "KIB": 1024,
    "MIB": 1024**2,
    "GIB": 1024**3,
    "TIB": 1024**4,
}


def parse_bytes(value: int | str) -> int:
    if isinstance(value, int):
        if value < 0:
            raise ValueError("byte value cannot be negative")
        return value
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([A-Za-z]+)\s*", value)
    if not m:
        raise ValueError(f"invalid byte quantity: {value!r}")
    n, unit = m.groups()
    unit = unit.upper()
    if unit not in _UNITS:
        raise ValueError(f"unsupported byte unit: {unit}")
    return int(float(n) * _UNITS[unit])


@dataclass(frozen=True)
class Config:
    data_root: Path
    storage_threshold: int
    observation_reservation: int
    max_parallel_processing: int = 3
    max_processing_attempts: int = 1
    tick_interval: float = 1.0
    runner: str = "fake"
    container_executable: str = "docker"
    container_image: str = "docker.io/pw410/ska-sdp-mock:0.1"
    observe_script: str = "/scripts/generate_visibilities.sh"
    process_script: str = "/scripts/process_visibilities.sh"
    max_observations: int | None = None
    watchdog_interval: float = 2.0
    reservation_overrun_action: str = "abort"
    fake_observation_size: int = 20 * 1024**2
    fake_observation_delay: float = 0.05
    fake_processing_delay: float = 0.05


def load_config(path: str | Path) -> Config:
    raw = yaml.safe_load(Path(path).read_text()) or {}
    root = Path(raw.get("data_root", "./workspace")).expanduser().resolve()
    action = raw.get("reservation_overrun_action", "abort")
    if action not in {"abort", "warn"}:
        raise ValueError("reservation_overrun_action must be 'abort' or 'warn'")
    cfg = Config(
        data_root=root,
        storage_threshold=parse_bytes(raw["storage_threshold"]),
        observation_reservation=parse_bytes(raw["observation_reservation"]),
        max_parallel_processing=int(raw.get("max_parallel_processing", 3)),
        max_processing_attempts=int(raw.get("max_processing_attempts", 1)),
        tick_interval=float(raw.get("tick_interval", 1.0)),
        runner=str(raw.get("runner", "fake")),
        container_executable=str(raw.get("container_executable", "docker")),
        container_image=str(raw.get("container_image", "docker.io/pw410/ska-sdp-mock:0.1")),
        observe_script=str(raw.get("observe_script", "/scripts/generate_visibilities.sh")),
        process_script=str(raw.get("process_script", "/scripts/process_visibilities.sh")),
        max_observations=raw.get("max_observations"),
        watchdog_interval=float(raw.get("watchdog_interval", 2.0)),
        reservation_overrun_action=action,
        fake_observation_size=parse_bytes(raw.get("fake_observation_size", "20MiB")),
        fake_observation_delay=float(raw.get("fake_observation_delay", 0.05)),
        fake_processing_delay=float(raw.get("fake_processing_delay", 0.05)),
    )
    if cfg.observation_reservation > cfg.storage_threshold:
        raise ValueError("observation_reservation exceeds storage_threshold")
    if cfg.max_parallel_processing < 1:
        raise ValueError("max_parallel_processing must be >= 1")
    if cfg.max_processing_attempts < 1:
        raise ValueError("max_processing_attempts must be >= 1")
    return cfg
