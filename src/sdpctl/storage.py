from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil

from .models import ObservationState
from .sizing import apparent_size


@dataclass(frozen=True)
class PhysicalStorage:
    managed_raw: int
    quarantine: int
    trash: int
    products: int

    @property
    def total_tracked(self) -> int:
        return self.managed_raw + self.quarantine + self.trash + self.products


# Admission only charges quarantine + trash in addition to DB-accounted raw bytes.
# Cache those two small trees rather than re-walking observations/products every tick.
# All controller-owned mutations invalidate this cache; top-level mtimes additionally
# detect ordinary out-of-process additions/removals.
_AUX_CACHE: dict[str, tuple[tuple[int, int], tuple[int, int]]] = {}


def ensure_layout(root: Path) -> None:
    for name in ("observations", "products", "logs", "trash", "quarantine"):
        (root / name).mkdir(parents=True, exist_ok=True)


def _mtime_signature(root: Path) -> tuple[int, int]:
    q = root / "quarantine"
    t = root / "trash"
    return (
        q.stat().st_mtime_ns if q.exists() else -1,
        t.stat().st_mtime_ns if t.exists() else -1,
    )


def invalidate_auxiliary_cache(root: Path) -> None:
    _AUX_CACHE.pop(str(root.resolve()), None)


def auxiliary_charged_storage(root: Path, *, force: bool = False) -> tuple[int, int]:
    """Return cached (quarantine_bytes, trash_bytes).

    Controller-owned mutations explicitly invalidate the cache. The top-level mtime
    signature also detects ordinary creation/removal/rename of quarantine/trash entries,
    but it is not a recursive watcher: an external process that mutates files *inside* an
    existing entry can leave the cached value stale until the next explicit invalidation,
    status snapshot, or controller restart.
    """
    root = root.resolve()
    key = str(root)
    sig = _mtime_signature(root)
    cached = _AUX_CACHE.get(key)
    if not force and cached is not None and cached[0] == sig:
        return cached[1]
    sizes = (apparent_size(root / "quarantine"), apparent_size(root / "trash"))
    _AUX_CACHE[key] = (sig, sizes)
    return sizes


def physical_snapshot(root: Path) -> PhysicalStorage:
    quarantine, trash = auxiliary_charged_storage(root, force=True)
    return PhysicalStorage(
        managed_raw=apparent_size(root / "observations"),
        quarantine=quarantine,
        trash=trash,
        products=apparent_size(root / "products"),
    )


def _unique_quarantine_target(root: Path, label: str) -> Path:
    base = root / "quarantine" / label
    if not base.exists():
        return base
    n = 1
    while True:
        candidate = base.with_name(f"{base.name}.{n:03d}")
        if not candidate.exists():
            return candidate
        n += 1


def quarantine_path(root: Path, source: Path, label: str) -> Path:
    """Move data into quarantine without ever replacing an earlier quarantined item."""
    if not source.exists():
        raise FileNotFoundError(source)
    q = _unique_quarantine_target(root, label)
    q.parent.mkdir(parents=True, exist_ok=True)
    source.rename(q)
    invalidate_auxiliary_cache(root)
    return q


def finish_observation_deletion(store, root: Path, observation_id: int) -> None:
    """Complete a crash-safe raw-data deletion for an observation already in DELETING."""
    obs = store.get_observation(observation_id)
    if obs.state != ObservationState.DELETING:
        raise ValueError("observation is not DELETING")

    src = Path(obs.ms_path)
    trash = root / "trash" / f"obs-{obs.id:06d}.ms"
    trash.parent.mkdir(parents=True, exist_ok=True)

    if src.exists():
        if trash.exists():
            raise RuntimeError(f"both live and trash copies exist for observation {obs.id}")
        src.rename(trash)
        invalidate_auxiliary_cache(root)

    if trash.exists():
        if trash.is_dir():
            shutil.rmtree(trash)
        else:
            trash.unlink()
        invalidate_auxiliary_cache(root)

    store.transition_observation(
        obs.id,
        ObservationState.DELETING,
        ObservationState.DELETED,
        reason="raw visibility removed",
        size_bytes=0,
        reserved_bytes=0,
    )


def force_drop_observation_data(store, root: Path, observation_id: int, *, reason: str) -> None:
    """Operator-authorised cleanup for a wedged DELETING/INCONSISTENT observation.

    This is intentionally destructive and is only used behind an explicit `resolve --drop`.
    It removes both possible sides of the rename boundary before marking the row DELETED.
    """
    obs = store.get_observation(observation_id)
    if obs.state not in {ObservationState.DELETING, ObservationState.INCONSISTENT}:
        raise ValueError("observation is neither DELETING nor INCONSISTENT")

    src = Path(obs.ms_path)
    trash = root / "trash" / f"obs-{obs.id:06d}.ms"
    for path in (src, trash):
        if path.exists():
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
    invalidate_auxiliary_cache(root)

    store.transition_observation(
        obs.id,
        obs.state,
        ObservationState.DELETED,
        reason=reason,
        size_bytes=0,
        reserved_bytes=0,
        error="operator resolved recovery inconsistency by dropping raw data",
    )
