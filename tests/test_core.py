from dataclasses import replace
from pathlib import Path
import asyncio
import pytest

from sdpctl.config import Config, load_config, parse_bytes
from sdpctl.models import ObservationState, RunState
from sdpctl.qa import accept_run, discard_failed_run, list_qa_items, reprocess_run
from sdpctl.reconcile import resolve_observation_drop, startup_reconcile
from sdpctl.runners.fake import FakeRunner
from sdpctl.scheduler import Scheduler
from sdpctl.storage import auxiliary_charged_storage, ensure_layout, invalidate_auxiliary_cache
from sdpctl.store import Store, TransitionError


def mkcfg(tmp_path: Path, threshold="40MiB", reservation="20MiB", max_obs=10, max_attempts=3):
    return Config(
        data_root=tmp_path,
        storage_threshold=parse_bytes(threshold),
        observation_reservation=parse_bytes(reservation),
        max_parallel_processing=2,
        max_processing_attempts=max_attempts,
        tick_interval=0.01,
        max_observations=max_obs,
        fake_observation_size=parse_bytes("20MiB"),
        fake_observation_delay=0.01,
        fake_processing_delay=0.01,
        watchdog_interval=0.01,
    )


def test_parse_bytes():
    assert parse_bytes("10GiB") == 10 * 1024**3
    assert parse_bytes("1.5GB") == 1_500_000_000


def test_guarded_transition(tmp_path):
    s = Store(tmp_path / "db.sqlite")
    oid = s.create_observation("x", 5)
    s.transition_observation(oid, ObservationState.PLANNED, ObservationState.OBSERVING)
    with pytest.raises(TransitionError):
        s.transition_observation(oid, ObservationState.PLANNED, ObservationState.OBSERVING)


def test_admission_boundary(tmp_path):
    cfg = mkcfg(tmp_path, threshold="40MiB", reservation="20MiB")
    ensure_layout(tmp_path)
    s = Store(tmp_path / "control.db")
    r = FakeRunner(cfg.fake_observation_size, 0, 0)
    sch = Scheduler(cfg, s, r)
    assert sch.can_admit()
    oid = s.create_observation(str(tmp_path / "observations" / "a.ms"), cfg.observation_reservation)
    s.transition_observation(oid, ObservationState.PLANNED, ObservationState.OBSERVING)
    assert sch.can_admit()  # exact boundary: 20 reserved + 20 next == 40
    oid2 = s.create_observation(str(tmp_path / "observations" / "b.ms"), 1)
    s.transition_observation(oid2, ObservationState.PLANNED, ObservationState.OBSERVING)
    assert not sch.can_admit()


@pytest.mark.asyncio
async def test_campaign_pauses_then_accept_resumes(tmp_path):
    cfg = mkcfg(tmp_path, threshold="40MiB", reservation="20MiB", max_obs=3)
    ensure_layout(tmp_path)
    s = Store(tmp_path / "control.db")
    r = FakeRunner(cfg.fake_observation_size, 0.01, 0.01)
    sch = Scheduler(cfg, s, r)

    await sch.run_until_idle()
    obs = s.list_observations()
    assert len(obs) == 2  # storage exactly full, third not admitted
    assert all(o.state == ObservationState.STORED for o in obs)
    runs = s.list_runs([RunState.AWAITING_QA])
    assert len(runs) == 2
    assert Path(runs[0].output_prefix).parent.joinpath("preview.png").exists()

    accept_run(s, tmp_path, runs[0].id)
    assert s.get_observation(runs[0].observation_id).state == ObservationState.DELETED

    await sch.run_until_idle()
    assert len(s.list_observations()) == 3


@pytest.mark.asyncio
async def test_reprocess_creates_new_attempt_without_freeing_raw(tmp_path):
    cfg = mkcfg(tmp_path, threshold="20MiB", reservation="20MiB", max_obs=1)
    ensure_layout(tmp_path)
    s = Store(tmp_path / "control.db")
    r = FakeRunner(cfg.fake_observation_size, 0, 0)
    sch = Scheduler(cfg, s, r)
    await sch.run_until_idle()
    run1 = s.list_runs([RunState.AWAITING_QA])[0]
    obs = s.get_observation(run1.observation_id)
    before = obs.size_bytes
    rid2 = reprocess_run(s, tmp_path, run1.id)
    assert s.get_run(run1.id).state == RunState.SUPERSEDED
    assert s.get_run(rid2).attempt == 2
    assert s.get_observation(obs.id).size_bytes == before
    await sch.run_until_idle()
    assert s.get_run(rid2).state == RunState.AWAITING_QA


@pytest.mark.asyncio
async def test_restart_reconciles_interrupted_observation_and_run(tmp_path):
    cfg = mkcfg(tmp_path, threshold="40MiB", reservation="20MiB")
    ensure_layout(tmp_path)
    s = Store(tmp_path / "control.db")
    r = FakeRunner(cfg.fake_observation_size)
    oid = s.create_observation(str(tmp_path / "observations" / "obs-000001.ms"), cfg.observation_reservation)
    s.transition_observation(oid, ObservationState.PLANNED, ObservationState.OBSERVING)
    p = Path(s.get_observation(oid).ms_path); p.mkdir(parents=True); (p / "partial").write_bytes(b"abc")

    oid2 = s.create_observation(str(tmp_path / "observations" / "obs-000002.ms"), 0)
    s.transition_observation(oid2, ObservationState.PLANNED, ObservationState.OBSERVING)
    s.transition_observation(oid2, ObservationState.OBSERVING, ObservationState.STORED, size_bytes=0, reserved_bytes=0)
    prefix = tmp_path / "products" / "obs-000002" / "run-001" / "product"
    rid = s.create_run(oid2, str(prefix), str(tmp_path / "logs" / "p.log"))
    s.transition_run(rid, RunState.QUEUED, RunState.RUNNING)
    prefix.parent.mkdir(parents=True); (prefix.parent / "partial.fits").write_bytes(b"bad")

    await startup_reconcile(s, r, tmp_path)
    assert s.get_observation(oid).state == ObservationState.OBSERVE_FAILED
    assert (tmp_path / "quarantine" / "obs-000001-partial.ms").exists()
    assert s.get_run(rid).state == RunState.FAILED
    assert not prefix.parent.exists()


@pytest.mark.asyncio
async def test_missing_stored_becomes_inconsistent(tmp_path):
    ensure_layout(tmp_path)
    s = Store(tmp_path / "control.db")
    r = FakeRunner(1)
    oid = s.create_observation(str(tmp_path / "observations" / "missing.ms"), 1)
    s.transition_observation(oid, ObservationState.PLANNED, ObservationState.OBSERVING)
    s.transition_observation(oid, ObservationState.OBSERVING, ObservationState.STORED, size_bytes=123, reserved_bytes=0)
    await startup_reconcile(s, r, tmp_path)
    o = s.get_observation(oid)
    assert o.state == ObservationState.INCONSISTENT
    assert o.size_bytes == 0
    assert o.reserved_bytes == 0

@pytest.mark.asyncio
async def test_reservation_overrun_is_failed_and_quarantined(tmp_path):
    cfg = Config(
        data_root=tmp_path,
        storage_threshold=parse_bytes("30MiB"),
        observation_reservation=parse_bytes("10MiB"),
        max_parallel_processing=1,
        tick_interval=0.01,
        max_observations=1,
        fake_observation_size=parse_bytes("20MiB"),
        fake_observation_delay=0,
        fake_processing_delay=0,
        watchdog_interval=0.01,
        reservation_overrun_action="abort",
    )
    ensure_layout(tmp_path)
    s = Store(tmp_path / "control.db")
    r = FakeRunner(cfg.fake_observation_size, 0, 0)
    sch = Scheduler(cfg, s, r)
    await sch.run_until_idle()
    obs = s.list_observations()[0]
    assert obs.state == ObservationState.OBSERVE_FAILED
    assert obs.reserved_bytes == 0
    assert (tmp_path / "quarantine" / "obs-000001-reservation-overrun.ms").exists()
    used, reserved = s.logical_storage()
    assert used == 0 and reserved == 0


def test_accept_validates_before_mutating(tmp_path):
    cfg = mkcfg(tmp_path, threshold="20MiB", reservation="20MiB", max_obs=1)
    ensure_layout(tmp_path)
    s = Store(tmp_path / "control.db")
    oid = s.create_observation(str(tmp_path / "observations" / "obs-000001.ms"), 0)
    ms = Path(s.get_observation(oid).ms_path)
    ms.mkdir(parents=True)
    (ms / "DATA").write_bytes(b"x")
    s.transition_observation(oid, ObservationState.PLANNED, ObservationState.OBSERVING)
    s.transition_observation(oid, ObservationState.OBSERVING, ObservationState.STORED, size_bytes=1, reserved_bytes=0)
    rid = s.create_run(oid, str(tmp_path / "products" / "x" / "product"), str(tmp_path / "logs" / "x.log"))
    s.transition_run(rid, RunState.QUEUED, RunState.RUNNING)
    s.transition_run(rid, RunState.RUNNING, RunState.AWAITING_QA)
    s.transition_observation(oid, ObservationState.STORED, ObservationState.INCONSISTENT, size_bytes=1, reserved_bytes=0)

    with pytest.raises(ValueError):
        accept_run(s, tmp_path, rid)

    assert s.get_run(rid).state == RunState.AWAITING_QA
    assert s.get_run(rid).qa_verdict is None
    assert s.get_observation(oid).state == ObservationState.INCONSISTENT


@pytest.mark.asyncio
async def test_accept_crash_after_atomic_commit_reconciles(tmp_path, monkeypatch):
    cfg = mkcfg(tmp_path, threshold="20MiB", reservation="20MiB", max_obs=1)
    ensure_layout(tmp_path)
    s = Store(tmp_path / "control.db")
    r = FakeRunner(cfg.fake_observation_size, 0, 0)
    sch = Scheduler(cfg, s, r)
    await sch.run_until_idle()
    run = s.list_runs([RunState.AWAITING_QA])[0]
    oid = run.observation_id

    import sdpctl.qa as qa_mod

    def crash_after_commit(*args, **kwargs):
        raise RuntimeError("simulated crash after DB commit")

    monkeypatch.setattr(qa_mod, "finish_observation_deletion", crash_after_commit)
    with pytest.raises(RuntimeError):
        qa_mod.accept_run(s, tmp_path, run.id)

    assert s.get_run(run.id).state == RunState.ACCEPTED
    assert s.get_observation(oid).state == ObservationState.DELETING

    await startup_reconcile(s, r, tmp_path)
    assert s.get_observation(oid).state == ObservationState.DELETED


@pytest.mark.asyncio
async def test_reconcile_legacy_stored_with_accepted_run(tmp_path):
    ensure_layout(tmp_path)
    s = Store(tmp_path / "control.db")
    r = FakeRunner(1)
    oid = s.create_observation(str(tmp_path / "observations" / "obs-000001.ms"), 0)
    ms = Path(s.get_observation(oid).ms_path)
    ms.mkdir(parents=True)
    (ms / "DATA").write_bytes(b"x")
    s.transition_observation(oid, ObservationState.PLANNED, ObservationState.OBSERVING)
    s.transition_observation(oid, ObservationState.OBSERVING, ObservationState.STORED, size_bytes=1, reserved_bytes=0)
    rid = s.create_run(oid, str(tmp_path / "products" / "x" / "product"), str(tmp_path / "logs" / "x.log"))
    s.transition_run(rid, RunState.QUEUED, RunState.RUNNING)
    s.transition_run(rid, RunState.RUNNING, RunState.AWAITING_QA)
    # Simulate the crash state produced by the previous controller version.
    s.transition_run(rid, RunState.AWAITING_QA, RunState.ACCEPTED, qa_verdict="accept")

    await startup_reconcile(s, r, tmp_path)
    assert s.get_observation(oid).state == ObservationState.DELETED
    assert not ms.exists()


def test_quarantine_bytes_block_admission(tmp_path):
    cfg = mkcfg(tmp_path, threshold="40MiB", reservation="20MiB", max_obs=2)
    ensure_layout(tmp_path)
    q = tmp_path / "quarantine" / "partial.ms"
    q.mkdir()
    (q / "DATA").write_bytes(b"\0" * (21 * 1024**2))
    s = Store(tmp_path / "control.db")
    sch = Scheduler(cfg, s, FakeRunner(cfg.fake_observation_size, 0, 0))
    assert not sch.can_admit()


def test_quarantine_collision_preserves_both(tmp_path):
    from sdpctl.storage import quarantine_path

    ensure_layout(tmp_path)
    a = tmp_path / "observations" / "a.ms"
    b = tmp_path / "observations" / "b.ms"
    a.mkdir(); b.mkdir()
    (a / "DATA").write_text("first")
    (b / "DATA").write_text("second")

    qa = quarantine_path(tmp_path, a, "same-label.ms")
    qb = quarantine_path(tmp_path, b, "same-label.ms")

    assert qa != qb
    assert (qa / "DATA").read_text() == "first"
    assert (qb / "DATA").read_text() == "second"
    assert sorted(x.name for x in (tmp_path / "quarantine").iterdir()) == ["same-label.ms", "same-label.ms.001"]


@pytest.mark.asyncio
async def test_preview_failure_is_best_effort(tmp_path, monkeypatch):
    cfg = mkcfg(tmp_path, threshold="20MiB", reservation="20MiB", max_obs=1)
    ensure_layout(tmp_path)
    s = Store(tmp_path / "control.db")
    sch = Scheduler(cfg, s, FakeRunner(cfg.fake_observation_size, 0, 0))

    import sdpctl.scheduler as scheduler_mod

    def broken_preview(*args, **kwargs):
        raise ValueError("non-finite image")

    monkeypatch.setattr(scheduler_mod, "make_preview", broken_preview)
    await sch.run_until_idle()

    runs = s.list_runs()
    assert len(runs) == 1
    assert runs[0].state == RunState.AWAITING_QA
    reasons = [row["reason"] for row in s.events_for("run", runs[0].id)]
    assert any("preview unavailable" in (x or "") for x in reasons)


@pytest.mark.asyncio
async def test_processing_exception_fails_once_without_respawn(tmp_path):
    class ExplodingRunner(FakeRunner):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.process_calls = 0

        async def process(self, *args, **kwargs):
            self.process_calls += 1
            raise RuntimeError("boom")

    cfg = mkcfg(tmp_path, threshold="20MiB", reservation="20MiB", max_obs=1, max_attempts=1)
    ensure_layout(tmp_path)
    s = Store(tmp_path / "control.db")
    r = ExplodingRunner(cfg.fake_observation_size, 0, 0)
    sch = Scheduler(cfg, s, r)
    await sch.run_until_idle()

    run = s.list_runs()[0]
    assert run.state == RunState.FAILED
    assert r.process_calls == 1
    for _ in range(5):
        await sch.tick()
    assert r.process_calls == 1


@pytest.mark.asyncio
async def test_randomised_qa_sequences_preserve_invariants(tmp_path):
    import random
    from sdpctl.storage import physical_snapshot

    rng = random.Random(20260920)
    cfg = mkcfg(tmp_path, threshold="60MiB", reservation="20MiB", max_obs=6)
    ensure_layout(tmp_path)
    s = Store(tmp_path / "control.db")
    r = FakeRunner(cfg.fake_observation_size, 0, 0)
    sch = Scheduler(cfg, s, r)

    def assert_invariants():
        observing = s.list_observations([ObservationState.OBSERVING])
        assert len(observing) <= 1

        active_states = [RunState.QUEUED, RunState.RUNNING, RunState.AWAITING_QA]
        for obs in s.list_observations():
            assert len(s.list_runs(active_states, observation_id=obs.id)) <= 1
            if obs.state == ObservationState.DELETED:
                assert any(x.state == RunState.ACCEPTED for x in s.list_runs(observation_id=obs.id))

        used, reserved = s.logical_storage()
        phys = physical_snapshot(tmp_path)
        assert used + reserved + phys.quarantine + phys.trash <= cfg.storage_threshold

    for _ in range(40):
        await sch.run_until_idle()
        assert_invariants()

        # Reconciliation at arbitrary idle points must preserve the same invariants.
        if rng.random() < 0.25:
            await startup_reconcile(s, r, tmp_path)
            assert_invariants()

        awaiting = s.list_runs([RunState.AWAITING_QA])
        if not awaiting:
            if s.count_observations() >= (cfg.max_observations or 0):
                break
            continue

        run = rng.choice(awaiting)
        if run.attempt < 3 and rng.random() < 0.35:
            reprocess_run(s, tmp_path, run.id, note="randomised test")
        else:
            accept_run(s, tmp_path, run.id, note="randomised test")
        assert_invariants()

    # Drain any remaining QA items by accepting them.
    for _ in range(20):
        await sch.run_until_idle()
        awaiting = s.list_runs([RunState.AWAITING_QA])
        if not awaiting:
            if s.count_observations() >= (cfg.max_observations or 0):
                break
            continue
        accept_run(s, tmp_path, awaiting[0].id, note="final drain")
        assert_invariants()


def test_reprocess_is_atomic_if_replacement_insert_fails(tmp_path):
    import sqlite3

    ensure_layout(tmp_path)
    s = Store(tmp_path / "control.db")
    oid = s.create_observation(str(tmp_path / "observations" / "obs-000001.ms"), 0)
    ms = Path(s.get_observation(oid).ms_path)
    ms.mkdir(parents=True)
    (ms / "DATA").write_bytes(b"x")
    s.transition_observation(oid, ObservationState.PLANNED, ObservationState.OBSERVING)
    s.transition_observation(oid, ObservationState.OBSERVING, ObservationState.STORED, size_bytes=1, reserved_bytes=0)
    rid = s.create_run(oid, str(tmp_path / "products" / "r1" / "product"), str(tmp_path / "logs" / "r1.log"))
    s.transition_run(rid, RunState.QUEUED, RunState.RUNNING)
    s.transition_run(rid, RunState.RUNNING, RunState.AWAITING_QA)

    with s.tx() as con:
        con.execute(
            """
            CREATE TRIGGER fail_second_attempt
            BEFORE INSERT ON processing_runs
            WHEN NEW.observation_id = %d AND NEW.attempt = 2
            BEGIN
              SELECT RAISE(ABORT, 'simulated insert failure');
            END;
            """ % oid
        )

    with pytest.raises(sqlite3.IntegrityError):
        reprocess_run(s, tmp_path, rid)

    assert s.get_run(rid).state == RunState.AWAITING_QA
    assert s.get_run(rid).qa_verdict is None
    assert len(s.list_runs(observation_id=oid)) == 1


@pytest.mark.asyncio
async def test_observation_exception_releases_reservation(tmp_path):
    class ExplodingObserveRunner(FakeRunner):
        async def observe(self, *args, **kwargs):
            raise RuntimeError("observe boom")

    cfg = mkcfg(tmp_path, threshold="20MiB", reservation="20MiB", max_obs=1)
    ensure_layout(tmp_path)
    s = Store(tmp_path / "control.db")
    r = ExplodingObserveRunner(cfg.fake_observation_size, 0, 0)
    sch = Scheduler(cfg, s, r)
    await sch.run_until_idle()

    obs = s.list_observations()[0]
    assert obs.state == ObservationState.OBSERVE_FAILED
    assert obs.reserved_bytes == 0
    assert s.logical_storage() == (0, 0)


@pytest.mark.asyncio
async def test_warn_overrun_records_event_and_allows_completion(tmp_path):
    cfg = Config(
        data_root=tmp_path,
        storage_threshold=parse_bytes("30MiB"),
        observation_reservation=parse_bytes("10MiB"),
        max_parallel_processing=1,
        tick_interval=0.01,
        max_observations=1,
        fake_observation_size=parse_bytes("20MiB"),
        fake_observation_delay=0,
        fake_processing_delay=0,
        watchdog_interval=0.01,
        reservation_overrun_action="warn",
    )
    ensure_layout(tmp_path)
    s = Store(tmp_path / "control.db")
    sch = Scheduler(cfg, s, FakeRunner(cfg.fake_observation_size, 0, 0))
    await sch.run_until_idle()

    obs = s.list_observations()[0]
    assert obs.state == ObservationState.STORED
    assert obs.size_bytes >= cfg.fake_observation_size
    reasons = [row["reason"] for row in s.events_for("observation", obs.id)]
    assert any("WARNING:" in (x or "") and "reservation" in (x or "") for x in reasons)


@pytest.mark.asyncio
async def test_failed_processing_retries_then_surfaces_for_operator_and_discard_frees_space(tmp_path):
    cfg = mkcfg(tmp_path, threshold="40MiB", reservation="20MiB", max_obs=3, max_attempts=3)
    ensure_layout(tmp_path)
    s = Store(tmp_path / "control.db")
    r = FakeRunner(cfg.fake_observation_size, 0, 0)
    # First processing run fails on every execution; second succeeds.
    r.fail_runs.add(1)
    sch = Scheduler(cfg, s, r)

    await sch.run_until_idle()

    run1 = s.get_run(1)
    assert run1.state == RunState.FAILED
    assert s.run_execution_attempts(1) == cfg.max_processing_attempts
    assert any(item.id == 1 for item in list_qa_items(s))
    assert len(s.list_observations()) == 2
    assert not sch.can_admit()

    discard_failed_run(s, tmp_path, 1, note="operator drops unprocessable observation")
    assert s.get_run(1).state == RunState.DISCARDED
    assert s.get_observation(run1.observation_id).state == ObservationState.DELETED

    await sch.run_until_idle()
    assert len(s.list_observations()) == 3


@pytest.mark.asyncio
async def test_failed_processing_can_be_human_reprocessed_after_retry_exhaustion(tmp_path):
    cfg = mkcfg(tmp_path, threshold="20MiB", reservation="20MiB", max_obs=1, max_attempts=2)
    ensure_layout(tmp_path)
    s = Store(tmp_path / "control.db")
    r = FakeRunner(cfg.fake_observation_size, 0, 0)
    r.fail_runs.add(1)
    sch = Scheduler(cfg, s, r)
    await sch.run_until_idle()

    assert s.get_run(1).state == RunState.FAILED
    assert s.run_execution_attempts(1) == 2
    rid2 = reprocess_run(s, tmp_path, 1, note="operator retries with a new logical run")
    assert s.get_run(1).state == RunState.SUPERSEDED
    await sch.run_until_idle()
    assert s.get_run(rid2).state == RunState.AWAITING_QA


@pytest.mark.asyncio
async def test_startup_reconcile_is_entity_scoped_and_operator_can_clear_wedged_delete(tmp_path):
    ensure_layout(tmp_path)
    s = Store(tmp_path / "control.db")
    r = FakeRunner(1)

    # Observation 1 is deliberately wedged across both sides of the delete rename boundary.
    oid1 = s.create_observation(str(tmp_path / "observations" / "obs-000001.ms"), 0)
    live1 = Path(s.get_observation(oid1).ms_path)
    live1.mkdir(parents=True)
    (live1 / "DATA").write_bytes(b"live")
    s.transition_observation(oid1, ObservationState.PLANNED, ObservationState.OBSERVING)
    s.transition_observation(oid1, ObservationState.OBSERVING, ObservationState.STORED, size_bytes=4, reserved_bytes=0)
    s.transition_observation(oid1, ObservationState.STORED, ObservationState.DELETING)
    trash1 = tmp_path / "trash" / f"obs-{oid1:06d}.ms"
    trash1.mkdir(parents=True)
    (trash1 / "DATA").write_bytes(b"trash")

    # Observation 2 should still be reconciled after observation 1 fails.
    oid2 = s.create_observation(str(tmp_path / "observations" / "missing.ms"), 0)
    s.transition_observation(oid2, ObservationState.PLANNED, ObservationState.OBSERVING)
    s.transition_observation(oid2, ObservationState.OBSERVING, ObservationState.STORED, size_bytes=10, reserved_bytes=0)

    # And orphan scanning should still run.
    orphan = tmp_path / "observations" / "untracked.ms"
    orphan.mkdir()
    (orphan / "DATA").write_bytes(b"orphan")

    await startup_reconcile(s, r, tmp_path)

    wedged = s.get_observation(oid1)
    assert wedged.state == ObservationState.DELETING
    assert wedged.error and "both live and trash copies" in wedged.error
    assert s.get_observation(oid2).state == ObservationState.INCONSISTENT
    assert any(x.name.startswith("orphan-untracked.ms") for x in (tmp_path / "quarantine").iterdir())

    resolve_observation_drop(s, tmp_path, oid1, note="operator explicitly clears duplicate delete state")
    assert s.get_observation(oid1).state == ObservationState.DELETED
    assert not live1.exists()
    assert not trash1.exists()


def test_admission_walks_only_quarantine_and_trash_and_cache_invalidates(tmp_path, monkeypatch):
    cfg = mkcfg(tmp_path, threshold="40MiB", reservation="20MiB", max_obs=2)
    ensure_layout(tmp_path)
    s = Store(tmp_path / "control.db")
    sch = Scheduler(cfg, s, FakeRunner(cfg.fake_observation_size, 0, 0))

    import sdpctl.storage as storage_mod
    real = storage_mod.apparent_size
    calls = []

    def counted(path):
        calls.append(Path(path).name)
        return real(path)

    invalidate_auxiliary_cache(tmp_path)
    monkeypatch.setattr(storage_mod, "apparent_size", counted)
    assert sch.can_admit()
    assert calls == ["quarantine", "trash"]

    calls.clear()
    assert sch.can_admit()
    assert calls == []  # cached

    qsrc = tmp_path / "observations" / "partial.ms"
    qsrc.mkdir()
    (qsrc / "DATA").write_bytes(b"x")
    from sdpctl.storage import quarantine_path
    quarantine_path(tmp_path, qsrc, "partial.ms")
    calls.clear()
    sch.can_admit()
    assert calls == ["quarantine", "trash"]  # controller mutation invalidated cache


def test_python_module_cli_invokes_main(tmp_path, monkeypatch):
    # Structural regression: the module now has an executable __main__ guard.
    text = Path("src/sdpctl/cli.py").read_text()
    assert 'if __name__ == "__main__":' in text
    assert "main()" in text.split('if __name__ == "__main__":', 1)[1]


@pytest.mark.asyncio
async def test_randomised_failures_preserve_safety_and_liveness(tmp_path):
    import random
    from sdpctl.storage import physical_snapshot

    rng = random.Random(20260920)
    cfg = mkcfg(tmp_path, threshold="60MiB", reservation="20MiB", max_obs=7, max_attempts=2)
    ensure_layout(tmp_path)
    s = Store(tmp_path / "control.db")
    r = FakeRunner(cfg.fake_observation_size, 0, 0)
    r.fail_observations.update({2, 6})
    r.fail_runs.add(3)  # exhaust one run so the human-action path is exercised
    r.fail_run_times[1] = 1  # transient processing failure should heal automatically
    sch = Scheduler(cfg, s, r)

    def assert_properties():
        assert len(s.list_observations([ObservationState.OBSERVING])) <= 1
        active_states = [RunState.QUEUED, RunState.RUNNING, RunState.AWAITING_QA]
        qa_ids = {x.id for x in list_qa_items(s)}
        for obs in s.list_observations():
            runs = s.list_runs(observation_id=obs.id)
            assert len(s.list_runs(active_states, observation_id=obs.id)) <= 1
            if obs.state == ObservationState.DELETED:
                assert any(x.state in {RunState.ACCEPTED, RunState.DISCARDED} for x in runs)
            if obs.state == ObservationState.STORED:
                # Liveness: a stored observation must have runnable work, a QA item,
                # or a legacy accepted state that reconciliation can complete.
                assert (
                    any(x.state in {RunState.QUEUED, RunState.RUNNING} for x in runs)
                    or any(x.id in qa_ids for x in runs)
                    or any(x.state == RunState.ACCEPTED for x in runs)
                )
            if obs.state == ObservationState.DELETING:
                # Deletion either has a durable human decision behind it and is resumable,
                # or an entity-scoped recovery error exposes the explicit resolve path.
                assert (
                    any(x.state in {RunState.ACCEPTED, RunState.DISCARDED} for x in runs)
                    or obs.error is not None
                )
            if obs.state == ObservationState.INCONSISTENT:
                # INCONSISTENT is never silent: it must carry operator-visible context and
                # has the explicit `resolve --drop` escape hatch.
                assert obs.error is not None

        used, reserved = s.logical_storage()
        phys = physical_snapshot(tmp_path)
        assert used + reserved + phys.quarantine + phys.trash <= cfg.storage_threshold

    for _ in range(60):
        await sch.run_until_idle()
        assert_properties()

        if rng.random() < 0.20:
            await startup_reconcile(s, r, tmp_path)
            assert_properties()

        items = list_qa_items(s)
        if not items:
            if s.count_observations() >= (cfg.max_observations or 0):
                break
            continue
        item = rng.choice(items)
        if item.state == RunState.FAILED:
            if rng.random() < 0.5:
                reprocess_run(s, tmp_path, item.id, note="random failure recovery")
            else:
                discard_failed_run(s, tmp_path, item.id, note="random failure discard")
        elif item.attempt < 3 and rng.random() < 0.3:
            reprocess_run(s, tmp_path, item.id, note="random QA reprocess")
        else:
            accept_run(s, tmp_path, item.id, note="random QA accept")
        assert_properties()


@pytest.mark.asyncio
async def test_transient_processing_failure_auto_retries_and_recovers(tmp_path):
    cfg = mkcfg(tmp_path, threshold="20MiB", reservation="20MiB", max_obs=1, max_attempts=3)
    ensure_layout(tmp_path)
    s = Store(tmp_path / "control.db")
    r = FakeRunner(cfg.fake_observation_size, 0, 0)
    r.fail_run_times[1] = 1
    sch = Scheduler(cfg, s, r)

    await sch.run_until_idle()

    run = s.get_run(1)
    assert run.state == RunState.AWAITING_QA
    assert s.run_execution_attempts(run.id) == 2
    reasons = [row["reason"] for row in s.events_for("run", run.id)]
    assert any("automatic retry 2/3" in (reason or "") for reason in reasons)


def test_retry_exhaustion_is_persisted_and_survives_reopen(tmp_path):
    s = Store(tmp_path / "control.db")
    oid = s.create_observation(str(tmp_path / "observations" / "obs-000001.ms"), 0)
    s.transition_observation(oid, ObservationState.PLANNED, ObservationState.OBSERVING)
    s.transition_observation(
        oid, ObservationState.OBSERVING, ObservationState.STORED,
        size_bytes=1, reserved_bytes=0,
    )
    rid = s.create_run(oid, str(tmp_path / "products" / "r1" / "product"), str(tmp_path / "r1.log"))
    s.transition_run(rid, RunState.QUEUED, RunState.RUNNING)
    s.transition_run(rid, RunState.RUNNING, RunState.FAILED, exit_code=1)

    newly = s.mark_exhausted_failed_runs(1)
    assert newly == [rid]
    assert s.get_run(rid).retry_exhausted is True
    assert s.list_retryable_failed_runs(1) == []

    reopened = Store(tmp_path / "control.db")
    assert reopened.get_run(rid).retry_exhausted is True
    assert [x.id for x in list_qa_items(reopened)] == [rid]
    assert reopened.mark_exhausted_failed_runs(1) == []


def test_retry_query_ignores_large_exhausted_history(tmp_path):
    s = Store(tmp_path / "control.db")
    for i in range(300):
        oid = s.create_observation(str(tmp_path / "observations" / f"obs-{i+1:06d}.ms"), 0)
        s.transition_observation(oid, ObservationState.PLANNED, ObservationState.OBSERVING)
        s.transition_observation(
            oid, ObservationState.OBSERVING, ObservationState.STORED,
            size_bytes=1, reserved_bytes=0,
        )
        rid = s.create_run(oid, str(tmp_path / "products" / f"r{oid}" / "product"), str(tmp_path / f"r{oid}.log"))
        s.transition_run(rid, RunState.QUEUED, RunState.RUNNING)
        s.transition_run(rid, RunState.RUNNING, RunState.FAILED, exit_code=1)
    assert len(s.mark_exhausted_failed_runs(1)) == 300
    assert s.list_retryable_failed_runs(1) == []
    # A second pass should not revisit/re-mark historical terminal failures.
    assert s.mark_exhausted_failed_runs(1) == []


def test_initial_run_repair_anti_join_returns_only_missing_runs(tmp_path):
    s = Store(tmp_path / "control.db")
    missing = []
    for i in range(20):
        oid = s.create_observation(str(tmp_path / "observations" / f"obs-{i+1:06d}.ms"), 0)
        s.transition_observation(oid, ObservationState.PLANNED, ObservationState.OBSERVING)
        s.transition_observation(
            oid, ObservationState.OBSERVING, ObservationState.STORED,
            size_bytes=1, reserved_bytes=0,
        )
        if i % 5 == 0:
            missing.append(oid)
        else:
            s.create_run(oid, str(tmp_path / "products" / f"r{oid}" / "product"), str(tmp_path / f"r{oid}.log"))
    assert [o.id for o in s.list_stored_observations_without_runs()] == missing


def test_cumulative_processing_execution_count_spans_reprocess_chain(tmp_path):
    ensure_layout(tmp_path)
    s = Store(tmp_path / "control.db")
    oid = s.create_observation(str(tmp_path / "observations" / "obs-000001.ms"), 0)
    ms = Path(s.get_observation(oid).ms_path)
    ms.mkdir(parents=True)
    (ms / "DATA").write_bytes(b"x")
    s.transition_observation(oid, ObservationState.PLANNED, ObservationState.OBSERVING)
    s.transition_observation(oid, ObservationState.OBSERVING, ObservationState.STORED, size_bytes=1, reserved_bytes=0)
    rid1 = s.create_run(oid, str(tmp_path / "products" / "obs-000001" / "run-001" / "product"), str(tmp_path / "r1.log"))
    s.transition_run(rid1, RunState.QUEUED, RunState.RUNNING)
    s.transition_run(rid1, RunState.RUNNING, RunState.FAILED, exit_code=1)
    s.mark_exhausted_failed_runs(1)
    rid2 = reprocess_run(s, tmp_path, rid1, note="operator retry")
    s.transition_run(rid2, RunState.QUEUED, RunState.RUNNING)
    s.transition_run(rid2, RunState.RUNNING, RunState.FAILED, exit_code=1)
    assert s.run_execution_attempts(rid1) == 1
    assert s.run_execution_attempts(rid2) == 1
    assert s.observation_execution_attempts(oid) == 2


def test_store_migrates_v3_database_retry_exhausted_column(tmp_path):
    import sqlite3
    db = tmp_path / "legacy.db"
    con = sqlite3.connect(db)
    con.executescript(
        """
        CREATE TABLE observations (
          id INTEGER PRIMARY KEY AUTOINCREMENT, state TEXT NOT NULL, ms_path TEXT NOT NULL,
          size_bytes INTEGER NOT NULL DEFAULT 0, reserved_bytes INTEGER NOT NULL DEFAULT 0,
          error TEXT, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE processing_runs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          observation_id INTEGER NOT NULL REFERENCES observations(id),
          attempt INTEGER NOT NULL, state TEXT NOT NULL, output_prefix TEXT NOT NULL,
          log_path TEXT NOT NULL, exit_code INTEGER, qa_verdict TEXT, qa_note TEXT,
          created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          UNIQUE(observation_id, attempt)
        );
        CREATE TABLE events (
          id INTEGER PRIMARY KEY AUTOINCREMENT, entity_type TEXT NOT NULL,
          entity_id INTEGER NOT NULL, at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          from_state TEXT, to_state TEXT, reason TEXT
        );
        """
    )
    con.close()
    Store(db)
    con = sqlite3.connect(db)
    cols = {row[1] for row in con.execute("PRAGMA table_info(processing_runs)")}
    con.close()
    assert "retry_exhausted" in cols


def test_default_config_uses_one_processing_attempt_for_deterministic_mock(tmp_path):
    from sdpctl.config import load_config
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text(
        f"data_root: {tmp_path / 'workspace'}\n"
        "storage_threshold: 2GiB\n"
        "observation_reservation: 1GiB\n"
    )
    assert load_config(cfg_path).max_processing_attempts == 1

def test_all_shipped_configs_load():
    config_dir = Path(__file__).resolve().parents[1] / "config"
    paths = sorted(config_dir.glob("*.yaml"))
    assert {p.name for p in paths} >= {"default.yaml", "real.yaml", "demo.yaml"}
    for path in paths:
        cfg = load_config(path)
        assert cfg.storage_threshold > 0
        assert cfg.observation_reservation > 0


@pytest.mark.asyncio
async def test_demo_config_admits_three_then_blocks_fourth(tmp_path):
    demo_path = Path(__file__).resolve().parents[1] / "config" / "demo.yaml"
    shipped = load_config(demo_path)
    cfg = replace(
        shipped,
        data_root=tmp_path,
        tick_interval=0.001,
        watchdog_interval=0.001,
        fake_observation_delay=0,
        fake_processing_delay=0,
    )
    ensure_layout(tmp_path)
    store = Store(tmp_path / "control.db")
    runner = FakeRunner(cfg.fake_observation_size, 0, 0)
    scheduler = Scheduler(cfg, store, runner)

    await scheduler.run_until_idle()

    observations = store.list_observations()
    runs = store.list_runs()
    assert len(observations) == 3
    assert all(o.state == ObservationState.STORED for o in observations)
    assert len(runs) == 3
    assert all(r.state == RunState.AWAITING_QA for r in runs)

    used, reserved = store.logical_storage()
    assert used == parse_bytes("60MiB")
    assert reserved == 0
    assert used + cfg.observation_reservation > cfg.storage_threshold
    assert not scheduler.can_admit()


def test_status_lists_processing_runs(tmp_path, capsys):
    from sdpctl.cli import main as cli_main

    ensure_layout(tmp_path)
    cfg_path = tmp_path / "status.yaml"
    cfg_path.write_text(
        f"data_root: {tmp_path}\n"
        "storage_threshold: 64MiB\n"
        "observation_reservation: 24MiB\n"
        "runner: fake\n"
        "max_observations: 1\n"
    )

    store = Store(tmp_path / "control.db")
    oid = store.create_observation(
        str(tmp_path / "observations" / "obs-000001.ms"),
        reserved_bytes=0,
    )
    rid = store.create_run(
        oid,
        str(tmp_path / "products" / "obs-000001" / "run-001" / "product"),
        str(tmp_path / "logs" / "process-000001-001.log"),
    )

    cli_main(["--config", str(cfg_path), "status"])
    out = capsys.readouterr().out

    assert rid == 1
    assert "run 000001 obs=000001 attempt=1 QUEUED output=" in out
    assert "products/obs-000001/run-001" in out
