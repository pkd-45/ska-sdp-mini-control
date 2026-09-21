from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from .config import load_config
from .models import RunState
from .qa import accept_run, discard_failed_run, list_qa_items, reprocess_run
from .reconcile import resolve_observation_drop, startup_reconcile
from .runners.container import ContainerRunner
from .runners.fake import FakeRunner
from .scheduler import Scheduler
from .storage import ensure_layout, invalidate_auxiliary_cache, physical_snapshot
from .store import Store


def build(cfg):
    ensure_layout(cfg.data_root)
    store = Store(cfg.data_root / "control.db")
    if cfg.runner == "fake":
        runner = FakeRunner(cfg.fake_observation_size, cfg.fake_observation_delay, cfg.fake_processing_delay)
    elif cfg.runner in {"docker", "podman", "container"}:
        runner = ContainerRunner(cfg.container_executable, cfg.container_image,
                                 cfg.observe_script, cfg.process_script, cfg.data_root)
    else:
        raise ValueError(f"unknown runner {cfg.runner!r}")
    return store, runner


def main(argv=None):
    p = argparse.ArgumentParser(prog="sdpctl")
    p.add_argument("--config", default="config/default.yaml")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("run")
    sub.add_parser("status")
    q = sub.add_parser("qa")
    qs = q.add_subparsers(dest="qcmd", required=True)
    qs.add_parser("list")
    for name in ("accept", "reprocess", "discard"):
        x = qs.add_parser(name)
        x.add_argument("run_id", type=int)
        x.add_argument("--note", default="")
    t = sub.add_parser("timeline")
    t.add_argument("entity_type", choices=["observation", "run", "reconcile"])
    t.add_argument("id", type=int)
    r = sub.add_parser("resolve")
    r.add_argument("observation_id", type=int)
    r.add_argument("--drop", action="store_true")
    qu = sub.add_parser("quarantine")
    qus = qu.add_subparsers(dest="qucmd", required=True)
    qus.add_parser("list")
    qp = qus.add_parser("purge")
    qp.add_argument("name")
    args = p.parse_args(argv)

    cfg = load_config(args.config)
    store, runner = build(cfg)

    if args.cmd == "run":
        async def go():
            await startup_reconcile(store, runner, cfg.data_root)
            await Scheduler(cfg, store, runner).run()
        try:
            asyncio.run(go())
        except KeyboardInterrupt:
            print("Controller stopped by operator.")
    elif args.cmd == "status":
        used, reserved = store.logical_storage()
        phys = physical_snapshot(cfg.data_root)
        print(f"logical raw used: {used} bytes")
        print(f"reserved:         {reserved} bytes")
        print(f"threshold:        {cfg.storage_threshold} bytes")
        print(f"quarantine:       {phys.quarantine} bytes")
        print(f"trash:            {phys.trash} bytes")
        print(f"charged now:      {used + reserved + phys.quarantine + phys.trash} bytes")
        print(f"products:         {phys.products} bytes")
        for o in store.list_observations():
            extra = f" error={o.error!r}" if o.error else ""
            print(f"obs {o.id:06d} {o.state} size={o.size_bytes} reserved={o.reserved_bytes}{extra}")
    elif args.cmd == "qa" and args.qcmd == "list":
        for x in list_qa_items(store):
            executions = store.run_execution_attempts(x.id)
            total_executions = store.observation_execution_attempts(x.observation_id)
            if x.state == RunState.AWAITING_QA:
                preview = Path(x.output_prefix).parent / "preview.png"
                print(
                    f"run={x.id} obs={x.observation_id} attempt={x.attempt} state={x.state} "
                    f"run_executions={executions} obs_executions={total_executions} "
                    f"actions=accept,reprocess preview={preview}"
                )
            else:
                print(
                    f"run={x.id} obs={x.observation_id} attempt={x.attempt} state={x.state} "
                    f"run_executions={executions}/{cfg.max_processing_attempts} "
                    f"obs_executions={total_executions} exhausted={x.retry_exhausted} "
                    "actions=reprocess,discard"
                )
    elif args.cmd == "qa" and args.qcmd == "accept":
        accept_run(store, cfg.data_root, args.run_id, args.note)
    elif args.cmd == "qa" and args.qcmd == "reprocess":
        rid = reprocess_run(store, cfg.data_root, args.run_id, args.note)
        print(rid)
    elif args.cmd == "qa" and args.qcmd == "discard":
        discard_failed_run(store, cfg.data_root, args.run_id, args.note)
    elif args.cmd == "timeline":
        for e in store.events_for(args.entity_type, args.id):
            print(dict(e))
    elif args.cmd == "resolve":
        if not args.drop:
            raise SystemExit("only --drop is supported")
        resolve_observation_drop(store, cfg.data_root, args.observation_id)
    elif args.cmd == "quarantine" and args.qucmd == "list":
        for x in sorted((cfg.data_root / "quarantine").iterdir()):
            print(x.name)
    elif args.cmd == "quarantine" and args.qucmd == "purge":
        import shutil
        target = cfg.data_root / "quarantine" / args.name
        if not target.exists():
            raise SystemExit("not found")
        if target.is_dir():
            shutil.rmtree(target)
        else:
            target.unlink()
        invalidate_auxiliary_cache(cfg.data_root)


if __name__ == "__main__":
    main()
