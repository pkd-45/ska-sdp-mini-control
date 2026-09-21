from __future__ import annotations

import asyncio
from pathlib import Path


class ContainerRunner:
    def __init__(self, executable: str, image: str, observe_script: str, process_script: str, data_root: Path):
        self.executable = executable
        self.image = image
        self.observe_script = observe_script
        self.process_script = process_script
        self.data_root = data_root.resolve()

    async def _run(self, args: list[str], log_path: Path) -> int:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("wb") as log:
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=log,
                stderr=asyncio.subprocess.STDOUT,
            )
            return await proc.wait()

    def _container_path(self, host_path: Path) -> str:
        rel = host_path.resolve().relative_to(self.data_root)
        return "/data/" + rel.as_posix()

    async def _container_ids(self, filters: list[str]) -> list[str]:
        # -a is essential: Created/Exited containers can retain deterministic names and
        # otherwise break the next launch even though they are not running.
        args = [self.executable, "ps", "-aq"]
        for f in filters:
            args += ["--filter", f]
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, _ = await proc.communicate()
        return [x for x in out.decode().split() if x]

    async def _remove_and_wait_gone(self, cid: str) -> None:
        rm = await asyncio.create_subprocess_exec(
            self.executable,
            "rm",
            "-f",
            cid,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await rm.wait()
        # Do not touch files owned by a container until it is absent from *all* states.
        for _ in range(100):
            ids = await self._container_ids([f"id={cid}"])
            if not ids:
                return
            await asyncio.sleep(0.05)
        raise RuntimeError(f"container {cid} still exists after rm -f")

    async def observe(self, observation_id: int, ms_path: Path, log_path: Path) -> int:
        ms_path.parent.mkdir(parents=True, exist_ok=True)
        name = f"sdpctl-observe-{observation_id:06d}"
        args = [
            self.executable,
            "run",
            "--rm",
            "--name",
            name,
            "--label",
            "sdpctl.managed=true",
            "--label",
            "sdpctl.kind=observation",
            "--label",
            f"sdpctl.observation_id={observation_id}",
            "-v",
            f"{self.data_root}:/data",
            self.image,
            self.observe_script,
            self._container_path(ms_path),
        ]
        return await self._run(args, log_path)

    async def process(
        self,
        observation_id: int,
        run_id: int,
        ms_path: Path,
        output_prefix: Path,
        log_path: Path,
    ) -> int:
        output_prefix.parent.mkdir(parents=True, exist_ok=True)
        name = f"sdpctl-process-{observation_id:06d}-{run_id:06d}"
        args = [
            self.executable,
            "run",
            "--rm",
            "--name",
            name,
            "--label",
            "sdpctl.managed=true",
            "--label",
            "sdpctl.kind=processing",
            "--label",
            f"sdpctl.observation_id={observation_id}",
            "--label",
            f"sdpctl.run_id={run_id}",
            "-v",
            f"{self.data_root}:/data",
            self.image,
            self.process_script,
            self._container_path(ms_path),
            self._container_path(output_prefix),
        ]
        return await self._run(args, log_path)

    async def abort_observation(self, observation_id: int) -> None:
        ids = await self._container_ids(
            [
                "label=sdpctl.managed=true",
                "label=sdpctl.kind=observation",
                f"label=sdpctl.observation_id={observation_id}",
            ]
        )
        for cid in ids:
            await self._remove_and_wait_gone(cid)

    async def kill_managed(self) -> None:
        ids = await self._container_ids(["label=sdpctl.managed=true"])
        for cid in ids:
            await self._remove_and_wait_gone(cid)
