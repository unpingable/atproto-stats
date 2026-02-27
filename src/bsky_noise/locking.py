from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class LockError(RuntimeError):
    message: str


def _pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


@contextmanager
def file_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    if lock_path.exists():
        try:
            payload = lock_path.read_text(encoding="utf-8").strip().splitlines()
            pid = int(payload[0]) if payload else -1
        except Exception:
            pid = -1

        if pid > 0 and _pid_is_alive(pid):
            raise LockError(f"Another run is active (pid={pid}). Lock: {lock_path}")
        # stale lock
        lock_path.unlink(missing_ok=True)

    fd = None
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        stamp = datetime.now(timezone.utc).isoformat()
        os.write(fd, f"{os.getpid()}\n{stamp}\n".encode("utf-8"))
        yield
    finally:
        if fd is not None:
            os.close(fd)
        lock_path.unlink(missing_ok=True)
