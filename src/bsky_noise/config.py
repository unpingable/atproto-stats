from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from platformdirs import user_config_path


def _load_dotenv(path: Path | None = None) -> None:
    env_path = path or Path.cwd() / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"'").strip()
        if key and key not in os.environ:
            os.environ[key] = value


_load_dotenv()


APP_DIR = Path(user_config_path("bsky_noise"))
SESSION_PATH = APP_DIR / "session.json"
_DB_ENV = os.environ.get("BSKY_DB_PATH")
DB_PATH = Path(_DB_ENV) if _DB_ENV else APP_DIR / "bsky_noise.db"


@dataclass(frozen=True)
class Session:
    handle: str
    did: str
    access_jwt: str
    refresh_jwt: str
    pds: str


def ensure_app_dir() -> None:
    APP_DIR.mkdir(parents=True, exist_ok=True)


def save_session(data: dict[str, Any]) -> None:
    ensure_app_dir()
    SESSION_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")


def load_session() -> Session:
    raw = json.loads(SESSION_PATH.read_text(encoding="utf-8"))
    return Session(
        handle=raw["handle"],
        did=raw["did"],
        access_jwt=raw["accessJwt"],
        refresh_jwt=raw["refreshJwt"],
        pds=raw.get("pds", "https://bsky.social"),
    )
