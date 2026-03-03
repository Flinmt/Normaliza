from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: str
    user: str
    password: str


def load_env_file(env_path: str) -> dict[str, str]:
    data: dict[str, str] = {}
    path = Path(env_path)
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            data[key.strip()] = value.strip()
    return data


def load_db_config(env_path: str) -> DbConfig:
    env = load_env_file(env_path)
    required = ["DB_HOST", "DB_USER", "DB_PASS"]
    missing = [k for k in required if not env.get(k)]
    if missing:
        raise RuntimeError(f"Variáveis ausentes no .env: {', '.join(missing)}")

    return DbConfig(
        host=env["DB_HOST"],
        port=env.get("DB_PORT", "1433"),
        user=env["DB_USER"],
        password=env["DB_PASS"],
    )
