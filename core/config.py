from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

import streamlit as st

CONFIG_FILE_NAME = "settings.json"
ENV_DATA_DIR = "FISH_ERP_DATA_DIR"


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    db_path: Path
    currency: str = "KES"


def _default_data_dir() -> Path:
    # Not a hard-coded absolute path: uses the user's home directory.
    return Path.home() / ".fish_erp_demo"


def _load_persisted_settings(data_dir: Path) -> dict:
    cfg = data_dir / CONFIG_FILE_NAME
    if cfg.exists():
        try:
            return json.loads(cfg.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def persist_data_dir(data_dir_str: str) -> None:
    data_dir = Path(data_dir_str).expanduser().resolve()
    data_dir.mkdir(parents=True, exist_ok=True)

    cfg = data_dir / CONFIG_FILE_NAME
    payload = {"data_dir": str(data_dir)}
    cfg.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    # Update session for immediate effect
    st.session_state["fish_erp_data_dir"] = str(data_dir)


@st.cache_resource
def get_settings() -> Settings:
    # Priority order:
    # 1) Session state (set via Data Management page)
    # 2) Environment variable
    # 3) Persisted settings in default folder
    # 4) Default folder
    if "fish_erp_data_dir" in st.session_state:
        data_dir = Path(st.session_state["fish_erp_data_dir"]).expanduser().resolve()
    elif os.getenv(ENV_DATA_DIR):
        data_dir = Path(os.getenv(ENV_DATA_DIR, "")).expanduser().resolve()
    else:
        default_dir = _default_data_dir()
        persisted = _load_persisted_settings(default_dir)
        data_dir = Path(persisted.get("data_dir", default_dir)).expanduser().resolve()

    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "app.db"
    return Settings(data_dir=data_dir, db_path=db_path)
