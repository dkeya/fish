from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Iterable

import streamlit as st

from core.schema import SCHEMA_SQL


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


@st.cache_resource
def get_conn(db_path: Path) -> sqlite3.Connection:
    return _connect(db_path)


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    cols = [r["name"] for r in rows]
    return column in cols


def ensure_schema(conn: sqlite3.Connection) -> None:
    # Create base schema (for new installs)
    conn.executescript(SCHEMA_SQL)

    # ---- migrations for existing installs ----
    # Add buy_price_per_kg to batches if missing
    if not _column_exists(conn, "batches", "buy_price_per_kg"):
        conn.execute("ALTER TABLE batches ADD COLUMN buy_price_per_kg REAL NOT NULL DEFAULT 0;")

    # Add price_basis to sales if missing (needed for auto total price computation)
    if not _column_exists(conn, "sales", "price_basis"):
        conn.execute("ALTER TABLE sales ADD COLUMN price_basis TEXT NOT NULL DEFAULT 'PER_KG';")

    conn.commit()


def q(conn: sqlite3.Connection, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
    cur = conn.execute(sql, tuple(params))
    rows = cur.fetchall()
    cur.close()
    return rows


def x(conn: sqlite3.Connection, sql: str, params: Iterable[Any] = ()) -> int:
    cur = conn.execute(sql, tuple(params))
    conn.commit()
    last = cur.lastrowid
    cur.close()
    return int(last)
