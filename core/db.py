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


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name=?
        """,
        (table,),
    ).fetchone()
    return row is not None


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    if not _table_exists(conn, table):
        return False
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    cols = [r["name"] for r in rows]
    return column in cols


def ensure_schema(conn: sqlite3.Connection) -> None:
    # Base schema (fresh installs)
    conn.executescript(SCHEMA_SQL)

    # ---- migrations for existing installs ----

    # batches.buy_price_per_kg
    if _table_exists(conn, "batches") and not _column_exists(conn, "batches", "buy_price_per_kg"):
        conn.execute("ALTER TABLE batches ADD COLUMN buy_price_per_kg REAL NOT NULL DEFAULT 0;")

    # batches.supplier_id
    if _table_exists(conn, "batches") and not _column_exists(conn, "batches", "supplier_id"):
        conn.execute("ALTER TABLE batches ADD COLUMN supplier_id INTEGER;")

    # sales.sale_group_code
    if _table_exists(conn, "sales") and not _column_exists(conn, "sales", "sale_group_code"):
        conn.execute("ALTER TABLE sales ADD COLUMN sale_group_code TEXT;")

    # sales.customer_id
    if _table_exists(conn, "sales") and not _column_exists(conn, "sales", "customer_id"):
        conn.execute("ALTER TABLE sales ADD COLUMN customer_id INTEGER;")

    # sales.product_id
    if _table_exists(conn, "sales") and not _column_exists(conn, "sales", "product_id"):
        conn.execute("ALTER TABLE sales ADD COLUMN product_id INTEGER;")

    # sales.price_basis
    if _table_exists(conn, "sales") and not _column_exists(conn, "sales", "price_basis"):
        conn.execute("ALTER TABLE sales ADD COLUMN price_basis TEXT NOT NULL DEFAULT 'PER_KG';")

    # promo-related sales columns
    if _table_exists(conn, "sales") and not _column_exists(conn, "sales", "promo_applied"):
        conn.execute("ALTER TABLE sales ADD COLUMN promo_applied INTEGER NOT NULL DEFAULT 0;")

    if _table_exists(conn, "sales") and not _column_exists(conn, "sales", "promo_code"):
        conn.execute("ALTER TABLE sales ADD COLUMN promo_code TEXT;")

    if _table_exists(conn, "sales") and not _column_exists(conn, "sales", "promo_name"):
        conn.execute("ALTER TABLE sales ADD COLUMN promo_name TEXT;")

    if _table_exists(conn, "sales") and not _column_exists(conn, "sales", "promo_buy_qty"):
        conn.execute("ALTER TABLE sales ADD COLUMN promo_buy_qty INTEGER;")

    if _table_exists(conn, "sales") and not _column_exists(conn, "sales", "promo_free_qty"):
        conn.execute("ALTER TABLE sales ADD COLUMN promo_free_qty INTEGER;")

    if _table_exists(conn, "sales") and not _column_exists(conn, "sales", "charged_pcs"):
        conn.execute("ALTER TABLE sales ADD COLUMN charged_pcs INTEGER;")

    if _table_exists(conn, "sales") and not _column_exists(conn, "sales", "free_pcs"):
        conn.execute("ALTER TABLE sales ADD COLUMN free_pcs INTEGER NOT NULL DEFAULT 0;")

    if _table_exists(conn, "sales") and not _column_exists(conn, "sales", "promo_discount_value"):
        conn.execute("ALTER TABLE sales ADD COLUMN promo_discount_value REAL;")

    # The newer tables below are already created by SCHEMA_SQL for fresh installs.
    # For existing installs, executescript(SCHEMA_SQL) above will create any missing tables.
    # These checks are included mainly as explicit guardrails for clarity.

    _table_exists(conn, "product_categories")
    _table_exists(conn, "products")
    _table_exists(conn, "branch_products")
    _table_exists(conn, "branch_product_prices")
    _table_exists(conn, "branch_visibility_rules")
    _table_exists(conn, "branch_procurement_rules")
    _table_exists(conn, "stock_transfers")
    _table_exists(conn, "stock_transfer_lines")
    _table_exists(conn, "product_stock_movements")
    _table_exists(conn, "product_transfers")
    _table_exists(conn, "service_sales")

    conn.commit()


def q(conn: sqlite3.Connection, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
    if params is None:
        params = ()
    cur = conn.execute(sql, tuple(params))
    rows = cur.fetchall()
    cur.close()
    return rows


def x(conn: sqlite3.Connection, sql: str, params: Iterable[Any] = ()) -> int:
    if params is None:
        params = ()
    cur = conn.execute(sql, tuple(params))
    conn.commit()
    last = cur.lastrowid
    cur.close()
    return int(last)
