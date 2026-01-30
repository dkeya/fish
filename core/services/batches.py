from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from core.db import q, x
from core.utils import safe_div


@dataclass
class BatchLineInput:
    size_id: int
    pieces: int
    kg: float


def list_open_batches(conn):
    return q(conn, "SELECT * FROM batches WHERE status='OPEN' ORDER BY id DESC")


def get_batch(conn, batch_id: int):
    rows = q(conn, "SELECT * FROM batches WHERE id=?", (batch_id,))
    return rows[0] if rows else None


def list_batch_lines(conn, batch_id: int):
    return q(
        conn,
        """
        SELECT bl.*, s.code AS size_code
        FROM batch_lines bl
        JOIN sizes s ON s.id = bl.size_id
        WHERE bl.batch_id=?
        ORDER BY s.sort_order, s.code
        """,
        (batch_id,),
    )


def create_batch(
    conn,
    *,
    batch_code: str,
    receipt_date: str,
    branch_id: int,
    supplier: Optional[str],
    notes: Optional[str],
    buy_price_per_kg: float,
    lines: list[BatchLineInput],
) -> int:
    if not batch_code:
        raise ValueError("Batch code is required.")
    if not lines:
        raise ValueError("At least one size line is required.")

    try:
        buy_price_per_kg = float(buy_price_per_kg)
    except Exception:
        raise ValueError("Buy price per kg must be a number.")

    if buy_price_per_kg <= 0:
        raise ValueError("Buy price per kg must be > 0.")

    total_pcs = sum(int(l.pieces) for l in lines)
    total_kg = sum(float(l.kg) for l in lines)

    if total_pcs <= 0:
        raise ValueError("Total pieces must be > 0.")
    if total_kg <= 0:
        raise ValueError("Total kg must be > 0.")

    batch_avg = safe_div(total_kg, total_pcs)

    batch_id = x(
        conn,
        """
        INSERT INTO batches (
            batch_code, receipt_date, branch_id, supplier, notes,
            buy_price_per_kg,
            initial_pieces, initial_kg, batch_avg_kg_per_piece, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN')
        """,
        (
            batch_code,
            receipt_date,
            branch_id,
            supplier,
            notes,
            float(buy_price_per_kg),
            total_pcs,
            total_kg,
            batch_avg,
        ),
    )

    for l in lines:
        avg = safe_div(l.kg, l.pieces)
        x(
            conn,
            """
            INSERT INTO batch_lines (batch_id, size_id, pieces, kg, avg_kg_per_piece)
            VALUES (?, ?, ?, ?, ?)
            """,
            (batch_id, l.size_id, int(l.pieces), float(l.kg), avg),
        )

    return batch_id
