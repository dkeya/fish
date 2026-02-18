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
    buy_price_per_kg: Optional[float] = None  # NEW: allow per-size price


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


def _normalize_branch_code(branch_name: str) -> str:
    # Simple predictable code: take first 3 letters of each word, joined.
    # "Nairobi East" -> "NAIEAS"
    parts = [p for p in str(branch_name).strip().upper().split() if p]
    if not parts:
        return "BR"
    if len(parts) == 1:
        return parts[0][:6]
    return "".join(p[:3] for p in parts)[:8]


def _get_branch_name(conn, branch_id: int) -> str:
    r = q(conn, "SELECT name FROM branches WHERE id=?", (int(branch_id),))
    if not r:
        raise ValueError("Branch not found.")
    return str(r[0]["name"])


def _get_size_code(conn, size_id: int) -> str:
    r = q(conn, "SELECT code FROM sizes WHERE id=?", (int(size_id),))
    if not r:
        raise ValueError("Size not found.")
    return str(r[0]["code"]).strip().upper()


def _generate_batch_code(conn, *, branch_id: int, size_id: int, receipt_date: str) -> str:
    """
    Consistent system code:
      {BRANCHCODE}-{SIZE}-{YYYYMMDD}-{NNN}

    Example:
      NAIWES-SIZE2-20260218-001
    """
    branch_name = _get_branch_name(conn, int(branch_id))
    br_code = _normalize_branch_code(branch_name)
    size_code = _get_size_code(conn, int(size_id))

    ymd = str(receipt_date).replace("-", "")
    prefix = f"{br_code}-{size_code}-{ymd}-"

    r = q(conn, "SELECT COUNT(1) AS n FROM batches WHERE batch_code LIKE ?", (prefix + "%",))
    n = int(r[0]["n"]) if r else 0
    seq = n + 1

    return f"{prefix}{seq:03d}"


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
    """
    Creates ONE batch (can still carry multiple lines, but FIFO-by-size works best when each
    batch is size-specific: i.e., lines contains exactly one entry).
    """
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
            int(branch_id),
            supplier,
            notes,
            float(buy_price_per_kg),
            int(total_pcs),
            float(total_kg),
            float(batch_avg),
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
            (int(batch_id), int(l.size_id), int(l.pieces), float(l.kg), float(avg)),
        )

    return int(batch_id)


def create_batches_from_purchase(
    conn,
    *,
    receipt_date: str,
    branch_id: int,
    supplier: Optional[str],
    notes: Optional[str],
    lines: list[BatchLineInput],
) -> list[int]:
    """
    NEW: One purchase can generate MULTIPLE batches (one per fish size).
    Each line becomes its own batch so buy_price_per_kg can differ per product/size.
    """
    if not lines:
        raise ValueError("At least one size line is required.")

    created_ids: list[int] = []

    for l in lines:
        pcs = int(l.pieces)
        kg = float(l.kg)
        if pcs <= 0 or kg <= 0:
            continue

        if l.buy_price_per_kg is None:
            raise ValueError("Buy price per kg is required for each size line.")
        try:
            bp = float(l.buy_price_per_kg)
        except Exception:
            raise ValueError("Buy price per kg must be a number.")
        if bp <= 0:
            raise ValueError("Buy price per kg must be > 0 for each size line.")

        batch_code = _generate_batch_code(
            conn,
            branch_id=int(branch_id),
            size_id=int(l.size_id),
            receipt_date=str(receipt_date),
        )

        batch_id = create_batch(
            conn,
            batch_code=batch_code,
            receipt_date=str(receipt_date),
            branch_id=int(branch_id),
            supplier=supplier,
            notes=notes,
            buy_price_per_kg=float(bp),
            lines=[BatchLineInput(size_id=int(l.size_id), pieces=pcs, kg=kg)],
        )
        created_ids.append(int(batch_id))

    if not created_ids:
        raise ValueError("No valid lines found. Enter pieces + kg > 0 for at least one size.")
    return created_ids
