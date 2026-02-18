from __future__ import annotations

from typing import Any

from core.db import q


def batch_on_hand(conn, batch_id: int) -> dict:
    """
    Batch-level on-hand (existing behavior).
    Uses batches.initial_* and subtracts ALL sales against the batch (regardless of size_id),
    then applies inventory_adjustments at batch level.
    """
    b = q(conn, "SELECT * FROM batches WHERE id=?", (batch_id,))
    if not b:
        return {"pcs": 0, "kg": 0.0}
    b = b[0]

    sold = q(
        conn,
        "SELECT COALESCE(SUM(pcs_sold),0) AS pcs, COALESCE(SUM(kg_sold),0) AS kg FROM sales WHERE batch_id=?",
        (batch_id,),
    )[0]
    adj = q(
        conn,
        "SELECT COALESCE(SUM(pcs_delta),0) AS pcs, COALESCE(SUM(kg_delta),0) AS kg FROM inventory_adjustments WHERE batch_id=?",
        (batch_id,),
    )[0]

    pcs = int(b["initial_pieces"]) - int(sold["pcs"]) + int(adj["pcs"])
    kg = float(b["initial_kg"]) - float(sold["kg"]) + float(adj["kg"])
    return {"pcs": pcs, "kg": kg}


def batch_line_on_hand(conn, batch_id: int, size_id: int) -> dict:
    """
    Size-level on-hand inside a batch.

    This is FIFO-ready: it treats batch_lines (per size) as the initial stock,
    then subtracts sales for the same batch+size_id.

    NOTE:
    - This relies on sales.size_id being set correctly. If sales.size_id is NULL,
      those sales won't be attributed to any size here.
    - inventory_adjustments are currently batch-level (no size_id), so they are not
      allocated to size lines here.
    """
    bl = q(
        conn,
        """
        SELECT pieces, kg
        FROM batch_lines
        WHERE batch_id=? AND size_id=?
        """,
        (batch_id, size_id),
    )
    if not bl:
        return {"pcs": 0, "kg": 0.0}

    bl = bl[0]
    initial_pcs = int(bl["pieces"])
    initial_kg = float(bl["kg"])

    sold = q(
        conn,
        """
        SELECT
          COALESCE(SUM(pcs_sold),0) AS pcs,
          COALESCE(SUM(kg_sold),0) AS kg
        FROM sales
        WHERE batch_id=? AND size_id=?
        """,
        (batch_id, size_id),
    )[0]

    pcs = initial_pcs - int(sold["pcs"])
    kg = initial_kg - float(sold["kg"])
    return {"pcs": pcs, "kg": kg}


def fifo_batches_for_size(conn, *, branch_id: int, size_id: int) -> list[dict[str, Any]]:
    """
    FIFO helper: returns OPEN batches for a branch+size, ordered oldest-first,
    with size-level pcs/kg on-hand (based on batch_lines and sales for that size).

    Sales page will use this to auto-pick batches instead of user selection.
    """
    rows = q(
        conn,
        """
        SELECT
          b.id AS batch_id,
          b.batch_code,
          b.receipt_date,
          b.branch_id,
          b.buy_price_per_kg,
          b.batch_avg_kg_per_piece,
          bl.pieces AS initial_pcs,
          bl.kg AS initial_kg
        FROM batches b
        JOIN batch_lines bl ON bl.batch_id = b.id
        WHERE b.status='OPEN'
          AND b.branch_id=?
          AND bl.size_id=?
        ORDER BY b.receipt_date ASC, b.id ASC
        """,
        (int(branch_id), int(size_id)),
    )

    out: list[dict[str, Any]] = []
    for r in rows:
        onhand = batch_line_on_hand(conn, int(r["batch_id"]), int(size_id))
        # Only return batches that still have stock on-hand for that size
        if onhand["pcs"] > 0 and onhand["kg"] > 0:
            out.append(
                {
                    "batch_id": int(r["batch_id"]),
                    "batch_code": str(r["batch_code"]),
                    "receipt_date": str(r["receipt_date"]),
                    "branch_id": int(r["branch_id"]),
                    "buy_price_per_kg": float(r["buy_price_per_kg"]),
                    "avg_kg_per_piece": float(r["batch_avg_kg_per_piece"]),
                    "pcs_on_hand": int(onhand["pcs"]),
                    "kg_on_hand": float(onhand["kg"]),
                }
            )
    return out


def inventory_summary(conn):
    return q(
        conn,
        """
        WITH sold AS (
          SELECT batch_id,
                 COALESCE(SUM(pcs_sold),0) AS pcs_sold,
                 COALESCE(SUM(kg_sold),0) AS kg_sold
          FROM sales
          GROUP BY batch_id
        ),
        adj AS (
          SELECT batch_id,
                 COALESCE(SUM(pcs_delta),0) AS pcs_adj,
                 COALESCE(SUM(kg_delta),0) AS kg_adj
          FROM inventory_adjustments
          GROUP BY batch_id
        )
        SELECT
          br.name AS branch,
          b.batch_code,
          b.receipt_date,
          ROUND(b.buy_price_per_kg, 2) AS buy_price_per_kg,
          ROUND(b.batch_avg_kg_per_piece, 4) AS avg_kg_per_piece,
          (b.initial_pieces - COALESCE(s.pcs_sold,0) + COALESCE(a.pcs_adj,0)) AS pcs_on_hand,
          ROUND((b.initial_kg - COALESCE(s.kg_sold,0) + COALESCE(a.kg_adj,0)), 3) AS kg_on_hand,
          ROUND(
            (b.initial_kg - COALESCE(s.kg_sold,0) + COALESCE(a.kg_adj,0)) * b.buy_price_per_kg,
            2
          ) AS stock_value
        FROM batches b
        JOIN branches br ON br.id = b.branch_id
        LEFT JOIN sold s ON s.batch_id = b.id
        LEFT JOIN adj a ON a.batch_id = b.id
        WHERE b.status='OPEN'
        ORDER BY b.id DESC
        """,
    )
