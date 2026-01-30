from __future__ import annotations

from core.db import q


def batch_on_hand(conn, batch_id: int) -> dict:
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
