from __future__ import annotations

from core.db import q, x
from core.utils import iso_now, safe_div
from core.services.inventory import batch_on_hand


def close_batch(conn, batch_id: int, *, notes: str = "", auto_zero_tolerance_kg: float = 0.25) -> dict:
    """
    Closure rule (per the provided logic brief):
    - Only close when pieces and kg are at zero (auditable adjustments allowed).
    - When depleted, compute: loss_kg = initial_kg - total_kg_sold
    """
    b = q(conn, "SELECT * FROM batches WHERE id=?", (batch_id,))
    if not b:
        raise ValueError("Batch not found.")
    b = b[0]
    if b["status"] == "CLOSED":
        raise ValueError("Batch already closed.")

    onhand = batch_on_hand(conn, batch_id)

    # If pcs=0 and kg is close-to-zero, auto-adjust kg to 0 for clean closure (auditable).
    if abs(onhand["kg"]) <= float(auto_zero_tolerance_kg) and onhand["pcs"] == 0:
        if onhand["kg"] != 0.0:
            x(
                conn,
                """
                INSERT INTO inventory_adjustments (ts, batch_id, reason, pcs_delta, kg_delta, notes)
                VALUES (?, ?, 'CLOSE_TO_ZERO', 0, ?, ?)
                """,
                (iso_now(), batch_id, float(-onhand["kg"]), "Auto-adjust to zero for closure within tolerance"),
            )
            onhand = batch_on_hand(conn, batch_id)

    if onhand["pcs"] != 0 or abs(onhand["kg"]) > 1e-6:
        raise ValueError("Batch is not depleted. Bring pieces and kg to zero (sales or adjustment) before closing.")

    sold = q(conn, "SELECT COALESCE(SUM(kg_sold),0) AS kg_sold FROM sales WHERE batch_id=?", (batch_id,))[0]
    kg_sold = float(sold["kg_sold"])
    loss_kg = float(b["initial_kg"]) - kg_sold
    loss_pct = safe_div(loss_kg, float(b["initial_kg"])) * 100.0

    x(
        conn,
        """
        INSERT INTO batch_closures (batch_id, closed_ts, loss_kg, loss_pct, notes)
        VALUES (?, ?, ?, ?, ?)
        """,
        (batch_id, iso_now(), float(loss_kg), float(loss_pct), notes),
    )

    x(conn, "UPDATE batches SET status='CLOSED', closed_at=? WHERE id=?", (iso_now(), batch_id))

    return {"loss_kg": loss_kg, "loss_pct": loss_pct}
