from __future__ import annotations

from datetime import datetime
from typing import Any

from core.db import q, x


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

    Sales page uses this to auto-pick batches instead of user selection.
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


def get_allowed_visible_branch_ids(
    conn,
    *,
    branch_id: int,
    role: str = "Admin",
    extra_visible_branch_ids: list[int] | None = None,
) -> list[int]:
    """
    Controlled stock visibility.

    Rules:
    - Admin can see all branches.
    - Non-admin sees own branch by default.
    - Non-admin also sees branches approved in branch_visibility_rules.
    - Optional extra_visible_branch_ids are preserved for backward compatibility.
    """
    role_norm = str(role).strip().lower()
    if role_norm == "admin":
        rows = q(conn, "SELECT id FROM branches ORDER BY id")
        return [int(r["id"]) for r in rows]

    visible = {int(branch_id)}

    db_rows = q(
        conn,
        """
        SELECT visible_branch_id
        FROM branch_visibility_rules
        WHERE viewer_branch_id=? AND is_active=1
        """,
        (int(branch_id),),
    )
    for r in db_rows:
        visible.add(int(r["visible_branch_id"]))

    if extra_visible_branch_ids:
        for bid in extra_visible_branch_ids:
            try:
                visible.add(int(bid))
            except Exception:
                continue

    return sorted(visible)


def get_branch_procurement_rule(conn, *, branch_id: int) -> dict:
    """
    Returns the procurement rule for a branch.
    Safe defaults preserve current behavior if no rule exists.
    """
    rows = q(
        conn,
        """
        SELECT
            bpr.branch_id,
            bpr.can_purchase_direct,
            bpr.can_receive_transfer,
            bpr.default_source_branch_id,
            bpr.notes,
            src.name AS default_source_branch_name
        FROM branch_procurement_rules bpr
        LEFT JOIN branches src ON src.id = bpr.default_source_branch_id
        WHERE bpr.branch_id=?
        """,
        (int(branch_id),),
    )

    if not rows:
        return {
            "branch_id": int(branch_id),
            "can_purchase_direct": 1,
            "can_receive_transfer": 1,
            "default_source_branch_id": None,
            "default_source_branch_name": None,
            "notes": None,
        }

    r = rows[0]
    return {
        "branch_id": int(r["branch_id"]),
        "can_purchase_direct": int(r["can_purchase_direct"]),
        "can_receive_transfer": int(r["can_receive_transfer"]),
        "default_source_branch_id": int(r["default_source_branch_id"]) if r["default_source_branch_id"] is not None else None,
        "default_source_branch_name": str(r["default_source_branch_name"]) if r["default_source_branch_name"] is not None else None,
        "notes": str(r["notes"]) if r["notes"] is not None else None,
    }


def inventory_summary(conn):
    """
    Full inventory summary across all branches.
    Existing behavior preserved.
    """
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
          b.branch_id AS branch_id,
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


def inventory_summary_for_branch(conn, *, branch_id: int):
    """
    Inventory summary limited to a single branch.
    """
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
          b.branch_id AS branch_id,
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
          AND b.branch_id=?
        ORDER BY b.id DESC
        """,
        (int(branch_id),),
    )


def inventory_summary_visible_to_branch(
    conn,
    *,
    branch_id: int,
    role: str = "Admin",
    extra_visible_branch_ids: list[int] | None = None,
):
    """
    Inventory summary filtered by the branches visible to the current user.
    """
    visible_branch_ids = get_allowed_visible_branch_ids(
        conn,
        branch_id=int(branch_id),
        role=str(role),
        extra_visible_branch_ids=extra_visible_branch_ids,
    )

    if not visible_branch_ids:
        return []

    placeholders = ",".join("?" for _ in visible_branch_ids)

    return q(
        conn,
        f"""
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
          b.branch_id AS branch_id,
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
          AND b.branch_id IN ({placeholders})
        ORDER BY br.name, b.id DESC
        """,
        tuple(int(bid) for bid in visible_branch_ids),
    )


def size_inventory_summary_visible_to_branch(
    conn,
    *,
    branch_id: int,
    role: str = "Admin",
    extra_visible_branch_ids: list[int] | None = None,
):
    """
    Size-level inventory view across visible branches.

    Useful for:
    - showing what a branch can see by fish size
    - future transfer workflows
    - future stock-availability lookup by size
    """
    visible_branch_ids = get_allowed_visible_branch_ids(
        conn,
        branch_id=int(branch_id),
        role=str(role),
        extra_visible_branch_ids=extra_visible_branch_ids,
    )

    if not visible_branch_ids:
        return []

    rows: list[dict[str, Any]] = []
    placeholders = ",".join("?" for _ in visible_branch_ids)

    batch_rows = q(
        conn,
        f"""
        SELECT
          b.id AS batch_id,
          b.batch_code,
          b.branch_id,
          br.name AS branch,
          b.receipt_date,
          b.buy_price_per_kg,
          bl.size_id,
          s.code AS size_code,
          bl.pieces AS initial_pcs,
          bl.kg AS initial_kg,
          bl.avg_kg_per_piece
        FROM batches b
        JOIN branches br ON br.id = b.branch_id
        JOIN batch_lines bl ON bl.batch_id = b.id
        JOIN sizes s ON s.id = bl.size_id
        WHERE b.status='OPEN'
          AND b.branch_id IN ({placeholders})
        ORDER BY br.name, s.sort_order, b.receipt_date, b.id
        """,
        tuple(int(bid) for bid in visible_branch_ids),
    )

    for r in batch_rows:
        onhand = batch_line_on_hand(conn, int(r["batch_id"]), int(r["size_id"]))
        rows.append(
            {
                "branch": str(r["branch"]),
                "branch_id": int(r["branch_id"]),
                "batch_code": str(r["batch_code"]),
                "receipt_date": str(r["receipt_date"]),
                "size_code": str(r["size_code"]),
                "buy_price_per_kg": round(float(r["buy_price_per_kg"]), 2),
                "avg_kg_per_piece": round(float(r["avg_kg_per_piece"]), 4),
                "pcs_on_hand": int(onhand["pcs"]),
                "kg_on_hand": round(float(onhand["kg"]), 3),
                "stock_value": round(float(onhand["kg"]) * float(r["buy_price_per_kg"]), 2),
            }
        )

    return rows


def transfer_candidates_for_size(
    conn,
    *,
    from_branch_id: int,
    size_id: int,
) -> list[dict[str, Any]]:
    """
    Returns transferable FIFO candidates for a source branch and fish size.
    """
    return fifo_batches_for_size(
        conn,
        branch_id=int(from_branch_id),
        size_id=int(size_id),
    )


def _generate_transfer_code(conn, *, prefix: str = "TRF") -> str:
    ymd = datetime.now().strftime("%Y%m%d")
    like_prefix = f"{prefix}-{ymd}-%"
    rows = q(
        conn,
        "SELECT COUNT(*) AS n FROM stock_transfers WHERE transfer_code LIKE ?",
        (like_prefix,),
    )
    seq = int(rows[0]["n"]) + 1 if rows else 1
    return f"{prefix}-{ymd}-{seq:03d}"


def create_stock_transfer(
    conn,
    *,
    from_branch_id: int,
    to_branch_id: int,
    size_id: int,
    pieces: int,
    notes: str | None = None,
    created_by: str | None = None,
) -> dict:
    """
    Fish stock transfer foundation.

    Behavior:
    - moves stock oldest-first from source branch for a given size
    - creates transfer header + lines
    - creates new destination batches preserving valuation
    - source stock is reduced because transfer-out is recorded against source batches
      using inventory_adjustments
    - destination receives new batches with the same unit cost basis

    This is intentionally conservative and auditable.
    """
    if int(from_branch_id) <= 0 or int(to_branch_id) <= 0:
        raise ValueError("Both source and destination branches are required.")
    if int(from_branch_id) == int(to_branch_id):
        raise ValueError("Source and destination branches must be different.")
    if int(size_id) <= 0:
        raise ValueError("Size is required.")
    if int(pieces) <= 0:
        raise ValueError("Transfer pieces must be greater than 0.")

    # Destination must be able to receive transfers
    dest_rule = get_branch_procurement_rule(conn, branch_id=int(to_branch_id))
    if int(dest_rule["can_receive_transfer"]) != 1:
        raise ValueError("Destination branch is not allowed to receive transfers.")

    fifo = transfer_candidates_for_size(
        conn,
        from_branch_id=int(from_branch_id),
        size_id=int(size_id),
    )
    if not fifo:
        raise ValueError("No transferable stock available for the selected size in the source branch.")

    remaining_pieces = int(pieces)
    allocations: list[dict[str, Any]] = []

    for row in fifo:
        if remaining_pieces <= 0:
            break

        take_pcs = min(int(row["pcs_on_hand"]), remaining_pieces)
        if take_pcs <= 0:
            continue

        avg = float(row["avg_kg_per_piece"])
        take_kg = round(float(take_pcs) * avg, 3)

        allocations.append(
            {
                "from_batch_id": int(row["batch_id"]),
                "from_batch_code": str(row["batch_code"]),
                "size_id": int(size_id),
                "pieces": int(take_pcs),
                "kg": float(take_kg),
                "avg_kg_per_piece": float(avg),
                "unit_cost_per_kg": float(row["buy_price_per_kg"]),
            }
        )
        remaining_pieces -= int(take_pcs)

    if remaining_pieces > 0:
        raise ValueError("Not enough pieces available to complete this transfer.")

    transfer_code = _generate_transfer_code(conn)

    transfer_id = x(
        conn,
        """
        INSERT INTO stock_transfers(
            transfer_code, transfer_ts, from_branch_id, to_branch_id, status, notes, created_by
        )
        VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?, 'POSTED', ?, ?)
        """,
        (int(from_branch_id), int(to_branch_id), notes.strip() if notes else None, created_by),
    )

    # Fix transfer code explicitly because insert above uses CURRENT_TIMESTAMP in first col slot otherwise
    # We do a safe corrective update tied to the created row.
    x(
        conn,
        """
        UPDATE stock_transfers
        SET transfer_code=?, transfer_ts=CURRENT_TIMESTAMP
        WHERE id=?
        """,
        (transfer_code, int(transfer_id)),
    )

    # Lookup helpers
    size_row = q(conn, "SELECT code FROM sizes WHERE id=?", (int(size_id),))
    size_code = str(size_row[0]["code"]) if size_row else f"SIZE{int(size_id)}"

    to_branch_row = q(conn, "SELECT name FROM branches WHERE id=?", (int(to_branch_id),))
    to_branch_name = str(to_branch_row[0]["name"]) if to_branch_row else f"BR{int(to_branch_id)}"

    created_destination_batch_ids: list[int] = []

    for idx, a in enumerate(allocations, start=1):
        x(
            conn,
            """
            INSERT INTO stock_transfer_lines(
                transfer_id, from_batch_id, size_id, pieces, kg, avg_kg_per_piece, unit_cost_per_kg
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(transfer_id),
                int(a["from_batch_id"]),
                int(a["size_id"]),
                int(a["pieces"]),
                float(a["kg"]),
                float(a["avg_kg_per_piece"]),
                float(a["unit_cost_per_kg"]),
            ),
        )

        # Reduce source stock through auditable adjustment
        x(
            conn,
            """
            INSERT INTO inventory_adjustments(ts, batch_id, reason, pcs_delta, kg_delta, notes)
            VALUES (
                CURRENT_TIMESTAMP, ?, 'TRANSFER_OUT', ?, ?,
                ?
            )
            """,
            (
                int(a["from_batch_id"]),
                -int(a["pieces"]),
                -float(a["kg"]),
                f"Transfer out via {transfer_code} to branch {to_branch_name}",
            ),
        )

        # Create destination batch to preserve auditable lineage + valuation
        dest_batch_code = f"{transfer_code}-{size_code}-{idx:03d}"
        dest_batch_id = x(
            conn,
            """
            INSERT INTO batches(
                batch_code, receipt_date, branch_id, supplier, supplier_id, notes,
                buy_price_per_kg, initial_pieces, initial_kg, batch_avg_kg_per_piece, status
            )
            VALUES (?, DATE('now'), ?, ?, NULL, ?, ?, ?, ?, ?, 'OPEN')
            """,
            (
                dest_batch_code,
                int(to_branch_id),
                "INTERNAL TRANSFER",
                f"Received from transfer {transfer_code}",
                float(a["unit_cost_per_kg"]),
                int(a["pieces"]),
                float(a["kg"]),
                float(a["avg_kg_per_piece"]),
            ),
        )

        x(
            conn,
            """
            INSERT INTO batch_lines(batch_id, size_id, pieces, kg, avg_kg_per_piece)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                int(dest_batch_id),
                int(a["size_id"]),
                int(a["pieces"]),
                float(a["kg"]),
                float(a["avg_kg_per_piece"]),
            ),
        )

        created_destination_batch_ids.append(int(dest_batch_id))

    return {
        "transfer_id": int(transfer_id),
        "transfer_code": str(transfer_code),
        "from_branch_id": int(from_branch_id),
        "to_branch_id": int(to_branch_id),
        "size_id": int(size_id),
        "pieces_transferred": int(sum(int(a["pieces"]) for a in allocations)),
        "kg_transferred": round(sum(float(a["kg"]) for a in allocations), 3),
        "lines_created": int(len(allocations)),
        "destination_batch_ids": created_destination_batch_ids,
    }
