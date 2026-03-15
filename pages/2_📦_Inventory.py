from __future__ import annotations

import streamlit as st
import pandas as pd

from core.config import get_settings
from core.db import get_conn, ensure_schema, q, x
from core.services.inventory import (
    inventory_summary_visible_to_branch,
    size_inventory_summary_visible_to_branch,
    batch_on_hand,
)
from core.utils import iso_now


st.set_page_config(page_title="Inventory", page_icon="📦", layout="wide")
st.title("📦 Inventory (Pieces + Kg move together)")
st.caption("Open-batch inventory is computed from: initial receipt − sales + adjustments (auditable).")

settings = get_settings()
conn = get_conn(settings.db_path)
ensure_schema(conn)

# -------------------------
# Session state / role handling
# -------------------------
if "inventory_adjustment_submitted" not in st.session_state:
    st.session_state["inventory_adjustment_submitted"] = False

if "inventory_adjustment_result" not in st.session_state:
    st.session_state["inventory_adjustment_result"] = None

current_role = (
    st.session_state.get("user_role")
    or st.session_state.get("erp_role")
    or "Admin"
)

user_branch_id = st.session_state.get("user_branch_id")
legacy_extra_visible_branch_ids = st.session_state.get("visible_branch_ids", [])


def _reset_adjustment_state() -> None:
    st.session_state["inventory_adjustment_submitted"] = False
    st.session_state["inventory_adjustment_result"] = None


def _get_effective_visible_branch_ids(
    *,
    conn,
    branch_id: int,
    role: str,
    legacy_extra_ids: list[int] | None = None,
) -> list[int]:
    role_norm = str(role).strip().lower()

    if role_norm == "admin":
        rows = q(conn, "SELECT id FROM branches ORDER BY id")
        return [int(r["id"]) for r in rows]

    visible_ids = {int(branch_id)}

    # DB-driven visibility rules
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
        visible_ids.add(int(r["visible_branch_id"]))

    # Keep backward compatibility with any legacy session-based visibility
    if legacy_extra_ids:
        for bid in legacy_extra_ids:
            try:
                visible_ids.add(int(bid))
            except Exception:
                continue

    return sorted(visible_ids)


# Safe fallback for current single-user setup
if user_branch_id is None:
    branch_rows = q(conn, "SELECT id FROM branches ORDER BY id LIMIT 1")
    user_branch_id = int(branch_rows[0]["id"]) if branch_rows else 0

effective_visible_branch_ids = _get_effective_visible_branch_ids(
    conn=conn,
    branch_id=int(user_branch_id),
    role=str(current_role),
    legacy_extra_ids=legacy_extra_visible_branch_ids,
)

visible_branch_rows = []
if effective_visible_branch_ids:
    placeholders = ",".join("?" for _ in effective_visible_branch_ids)
    visible_branch_rows = q(
        conn,
        f"""
        SELECT id, name
        FROM branches
        WHERE id IN ({placeholders})
        ORDER BY name
        """,
        tuple(int(x) for x in effective_visible_branch_ids),
    )

tab1, tab2 = st.tabs(["Inventory View", "Adjustments (Admin Only)"])

with tab1:
    st.subheader("Visible inventory")

    if str(current_role).strip().lower() == "admin":
        st.caption("Admin view: all branches visible.")
    else:
        visible_names = [str(r["name"]) for r in visible_branch_rows]
        if visible_names:
            st.caption("Visible branches: " + ", ".join(visible_names))
        else:
            st.caption("Visible branches: own branch only.")

    rows = inventory_summary_visible_to_branch(
        conn,
        branch_id=int(user_branch_id),
        role=str(current_role),
        extra_visible_branch_ids=effective_visible_branch_ids,
    )

    if rows:
        inv_df = pd.DataFrame([dict(r) for r in rows])
        st.dataframe(inv_df, use_container_width=True, hide_index=True)

        c1, c2, c3 = st.columns(3)
        c1.metric("Visible Batches", f"{len(inv_df)}")
        c2.metric("Visible Pieces", f"{int(inv_df['pcs_on_hand'].sum()):,}")
        c3.metric("Visible Kg", f"{float(inv_df['kg_on_hand'].sum()):,.3f}")
    else:
        st.info("No visible open batches found.")

    st.divider()
    st.subheader("Visible inventory by size")

    size_rows = size_inventory_summary_visible_to_branch(
        conn,
        branch_id=int(user_branch_id),
        role=str(current_role),
        extra_visible_branch_ids=effective_visible_branch_ids,
    )

    if size_rows:
        size_df = pd.DataFrame(size_rows)
        st.dataframe(size_df, use_container_width=True, hide_index=True)
    else:
        st.caption("No visible size-level inventory found.")

with tab2:
    st.subheader("Create an inventory adjustment (auditable)")
    st.caption("Adjustments are restricted to Admin. Input is in pieces only; kg is auto-calculated from the batch average.")

    if str(current_role).strip().lower() != "admin":
        st.warning("Stock adjustment is restricted to Admin only.")
        st.info("Your current role does not permit stock adjustments.")
    elif st.session_state["inventory_adjustment_submitted"]:
        result = st.session_state.get("inventory_adjustment_result") or {}
        st.success("Adjustment posted successfully.")

        if result:
            st.write(
                f"**Batch:** {result.get('batch_code', '-')}"
                f"  |  **Reason:** {result.get('reason', '-')}"
                f"  |  **Pieces Delta:** {result.get('pcs_delta', 0):+d}"
                f"  |  **Kg Delta:** {result.get('kg_delta', 0.0):+.3f}"
            )
            st.caption(
                f"Updated on hand: {result.get('projected_pcs', 0)} pcs • "
                f"{result.get('projected_kg', 0.0):.3f} kg"
            )

        st.info("This adjustment has already been posted. Start a new adjustment only if needed.")
        if st.button("Create Another Adjustment", type="primary"):
            _reset_adjustment_state()
            st.rerun()
    else:
        batches = q(
            conn,
            """
            SELECT id, batch_code, batch_avg_kg_per_piece
            FROM batches
            WHERE status='OPEN'
            ORDER BY id DESC
            """
        )

        if not batches:
            st.info("No open batches to adjust.")
        else:
            batch_code = st.selectbox("Batch", options=[b["batch_code"] for b in batches])
            batch_row = next(b for b in batches if b["batch_code"] == batch_code)
            batch_id = int(batch_row["id"])
            batch_avg = float(batch_row["batch_avg_kg_per_piece"])

            onhand = batch_on_hand(conn, batch_id)
            st.write(f"On hand now: **{onhand['pcs']} pcs** • **{onhand['kg']:.3f} kg**")
            st.write(f"Batch average: **{batch_avg:.4f} kg/pc**")

            reason = st.selectbox("Reason", options=["STOCKTAKE", "WRITE_OFF", "QUALITY_TRIM", "OTHER"])
            pcs_delta = st.number_input("Pieces delta (+/-)", value=0, step=1)
            auto_kg_delta = round(float(pcs_delta) * batch_avg, 3)
            notes = st.text_input("Notes (optional)", value="")

            st.info(f"Auto-calculated kg delta: **{auto_kg_delta:+.3f} kg**")

            projected_pcs = int(onhand["pcs"]) + int(pcs_delta)
            projected_kg = float(onhand["kg"]) + float(auto_kg_delta)
            st.caption(f"Projected on hand after adjustment: {projected_pcs} pcs • {projected_kg:.3f} kg")

            if st.button("Post Adjustment", type="primary"):
                try:
                    if int(pcs_delta) == 0:
                        raise ValueError("Pieces delta cannot be 0.")

                    x(
                        conn,
                        """
                        INSERT INTO inventory_adjustments (ts, batch_id, reason, pcs_delta, kg_delta, notes)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            iso_now(),
                            batch_id,
                            reason,
                            int(pcs_delta),
                            float(auto_kg_delta),
                            notes.strip() or None,
                        ),
                    )

                    st.session_state["inventory_adjustment_submitted"] = True
                    st.session_state["inventory_adjustment_result"] = {
                        "batch_code": batch_code,
                        "reason": reason,
                        "pcs_delta": int(pcs_delta),
                        "kg_delta": float(auto_kg_delta),
                        "projected_pcs": int(projected_pcs),
                        "projected_kg": float(projected_kg),
                    }
                    st.rerun()
                except Exception as e:
                    st.error(str(e))

    st.divider()
    st.subheader("Recent adjustments")

    if effective_visible_branch_ids:
        placeholders = ",".join("?" for _ in effective_visible_branch_ids)
        adj = q(
            conn,
            f"""
            SELECT
                ia.ts,
                br.name AS branch,
                b.batch_code,
                ia.reason,
                ia.pcs_delta,
                ROUND(ia.kg_delta,3) AS kg_delta,
                ia.notes
            FROM inventory_adjustments ia
            JOIN batches b ON b.id = ia.batch_id
            JOIN branches br ON br.id = b.branch_id
            WHERE b.branch_id IN ({placeholders})
            ORDER BY ia.id DESC
            LIMIT 25
            """,
            tuple(int(x) for x in effective_visible_branch_ids),
        )
    else:
        adj = []

    if adj:
        st.dataframe(pd.DataFrame([dict(r) for r in adj]), use_container_width=True, hide_index=True)
    else:
        st.caption("No adjustments yet.")