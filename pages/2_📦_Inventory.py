from __future__ import annotations

import streamlit as st
import pandas as pd

from core.config import get_settings
from core.db import get_conn, ensure_schema, q, x
from core.services.inventory import inventory_summary, batch_on_hand
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

# For now, respect a session role if already set elsewhere.
# If none is set yet, default to Admin so current single-user workflow keeps working.
current_role = (
    st.session_state.get("user_role")
    or st.session_state.get("erp_role")
    or "Admin"
)

tab1, tab2 = st.tabs(["Inventory View", "Adjustments (Admin Only)"])

with tab1:
    rows = inventory_summary(conn)
    if rows:
        st.dataframe(pd.DataFrame([dict(r) for r in rows]), use_container_width=True, hide_index=True)
    else:
        st.info("No open batches found.")

with tab2:
    st.subheader("Create an inventory adjustment (auditable)")
    st.caption("Adjustments are restricted to Admin. Input is in pieces only; kg is auto-calculated from the batch average.")

    if str(current_role).strip().lower() != "admin":
        st.warning("Stock adjustment is restricted to Admin only.")
        st.info("Your current role does not permit stock adjustments.")
    elif st.session_state["inventory_adjustment_submitted"]:
        st.success("Adjustment posted successfully.")
        st.info("The action has been completed. Reload or navigate away to make another adjustment.")
        if st.button("Create Another Adjustment"):
            st.session_state["inventory_adjustment_submitted"] = False
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
                    st.rerun()
                except Exception as e:
                    st.error(str(e))

    st.divider()
    st.subheader("Recent adjustments")
    adj = q(
        conn,
        """
        SELECT ia.ts, b.batch_code, ia.reason, ia.pcs_delta, ROUND(ia.kg_delta,3) AS kg_delta, ia.notes
        FROM inventory_adjustments ia
        JOIN batches b ON b.id = ia.batch_id
        ORDER BY ia.id DESC
        LIMIT 25
        """,
    )
    if adj:
        st.dataframe(pd.DataFrame([dict(r) for r in adj]), use_container_width=True, hide_index=True)
    else:
        st.caption("No adjustments yet.")