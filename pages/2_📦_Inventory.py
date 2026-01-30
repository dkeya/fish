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

tab1, tab2 = st.tabs(["Inventory View", "Adjustments (Stocktake/Write-off)"])

with tab1:
    rows = inventory_summary(conn)
    if rows:
        st.dataframe(pd.DataFrame([dict(r) for r in rows]), use_container_width=True, hide_index=True)
    else:
        st.info("No open batches found.")

with tab2:
    st.subheader("Create an inventory adjustment (auditable)")
    batches = q(conn, "SELECT id, batch_code FROM batches WHERE status='OPEN' ORDER BY id DESC")
    if not batches:
        st.info("No open batches to adjust.")
    else:
        batch_code = st.selectbox("Batch", options=[b["batch_code"] for b in batches])
        batch_id = next(int(b["id"]) for b in batches if b["batch_code"] == batch_code)

        onhand = batch_on_hand(conn, batch_id)
        st.write(f"On hand now: **{onhand['pcs']} pcs** • **{onhand['kg']:.3f} kg**")

        reason = st.selectbox("Reason", options=["STOCKTAKE", "WRITE_OFF", "QUALITY_TRIM", "OTHER"])
        pcs_delta = st.number_input("Pieces delta (+/-)", value=0, step=1)
        kg_delta = st.number_input("Kg delta (+/-)", value=0.0, step=0.1, format="%.3f")
        notes = st.text_input("Notes (optional)", value="")

        if st.button("Post Adjustment", type="primary"):
            try:
                x(
                    conn,
                    """
                    INSERT INTO inventory_adjustments (ts, batch_id, reason, pcs_delta, kg_delta, notes)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (iso_now(), batch_id, reason, int(pcs_delta), float(kg_delta), notes.strip() or None),
                )
                st.success("Adjustment posted.")
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
