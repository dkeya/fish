from __future__ import annotations

import streamlit as st
import pandas as pd

from core.config import get_settings
from core.db import get_conn, ensure_schema, q
from core.services.inventory import batch_on_hand
from core.services.closures import close_batch


st.set_page_config(page_title="Batch Close & Loss", page_icon="✅", layout="wide")
st.title("✅ Batch Close & Loss (Yield variance via water-loss shrinkage)")
st.caption("When a batch is depleted, compute **loss = initial kg − total kg sold** (handling/water-loss).")

settings = get_settings()
conn = get_conn(settings.db_path)
ensure_schema(conn)

open_batches = q(
    conn,
    """
    SELECT b.id, b.batch_code, br.name AS branch, b.initial_pieces, ROUND(b.initial_kg,3) AS initial_kg
    FROM batches b
    JOIN branches br ON br.id = b.branch_id
    WHERE b.status='OPEN'
    ORDER BY b.id DESC
    """,
)

if not open_batches:
    st.info("No open batches to close.")
    st.stop()

batch_code = st.selectbox("Select open batch", options=[b["batch_code"] for b in open_batches])
batch_id = next(int(b["id"]) for b in open_batches if b["batch_code"] == batch_code)

b = q(conn, "SELECT * FROM batches WHERE id=?", (batch_id,))[0]
onhand = batch_on_hand(conn, batch_id)

c1, c2, c3 = st.columns(3)
c1.metric("On-hand pieces", f"{onhand['pcs']}")
c2.metric("On-hand kg", f"{onhand['kg']:.3f}")
c3.metric("Batch avg kg/pc", f"{float(b['batch_avg_kg_per_piece']):.4f}")

st.markdown("**Closure rule:** close only when pieces and kg are at zero; then compute loss.")
st.caption("Use Inventory adjustments to bring close-to-zero kg to 0 within tolerance, while keeping an audit trail.")

notes = st.text_area("Closure notes (optional)", value="", height=80)
tolerance = st.number_input("Auto-zero tolerance (kg) when pcs=0", min_value=0.0, value=0.25, step=0.05, format="%.3f")

if st.button("Close Batch & Compute Loss", type="primary"):
    try:
        res = close_batch(conn, batch_id, notes=notes.strip(), auto_zero_tolerance_kg=float(tolerance))
        st.success(f"Batch closed. Loss: {res['loss_kg']:.3f} kg ({res['loss_pct']:.2f}%).")
        st.rerun()
    except Exception as e:
        st.error(str(e))

st.divider()
st.subheader("Recently closed batches")
closed = q(
    conn,
    """
    SELECT b.batch_code, br.name AS branch, b.receipt_date,
           ROUND(c.loss_kg,3) AS loss_kg, ROUND(c.loss_pct,2) AS loss_pct,
           c.closed_ts
    FROM batch_closures c
    JOIN batches b ON b.id = c.batch_id
    JOIN branches br ON br.id = b.branch_id
    ORDER BY c.id DESC
    LIMIT 25
    """,
)
if closed:
    st.dataframe(pd.DataFrame([dict(r) for r in closed]), use_container_width=True, hide_index=True)
else:
    st.caption("No closed batches yet.")
