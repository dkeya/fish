from __future__ import annotations

import streamlit as st
import pandas as pd

from core.config import get_settings
from core.db import get_conn, ensure_schema, q
from core.services.batches import create_batch, BatchLineInput


st.set_page_config(page_title="Batch Receipt", page_icon="📥", layout="wide")
st.title("📥 Batch Receipt (Receipt ≈ Production Run)")
st.caption("Each receipt creates a batch with size lines capturing **pieces + kg**, a batch-average fingerprint, and **buy price per kg** for valuation.")

settings = get_settings()
conn = get_conn(settings.db_path)
ensure_schema(conn)

branches = q(conn, "SELECT id, name FROM branches ORDER BY name")
sizes = q(conn, "SELECT id, code, description FROM sizes ORDER BY sort_order")

col1, col2 = st.columns([1, 1], gap="large")

with col1:
    st.subheader("Create a new receipt batch")

    batch_code = st.text_input("Batch code", value="")
    receipt_date = st.date_input("Receipt date")
    branch_name = st.selectbox("Branch", options=[b["name"] for b in branches])
    supplier = st.text_input("Supplier (optional)", value="")
    buy_price_per_kg = st.number_input(
        "Supplier buy price (per kg)",
        min_value=0.0,
        value=0.0,
        step=10.0,
        help="This drives stock valuation, COGS, and gross margin.",
    )
    notes = st.text_area("Notes (optional)", value="", height=80)

    st.markdown("**Enter size lines** (pieces + kg for each size):")

    default_rows = [{"Size": s["code"], "Pieces": 0, "Kg": 0.0} for s in sizes]
    df_in = st.data_editor(
        pd.DataFrame(default_rows),
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        column_config={
            "Pieces": st.column_config.NumberColumn(min_value=0, step=1),
            "Kg": st.column_config.NumberColumn(min_value=0.0, step=0.1),
        },
    )

    if st.button("Create Batch", type="primary"):
        try:
            br_id = next(int(b["id"]) for b in branches if b["name"] == branch_name)
            code_to_id = {s["code"]: int(s["id"]) for s in sizes}

            lines: list[BatchLineInput] = []
            for _, r in df_in.iterrows():
                pcs = int(r["Pieces"])
                kg = float(r["Kg"])
                if pcs > 0 and kg > 0:
                    lines.append(BatchLineInput(size_id=code_to_id[str(r["Size"])], pieces=pcs, kg=kg))

            batch_id = create_batch(
                conn,
                batch_code=batch_code.strip(),
                receipt_date=receipt_date.isoformat(),
                branch_id=br_id,
                supplier=supplier.strip() or None,
                notes=notes.strip() or None,
                buy_price_per_kg=float(buy_price_per_kg),
                lines=lines,
            )
            st.success(f"Batch created (ID: {batch_id}).")
            st.rerun()
        except Exception as e:
            st.error(str(e))

with col2:
    st.subheader("Recent open batches")
    batches = q(
        conn,
        """
        SELECT b.id, b.batch_code, b.receipt_date, br.name AS branch,
               ROUND(b.buy_price_per_kg,2) AS buy_price_per_kg,
               b.initial_pieces, ROUND(b.initial_kg,3) AS initial_kg,
               ROUND(b.batch_avg_kg_per_piece,4) AS avg_kg_per_piece
        FROM batches b
        JOIN branches br ON br.id = b.branch_id
        WHERE b.status='OPEN'
        ORDER BY b.id DESC
        LIMIT 15
        """,
    )
    if batches:
        st.dataframe(pd.DataFrame([dict(r) for r in batches]), use_container_width=True, hide_index=True)
    else:
        st.info("No open batches yet. Create one on the left.")
