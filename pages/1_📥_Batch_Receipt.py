from __future__ import annotations

import streamlit as st
import pandas as pd

from core.config import get_settings
from core.db import get_conn, ensure_schema, q
from core.services.batches import create_batches_from_purchase, BatchLineInput


st.set_page_config(page_title="Stock In", page_icon="📥", layout="wide")
st.title("📥 Stock In (Auto batch creation per size)")
st.caption(
    "One purchase can create **multiple FIFO batches** (one per fish size). "
    "Batch codes are system-generated. Buy price is captured **per size**."
)

settings = get_settings()
conn = get_conn(settings.db_path)
ensure_schema(conn)

branches = q(conn, "SELECT id, name FROM branches ORDER BY name")
sizes = q(conn, "SELECT id, code, description FROM sizes ORDER BY sort_order")

col1, col2 = st.columns([1, 1], gap="large")

with col1:
    st.subheader("Create stock-in (auto batches per size)")

    receipt_date = st.date_input("Receipt date")
    branch_name = st.selectbox("Branch", options=[b["name"] for b in branches])
    supplier = st.text_input("Supplier (optional)", value="")
    notes = st.text_area("Notes (optional)", value="", height=80)

    st.markdown("**Enter lines per size** (pieces + kg + buy price/kg):")

    default_rows = [{"Size": s["code"], "Pieces": 0, "Kg": 0.0, "BuyPrice/kg": 0.0} for s in sizes]
    df_in = st.data_editor(
        pd.DataFrame(default_rows),
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        column_config={
            "Pieces": st.column_config.NumberColumn(min_value=0, step=1),
            "Kg": st.column_config.NumberColumn(min_value=0.0, step=0.1),
            "BuyPrice/kg": st.column_config.NumberColumn(min_value=0.0, step=1.0),
        },
    )

    if st.button("Create Stock-In (Auto Batches)", type="primary"):
        try:
            br_id = next(int(b["id"]) for b in branches if b["name"] == branch_name)
            code_to_id = {str(s["code"]): int(s["id"]) for s in sizes}

            lines: list[BatchLineInput] = []
            for _, r in df_in.iterrows():
                size_code = str(r["Size"])
                pcs = int(r["Pieces"])
                kg = float(r["Kg"])
                bp = float(r["BuyPrice/kg"])

                if pcs > 0 and kg > 0:
                    if bp <= 0:
                        raise ValueError(f"BuyPrice/kg must be > 0 for size {size_code}.")
                    lines.append(
                        BatchLineInput(
                            size_id=code_to_id[size_code],
                            pieces=pcs,
                            kg=kg,
                            buy_price_per_kg=bp,
                        )
                    )

            created = create_batches_from_purchase(
                conn,
                receipt_date=receipt_date.isoformat(),
                branch_id=br_id,
                supplier=supplier.strip() or None,
                notes=notes.strip() or None,
                lines=lines,
            )
            st.success(f"Created {len(created)} batch(es): {', '.join(map(str, created))}")
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
        LIMIT 20
        """,
    )
    if batches:
        st.dataframe(pd.DataFrame([dict(r) for r in batches]), use_container_width=True, hide_index=True)
    else:
        st.info("No open batches yet. Create stock-in on the left.")
