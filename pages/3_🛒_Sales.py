from __future__ import annotations

import streamlit as st
import pandas as pd

from core.config import get_settings
from core.db import get_conn, ensure_schema, q
from core.services.sales import create_retail_sale, create_wholesale_sale
from core.services.inventory import batch_on_hand


st.set_page_config(page_title="Sales", page_icon="🛒", layout="wide")
st.title("🛒 Sales (Controlled batch depletion)")
st.caption("Retail: enter **pieces**, system derives **kg**. Wholesale: enter **kg**, confirm **pieces** (control).")

settings = get_settings()
conn = get_conn(settings.db_path)
ensure_schema(conn)

branches = q(conn, "SELECT id, name FROM branches ORDER BY name")
batches = q(
    conn,
    "SELECT id, batch_code, branch_id, batch_avg_kg_per_piece FROM batches WHERE status='OPEN' ORDER BY id DESC",
)

if not batches:
    st.info("No open batches. Create a batch in 📥 Batch Receipt or load demo data in 🧪 Data Management.")
    st.stop()

batches_by_code = {b["batch_code"]: b for b in batches}
branch_by_id = {int(b["id"]): b["name"] for b in branches}


def compute_total_price(unit_price: float | None, price_basis: str, kg_sold: float, pcs_sold: int) -> float | None:
    """
    Best UX rule:
      - PER_KG: total = kg_sold * unit_price
      - PER_PIECE: total = pcs_sold * unit_price
    """
    if unit_price is None:
        return None
    try:
        up = float(unit_price)
    except Exception:
        return None
    if up <= 0:
        return None

    if price_basis == "PER_PIECE":
        return round(int(pcs_sold) * up, 2)
    # default PER_KG
    return round(float(kg_sold) * up, 2)


left, right = st.columns([1, 1], gap="large")

with left:
    st.subheader("Retail sale")

    batch_code = st.selectbox("Batch", options=list(batches_by_code.keys()), key="ret_batch")
    batch = batches_by_code[batch_code]
    branch_name = branch_by_id[int(batch["branch_id"])]
    avg = float(batch["batch_avg_kg_per_piece"])

    st.write(f"Branch: **{branch_name}** • Batch avg: **{avg:.4f} kg/pc**")

    customer = st.text_input("Customer (optional)", value="Walk-in", key="ret_cust")
    pcs_sold = st.number_input("Pieces sold", min_value=1, value=10, step=1, key="ret_pcs")

    price_basis = st.radio(
        "Price basis",
        options=["PER_KG", "PER_PIECE"],
        index=0,  # default PER_KG (most common for fish)
        horizontal=True,
        key="ret_basis",
        help="If PER_KG, unit price is per kg. If PER_PIECE, unit price is per piece.",
    )
    unit_price = st.number_input(
        "Unit price",
        min_value=0.0,
        value=0.0,
        step=10.0,
        key="ret_price",
        help="Per kg (default) or per piece depending on Price basis.",
    )

    implied_kg = float(pcs_sold) * avg
    st.write(f"Implied kg: **{implied_kg:.3f} kg**")

    total_price = compute_total_price(
        unit_price=(float(unit_price) if unit_price > 0 else None),
        price_basis=price_basis,
        kg_sold=implied_kg,
        pcs_sold=int(pcs_sold),
    )
    if total_price is None:
        st.info("Enter a unit price to compute total price automatically.")
    else:
        st.success(f"Total price (auto): **{total_price:,.2f}**")

    onhand = batch_on_hand(conn, int(batch["id"]))
    st.caption(f"On hand: {onhand['pcs']} pcs • {onhand['kg']:.3f} kg")

    if st.button("Post Retail Sale", type="primary"):
        try:
            if pcs_sold > onhand["pcs"]:
                raise ValueError("Not enough pieces on hand.")

            create_retail_sale(
                conn,
                branch_id=int(batch["branch_id"]),
                batch_id=int(batch["id"]),
                size_id=None,
                customer=customer.strip() or None,
                pcs_sold=int(pcs_sold),
                unit_price=(float(unit_price) if unit_price > 0 else None),
                price_basis=price_basis,
                total_price=total_price,
            )
            st.success("Retail sale posted.")
            st.rerun()
        except Exception as e:
            st.error(str(e))

with right:
    st.subheader("Wholesale sale")

    batch_code_w = st.selectbox("Batch", options=list(batches_by_code.keys()), key="wh_batch")
    batch_w = batches_by_code[batch_code_w]
    branch_name_w = branch_by_id[int(batch_w["branch_id"])]
    avg_w = float(batch_w["batch_avg_kg_per_piece"])

    st.write(f"Branch: **{branch_name_w}** • Batch avg: **{avg_w:.4f} kg/pc**")

    customer_w = st.text_input("Customer (optional)", value="Wholesale Customer", key="wh_cust")
    kg_sold = st.number_input("Kg sold", min_value=0.1, value=25.0, step=1.0, format="%.3f", key="wh_kg")

    pcs_suggested = int(round(float(kg_sold) / avg_w))
    pcs_counted = st.number_input(
        "Pieces counted (confirm)",
        min_value=1,
        value=max(1, pcs_suggested),
        step=1,
        key="wh_pcs",
    )
    tolerance = st.number_input("Tolerance (pcs) for variance flag", min_value=0, value=2, step=1, key="wh_tol")

    price_basis_w = st.radio(
        "Price basis",
        options=["PER_KG", "PER_PIECE"],
        index=0,  # default PER_KG
        horizontal=True,
        key="wh_basis",
        help="If PER_KG, unit price is per kg. If PER_PIECE, unit price is per piece.",
    )
    unit_price_w = st.number_input(
        "Unit price",
        min_value=0.0,
        value=0.0,
        step=10.0,
        key="wh_price",
        help="Per kg (default) or per piece depending on Price basis.",
    )

    st.write(f"Suggested pieces from kg/avg: **{pcs_suggested} pcs**")

    total_price_w = compute_total_price(
        unit_price=(float(unit_price_w) if unit_price_w > 0 else None),
        price_basis=price_basis_w,
        kg_sold=float(kg_sold),
        pcs_sold=int(pcs_counted),
    )
    if total_price_w is None:
        st.info("Enter a unit price to compute total price automatically.")
    else:
        st.success(f"Total price (auto): **{total_price_w:,.2f}**")

    onhand_w = batch_on_hand(conn, int(batch_w["id"]))
    st.caption(f"On hand: {onhand_w['pcs']} pcs • {onhand_w['kg']:.3f} kg")

    if st.button("Post Wholesale Sale", type="primary"):
        try:
            if kg_sold > onhand_w["kg"] + 1e-6:
                raise ValueError("Not enough kg on hand (based on posted movements).")
            if pcs_counted > onhand_w["pcs"]:
                raise ValueError("Not enough pieces on hand.")

            res = create_wholesale_sale(
                conn,
                branch_id=int(batch_w["branch_id"]),
                batch_id=int(batch_w["id"]),
                size_id=None,
                customer=customer_w.strip() or None,
                kg_sold=float(kg_sold),
                pcs_counted=int(pcs_counted),
                tolerance_pcs=int(tolerance),
                unit_price=(float(unit_price_w) if unit_price_w > 0 else None),
                price_basis=price_basis_w,
                total_price=total_price_w,
            )
            if res.variance_flag:
                st.warning("Variance flagged: counted pieces differ materially from suggested pieces.")
            st.success("Wholesale sale posted.")
            st.rerun()
        except Exception as e:
            st.error(str(e))

st.divider()
st.subheader("Recent sales")
sales = q(
    conn,
    """
    SELECT s.sale_ts, br.name AS branch, b.batch_code, s.mode,
           s.pcs_sold, ROUND(s.kg_sold,3) AS kg_sold,
           s.unit_price, s.price_basis, s.total_price,
           s.pcs_suggested, s.variance_flag,
           s.customer
    FROM sales s
    JOIN branches br ON br.id = s.branch_id
    JOIN batches b ON b.id = s.batch_id
    ORDER BY s.id DESC
    LIMIT 30
    """,
)
if sales:
    st.dataframe(pd.DataFrame([dict(r) for r in sales]), use_container_width=True, hide_index=True)
else:
    st.caption("No sales yet.")
