from __future__ import annotations

import streamlit as st
import pandas as pd

from core.config import get_settings
from core.db import get_conn, ensure_schema, q
from core.services.sales import create_retail_sale_fifo, create_wholesale_sale_fifo


st.set_page_config(page_title="Sales", page_icon="🛒", layout="wide")
st.title("🛒 Sales")
st.caption("FIFO is automatic: pick **Branch + Size** only. Retail/Wholesale are tabs. Totals auto-compute.")

settings = get_settings()
conn = get_conn(settings.db_path)
ensure_schema(conn)

branches = q(conn, "SELECT id, name FROM branches ORDER BY name")
sizes = q(conn, "SELECT id, code, description FROM sizes ORDER BY sort_order, code")

if not branches or not sizes:
    st.info("Reference data missing. Load demo data in 🧪 Data Management first.")
    st.stop()

branch_name = st.selectbox("Branch", options=[b["name"] for b in branches], index=0, key="sale_branch")
branch_id = next(int(b["id"]) for b in branches if b["name"] == branch_name)

size_label = st.selectbox(
    "Fish size",
    options=[f"{s['code']}" for s in sizes],
    index=0,
    key="sale_size",
)
size_id = next(int(s["id"]) for s in sizes if str(s["code"]) == str(size_label))

tab_retail, tab_wholesale = st.tabs(["Retail (per pieces)", "Wholesale (per kg)"])


def _compute_total_price(unit_price: float | None, price_basis: str, kg_sold: float, pcs_sold: int) -> float | None:
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
    return round(float(kg_sold) * up, 2)


with tab_retail:
    st.subheader("Retail sale (FIFO auto)")

    customer = st.text_input("Customer (optional)", value="Walk-in", key="ret_cust")
    pcs_sold = st.number_input("Pieces sold", min_value=1, value=10, step=1, key="ret_pcs")

    price_basis = st.radio(
        "Price basis",
        options=["PER_KG", "PER_PIECE"],
        index=0,
        horizontal=True,
        key="ret_basis",
        help="Default is PER_KG (most common).",
    )
    unit_price = st.number_input("Unit price", min_value=0.0, value=0.0, step=10.0, key="ret_price")

    st.caption("FIFO will draw from the oldest available batch for this size.")

    if st.button("Post Retail Sale", type="primary", key="post_retail"):
        try:
            results = create_retail_sale_fifo(
                conn,
                branch_id=int(branch_id),
                size_id=int(size_id),
                customer=customer.strip() or None,
                pcs_sold=int(pcs_sold),
                unit_price=(float(unit_price) if unit_price > 0 else None),
                price_basis=str(price_basis),
            )

            total_pcs = sum(r.pcs_sold for r in results)
            total_kg = sum(r.kg_sold for r in results)

            # total price (sum per FIFO row)
            total_sales = 0.0
            if float(unit_price) > 0:
                for r in results:
                    total_sales += _compute_total_price(
                        unit_price=float(unit_price),
                        price_basis=str(price_basis),
                        kg_sold=r.kg_sold,
                        pcs_sold=r.pcs_sold,
                    ) or 0.0

            if float(unit_price) > 0:
                st.success(f"Retail sale posted via FIFO: {total_pcs} pcs • {total_kg:.3f} kg • Total: {total_sales:,.2f}")
            else:
                st.success(f"Retail sale posted via FIFO: {total_pcs} pcs • {total_kg:.3f} kg")

            st.rerun()
        except Exception as e:
            st.error(str(e))


with tab_wholesale:
    st.subheader("Wholesale sale (FIFO auto)")

    customer_w = st.text_input("Customer (optional)", value="Wholesale Customer", key="wh_cust")
    kg_sold = st.number_input("Kg sold", min_value=0.1, value=25.0, step=1.0, format="%.3f", key="wh_kg")

    st.warning("System estimates expected pieces in the background. Please count pieces physically and confirm below.")

    # Better default than 1: nudge users to type the real count
    pcs_counted = st.number_input(
        "Pieces counted (required)",
        min_value=1,
        value=10,
        step=1,
        key="wh_pcs",
        help="Count pieces physically. This is the control check.",
    )

    tolerance = st.number_input("Tolerance (pcs) for variance flag", min_value=0, value=2, step=1, key="wh_tol")

    price_basis_w = st.radio(
        "Price basis",
        options=["PER_KG", "PER_PIECE"],
        index=0,
        horizontal=True,
        key="wh_basis",
    )
    unit_price_w = st.number_input("Unit price", min_value=0.0, value=0.0, step=10.0, key="wh_price")

    total_price_w = _compute_total_price(
        unit_price=(float(unit_price_w) if unit_price_w > 0 else None),
        price_basis=str(price_basis_w),
        kg_sold=float(kg_sold),
        pcs_sold=int(pcs_counted),
    )
    if total_price_w is None:
        st.info("Enter a unit price to compute total price automatically.")
    else:
        st.success(f"Total price (auto): **{total_price_w:,.2f}**")

    if st.button("Post Wholesale Sale", type="primary", key="post_wholesale"):
        try:
            results = create_wholesale_sale_fifo(
                conn,
                branch_id=int(branch_id),
                size_id=int(size_id),
                customer=customer_w.strip() or None,
                kg_sold=float(kg_sold),
                pcs_counted=int(pcs_counted),
                tolerance_pcs=int(tolerance),
                unit_price=(float(unit_price_w) if unit_price_w > 0 else None),
                price_basis=str(price_basis_w),
            )

            total_kg = sum(r.kg_sold for r in results)
            total_pcs = sum(r.pcs_sold for r in results)
            flagged = sum(1 for r in results if r.variance_flag == 1)

            st.success(f"Wholesale sale posted via FIFO: {total_kg:.3f} kg • {total_pcs} pcs • Flagged rows: {flagged}")
            if flagged:
                st.warning("Variance flagged: counted pieces differed materially from expected pieces (per FIFO batch avg).")

            st.rerun()
        except Exception as e:
            st.error(str(e))


st.divider()
st.subheader("Recent sales (this branch only)")

sales = q(
    conn,
    """
    SELECT s.sale_ts, br.name AS branch, b.batch_code, s.mode,
           s.size_id,
           s.pcs_sold, ROUND(s.kg_sold,3) AS kg_sold,
           s.unit_price, s.price_basis, s.total_price,
           s.pcs_suggested, s.variance_flag,
           s.customer
    FROM sales s
    JOIN branches br ON br.id = s.branch_id
    JOIN batches b ON b.id = s.batch_id
    WHERE s.branch_id = ?
    ORDER BY s.id DESC
    LIMIT 50
    """,
    (int(branch_id),),
)

if sales:
    df = pd.DataFrame([dict(r) for r in sales])
    st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.caption("No sales yet for this branch.")
