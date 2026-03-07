from __future__ import annotations

import streamlit as st
import pandas as pd

from core.config import get_settings
from core.db import get_conn, ensure_schema, q, x
from core.services.sales import (
    create_retail_sale_fifo,
    create_wholesale_sale_fifo,
    get_branch_size_prices,
    get_active_branch_retail_promo,
    calculate_retail_promo_summary,
)


st.set_page_config(page_title="Sales", page_icon="🛒", layout="wide")
st.title("🛒 Sales")
st.caption("FIFO is automatic. Choose branch, size, and customer. Prices are preset per branch and size.")

settings = get_settings()
conn = get_conn(settings.db_path)
ensure_schema(conn)

branches = q(conn, "SELECT id, name FROM branches ORDER BY name")
sizes = q(conn, "SELECT id, code, description FROM sizes ORDER BY sort_order, code")

if not branches or not sizes:
    st.info("Reference data missing. Load demo data in 🧪 Data Management first.")
    st.stop()

user_role = st.session_state.get("user_role") or st.session_state.get("erp_role") or "Admin"
user_branch_id = st.session_state.get("user_branch_id")

if user_branch_id and str(user_role).strip().lower() != "admin":
    branch_row = next((b for b in branches if int(b["id"]) == int(user_branch_id)), None)
    if not branch_row:
        st.error("Assigned branch not found.")
        st.stop()
    branch_name = str(branch_row["name"])
    st.selectbox("Branch", options=[branch_name], index=0, disabled=True)
    branch_id = int(branch_row["id"])
else:
    branch_name = st.selectbox("Branch", options=[b["name"] for b in branches], index=0, key="sale_branch")
    branch_id = next(int(b["id"]) for b in branches if b["name"] == branch_name)

size_label = st.selectbox(
    "Fish size",
    options=[f"{s['code']}" for s in sizes],
    index=0,
    key="sale_size",
)
size_id = next(int(s["id"]) for s in sizes if str(s["code"]) == str(size_label))

# Preset pricing
try:
    price_cfg = get_branch_size_prices(conn, branch_id=int(branch_id), size_id=int(size_id))
    retail_price_per_piece = float(price_cfg["retail_price_per_piece"])
    wholesale_price_per_kg = float(price_cfg["wholesale_price_per_kg"])
except Exception as e:
    st.error(str(e))
    st.stop()

p1, p2 = st.columns(2)
p1.metric("Retail price / piece", f"{retail_price_per_piece:,.2f}")
p2.metric("Wholesale price / kg", f"{wholesale_price_per_kg:,.2f}")

# Promo preview
active_promo = get_active_branch_retail_promo(conn, branch_id=int(branch_id))
if active_promo:
    st.info(
        f"Active retail promo for {branch_name}: {active_promo['name']} "
        f"({active_promo['buy_qty']} + {active_promo['free_qty']})"
    )

# -------------------------
# Customers
# -------------------------
def load_branch_customers(branch_id_value: int) -> list[dict]:
    rows = q(
        conn,
        """
        SELECT id, display_name, category, phone, house_number
        FROM customers
        WHERE branch_id=? AND is_active=1
        ORDER BY display_name
        """,
        (int(branch_id_value),),
    )
    return [dict(r) for r in rows]


def customer_option_label(c: dict) -> str:
    ident = c.get("phone") or c.get("house_number") or "No ID"
    return f"{c['display_name']} | {c['category']} | {ident}"


def create_customer_quick(
    *,
    branch_id_value: int,
    display_name: str,
    category: str,
    phone: str | None,
    house_number: str | None,
    notes: str | None,
) -> int:
    display_name = str(display_name).strip()
    phone = (str(phone).strip() if phone else None) or None
    house_number = (str(house_number).strip() if house_number else None) or None
    notes = (str(notes).strip() if notes else None) or None

    if not display_name:
        raise ValueError("Customer name is required.")
    if not phone and not house_number:
        raise ValueError("Provide at least a phone number or a house number.")

    existing = None
    if phone:
        rows = q(conn, "SELECT id FROM customers WHERE phone=?", (phone,))
        if rows:
            existing = int(rows[0]["id"])
    if existing is None and house_number:
        rows = q(conn, "SELECT id FROM customers WHERE house_number=?", (house_number,))
        if rows:
            existing = int(rows[0]["id"])

    if existing is not None:
        raise ValueError("A customer with this phone number or house number already exists.")

    customer_id = x(
        conn,
        """
        INSERT INTO customers(branch_id, display_name, category, phone, house_number, notes, is_active)
        VALUES (?, ?, ?, ?, ?, ?, 1)
        """,
        (
            int(branch_id_value),
            display_name,
            category,
            phone,
            house_number,
            notes,
        ),
    )
    return int(customer_id)


customer_rows = load_branch_customers(int(branch_id))
customer_options = ["+ Add New Customer"] + [customer_option_label(c) for c in customer_rows]

selected_customer_label = st.selectbox(
    "Customer",
    options=customer_options,
    index=0 if customer_rows else 0,
    help="Search/select an existing customer, or choose + Add New Customer.",
)

selected_customer = None
selected_customer_id = None
selected_customer_name = None

if selected_customer_label != "+ Add New Customer":
    selected_customer = next(c for c in customer_rows if customer_option_label(c) == selected_customer_label)
    selected_customer_id = int(selected_customer["id"])
    selected_customer_name = str(selected_customer["display_name"])

with st.expander("Quick Add Customer", expanded=(selected_customer_label == "+ Add New Customer")):
    qc1, qc2 = st.columns(2)
    with qc1:
        new_name = st.text_input("Customer name", value="", key="new_cust_name")
        new_category = st.selectbox(
            "Category",
            options=["Individual", "Market reseller", "Restaurant", "Walk-in"],
            index=3,
            key="new_cust_cat",
        )
        new_phone = st.text_input("Phone number (primary unique ID)", value="", key="new_cust_phone")
    with qc2:
        new_house = st.text_input("House number (used if no phone)", value="", key="new_cust_house")
        new_notes = st.text_input("Notes (optional)", value="", key="new_cust_notes")

    if st.button("Save Customer", type="secondary", key="save_customer"):
        try:
            new_id = create_customer_quick(
                branch_id_value=int(branch_id),
                display_name=new_name,
                category=new_category,
                phone=new_phone,
                house_number=new_house,
                notes=new_notes,
            )
            st.session_state["last_created_customer_id"] = int(new_id)
            st.success("Customer created successfully.")
            st.rerun()
        except Exception as e:
            st.error(str(e))

if selected_customer_id is None and "last_created_customer_id" in st.session_state:
    last_id = int(st.session_state["last_created_customer_id"])
    rows = q(conn, "SELECT id, display_name FROM customers WHERE id=?", (last_id,))
    if rows:
        selected_customer_id = int(rows[0]["id"])
        selected_customer_name = str(rows[0]["display_name"])
        st.info(f"Using newly created customer: {selected_customer_name}")

tab_retail, tab_wholesale = st.tabs(["Retail", "Wholesale"])

with tab_retail:
    st.subheader("Retail sale (FIFO auto)")

    pcs_sold = st.number_input("Pieces sold", min_value=1, value=10, step=1, key="ret_pcs")
    kg_sold_actual = st.number_input(
        "Kg sold (for tracking)",
        min_value=0.001,
        value=1.000,
        step=0.1,
        format="%.3f",
        key="ret_kg",
        help="Used for average weight tracking and internal variance checks.",
    )

    st.caption("Retail price is preset per piece for this branch and size.")

    promo_preview = calculate_retail_promo_summary(
        total_pcs=int(pcs_sold),
        unit_price=float(retail_price_per_piece),
        promo=active_promo,
    )

    if promo_preview["promo_applied"] == 1 and active_promo:
        st.success(
            f"Promo active: {active_promo['name']} • "
            f"Charged: {promo_preview['charged_pcs']} pcs • "
            f"Free: {promo_preview['free_pcs']} pcs • "
            f"Discount: {promo_preview['promo_discount_value']:,.2f}"
        )
    retail_total = float(promo_preview["total_price"])
    st.success(f"Retail total (auto): **{retail_total:,.2f}**")

    if st.button("Post Retail Sale", type="primary", key="post_retail"):
        try:
            customer_name_for_sale = selected_customer_name
            customer_id_for_sale = selected_customer_id

            if not customer_name_for_sale:
                customer_name_for_sale = "Walk-in"
                customer_id_for_sale = None

            results = create_retail_sale_fifo(
                conn,
                branch_id=int(branch_id),
                size_id=int(size_id),
                customer=customer_name_for_sale,
                customer_id=customer_id_for_sale,
                pcs_sold=int(pcs_sold),
                kg_sold_actual=float(kg_sold_actual),
                unit_price=float(retail_price_per_piece),
            )

            total_pcs = sum(r.pcs_sold for r in results)
            total_kg = sum(r.kg_sold for r in results)
            flagged = sum(1 for r in results if r.variance_flag == 1)
            total_free = sum(int(r.free_pcs) for r in results)
            total_charged = sum(int(r.charged_pcs or r.pcs_sold) for r in results)
            total_discount = round(sum(float(r.promo_discount_value or 0.0) for r in results), 2)
            total_amount = round(sum(float((q(conn, "SELECT total_price FROM sales WHERE id=?", (int(r.sale_id),))[0]["total_price"])) for r in results), 2)

            st.success(
                f"Retail sale posted via FIFO: {total_pcs} pcs • {total_kg:.3f} kg • "
                f"Charged: {total_charged} pcs • Free: {total_free} pcs • Total: {total_amount:,.2f}"
            )
            if total_discount > 0:
                st.info(f"Promo discount applied: {total_discount:,.2f}")
            if flagged:
                st.warning("Internal variance flagged: entered kg differed materially from batch-average expectation.")

            st.rerun()
        except Exception as e:
            st.error(str(e))

with tab_wholesale:
    st.subheader("Wholesale sale (FIFO auto)")

    kg_sold = st.number_input("Kg sold", min_value=0.1, value=25.0, step=1.0, format="%.3f", key="wh_kg")
    pcs_counted = st.number_input(
        "Pieces counted (for tracking)",
        min_value=1,
        value=10,
        step=1,
        key="wh_pcs",
        help="Used for internal variance checks only.",
    )

    tolerance = st.number_input("Tolerance (pcs) for variance flag", min_value=0, value=2, step=1, key="wh_tol")

    st.caption("Wholesale price is preset per kg for this branch and size.")
    wholesale_total = round(float(kg_sold) * float(wholesale_price_per_kg), 2)
    st.success(f"Wholesale total (auto): **{wholesale_total:,.2f}**")

    if st.button("Post Wholesale Sale", type="primary", key="post_wholesale"):
        try:
            customer_name_for_sale = selected_customer_name
            customer_id_for_sale = selected_customer_id

            if not customer_name_for_sale:
                customer_name_for_sale = "Wholesale Customer"
                customer_id_for_sale = None

            results = create_wholesale_sale_fifo(
                conn,
                branch_id=int(branch_id),
                size_id=int(size_id),
                customer=customer_name_for_sale,
                customer_id=customer_id_for_sale,
                kg_sold=float(kg_sold),
                pcs_counted=int(pcs_counted),
                tolerance_pcs=int(tolerance),
                unit_price=float(wholesale_price_per_kg),
            )

            total_kg = sum(r.kg_sold for r in results)
            total_pcs = sum(r.pcs_sold for r in results)
            flagged = sum(1 for r in results if r.variance_flag == 1)
            total_amount = round(sum(float((q(conn, "SELECT total_price FROM sales WHERE id=?", (int(r.sale_id),))[0]["total_price"])) for r in results), 2)

            st.success(
                f"Wholesale sale posted via FIFO: {total_kg:.3f} kg • {total_pcs} pcs • Total: {total_amount:,.2f}"
            )
            if flagged:
                st.warning("Internal variance flagged: counted pieces differed materially from expected pieces.")

            st.rerun()
        except Exception as e:
            st.error(str(e))

st.divider()
st.subheader("Recent sales (this branch only)")

sales = q(
    conn,
    """
    SELECT
        s.sale_ts,
        br.name AS branch,
        b.batch_code,
        sz.code AS size_code,
        s.mode,
        COALESCE(c.display_name, s.customer) AS customer_name,
        s.pcs_sold,
        ROUND(s.kg_sold, 3) AS kg_sold,
        s.unit_price,
        s.price_basis,
        s.total_price,
        s.pcs_suggested,
        s.variance_flag,
        s.charged_pcs,
        s.free_pcs,
        s.promo_applied,
        s.promo_code,
        s.promo_discount_value
    FROM sales s
    JOIN branches br ON br.id = s.branch_id
    JOIN batches b ON b.id = s.batch_id
    LEFT JOIN sizes sz ON sz.id = s.size_id
    LEFT JOIN customers c ON c.id = s.customer_id
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