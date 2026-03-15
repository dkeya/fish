from __future__ import annotations

from datetime import datetime
from uuid import uuid4

import pandas as pd
import streamlit as st

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
st.caption(
    "Cart-based sales flow with automatic FIFO, preset pricing, customer linking, "
    "promo support, and support for fish, packaging, and services."
)

settings = get_settings()
conn = get_conn(settings.db_path)
ensure_schema(conn)

# -------------------------
# Session state
# -------------------------
if "sales_cart" not in st.session_state:
    st.session_state["sales_cart"] = []

if "sales_posted" not in st.session_state:
    st.session_state["sales_posted"] = False

if "sales_last_summary" not in st.session_state:
    st.session_state["sales_last_summary"] = None


def _reset_sales_state() -> None:
    st.session_state["sales_cart"] = []
    st.session_state["sales_posted"] = False
    st.session_state["sales_last_summary"] = None


def _generate_sale_group_code() -> str:
    return f"SALE-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid4().hex[:6].upper()}"


branches = q(conn, "SELECT id, name FROM branches ORDER BY name")
sizes = q(conn, "SELECT id, code, description FROM sizes ORDER BY sort_order, code")

if not branches:
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
customer_search = st.text_input(
    "Search customer",
    value="",
    help="Search by name, phone, or house number.",
).strip().lower()

filtered_customers = []
if customer_search:
    for c in customer_rows:
        hay = " ".join(
            [
                str(c.get("display_name", "")),
                str(c.get("category", "")),
                str(c.get("phone") or ""),
                str(c.get("house_number") or ""),
            ]
        ).lower()
        if customer_search in hay:
            filtered_customers.append(c)
else:
    filtered_customers = customer_rows

customer_options = ["+ Add New Customer"] + [customer_option_label(c) for c in filtered_customers]

selected_customer_label = st.selectbox(
    "Customer",
    options=customer_options,
    index=0,
    help="Search/select an existing customer, or choose + Add New Customer.",
)

selected_customer = None
selected_customer_id = None
selected_customer_name = None

if selected_customer_label != "+ Add New Customer":
    selected_customer = next(c for c in filtered_customers if customer_option_label(c) == selected_customer_label)
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

# -------------------------
# Product helpers
# -------------------------
def load_branch_products(branch_id_value: int, product_type: str | None = None) -> list[dict]:
    where_type = ""
    params: list[object] = [int(branch_id_value)]
    if product_type:
        where_type = " AND p.product_type=? "
        params.append(str(product_type))

    rows = q(
        conn,
        f"""
        SELECT
            p.id,
            p.sku,
            p.name,
            p.product_type,
            p.stock_uom,
            p.tracks_stock,
            p.uses_batch_fifo,
            p.uses_size_dimension,
            p.requires_piece_entry,
            p.requires_weight_entry,
            p.service_non_stock,
            bp.is_active AS branch_enabled
        FROM branch_products bp
        JOIN products p ON p.id = bp.product_id
        WHERE bp.branch_id=?
          AND bp.is_active=1
          AND p.is_active=1
          {where_type}
        ORDER BY p.name
        """,
        tuple(params),
    )
    return [dict(r) for r in rows]


def get_branch_product_prices(branch_id_value: int, product_id: int) -> dict:
    rows = q(
        conn,
        """
        SELECT retail_price, wholesale_price
        FROM branch_product_prices
        WHERE branch_id=? AND product_id=? AND is_active=1
        """,
        (int(branch_id_value), int(product_id)),
    )
    if not rows:
        raise ValueError("No active branch product price found for this item.")
    return {
        "retail_price": float(rows[0]["retail_price"]),
        "wholesale_price": float(rows[0]["wholesale_price"]),
    }


def get_branch_product_stock_on_hand(branch_id_value: int, product_id: int) -> float:
    rows = q(
        conn,
        """
        SELECT COALESCE(SUM(qty_delta), 0) AS qty_on_hand
        FROM product_stock_movements
        WHERE branch_id=? AND product_id=?
        """,
        (int(branch_id_value), int(product_id)),
    )
    return float(rows[0]["qty_on_hand"]) if rows else 0.0


def _load_sale_group_summary(sale_group_code: str) -> dict | None:
    fish_rows = q(
        conn,
        """
        SELECT
            'FISH' AS entry_type,
            s.mode,
            COALESCE(sz.code, '') AS size_code,
            COALESCE(c.display_name, s.customer, 'Walk-in') AS customer_name,
            s.pcs_sold,
            ROUND(s.kg_sold, 3) AS kg_sold,
            NULL AS qty,
            s.unit_price,
            s.total_price,
            s.free_pcs,
            s.promo_code,
            s.promo_discount_value
        FROM sales s
        LEFT JOIN sizes sz ON sz.id = s.size_id
        LEFT JOIN customers c ON c.id = s.customer_id
        WHERE s.sale_group_code=?
          AND s.mode IN ('RETAIL_PCS', 'WHOLESALE_KG')
        ORDER BY s.id
        """,
        (sale_group_code,),
    )

    product_rows = q(
        conn,
        """
        SELECT
            'PACKAGING' AS entry_type,
            'PRODUCT' AS mode,
            '' AS size_code,
            '' AS customer_name,
            NULL AS pcs_sold,
            NULL AS kg_sold,
            ABS(psm.qty_delta) AS qty,
            NULL AS unit_price,
            NULL AS total_price,
            NULL AS free_pcs,
            NULL AS promo_code,
            NULL AS promo_discount_value,
            p.name AS item_name
        FROM product_stock_movements psm
        JOIN products p ON p.id = psm.product_id
        WHERE psm.reference_no=?
          AND psm.movement_type='SALE'
        ORDER BY psm.id
        """,
        (sale_group_code,),
    )

    service_rows = q(
        conn,
        """
        SELECT
            'SERVICE' AS entry_type,
            'SERVICE' AS mode,
            '' AS size_code,
            COALESCE(c.display_name, 'Walk-in / Unlinked') AS customer_name,
            NULL AS pcs_sold,
            NULL AS kg_sold,
            ss.quantity AS qty,
            ss.unit_price,
            ss.total_price,
            NULL AS free_pcs,
            NULL AS promo_code,
            NULL AS promo_discount_value,
            p.name AS item_name
        FROM service_sales ss
        JOIN products p ON p.id = ss.product_id
        LEFT JOIN customers c ON c.id = ss.customer_id
        WHERE ss.sale_group_code=?
        ORDER BY ss.id
        """,
        (sale_group_code,),
    )

    fish_df = pd.DataFrame([dict(r) for r in fish_rows]) if fish_rows else pd.DataFrame()
    product_df = pd.DataFrame([dict(r) for r in product_rows]) if product_rows else pd.DataFrame()
    service_df = pd.DataFrame([dict(r) for r in service_rows]) if service_rows else pd.DataFrame()

    if fish_df.empty and product_df.empty and service_df.empty:
        return None

    summary = {
        "fish_df": fish_df,
        "product_df": product_df,
        "service_df": service_df,
    }
    return summary


# -------------------------
# Cart helpers
# -------------------------
def _cart_df(cart: list[dict]) -> pd.DataFrame:
    rows = []
    for i, item in enumerate(cart, start=1):
        rows.append(
            {
                "Line": i,
                "Entry Type": item["entry_type"],
                "Mode": item.get("mode", ""),
                "Item": item.get("item_name", item.get("size_code", "")),
                "Size": item.get("size_code", ""),
                "Pieces": item.get("pcs", ""),
                "Kg": item.get("kg", ""),
                "Qty": item.get("qty", ""),
                "Unit Price": item["unit_price"],
                "Total": item["line_total"],
                "Promo": item.get("promo_name") if item.get("promo_applied") else "",
                "Free Pcs": item.get("free_pcs", 0),
            }
        )
    return pd.DataFrame(rows)


def _cart_total(cart: list[dict]) -> float:
    return round(sum(float(item["line_total"]) for item in cart), 2)


def _cart_total_discount(cart: list[dict]) -> float:
    return round(sum(float(item.get("promo_discount_value", 0.0)) for item in cart), 2)


def _render_posted_sale_summary(summary: dict) -> None:
    st.success("Sale posted successfully.")

    st.write(
        f"**Summary:** {summary.get('lines', 0)} line(s) • "
        f"{summary.get('pcs', 0)} pcs • "
        f"{summary.get('kg', 0.0):,.3f} kg • "
        f"Other Qty {summary.get('qty', 0.0):,.2f} • "
        f"Total {summary.get('total', 0.0):,.2f}"
    )

    if summary.get("discount", 0.0) > 0:
        st.info(f"Promo discount applied: {summary['discount']:,.2f}")

    if summary.get("sale_group_code"):
        st.caption(f"Sale Group Code: {summary['sale_group_code']}")

        sale_group_summary = _load_sale_group_summary(str(summary["sale_group_code"]))
        if sale_group_summary:
            with st.expander("Posted sale summary", expanded=True):
                if not sale_group_summary["fish_df"].empty:
                    st.markdown("**Fish lines**")
                    st.dataframe(sale_group_summary["fish_df"], use_container_width=True, hide_index=True)

                if not sale_group_summary["product_df"].empty:
                    st.markdown("**Packaging lines**")
                    st.dataframe(sale_group_summary["product_df"], use_container_width=True, hide_index=True)

                if not sale_group_summary["service_df"].empty:
                    st.markdown("**Service lines**")
                    st.dataframe(sale_group_summary["service_df"], use_container_width=True, hide_index=True)


# -------------------------
# Entry tabs
# -------------------------
main_tab_fish, main_tab_packaging, main_tab_service = st.tabs(["Fish", "Packaging", "Services"])

with main_tab_fish:
    if not sizes:
        st.info("Fish sizes are missing.")
    else:
        tab_retail, tab_wholesale = st.tabs(["Retail", "Wholesale"])

        with tab_retail:
            st.subheader("Retail fish line")

            retail_size_label = st.selectbox(
                "Fish size",
                options=[f"{s['code']}" for s in sizes],
                index=0,
                key="ret_size",
            )
            retail_size_id = next(int(s["id"]) for s in sizes if str(s["code"]) == str(retail_size_label))

            try:
                retail_price_cfg = get_branch_size_prices(conn, branch_id=int(branch_id), size_id=int(retail_size_id))
                retail_price_per_piece = float(retail_price_cfg["retail_price_per_piece"])
            except Exception as e:
                st.error(str(e))
                retail_price_per_piece = 0.0

            active_promo = get_active_branch_retail_promo(conn, branch_id=int(branch_id))
            if active_promo:
                st.info(
                    f"Active retail promo for {branch_name}: {active_promo['name']} "
                    f"({active_promo['buy_qty']} + {active_promo['free_qty']})"
                )

            c1, c2, c3 = st.columns(3)
            with c1:
                ret_pcs = st.number_input("Pieces sold", min_value=0, value=0, step=1, key="ret_pcs")
            with c2:
                ret_kg = st.number_input(
                    "Kg sold (for tracking)",
                    min_value=0.0,
                    value=0.0,
                    step=0.1,
                    format="%.3f",
                    key="ret_kg",
                )
            with c3:
                st.metric("Retail price / piece", f"{retail_price_per_piece:,.2f}")

            promo_preview = calculate_retail_promo_summary(
                total_pcs=int(ret_pcs),
                unit_price=float(retail_price_per_piece),
                promo=active_promo,
            )

            ret_c1, ret_c2 = st.columns([1, 1])
            with ret_c1:
                if promo_preview["promo_applied"] == 1 and active_promo:
                    st.success(
                        f"Promo: {active_promo['name']} • Charged {promo_preview['charged_pcs']} pcs • "
                        f"Free {promo_preview['free_pcs']} pcs • Discount {promo_preview['promo_discount_value']:,.2f}"
                    )
                st.success(f"Retail line total: **{promo_preview['total_price']:,.2f}**")

            with ret_c2:
                if st.button("Add Retail Fish Line", type="secondary", key="add_retail_line"):
                    try:
                        if int(ret_pcs) <= 0:
                            raise ValueError("Retail pieces must be greater than 0.")
                        if float(ret_kg) <= 0:
                            raise ValueError("Retail kg must be greater than 0.")

                        st.session_state["sales_cart"].append(
                            {
                                "entry_type": "FISH",
                                "mode": "RETAIL",
                                "size_id": int(retail_size_id),
                                "size_code": str(retail_size_label),
                                "item_name": f"Fish {retail_size_label}",
                                "pcs": int(ret_pcs),
                                "kg": float(ret_kg),
                                "unit_price": float(retail_price_per_piece),
                                "line_total": float(promo_preview["total_price"]),
                                "promo_applied": int(promo_preview["promo_applied"]),
                                "promo_name": (active_promo["name"] if active_promo and promo_preview["promo_applied"] else None),
                                "promo_code": (active_promo["code"] if active_promo and promo_preview["promo_applied"] else None),
                                "promo_buy_qty": (int(active_promo["buy_qty"]) if active_promo and promo_preview["promo_applied"] else None),
                                "promo_free_qty": (int(active_promo["free_qty"]) if active_promo and promo_preview["promo_applied"] else None),
                                "charged_pcs": int(promo_preview["charged_pcs"]),
                                "free_pcs": int(promo_preview["free_pcs"]),
                                "promo_discount_value": float(promo_preview["promo_discount_value"]),
                            }
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))

        with tab_wholesale:
            st.subheader("Wholesale fish line")

            wholesale_size_label = st.selectbox(
                "Fish size",
                options=[f"{s['code']}" for s in sizes],
                index=0,
                key="wh_size",
            )
            wholesale_size_id = next(int(s["id"]) for s in sizes if str(s["code"]) == str(wholesale_size_label))

            try:
                wholesale_price_cfg = get_branch_size_prices(conn, branch_id=int(branch_id), size_id=int(wholesale_size_id))
                wholesale_price_per_kg = float(wholesale_price_cfg["wholesale_price_per_kg"])
            except Exception as e:
                st.error(str(e))
                wholesale_price_per_kg = 0.0

            c1, c2, c3, c4 = st.columns(4)
            with c1:
                wh_kg = st.number_input("Kg sold", min_value=0.0, value=0.0, step=1.0, format="%.3f", key="wh_kg")
            with c2:
                wh_pcs = st.number_input("Pieces counted", min_value=0, value=0, step=1, key="wh_pcs")
            with c3:
                wh_tol = st.number_input("Tolerance", min_value=0, value=2, step=1, key="wh_tol")
            with c4:
                st.metric("Wholesale price / kg", f"{wholesale_price_per_kg:,.2f}")

            wholesale_total = round(float(wh_kg) * float(wholesale_price_per_kg), 2)
            st.success(f"Wholesale line total: **{wholesale_total:,.2f}**")

            if st.button("Add Wholesale Fish Line", type="secondary", key="add_wholesale_line"):
                try:
                    if float(wh_kg) <= 0:
                        raise ValueError("Wholesale kg must be greater than 0.")
                    if int(wh_pcs) <= 0:
                        raise ValueError("Wholesale pieces counted must be greater than 0.")

                    st.session_state["sales_cart"].append(
                        {
                            "entry_type": "FISH",
                            "mode": "WHOLESALE",
                            "size_id": int(wholesale_size_id),
                            "size_code": str(wholesale_size_label),
                            "item_name": f"Fish {wholesale_size_label}",
                            "pcs": int(wh_pcs),
                            "kg": float(wh_kg),
                            "tolerance": int(wh_tol),
                            "unit_price": float(wholesale_price_per_kg),
                            "line_total": float(wholesale_total),
                            "promo_applied": 0,
                            "promo_name": None,
                            "promo_code": None,
                            "promo_buy_qty": None,
                            "promo_free_qty": None,
                            "charged_pcs": int(wh_pcs),
                            "free_pcs": 0,
                            "promo_discount_value": 0.0,
                        }
                    )
                    st.rerun()
                except Exception as e:
                    st.error(str(e))

with main_tab_packaging:
    st.subheader("Packaging line")

    packaging_products = load_branch_products(int(branch_id), product_type="PACKAGING")
    if not packaging_products:
        st.info("No packaging products enabled for this branch.")
    else:
        packaging_name = st.selectbox(
            "Packaging product",
            options=[str(p["name"]) for p in packaging_products],
            key="pkg_product_name",
        )
        packaging_product = next(p for p in packaging_products if str(p["name"]) == str(packaging_name))
        packaging_product_id = int(packaging_product["id"])

        try:
            packaging_prices = get_branch_product_prices(int(branch_id), int(packaging_product_id))
            packaging_retail_price = float(packaging_prices["retail_price"])
            packaging_wholesale_price = float(packaging_prices["wholesale_price"])
        except Exception as e:
            st.error(str(e))
            packaging_retail_price = 0.0
            packaging_wholesale_price = 0.0

        packaging_mode = st.radio(
            "Packaging sale mode",
            options=["RETAIL", "WHOLESALE"],
            horizontal=True,
            key="pkg_mode",
        )

        current_packaging_price = packaging_retail_price if packaging_mode == "RETAIL" else packaging_wholesale_price
        current_packaging_stock = get_branch_product_stock_on_hand(int(branch_id), int(packaging_product_id))

        p1, p2, p3 = st.columns(3)
        with p1:
            pkg_qty = st.number_input("Quantity", min_value=0.0, value=0.0, step=1.0, key="pkg_qty")
        with p2:
            st.metric("Unit price", f"{current_packaging_price:,.2f}")
        with p3:
            st.metric("Stock on hand", f"{current_packaging_stock:,.2f}")

        packaging_total = round(float(pkg_qty) * float(current_packaging_price), 2)
        st.success(f"Packaging line total: **{packaging_total:,.2f}**")

        if st.button("Add Packaging Line", type="secondary", key="add_packaging_line"):
            try:
                if float(pkg_qty) <= 0:
                    raise ValueError("Packaging quantity must be greater than 0.")
                if float(current_packaging_price) < 0:
                    raise ValueError("Packaging price is invalid.")
                if float(pkg_qty) > float(current_packaging_stock):
                    raise ValueError("Not enough packaging stock on hand.")

                st.session_state["sales_cart"].append(
                    {
                        "entry_type": "PACKAGING",
                        "mode": packaging_mode,
                        "product_id": int(packaging_product_id),
                        "item_name": str(packaging_product["name"]),
                        "qty": float(pkg_qty),
                        "unit_price": float(current_packaging_price),
                        "line_total": float(packaging_total),
                        "promo_applied": 0,
                        "promo_name": None,
                        "promo_code": None,
                        "promo_discount_value": 0.0,
                    }
                )
                st.rerun()
            except Exception as e:
                st.error(str(e))

with main_tab_service:
    st.subheader("Service line")

    service_products = load_branch_products(int(branch_id), product_type="SERVICE")
    if not service_products:
        st.info("No service products enabled for this branch.")
    else:
        service_name = st.selectbox(
            "Service",
            options=[str(p["name"]) for p in service_products],
            key="svc_product_name",
        )
        service_product = next(p for p in service_products if str(p["name"]) == str(service_name))
        service_product_id = int(service_product["id"])

        try:
            service_prices = get_branch_product_prices(int(branch_id), int(service_product_id))
            service_price = float(service_prices["retail_price"])
        except Exception as e:
            st.error(str(e))
            service_price = 0.0

        s1, s2, s3 = st.columns(3)
        with s1:
            svc_qty = st.number_input("Service quantity", min_value=0.0, value=0.0, step=1.0, key="svc_qty")
        with s2:
            st.metric("Unit price", f"{service_price:,.2f}")
        with s3:
            svc_notes = st.text_input("Service notes (optional)", value="", key="svc_notes")

        service_total = round(float(svc_qty) * float(service_price), 2)
        st.success(f"Service line total: **{service_total:,.2f}**")

        if st.button("Add Service Line", type="secondary", key="add_service_line"):
            try:
                if float(svc_qty) <= 0:
                    raise ValueError("Service quantity must be greater than 0.")
                if float(service_price) < 0:
                    raise ValueError("Service price is invalid.")

                st.session_state["sales_cart"].append(
                    {
                        "entry_type": "SERVICE",
                        "mode": "SERVICE",
                        "product_id": int(service_product_id),
                        "item_name": str(service_product["name"]),
                        "qty": float(svc_qty),
                        "unit_price": float(service_price),
                        "line_total": float(service_total),
                        "notes": svc_notes.strip() or None,
                        "promo_applied": 0,
                        "promo_name": None,
                        "promo_code": None,
                        "promo_discount_value": 0.0,
                    }
                )
                st.rerun()
            except Exception as e:
                st.error(str(e))

# -------------------------
# Cart summary + final post
# -------------------------
st.divider()
st.subheader("Sale cart")

cart = st.session_state["sales_cart"]

if cart:
    cart_df = _cart_df(cart)
    st.dataframe(cart_df, use_container_width=True, hide_index=True)

    total_pcs = sum(int(i.get("pcs", 0) or 0) for i in cart)
    total_kg = sum(float(i.get("kg", 0.0) or 0.0) for i in cart)
    total_qty = sum(float(i.get("qty", 0.0) or 0.0) for i in cart)

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Lines", f"{len(cart)}")
    m2.metric("Total Pieces", f"{total_pcs}")
    m3.metric("Total Kg", f"{total_kg:,.3f}")
    m4.metric("Other Qty", f"{total_qty:,.2f}")
    m5.metric("Cart Total", f"{_cart_total(cart):,.2f}")

    discount_total = _cart_total_discount(cart)
    if discount_total > 0:
        st.info(f"Promo discount in cart: {discount_total:,.2f}")

    remove_options = [
        f"Line {i+1} - {item['entry_type']} - {item.get('item_name', item.get('size_code', ''))}"
        for i, item in enumerate(cart)
    ]
    remove_choice = st.selectbox("Remove line (optional)", options=["None"] + remove_options, key="remove_sale_line")
    if remove_choice != "None":
        idx = int(remove_choice.split(" - ")[0].replace("Line ", "")) - 1
        if st.button("Remove Selected Line", type="secondary"):
            del st.session_state["sales_cart"][idx]
            st.rerun()

    if st.session_state["sales_posted"]:
        summary = st.session_state.get("sales_last_summary") or {}
        _render_posted_sale_summary(summary)
        if st.button("Start New Sale", type="primary"):
            _reset_sales_state()
            st.rerun()
    else:
        if st.button("Post Full Sale", type="primary", use_container_width=True):
            try:
                customer_name_for_sale = selected_customer_name
                customer_id_for_sale = selected_customer_id

                if not customer_name_for_sale:
                    customer_name_for_sale = "Walk-in"
                    customer_id_for_sale = None

                sale_group_code = _generate_sale_group_code()

                total_pcs_posted = 0
                total_kg_posted = 0.0
                total_qty_posted = 0.0
                total_amount = 0.0
                total_discount = 0.0

                for item in cart:
                    entry_type = str(item["entry_type"]).upper()

                    if entry_type == "FISH":
                        if item["mode"] == "RETAIL":
                            results = create_retail_sale_fifo(
                                conn,
                                branch_id=int(branch_id),
                                size_id=int(item["size_id"]),
                                customer=customer_name_for_sale,
                                customer_id=customer_id_for_sale,
                                pcs_sold=int(item["pcs"]),
                                kg_sold_actual=float(item["kg"]),
                                unit_price=float(item["unit_price"]),
                                allow_negative_stock=True,
                            )
                        else:
                            results = create_wholesale_sale_fifo(
                                conn,
                                branch_id=int(branch_id),
                                size_id=int(item["size_id"]),
                                customer=customer_name_for_sale,
                                customer_id=customer_id_for_sale,
                                kg_sold=float(item["kg"]),
                                pcs_counted=int(item["pcs"]),
                                tolerance_pcs=int(item.get("tolerance", 2)),
                                unit_price=float(item["unit_price"]),
                                allow_negative_stock=True,
                            )

                        for r in results:
                            x(
                                conn,
                                """
                                UPDATE sales
                                SET sale_group_code=?
                                WHERE id=?
                                """,
                                (sale_group_code, int(r.sale_id)),
                            )

                        total_pcs_posted += int(item["pcs"])
                        total_kg_posted += float(item["kg"])
                        total_amount += float(item["line_total"])
                        total_discount += float(item.get("promo_discount_value", 0.0) or 0.0)

                    elif entry_type == "PACKAGING":
                        qty = float(item["qty"])
                        total_price = float(item["line_total"])

                        x(
                            conn,
                            """
                            INSERT INTO product_stock_movements(
                                ts, branch_id, product_id, movement_type, qty_delta, unit_cost, reference_no, notes
                            )
                            VALUES (CURRENT_TIMESTAMP, ?, ?, 'SALE', ?, NULL, ?, ?)
                            """,
                            (
                                int(branch_id),
                                int(item["product_id"]),
                                -float(qty),
                                sale_group_code,
                                f"Packaging sale to {customer_name_for_sale}",
                            ),
                        )

                        x(
                            conn,
                            """
                            INSERT INTO sales(
                                sale_ts, sale_group_code, branch_id, mode, customer, customer_id,
                                batch_id, size_id, product_id,
                                pcs_sold, kg_sold,
                                unit_price, price_basis, total_price,
                                pcs_suggested, variance_flag,
                                promo_applied, charged_pcs, free_pcs, promo_discount_value
                            )
                            VALUES (
                                CURRENT_TIMESTAMP, ?, ?, 'PRODUCT', ?, ?,
                                0, NULL, ?,
                                0, 0,
                                ?, 'PER_UNIT', ?,
                                NULL, 0,
                                0, NULL, 0, NULL
                            )
                            """,
                            (
                                sale_group_code,
                                int(branch_id),
                                customer_name_for_sale,
                                customer_id_for_sale,
                                int(item["product_id"]),
                                float(item["unit_price"]),
                                float(total_price),
                            ),
                        )

                        total_qty_posted += float(qty)
                        total_amount += float(total_price)

                    elif entry_type == "SERVICE":
                        qty = float(item["qty"])
                        total_price = float(item["line_total"])

                        x(
                            conn,
                            """
                            INSERT INTO service_sales(
                                service_ts, sale_group_code, branch_id, customer_id, product_id,
                                quantity, unit_price, total_price, notes
                            )
                            VALUES (
                                CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?
                            )
                            """,
                            (
                                sale_group_code,
                                int(branch_id),
                                customer_id_for_sale,
                                int(item["product_id"]),
                                float(qty),
                                float(item["unit_price"]),
                                float(total_price),
                                item.get("notes"),
                            ),
                        )

                        total_qty_posted += float(qty)
                        total_amount += float(total_price)

                st.session_state["sales_last_summary"] = {
                    "sale_group_code": sale_group_code,
                    "lines": len(cart),
                    "pcs": total_pcs_posted,
                    "kg": round(total_kg_posted, 3),
                    "qty": round(total_qty_posted, 2),
                    "total": round(total_amount, 2),
                    "discount": round(total_discount, 2),
                }
                st.session_state["sales_cart"] = []
                st.session_state["sales_posted"] = True
                st.rerun()

            except Exception as e:
                st.error(str(e))
else:
    if st.session_state["sales_posted"]:
        summary = st.session_state.get("sales_last_summary") or {}
        _render_posted_sale_summary(summary)
        if st.button("Start New Sale", type="primary"):
            _reset_sales_state()
            st.rerun()
    else:
        st.caption("No sale lines added yet.")

# -------------------------
# Recent activity
# -------------------------
st.divider()
st.subheader("Recent fish sales (this branch only)")

fish_sales = q(
    conn,
    """
    SELECT
        s.sale_ts,
        s.sale_group_code,
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
      AND s.mode IN ('RETAIL_PCS', 'WHOLESALE_KG')
    ORDER BY s.id DESC
    LIMIT 50
    """,
    (int(branch_id),),
)

if fish_sales:
    df = pd.DataFrame([dict(r) for r in fish_sales])
    st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.caption("No fish sales yet for this branch.")

st.divider()
st.subheader("Recent packaging/product sales activity")

product_activity = q(
    conn,
    """
    SELECT
        psm.ts,
        psm.reference_no,
        br.name AS branch,
        p.name AS product_name,
        p.product_type,
        psm.movement_type,
        psm.qty_delta,
        psm.notes
    FROM product_stock_movements psm
    JOIN branches br ON br.id = psm.branch_id
    JOIN products p ON p.id = psm.product_id
    WHERE psm.branch_id = ?
      AND psm.movement_type = 'SALE'
    ORDER BY psm.id DESC
    LIMIT 50
    """,
    (int(branch_id),),
)

if product_activity:
    df = pd.DataFrame([dict(r) for r in product_activity])
    st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.caption("No packaging/product sales activity yet for this branch.")

st.divider()
st.subheader("Recent service sales")

service_activity = q(
    conn,
    """
    SELECT
        ss.service_ts,
        ss.sale_group_code,
        br.name AS branch,
        p.name AS service_name,
        COALESCE(c.display_name, 'Walk-in / Unlinked') AS customer_name,
        ss.quantity,
        ss.unit_price,
        ss.total_price,
        ss.notes
    FROM service_sales ss
    JOIN branches br ON br.id = ss.branch_id
    JOIN products p ON p.id = ss.product_id
    LEFT JOIN customers c ON c.id = ss.customer_id
    WHERE ss.branch_id = ?
    ORDER BY ss.id DESC
    LIMIT 50
    """,
    (int(branch_id),),
)

if service_activity:
    df = pd.DataFrame([dict(r) for r in service_activity])
    st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.caption("No service sales yet for this branch.")
