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
suppliers = q(conn, "SELECT id, name FROM suppliers WHERE is_active=1 ORDER BY name")

# -------------------------
# Session state for cart / submission guard
# -------------------------
if "stock_in_cart" not in st.session_state:
    st.session_state["stock_in_cart"] = []

if "stock_in_submitted" not in st.session_state:
    st.session_state["stock_in_submitted"] = False


def _reset_stock_in_state() -> None:
    st.session_state["stock_in_cart"] = []
    st.session_state["stock_in_submitted"] = False


def _cart_to_df(cart: list[dict]) -> pd.DataFrame:
    rows = []
    for i, item in enumerate(cart, start=1):
        line_total = float(item["kg"]) * float(item["buy_price_per_kg"])
        rows.append(
            {
                "Line": i,
                "Size": item["size_code"],
                "Pieces": int(item["pieces"]),
                "Kg": float(item["kg"]),
                "BuyPrice/kg": float(item["buy_price_per_kg"]),
                "Line Total": round(line_total, 2),
            }
        )
    return pd.DataFrame(rows)


def _cart_total(cart: list[dict]) -> float:
    return round(sum(float(i["kg"]) * float(i["buy_price_per_kg"]) for i in cart), 2)


# -------------------------
# Reference lookups
# -------------------------
branch_name_to_id = {str(b["name"]): int(b["id"]) for b in branches}
size_code_to_id = {str(s["code"]): int(s["id"]) for s in sizes}
supplier_names = [str(s["name"]) for s in suppliers]

col1, col2 = st.columns([1.15, 1], gap="large")

with col1:
    st.subheader("Create stock-in (cart-style entry)")

    receipt_date = st.date_input("Receipt date")
    branch_name = st.selectbox("Branch", options=list(branch_name_to_id.keys()))
    supplier_name = st.selectbox(
        "Supplier",
        options=supplier_names if supplier_names else ["No suppliers configured"],
        disabled=(len(supplier_names) == 0),
        help="Suppliers are pre-configured to avoid duplicates and typing errors.",
    )
    notes = st.text_area("Notes (optional)", value="", height=80)

    st.markdown("**Add one line at a time**")

    add_c1, add_c2, add_c3, add_c4 = st.columns([1, 1, 1, 1], gap="small")

    with add_c1:
        size_code = st.selectbox("Size", options=list(size_code_to_id.keys()), key="stockin_size")

    with add_c2:
        pieces = st.number_input("Pieces", min_value=1, value=1, step=1, key="stockin_pieces")

    with add_c3:
        kg = st.number_input("Kg", min_value=0.001, value=1.000, step=0.1, format="%.3f", key="stockin_kg")

    with add_c4:
        buy_price_per_kg = st.number_input(
            "Buy price/kg",
            min_value=0.0,
            value=0.0,
            step=1.0,
            key="stockin_bp",
            help="Captured per size line for clean valuation and margin tracking.",
        )

    add_line_col1, add_line_col2 = st.columns([1, 1])

    with add_line_col1:
        if st.button("Add Line to Cart", type="secondary", use_container_width=True):
            try:
                if float(buy_price_per_kg) <= 0:
                    raise ValueError("Buy price/kg must be greater than 0.")

                st.session_state["stock_in_cart"].append(
                    {
                        "size_id": int(size_code_to_id[size_code]),
                        "size_code": str(size_code),
                        "pieces": int(pieces),
                        "kg": float(kg),
                        "buy_price_per_kg": float(buy_price_per_kg),
                    }
                )
                st.rerun()
            except Exception as e:
                st.error(str(e))

    with add_line_col2:
        if st.button("Clear Cart", type="secondary", use_container_width=True):
            _reset_stock_in_state()
            st.rerun()

    st.divider()
    st.subheader("Purchase cart")

    cart = st.session_state["stock_in_cart"]
    if cart:
        cart_df = _cart_to_df(cart)
        st.dataframe(cart_df, use_container_width=True, hide_index=True)

        total_pieces = int(cart_df["Pieces"].sum())
        total_kg = float(cart_df["Kg"].sum())
        total_value = _cart_total(cart)

        m1, m2, m3 = st.columns(3)
        m1.metric("Total pieces", f"{total_pieces}")
        m2.metric("Total kg", f"{total_kg:,.3f}")
        m3.metric("Tentative total value", f"{total_value:,.2f}")

        remove_options = [f"Line {i+1} - {item['size_code']}" for i, item in enumerate(cart)]
        remove_choice = st.selectbox("Remove line (optional)", options=["None"] + remove_options)
        if remove_choice != "None":
            idx = int(remove_choice.split(" - ")[0].replace("Line ", "")) - 1
            if st.button("Remove Selected Line", type="secondary"):
                del st.session_state["stock_in_cart"][idx]
                st.rerun()

        with st.expander("Purchase Summary Preview", expanded=True):
            st.markdown("**Purchase summary (preview before final confirmation)**")
            summary_df = cart_df.copy()
            st.dataframe(summary_df, use_container_width=True, hide_index=True)
            st.write(f"**Branch:** {branch_name}")
            st.write(f"**Supplier:** {supplier_name}")
            st.write(f"**Receipt date:** {receipt_date.isoformat()}")
            if notes.strip():
                st.write(f"**Notes:** {notes.strip()}")
            st.write(f"**Tentative total value:** KES {total_value:,.2f}")

        finalize_disabled = st.session_state["stock_in_submitted"] or len(cart) == 0 or len(supplier_names) == 0
        if st.button(
            "Create Stock-In (Auto Batches)",
            type="primary",
            use_container_width=True,
            disabled=finalize_disabled,
        ):
            try:
                st.session_state["stock_in_submitted"] = True

                br_id = int(branch_name_to_id[branch_name])

                lines = [
                    BatchLineInput(
                        size_id=int(item["size_id"]),
                        pieces=int(item["pieces"]),
                        kg=float(item["kg"]),
                        buy_price_per_kg=float(item["buy_price_per_kg"]),
                    )
                    for item in cart
                ]

                created = create_batches_from_purchase(
                    conn,
                    receipt_date=receipt_date.isoformat(),
                    branch_id=br_id,
                    supplier=supplier_name.strip() if supplier_name else None,
                    notes=notes.strip() or None,
                    lines=lines,
                )

                # Clear cart immediately to prevent accidental repeat submission
                created_count = len(created)
                _reset_stock_in_state()
                st.success(f"Stock-In saved successfully. {created_count} batch(es) created.")

                # Exit screen / redirect away after save
                try:
                    st.switch_page("home.py")
                except Exception:
                    st.rerun()

            except Exception as e:
                st.session_state["stock_in_submitted"] = False
                st.error(str(e))
    else:
        st.info("No lines added yet. Add sizes to the cart first.")

with col2:
    st.subheader("Recent open batches")
    batches = q(
        conn,
        """
        SELECT
            b.id,
            b.batch_code,
            b.receipt_date,
            br.name AS branch,
            COALESCE(sup.name, b.supplier) AS supplier_name,
            ROUND(b.buy_price_per_kg, 2) AS buy_price_per_kg,
            b.initial_pieces,
            ROUND(b.initial_kg, 3) AS initial_kg,
            ROUND(b.batch_avg_kg_per_piece, 4) AS avg_kg_per_piece
        FROM batches b
        JOIN branches br ON br.id = b.branch_id
        LEFT JOIN suppliers sup ON sup.id = b.supplier_id
        WHERE b.status='OPEN'
        ORDER BY b.id DESC
        LIMIT 20
        """,
    )
    if batches:
        st.dataframe(pd.DataFrame([dict(r) for r in batches]), use_container_width=True, hide_index=True)
    else:
        st.info("No open batches yet. Create stock-in on the left.")