from __future__ import annotations

import io

import pandas as pd
import streamlit as st

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
# Session state / role handling
# -------------------------
current_role = (
    st.session_state.get("user_role")
    or st.session_state.get("erp_role")
    or "Admin"
)
is_admin = str(current_role).strip().lower() == "admin"

user_branch_id = st.session_state.get("user_branch_id")

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
                "Kg": round(float(item["kg"]), 3),
                "Buy Price / Kg": round(float(item["buy_price_per_kg"]), 2),
                "Line Total": round(line_total, 2),
            }
        )
    return pd.DataFrame(rows)


def _cart_total(cart: list[dict]) -> float:
    return round(sum(float(i["kg"]) * float(i["buy_price_per_kg"]) for i in cart), 2)


def _build_summary_text(
    *,
    branch_name: str,
    supplier_name: str,
    receipt_date: str,
    notes: str,
    cart_df: pd.DataFrame,
    total_pieces: int,
    total_kg: float,
    total_value: float,
) -> str:
    lines = [
        "STOCK-IN PURCHASE SUMMARY",
        "=========================",
        f"Branch: {branch_name}",
        f"Supplier: {supplier_name}",
        f"Receipt Date: {receipt_date}",
        "",
        "LINES",
        "-----",
    ]

    for _, row in cart_df.iterrows():
        lines.append(
            f"Line {int(row['Line'])}: {row['Size']} | "
            f"Pieces={int(row['Pieces'])} | "
            f"Kg={float(row['Kg']):.3f} | "
            f"BuyPrice/Kg={float(row['Buy Price / Kg']):,.2f} | "
            f"Line Total={float(row['Line Total']):,.2f}"
        )

    lines.extend(
        [
            "",
            "TOTALS",
            "------",
            f"Total Pieces: {total_pieces}",
            f"Total Kg: {total_kg:.3f}",
            f"Tentative Total Value: KES {total_value:,.2f}",
        ]
    )

    if notes.strip():
        lines.extend(["", f"Notes: {notes.strip()}"])

    return "\n".join(lines)


def _get_procurement_rule(branch_id: int) -> dict:
    rows = q(
        conn,
        """
        SELECT
            bpr.can_purchase_direct,
            bpr.can_receive_transfer,
            bpr.default_source_branch_id,
            bpr.notes,
            src.name AS default_source_branch_name
        FROM branch_procurement_rules bpr
        LEFT JOIN branches src ON src.id = bpr.default_source_branch_id
        WHERE bpr.branch_id=?
        """,
        (int(branch_id),),
    )
    if not rows:
        return {
            "can_purchase_direct": 1,
            "can_receive_transfer": 1,
            "default_source_branch_id": None,
            "default_source_branch_name": None,
            "notes": None,
        }

    r = rows[0]
    return {
        "can_purchase_direct": int(r["can_purchase_direct"]),
        "can_receive_transfer": int(r["can_receive_transfer"]),
        "default_source_branch_id": int(r["default_source_branch_id"]) if r["default_source_branch_id"] is not None else None,
        "default_source_branch_name": str(r["default_source_branch_name"]) if r["default_source_branch_name"] is not None else None,
        "notes": str(r["notes"]) if r["notes"] is not None else None,
    }


# -------------------------
# Reference lookups
# -------------------------
branch_name_to_id = {str(b["name"]): int(b["id"]) for b in branches}
size_code_to_id = {str(s["code"]): int(s["id"]) for s in sizes}
supplier_names = [str(s["name"]) for s in suppliers]

# Branch selector logic
if user_branch_id and not is_admin:
    assigned_branch = next((b for b in branches if int(b["id"]) == int(user_branch_id)), None)
    if not assigned_branch:
        st.error("Assigned branch not found.")
        st.stop()
    selected_branch_name = str(assigned_branch["name"])
    selected_branch_id = int(assigned_branch["id"])
else:
    selected_branch_name = None
    selected_branch_id = None

col1, col2 = st.columns([1.15, 1], gap="large")

with col1:
    st.subheader("Create stock-in (cart-style entry)")

    receipt_date = st.date_input("Receipt date")

    if selected_branch_name is not None:
        st.selectbox("Branch", options=[selected_branch_name], index=0, disabled=True)
        branch_name = selected_branch_name
        branch_id = int(selected_branch_id)
    else:
        branch_name = st.selectbox("Branch", options=list(branch_name_to_id.keys()))
        branch_id = int(branch_name_to_id[branch_name])

    procurement_rule = _get_procurement_rule(int(branch_id))
    can_purchase_direct = bool(procurement_rule["can_purchase_direct"])
    can_receive_transfer = bool(procurement_rule["can_receive_transfer"])
    default_source_branch_name = procurement_rule["default_source_branch_name"]
    procurement_notes = procurement_rule["notes"] or ""

    if is_admin:
        st.info("Admin override: direct stock-in is allowed.")
    else:
        if can_purchase_direct:
            st.success("This branch is allowed to purchase directly into stock.")
        else:
            msg = "This branch is not allowed to purchase directly."
            if default_source_branch_name:
                msg += f" Default source branch: {default_source_branch_name}."
            st.warning(msg)
            if can_receive_transfer:
                st.info("Use stock transfer / replenishment from the approved source branch.")
            if procurement_notes:
                st.caption(f"Rule note: {procurement_notes}")

    supplier_name = st.selectbox(
        "Supplier",
        options=supplier_names if supplier_names else ["No suppliers configured"],
        disabled=(len(supplier_names) == 0 or (not is_admin and not can_purchase_direct)),
        help="Suppliers are pre-configured to avoid duplicates and typing errors.",
    )
    notes = st.text_area("Notes (optional)", value="", height=80, disabled=(not is_admin and not can_purchase_direct))

    st.markdown("**Add one line at a time**")

    add_c1, add_c2, add_c3, add_c4 = st.columns([1, 1, 1, 1], gap="small")

    with add_c1:
        size_code = st.selectbox(
            "Size",
            options=list(size_code_to_id.keys()),
            key="stockin_size",
            disabled=(not is_admin and not can_purchase_direct),
        )

    with add_c2:
        pieces = st.number_input(
            "Pieces",
            min_value=1,
            value=1,
            step=1,
            key="stockin_pieces",
            disabled=(not is_admin and not can_purchase_direct),
        )

    with add_c3:
        kg = st.number_input(
            "Kg",
            min_value=0.001,
            value=1.000,
            step=0.1,
            format="%.3f",
            key="stockin_kg",
            disabled=(not is_admin and not can_purchase_direct),
        )

    with add_c4:
        buy_price_per_kg = st.number_input(
            "Buy price/kg",
            min_value=0.0,
            value=0.0,
            step=1.0,
            key="stockin_bp",
            help="Captured per size line for clean valuation and margin tracking.",
            disabled=(not is_admin and not can_purchase_direct),
        )

    add_line_col1, add_line_col2 = st.columns([1, 1])

    with add_line_col1:
        if st.button(
            "Add Line to Cart",
            type="secondary",
            use_container_width=True,
            disabled=(not is_admin and not can_purchase_direct),
        ):
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

        summary_text = _build_summary_text(
            branch_name=branch_name,
            supplier_name=supplier_name,
            receipt_date=receipt_date.isoformat(),
            notes=notes,
            cart_df=cart_df,
            total_pieces=total_pieces,
            total_kg=total_kg,
            total_value=total_value,
        )

        with st.expander("Purchase Summary Preview", expanded=True):
            st.markdown("**Purchase summary (preview before final confirmation)**")
            st.write(f"**Branch:** {branch_name}")
            st.write(f"**Supplier:** {supplier_name}")
            st.write(f"**Receipt date:** {receipt_date.isoformat()}")
            if notes.strip():
                st.write(f"**Notes:** {notes.strip()}")

            st.dataframe(cart_df, use_container_width=True, hide_index=True)
            st.write(f"**Total Pieces:** {total_pieces}")
            st.write(f"**Total Kg:** {total_kg:,.3f}")
            st.write(f"**Tentative Total Value:** KES {total_value:,.2f}")

            csv_buffer = io.StringIO()
            cart_df.to_csv(csv_buffer, index=False)

            st.download_button(
                "Download Purchase Summary (CSV)",
                data=csv_buffer.getvalue(),
                file_name=f"stock_in_summary_{receipt_date.isoformat()}_{branch_name.replace(' ', '_')}.csv",
                mime="text/csv",
                use_container_width=True,
            )

            st.download_button(
                "Download Purchase Summary (TXT)",
                data=summary_text,
                file_name=f"stock_in_summary_{receipt_date.isoformat()}_{branch_name.replace(' ', '_')}.txt",
                mime="text/plain",
                use_container_width=True,
            )

        finalize_disabled = (
            st.session_state["stock_in_submitted"]
            or len(cart) == 0
            or len(supplier_names) == 0
            or (not is_admin and not can_purchase_direct)
        )

        if st.button(
            "Create Stock-In (Auto Batches)",
            type="primary",
            use_container_width=True,
            disabled=finalize_disabled,
        ):
            try:
                if not is_admin and not can_purchase_direct:
                    raise ValueError("This branch is not allowed to purchase directly. Use transfer from the approved source branch.")

                st.session_state["stock_in_submitted"] = True

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
                    branch_id=int(branch_id),
                    supplier=supplier_name.strip() if supplier_name else None,
                    notes=notes.strip() or None,
                    lines=lines,
                )

                created_count = len(created)
                _reset_stock_in_state()
                st.success(f"Stock-In saved successfully. {created_count} batch(es) created.")

                try:
                    st.switch_page("home.py")
                except Exception:
                    st.rerun()

            except Exception as e:
                st.session_state["stock_in_submitted"] = False
                st.error(str(e))
    else:
        if not is_admin and not can_purchase_direct:
            st.info("Direct stock-in is disabled for this branch under the current procurement rule.")
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
