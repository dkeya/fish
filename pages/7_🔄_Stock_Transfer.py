from __future__ import annotations

import pandas as pd
import streamlit as st

from core.config import get_settings
from core.db import get_conn, ensure_schema, q
from core.services.inventory import (
    create_stock_transfer,
    get_allowed_visible_branch_ids,
    get_branch_procurement_rule,
    size_inventory_summary_visible_to_branch,
    transfer_candidates_for_size,
)


st.set_page_config(page_title="Stock Transfer", page_icon="🔄", layout="wide")
st.title("🔄 Stock Transfer")
st.caption(
    "Move fish stock from one branch to another in a controlled, auditable way. "
    "Transfers respect branch visibility and branch procurement rules."
)

settings = get_settings()
conn = get_conn(settings.db_path)
ensure_schema(conn)

# -------------------------
# Session / role context
# -------------------------
current_role = (
    st.session_state.get("user_role")
    or st.session_state.get("erp_role")
    or "Admin"
)
is_admin = str(current_role).strip().lower() == "admin"

user_branch_id = st.session_state.get("user_branch_id")
extra_visible_branch_ids = st.session_state.get("visible_branch_ids", [])

if "stock_transfer_submitted" not in st.session_state:
    st.session_state["stock_transfer_submitted"] = False

if "stock_transfer_result" not in st.session_state:
    st.session_state["stock_transfer_result"] = None


def _reset_transfer_state() -> None:
    st.session_state["stock_transfer_submitted"] = False
    st.session_state["stock_transfer_result"] = None


branches = q(conn, "SELECT id, name FROM branches ORDER BY name")
sizes = q(conn, "SELECT id, code, description FROM sizes ORDER BY sort_order, code")

if not branches or not sizes:
    st.info("Branches or sizes are missing. Initialize the database first in 🧪 Data Management.")
    st.stop()

if user_branch_id is None:
    fallback = q(conn, "SELECT id FROM branches ORDER BY id LIMIT 1")
    user_branch_id = int(fallback[0]["id"]) if fallback else 0

visible_branch_ids = get_allowed_visible_branch_ids(
    conn,
    branch_id=int(user_branch_id),
    role=str(current_role),
    extra_visible_branch_ids=extra_visible_branch_ids,
)

visible_branch_rows = [b for b in branches if int(b["id"]) in set(int(v) for v in visible_branch_ids)]
visible_branch_names = [str(b["name"]) for b in visible_branch_rows]

branch_name_to_id = {str(b["name"]): int(b["id"]) for b in branches}
size_code_to_id = {str(s["code"]): int(s["id"]) for s in sizes}

tab1, tab2 = st.tabs(["Create Transfer", "Visible Inventory by Size"])

with tab1:
    st.subheader("Create stock transfer")

    if not visible_branch_rows:
        st.warning("No visible source branches are configured for your current role/branch.")
        st.stop()

    if st.session_state["stock_transfer_submitted"]:
        result = st.session_state.get("stock_transfer_result") or {}
        st.success("Stock transfer posted successfully.")

        if result:
            st.write(
                f"**Transfer Code:** {result.get('transfer_code', '-')}"
                f"  |  **Pieces:** {result.get('pieces_transferred', 0)}"
                f"  |  **Kg:** {result.get('kg_transferred', 0.0):.3f}"
                f"  |  **Lines:** {result.get('lines_created', 0)}"
            )
            if result.get("destination_batch_ids"):
                st.caption(f"Destination batch IDs created: {', '.join(str(x) for x in result['destination_batch_ids'])}")

        st.info("This transfer has already been posted. Start a new transfer only if needed.")
        if st.button("Create Another Transfer", type="primary"):
            _reset_transfer_state()
            st.rerun()

    else:
        c1, c2 = st.columns(2)

        with c1:
            if not is_admin:
                default_branch_row = next((b for b in branches if int(b["id"]) == int(user_branch_id)), None)
                if default_branch_row:
                    from_branch_name = str(default_branch_row["name"])
                    st.selectbox("Source branch", options=[from_branch_name], index=0, disabled=True)
                    from_branch_id = int(default_branch_row["id"])
                else:
                    st.error("Assigned user branch was not found.")
                    st.stop()
            else:
                from_branch_name = st.selectbox("Source branch", options=visible_branch_names, key="transfer_from_branch")
                from_branch_id = int(branch_name_to_id[from_branch_name])

            allowed_destinations = [b for b in branches if int(b["id"]) != int(from_branch_id)]
            if not allowed_destinations:
                st.info("No destination branches available.")
                st.stop()

            to_branch_name = st.selectbox(
                "Destination branch",
                options=[str(b["name"]) for b in allowed_destinations],
                key="transfer_to_branch",
            )
            to_branch_id = int(branch_name_to_id[to_branch_name])

            transfer_size_code = st.selectbox(
                "Fish size",
                options=[str(s["code"]) for s in sizes],
                key="transfer_size_code",
            )
            transfer_size_id = int(size_code_to_id[transfer_size_code])

        with c2:
            transfer_pieces = st.number_input(
                "Pieces to transfer",
                min_value=1,
                value=1,
                step=1,
                key="transfer_pieces",
            )
            transfer_notes = st.text_area(
                "Notes (optional)",
                value="",
                height=120,
                key="transfer_notes",
            )

        dest_rule = get_branch_procurement_rule(conn, branch_id=int(to_branch_id))

        destination_can_receive = int(dest_rule["can_receive_transfer"]) == 1
        required_source_branch_name = dest_rule.get("default_source_branch_name")
        required_source_branch_id = dest_rule.get("default_source_branch_id")

        source_rule_ok = True
        if required_source_branch_id is not None and int(from_branch_id) != int(required_source_branch_id):
            source_rule_ok = False

        if destination_can_receive:
            st.success(f"{to_branch_name} is allowed to receive transfers.")
        else:
            st.warning(f"{to_branch_name} is not allowed to receive transfers under current procurement rules.")

        if required_source_branch_name:
            if source_rule_ok:
                st.info(f"Configured source branch for {to_branch_name}: {required_source_branch_name}")
            else:
                st.warning(
                    f"{to_branch_name} is configured to receive stock from **{required_source_branch_name}**. "
                    f"Current source branch **{from_branch_name}** does not match that rule."
                )

        if dest_rule.get("notes"):
            st.caption(f"Rule note: {dest_rule['notes']}")

        st.divider()
        st.subheader("Transfer availability preview")

        candidates = transfer_candidates_for_size(
            conn,
            from_branch_id=int(from_branch_id),
            size_id=int(transfer_size_id),
        )

        if candidates:
            cand_df = pd.DataFrame(candidates)
            st.dataframe(cand_df, use_container_width=True, hide_index=True)

            total_source_pcs = int(cand_df["pcs_on_hand"].sum())
            total_source_kg = float(cand_df["kg_on_hand"].sum())

            p1, p2 = st.columns(2)
            p1.metric("Available pieces", f"{total_source_pcs:,}")
            p2.metric("Available kg", f"{total_source_kg:,.3f}")

            projected_rows = []
            remaining = int(transfer_pieces)

            for row in candidates:
                if remaining <= 0:
                    break
                take_pcs = min(int(row["pcs_on_hand"]), remaining)
                if take_pcs <= 0:
                    continue
                take_kg = round(float(take_pcs) * float(row["avg_kg_per_piece"]), 3)
                projected_rows.append(
                    {
                        "From Batch": row["batch_code"],
                        "Size": transfer_size_code,
                        "Pieces to Move": int(take_pcs),
                        "Kg to Move": float(take_kg),
                        "Unit Cost / Kg": float(row["buy_price_per_kg"]),
                    }
                )
                remaining -= int(take_pcs)

            if projected_rows:
                st.markdown("**Projected transfer allocation**")
                projected_df = pd.DataFrame(projected_rows)
                st.dataframe(projected_df, use_container_width=True, hide_index=True)

                proj_pcs = int(projected_df["Pieces to Move"].sum())
                proj_kg = float(projected_df["Kg to Move"].sum())

                s1, s2 = st.columns(2)
                s1.metric("Projected pieces to transfer", f"{proj_pcs:,}")
                s2.metric("Projected kg to transfer", f"{proj_kg:,.3f}")

                if proj_pcs < int(transfer_pieces):
                    st.warning("Requested pieces exceed available stock for this size.")
            else:
                st.info("No projected allocation could be built for this request.")
        else:
            st.info("No transferable stock found for the selected source branch and size.")

        st.divider()

        disable_post = (
            len(candidates) == 0
            or not destination_can_receive
            or (not is_admin and not source_rule_ok)
        )

        if disable_post and not destination_can_receive:
            st.caption("Posting is disabled because the destination branch cannot receive transfers.")

        if disable_post and destination_can_receive and (not is_admin and not source_rule_ok):
            st.caption("Posting is disabled because the source branch does not match the destination branch’s configured default source.")

        if st.button("Post Stock Transfer", type="primary", use_container_width=True, disabled=disable_post):
            try:
                if not is_admin and not source_rule_ok:
                    raise ValueError(
                        f"This destination branch must receive stock from {required_source_branch_name}. "
                        f"Current source branch {from_branch_name} is not allowed."
                    )

                result = create_stock_transfer(
                    conn,
                    from_branch_id=int(from_branch_id),
                    to_branch_id=int(to_branch_id),
                    size_id=int(transfer_size_id),
                    pieces=int(transfer_pieces),
                    notes=transfer_notes.strip() or None,
                    created_by=str(current_role),
                )

                st.session_state["stock_transfer_submitted"] = True
                st.session_state["stock_transfer_result"] = result
                st.rerun()

            except Exception as e:
                st.error(str(e))

with tab2:
    st.subheader("Visible inventory by size")
    st.caption("This view shows the fish stock you are allowed to see based on branch visibility rules.")

    rows = size_inventory_summary_visible_to_branch(
        conn,
        branch_id=int(user_branch_id),
        role=str(current_role),
        extra_visible_branch_ids=extra_visible_branch_ids,
    )

    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)

        summary = (
            df.groupby(["branch", "size_code"], as_index=False)
            .agg(
                pcs_on_hand=("pcs_on_hand", "sum"),
                kg_on_hand=("kg_on_hand", "sum"),
                stock_value=("stock_value", "sum"),
            )
            .sort_values(["branch", "size_code"])
        )

        st.markdown("**Summarized visible stock by branch and size**")
        st.dataframe(summary, use_container_width=True, hide_index=True)
    else:
        st.info("No visible stock found.")
