from __future__ import annotations

import streamlit as st
import pandas as pd

from core.config import get_settings, persist_data_dir
from core.db import get_conn, ensure_schema, q, x
from core.services.demo_data import load_demo_data, wipe_all, upsert_reference_data


st.set_page_config(page_title="Data Management", page_icon="🧪", layout="wide")
st.title("🧪 Data Management (Demo mode)")
st.caption("Initialize database, load/reset demo data, configure storage, and manage reference/admin setup.")

settings = get_settings()
conn = get_conn(settings.db_path)
ensure_schema(conn)

# -------------------------
# Role handling
# -------------------------
current_role = (
    st.session_state.get("user_role")
    or st.session_state.get("erp_role")
    or "Admin"
)

is_admin = str(current_role).strip().lower() == "admin"

# -------------------------
# Storage
# -------------------------
st.subheader("Storage location")
st.write(f"Current data directory: `{settings.data_dir}`")

new_dir = st.text_input("Set a different data directory (optional)", value=str(settings.data_dir))
if st.button("Save & use this data directory"):
    try:
        persist_data_dir(new_dir)
        st.success("Saved. The app will reload using the new directory.")
        st.cache_resource.clear()
        st.rerun()
    except Exception as e:
        st.error(str(e))

st.divider()

# -------------------------
# Database actions
# -------------------------
st.subheader("Database actions")

c1, c2, c3 = st.columns(3)
with c1:
    if st.button("Initialize / Repair Database", type="primary"):
        try:
            ensure_schema(conn)
            upsert_reference_data(conn)
            st.success("Database is ready.")
        except Exception as e:
            st.error(str(e))

with c2:
    if st.button("Load Demo Data"):
        try:
            load_demo_data(conn)
            st.success("Demo data loaded.")
            st.rerun()
        except Exception as e:
            st.error(str(e))

with c3:
    if st.button("Reset (Wipe All Data)"):
        try:
            wipe_all(conn)
            st.success("All data wiped.")
            st.rerun()
        except Exception as e:
            st.error(str(e))

st.divider()

# -------------------------
# Supplier management
# -------------------------
st.subheader("Supplier Management")
st.caption("Suppliers are pre-configured so stock-in uses dropdown selection and avoids duplicates.")

if not is_admin:
    st.warning("Supplier management is restricted to Admin only.")
else:
    sup_tab1, sup_tab2 = st.tabs(["Add / Update Supplier", "Supplier List"])

    with sup_tab1:
        s1, s2 = st.columns(2)
        with s1:
            supplier_name = st.text_input("Supplier name", value="", key="supplier_name")
            supplier_contact = st.text_input("Contact person (optional)", value="", key="supplier_contact")
        with s2:
            supplier_phone = st.text_input("Phone (optional)", value="", key="supplier_phone")
            supplier_active = st.checkbox("Active", value=True, key="supplier_active")

        if st.button("Save Supplier", type="primary"):
            try:
                name = supplier_name.strip()
                contact = supplier_contact.strip() or None
                phone = supplier_phone.strip() or None
                active = 1 if supplier_active else 0

                if not name:
                    raise ValueError("Supplier name is required.")

                existing = q(conn, "SELECT id FROM suppliers WHERE LOWER(name)=LOWER(?)", (name,))
                if existing:
                    x(
                        conn,
                        """
                        UPDATE suppliers
                        SET contact_person=?, phone=?, is_active=?
                        WHERE id=?
                        """,
                        (contact, phone, active, int(existing[0]["id"])),
                    )
                    st.success("Supplier updated.")
                else:
                    x(
                        conn,
                        """
                        INSERT INTO suppliers(name, contact_person, phone, is_active)
                        VALUES (?, ?, ?, ?)
                        """,
                        (name, contact, phone, active),
                    )
                    st.success("Supplier added.")

                st.rerun()
            except Exception as e:
                st.error(str(e))

    with sup_tab2:
        suppliers = q(
            conn,
            """
            SELECT id, name, contact_person, phone, is_active
            FROM suppliers
            ORDER BY name
            """
        )
        if suppliers:
            sup_df = pd.DataFrame([dict(r) for r in suppliers])
            sup_df["is_active"] = sup_df["is_active"].map({1: "Yes", 0: "No"})
            st.dataframe(sup_df, use_container_width=True, hide_index=True)
        else:
            st.info("No suppliers found yet.")

st.divider()

# -------------------------
# Customer preview / search
# -------------------------
st.subheader("Customer Preview / Search")
st.caption("Preview customers by branch and search by name, phone, or house number.")

branches = q(conn, "SELECT id, name FROM branches ORDER BY name")
branch_names = [b["name"] for b in branches] if branches else []

if not branch_names:
    st.info("No branches found. Initialize the database first.")
else:
    cp1, cp2 = st.columns([1, 1.5])
    with cp1:
        customer_branch_name = st.selectbox("Branch", options=branch_names, key="cust_preview_branch")
        customer_branch_id = next(int(b["id"]) for b in branches if b["name"] == customer_branch_name)
    with cp2:
        customer_search = st.text_input(
            "Search customer (name / phone / house number)",
            value="",
            key="cust_preview_search",
        ).strip()

    if customer_search:
        customer_rows = q(
            conn,
            """
            SELECT
                c.id,
                br.name AS branch,
                c.display_name,
                c.category,
                c.phone,
                c.house_number,
                c.notes,
                c.is_active,
                c.created_at
            FROM customers c
            JOIN branches br ON br.id = c.branch_id
            WHERE c.branch_id=?
              AND (
                LOWER(c.display_name) LIKE LOWER(?)
                OR LOWER(COALESCE(c.phone, '')) LIKE LOWER(?)
                OR LOWER(COALESCE(c.house_number, '')) LIKE LOWER(?)
              )
            ORDER BY c.display_name
            """,
            (
                int(customer_branch_id),
                f"%{customer_search}%",
                f"%{customer_search}%",
                f"%{customer_search}%",
            ),
        )
    else:
        customer_rows = q(
            conn,
            """
            SELECT
                c.id,
                br.name AS branch,
                c.display_name,
                c.category,
                c.phone,
                c.house_number,
                c.notes,
                c.is_active,
                c.created_at
            FROM customers c
            JOIN branches br ON br.id = c.branch_id
            WHERE c.branch_id=?
            ORDER BY c.display_name
            LIMIT 200
            """,
            (int(customer_branch_id),),
        )

    if customer_rows:
        cust_df = pd.DataFrame([dict(r) for r in customer_rows])
        cust_df["is_active"] = cust_df["is_active"].map({1: "Yes", 0: "No"})
        st.dataframe(cust_df, use_container_width=True, hide_index=True)
    else:
        st.caption("No customers found for this branch/search.")

st.divider()

# -------------------------
# Branch price setup
# -------------------------
st.subheader("Branch Price Setup")
st.caption("Set retail price per piece and wholesale price per kg by branch and fish size.")

if not is_admin:
    st.warning("Branch price setup is restricted to Admin only.")
else:
    sizes = q(conn, "SELECT id, code, description FROM sizes ORDER BY sort_order, code")

    if not branch_names or not sizes:
        st.info("Branches or sizes are missing. Initialize the database first.")
    else:
        bp1, bp2, bp3, bp4 = st.columns(4)

        with bp1:
            price_branch_name = st.selectbox("Branch for pricing", options=branch_names, key="price_branch")
            price_branch_id = next(int(b["id"]) for b in branches if b["name"] == price_branch_name)

        with bp2:
            size_label = st.selectbox(
                "Size",
                options=[str(s["code"]) for s in sizes],
                key="price_size",
            )
            size_id = next(int(s["id"]) for s in sizes if str(s["code"]) == str(size_label))

        existing_price = q(
            conn,
            """
            SELECT retail_price_per_piece, wholesale_price_per_kg, is_active
            FROM branch_size_prices
            WHERE branch_id=? AND size_id=?
            """,
            (int(price_branch_id), int(size_id)),
        )

        default_retail = float(existing_price[0]["retail_price_per_piece"]) if existing_price else 0.0
        default_wholesale = float(existing_price[0]["wholesale_price_per_kg"]) if existing_price else 0.0
        default_active = bool(existing_price[0]["is_active"]) if existing_price else True

        with bp3:
            retail_price = st.number_input(
                "Retail price / piece",
                min_value=0.0,
                value=default_retail,
                step=1.0,
                key="price_retail_piece",
            )

        with bp4:
            wholesale_price = st.number_input(
                "Wholesale price / kg",
                min_value=0.0,
                value=default_wholesale,
                step=1.0,
                key="price_wholesale_kg",
            )

        price_active = st.checkbox("Pricing active", value=default_active, key="price_active")

        if st.button("Save Branch Price", type="primary"):
            try:
                if float(retail_price) <= 0:
                    raise ValueError("Retail price per piece must be greater than 0.")
                if float(wholesale_price) <= 0:
                    raise ValueError("Wholesale price per kg must be greater than 0.")

                if existing_price:
                    x(
                        conn,
                        """
                        UPDATE branch_size_prices
                        SET retail_price_per_piece=?, wholesale_price_per_kg=?, is_active=?
                        WHERE branch_id=? AND size_id=?
                        """,
                        (
                            float(retail_price),
                            float(wholesale_price),
                            1 if price_active else 0,
                            int(price_branch_id),
                            int(size_id),
                        ),
                    )
                    st.success("Branch price updated.")
                else:
                    x(
                        conn,
                        """
                        INSERT INTO branch_size_prices(
                            branch_id, size_id, retail_price_per_piece, wholesale_price_per_kg, is_active
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            int(price_branch_id),
                            int(size_id),
                            float(retail_price),
                            float(wholesale_price),
                            1 if price_active else 0,
                        ),
                    )
                    st.success("Branch price added.")

                st.rerun()
            except Exception as e:
                st.error(str(e))

        st.markdown("**Current price matrix**")
        price_rows = q(
            conn,
            """
            SELECT
                br.name AS branch,
                sz.code AS size_code,
                bsp.retail_price_per_piece,
                bsp.wholesale_price_per_kg,
                bsp.is_active
            FROM branch_size_prices bsp
            JOIN branches br ON br.id = bsp.branch_id
            JOIN sizes sz ON sz.id = bsp.size_id
            ORDER BY br.name, sz.sort_order, sz.code
            """
        )
        if price_rows:
            price_df = pd.DataFrame([dict(r) for r in price_rows])
            price_df["is_active"] = price_df["is_active"].map({1: "Yes", 0: "No"})
            st.dataframe(price_df, use_container_width=True, hide_index=True)
        else:
            st.caption("No branch prices configured yet.")

st.divider()

# -------------------------
# Branch promo management
# -------------------------
st.subheader("Branch Promo Management")
st.caption("Activate or deactivate retail promos per branch. Promos apply automatically in Retail sales when active.")

if not is_admin:
    st.warning("Promo management is restricted to Admin only.")
else:
    promos = q(
        conn,
        """
        SELECT id, code, name, buy_qty, free_qty, applies_mode, is_active
        FROM promos
        ORDER BY name
        """
    )

    if not branches:
        st.info("No branches found. Initialize the database first.")
    elif not promos:
        st.info("No promos found. Initialize the database first.")
    else:
        branch_name = st.selectbox(
            "Select branch for promo setup",
            options=[b["name"] for b in branches],
            key="promo_branch",
        )
        branch_id = next(int(b["id"]) for b in branches if b["name"] == branch_name)

        active_map_rows = q(
            conn,
            """
            SELECT bp.id, bp.branch_id, bp.promo_id, bp.is_active,
                   p.code, p.name, p.buy_qty, p.free_qty, p.applies_mode
            FROM branch_promos bp
            JOIN promos p ON p.id = bp.promo_id
            WHERE bp.branch_id = ?
            ORDER BY p.name
            """,
            (branch_id,),
        )

        active_map = {int(r["promo_id"]): dict(r) for r in active_map_rows}

        promo_rows = []
        for p in promos:
            promo_id = int(p["id"])
            assigned = active_map.get(promo_id)
            promo_rows.append(
                {
                    "Promo ID": promo_id,
                    "Code": str(p["code"]),
                    "Name": str(p["name"]),
                    "Mode": str(p["applies_mode"]),
                    "Rule": f"Buy {int(p['buy_qty'])} Get {int(p['free_qty'])} Free",
                    "Global Active": "Yes" if int(p["is_active"]) == 1 else "No",
                    "Branch Active": "Yes" if assigned and int(assigned["is_active"]) == 1 else "No",
                }
            )

        st.markdown("**Promo catalogue for selected branch**")
        st.dataframe(pd.DataFrame(promo_rows), use_container_width=True, hide_index=True)

        promo_label_map = {
            f"{p['name']} ({p['code']})": int(p["id"])
            for p in promos
        }

        manage_c1, manage_c2 = st.columns(2)

        with manage_c1:
            promo_to_activate = st.selectbox(
                "Activate promo",
                options=list(promo_label_map.keys()),
                key="promo_activate_select",
            )
            if st.button("Activate / Enable Selected Promo", type="primary"):
                try:
                    promo_id = int(promo_label_map[promo_to_activate])

                    existing = q(
                        conn,
                        """
                        SELECT id, is_active
                        FROM branch_promos
                        WHERE branch_id=? AND promo_id=?
                        """,
                        (branch_id, promo_id),
                    )

                    if existing:
                        x(
                            conn,
                            """
                            UPDATE branch_promos
                            SET is_active=1
                            WHERE id=?
                            """,
                            (int(existing[0]["id"]),),
                        )
                    else:
                        x(
                            conn,
                            """
                            INSERT INTO branch_promos(branch_id, promo_id, is_active)
                            VALUES (?, ?, 1)
                            """,
                            (branch_id, promo_id),
                        )

                    st.success("Promo activated for this branch.")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))

        with manage_c2:
            active_only_labels = [
                f"{r['name']} ({r['code']})"
                for r in active_map_rows
                if int(r["is_active"]) == 1
            ]

            if active_only_labels:
                promo_to_deactivate = st.selectbox(
                    "Deactivate promo",
                    options=active_only_labels,
                    key="promo_deactivate_select",
                )
                if st.button("Deactivate Selected Promo"):
                    try:
                        selected_code = promo_to_deactivate.split("(")[-1].replace(")", "").strip()
                        row = next(r for r in active_map_rows if str(r["code"]) == selected_code and int(r["is_active"]) == 1)

                        x(
                            conn,
                            """
                            UPDATE branch_promos
                            SET is_active=0
                            WHERE id=?
                            """,
                            (int(row["id"]),),
                        )

                        st.success("Promo deactivated for this branch.")
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))
            else:
                st.info("No active promos to deactivate for this branch.")

        st.markdown("**Current active promos for this branch**")
        active_now = q(
            conn,
            """
            SELECT
                p.code,
                p.name,
                p.buy_qty,
                p.free_qty,
                p.applies_mode
            FROM branch_promos bp
            JOIN promos p ON p.id = bp.promo_id
            WHERE bp.branch_id=? AND bp.is_active=1 AND p.is_active=1
            ORDER BY p.name
            """,
            (branch_id,),
        )

        if active_now:
            active_now_df = pd.DataFrame(
                [
                    {
                        "Code": r["code"],
                        "Name": r["name"],
                        "Mode": r["applies_mode"],
                        "Rule": f"Buy {int(r['buy_qty'])} Get {int(r['free_qty'])} Free",
                    }
                    for r in active_now
                ]
            )
            st.dataframe(active_now_df, use_container_width=True, hide_index=True)
        else:
            st.caption("No active promos for this branch.")

st.divider()

# -------------------------
# Data preview
# -------------------------
st.subheader("Data preview")

counts = q(
    conn,
    """
    SELECT 'branches' AS table_name, COUNT(*) AS n FROM branches
    UNION ALL SELECT 'sizes', COUNT(*) FROM sizes
    UNION ALL SELECT 'suppliers', COUNT(*) FROM suppliers
    UNION ALL SELECT 'customers', COUNT(*) FROM customers
    UNION ALL SELECT 'branch_size_prices', COUNT(*) FROM branch_size_prices
    UNION ALL SELECT 'promos', COUNT(*) FROM promos
    UNION ALL SELECT 'branch_promos', COUNT(*) FROM branch_promos
    UNION ALL SELECT 'batches', COUNT(*) FROM batches
    UNION ALL SELECT 'sales', COUNT(*) FROM sales
    UNION ALL SELECT 'inventory_adjustments', COUNT(*) FROM inventory_adjustments
    UNION ALL SELECT 'batch_closures', COUNT(*) FROM batch_closures
    """,
)
st.dataframe(pd.DataFrame([dict(r) for r in counts]), use_container_width=True, hide_index=True)