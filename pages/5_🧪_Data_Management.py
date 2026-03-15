from __future__ import annotations

import math

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
# Shared lookups
# -------------------------
branches = q(conn, "SELECT id, name FROM branches ORDER BY name")
branch_names = [b["name"] for b in branches] if branches else []
branch_id_by_name = {str(b["name"]): int(b["id"]) for b in branches}

sizes = q(conn, "SELECT id, code, description FROM sizes ORDER BY sort_order, code")

product_categories = q(
    conn,
    """
    SELECT id, code, name, description, is_active
    FROM product_categories
    ORDER BY name
    """
)
category_names = [str(r["name"]) for r in product_categories]
category_id_by_name = {str(r["name"]): int(r["id"]) for r in product_categories}
category_code_by_name = {str(r["name"]): str(r["code"]) for r in product_categories}

products = q(
    conn,
    """
    SELECT
        p.id,
        p.sku,
        p.name,
        p.category_id,
        pc.name AS category_name,
        pc.code AS category_code,
        p.product_type,
        p.stock_uom,
        p.tracks_stock,
        p.uses_batch_fifo,
        p.uses_size_dimension,
        p.requires_piece_entry,
        p.requires_weight_entry,
        p.service_non_stock,
        p.default_notes,
        p.is_active
    FROM products p
    JOIN product_categories pc ON pc.id = p.category_id
    ORDER BY pc.name, p.name
    """
)
product_names = [str(r["name"]) for r in products]

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
# Product category preview
# -------------------------
st.subheader("Product Categories")
st.caption("High-level categories now supported by the system: Fish, Packaging, and Services.")

if product_categories:
    cat_df = pd.DataFrame([dict(r) for r in product_categories])
    cat_df["is_active"] = cat_df["is_active"].map({1: "Yes", 0: "No"})
    st.dataframe(cat_df, use_container_width=True, hide_index=True)
else:
    st.info("No product categories found yet. Initialize the database first.")

st.divider()

# -------------------------
# Product management
# -------------------------
st.subheader("Product Management")
st.caption("Manage the product/service master structure used by future operational workflows.")

if not is_admin:
    st.warning("Product management is restricted to Admin only.")
else:
    prod_tab1, prod_tab2 = st.tabs(["Add / Update Product", "Product List"])

    with prod_tab1:
        if not category_names:
            st.info("No product categories found. Initialize the database first.")
        else:
            p1, p2, p3 = st.columns(3)

            with p1:
                product_sku = st.text_input("SKU", value="", key="product_sku")
                product_name = st.text_input("Product name", value="", key="product_name")
                product_category_name = st.selectbox("Category", options=category_names, key="product_category_name")

            with p2:
                default_type = category_code_by_name.get(product_category_name, "FISH")
                product_type = st.selectbox(
                    "Product type",
                    options=["FISH", "PACKAGING", "SERVICE"],
                    index=["FISH", "PACKAGING", "SERVICE"].index(default_type) if default_type in ["FISH", "PACKAGING", "SERVICE"] else 0,
                    key="product_type",
                )
                stock_uom = st.selectbox("Stock UOM", options=["KG", "PIECE", "UNIT", "SERVICE"], index=0, key="stock_uom")
                product_active = st.checkbox("Active", value=True, key="product_active")

            with p3:
                tracks_stock = st.checkbox("Tracks stock", value=True, key="tracks_stock")
                uses_batch_fifo = st.checkbox("Uses batch FIFO", value=False, key="uses_batch_fifo")
                uses_size_dimension = st.checkbox("Uses size dimension", value=False, key="uses_size_dimension")

            p4, p5, p6 = st.columns(3)
            with p4:
                requires_piece_entry = st.checkbox("Requires piece entry", value=False, key="requires_piece_entry")
            with p5:
                requires_weight_entry = st.checkbox("Requires weight entry", value=False, key="requires_weight_entry")
            with p6:
                service_non_stock = st.checkbox("Service / non-stock", value=False, key="service_non_stock")

            default_notes = st.text_input("Default notes (optional)", value="", key="product_default_notes")

            if st.button("Save Product", type="primary"):
                try:
                    sku = product_sku.strip()
                    name = product_name.strip()
                    category_id = category_id_by_name.get(product_category_name)

                    if not sku:
                        raise ValueError("SKU is required.")
                    if not name:
                        raise ValueError("Product name is required.")
                    if not category_id:
                        raise ValueError("Product category is required.")

                    existing = q(conn, "SELECT id FROM products WHERE LOWER(sku)=LOWER(?)", (sku,))
                    if existing:
                        x(
                            conn,
                            """
                            UPDATE products
                            SET name=?, category_id=?, product_type=?, stock_uom=?,
                                tracks_stock=?, uses_batch_fifo=?, uses_size_dimension=?,
                                requires_piece_entry=?, requires_weight_entry=?,
                                service_non_stock=?, default_notes=?, is_active=?
                            WHERE id=?
                            """,
                            (
                                name,
                                int(category_id),
                                str(product_type),
                                str(stock_uom),
                                1 if tracks_stock else 0,
                                1 if uses_batch_fifo else 0,
                                1 if uses_size_dimension else 0,
                                1 if requires_piece_entry else 0,
                                1 if requires_weight_entry else 0,
                                1 if service_non_stock else 0,
                                default_notes.strip() or None,
                                1 if product_active else 0,
                                int(existing[0]["id"]),
                            ),
                        )
                        st.success("Product updated.")
                    else:
                        x(
                            conn,
                            """
                            INSERT INTO products(
                                sku, name, category_id, product_type, stock_uom,
                                tracks_stock, uses_batch_fifo, uses_size_dimension,
                                requires_piece_entry, requires_weight_entry,
                                service_non_stock, default_notes, is_active
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                sku,
                                name,
                                int(category_id),
                                str(product_type),
                                str(stock_uom),
                                1 if tracks_stock else 0,
                                1 if uses_batch_fifo else 0,
                                1 if uses_size_dimension else 0,
                                1 if requires_piece_entry else 0,
                                1 if requires_weight_entry else 0,
                                1 if service_non_stock else 0,
                                default_notes.strip() or None,
                                1 if product_active else 0,
                            ),
                        )
                        st.success("Product added.")

                    st.rerun()
                except Exception as e:
                    st.error(str(e))

    with prod_tab2:
        product_rows = q(
            conn,
            """
            SELECT
                p.sku,
                p.name,
                pc.name AS category,
                p.product_type,
                p.stock_uom,
                p.tracks_stock,
                p.uses_batch_fifo,
                p.uses_size_dimension,
                p.requires_piece_entry,
                p.requires_weight_entry,
                p.service_non_stock,
                p.is_active
            FROM products p
            JOIN product_categories pc ON pc.id = p.category_id
            ORDER BY pc.name, p.name
            """
        )
        if product_rows:
            prod_df = pd.DataFrame([dict(r) for r in product_rows])
            for col in [
                "tracks_stock",
                "uses_batch_fifo",
                "uses_size_dimension",
                "requires_piece_entry",
                "requires_weight_entry",
                "service_non_stock",
                "is_active",
            ]:
                prod_df[col] = prod_df[col].map({1: "Yes", 0: "No"})
            st.dataframe(prod_df, use_container_width=True, hide_index=True)
        else:
            st.info("No products found yet.")

st.divider()

# -------------------------
# Branch product enablement
# -------------------------
st.subheader("Branch Product Enablement")
st.caption("Enable or disable products/services by branch.")

if not is_admin:
    st.warning("Branch product enablement is restricted to Admin only.")
else:
    if not branch_names or not products:
        st.info("Branches or products are missing. Initialize the database first.")
    else:
        be1, be2, be3 = st.columns(3)

        with be1:
            enable_branch_name = st.selectbox("Branch", options=branch_names, key="enable_branch")
            enable_branch_id = next(int(b["id"]) for b in branches if b["name"] == enable_branch_name)

        with be2:
            enable_product_name = st.selectbox("Product", options=product_names, key="enable_product")
            enable_product = next(p for p in products if str(p["name"]) == str(enable_product_name))
            enable_product_id = int(enable_product["id"])

        existing_branch_product = q(
            conn,
            """
            SELECT id, is_active
            FROM branch_products
            WHERE branch_id=? AND product_id=?
            """,
            (int(enable_branch_id), int(enable_product_id)),
        )
        existing_active = bool(existing_branch_product[0]["is_active"]) if existing_branch_product else True

        with be3:
            enable_active = st.checkbox("Enabled for branch", value=existing_active, key="enable_active")

        if st.button("Save Branch Product Enablement", type="primary"):
            try:
                if existing_branch_product:
                    x(
                        conn,
                        """
                        UPDATE branch_products
                        SET is_active=?
                        WHERE id=?
                        """,
                        (
                            1 if enable_active else 0,
                            int(existing_branch_product[0]["id"]),
                        ),
                    )
                    st.success("Branch product updated.")
                else:
                    x(
                        conn,
                        """
                        INSERT INTO branch_products(branch_id, product_id, is_active)
                        VALUES (?, ?, ?)
                        """,
                        (
                            int(enable_branch_id),
                            int(enable_product_id),
                            1 if enable_active else 0,
                        ),
                    )
                    st.success("Branch product added.")

                st.rerun()
            except Exception as e:
                st.error(str(e))

        enabled_rows = q(
            conn,
            """
            SELECT
                br.name AS branch,
                p.sku,
                p.name AS product_name,
                p.product_type,
                bp.is_active
            FROM branch_products bp
            JOIN branches br ON br.id = bp.branch_id
            JOIN products p ON p.id = bp.product_id
            ORDER BY br.name, p.name
            """
        )
        if enabled_rows:
            enable_df = pd.DataFrame([dict(r) for r in enabled_rows])
            enable_df["is_active"] = enable_df["is_active"].map({1: "Yes", 0: "No"})
            st.dataframe(enable_df, use_container_width=True, hide_index=True)
        else:
            st.caption("No branch product mappings configured yet.")

st.divider()

# -------------------------
# Customer preview / search
# -------------------------
st.subheader("Customer Preview / Search")
st.caption("Preview customers by branch, search by name/phone/house number, and paginate results.")

if not branch_names:
    st.info("No branches found. Initialize the database first.")
else:
    cp1, cp2, cp3 = st.columns([1, 1.5, 1])

    with cp1:
        customer_branch_name = st.selectbox("Branch", options=branch_names, key="cust_preview_branch")
        customer_branch_id = next(int(b["id"]) for b in branches if b["name"] == customer_branch_name)

    with cp2:
        customer_search = st.text_input(
            "Search customer (name / phone / house number)",
            value="",
            key="cust_preview_search",
        ).strip()

    with cp3:
        customers_per_page = st.selectbox(
            "Rows per page",
            options=[10, 25, 50, 100],
            index=1,
            key="cust_rows_per_page",
        )

    count_rows = q(
        conn,
        """
        SELECT COUNT(*) AS n
        FROM customers c
        WHERE c.branch_id=?
          AND (
            ? = ''
            OR LOWER(c.display_name) LIKE LOWER(?)
            OR LOWER(COALESCE(c.phone, '')) LIKE LOWER(?)
            OR LOWER(COALESCE(c.house_number, '')) LIKE LOWER(?)
          )
        """,
        (
            int(customer_branch_id),
            customer_search,
            f"%{customer_search}%",
            f"%{customer_search}%",
            f"%{customer_search}%",
        ),
    )
    total_customers = int(count_rows[0]["n"]) if count_rows else 0
    total_pages = max(1, math.ceil(total_customers / int(customers_per_page)))

    page_col1, page_col2, page_col3 = st.columns([1, 1, 2])
    with page_col1:
        customer_page = st.number_input(
            "Page",
            min_value=1,
            max_value=total_pages,
            value=1,
            step=1,
            key="cust_page",
        )
    with page_col2:
        st.metric("Matching customers", f"{total_customers}")
    with page_col3:
        st.caption(f"Showing page {int(customer_page)} of {int(total_pages)}")

    offset = (int(customer_page) - 1) * int(customers_per_page)

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
            ? = ''
            OR LOWER(c.display_name) LIKE LOWER(?)
            OR LOWER(COALESCE(c.phone, '')) LIKE LOWER(?)
            OR LOWER(COALESCE(c.house_number, '')) LIKE LOWER(?)
          )
        ORDER BY c.display_name
        LIMIT ? OFFSET ?
        """,
        (
            int(customer_branch_id),
            customer_search,
            f"%{customer_search}%",
            f"%{customer_search}%",
            f"%{customer_search}%",
            int(customers_per_page),
            int(offset),
        ),
    )

    if customer_rows:
        cust_df = pd.DataFrame([dict(r) for r in customer_rows])
        cust_df["is_active"] = cust_df["is_active"].map({1: "Yes", 0: "No"})
        st.dataframe(cust_df, use_container_width=True, hide_index=True)
    else:
        st.caption("No customers found for this branch/search.")

st.divider()

# -------------------------
# Fish branch price setup
# -------------------------
st.subheader("Fish Branch Price Setup")
st.caption("Set retail price per piece and wholesale price per kg by branch and fish size.")

if not is_admin:
    st.warning("Fish branch price setup is restricted to Admin only.")
else:
    if not branch_names or not sizes:
        st.info("Branches or sizes are missing. Initialize the database first.")
    else:
        bp1, bp2, bp3, bp4 = st.columns(4)

        with bp1:
            price_branch_name = st.selectbox("Branch for fish pricing", options=branch_names, key="price_branch")
            price_branch_id = next(int(b["id"]) for b in branches if b["name"] == price_branch_name)

        with bp2:
            size_label = st.selectbox(
                "Fish size",
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

        if st.button("Save Fish Branch Price", type="primary"):
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
                    st.success("Fish branch price updated.")
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
                    st.success("Fish branch price added.")

                st.rerun()
            except Exception as e:
                st.error(str(e))

        st.markdown("**Current fish price matrix**")

        pf1, pf2 = st.columns(2)
        with pf1:
            price_filter_branch = st.selectbox(
                "Filter fish matrix by branch",
                options=["All Branches"] + branch_names,
                key="price_filter_branch",
            )
        with pf2:
            price_filter_size = st.selectbox(
                "Filter fish matrix by size",
                options=["All Sizes"] + [str(s["code"]) for s in sizes],
                key="price_filter_size",
            )

        where_clauses = []
        params: list[object] = []

        if price_filter_branch != "All Branches":
            where_clauses.append("br.name = ?")
            params.append(price_filter_branch)

        if price_filter_size != "All Sizes":
            where_clauses.append("sz.code = ?")
            params.append(price_filter_size)

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        price_rows = q(
            conn,
            f"""
            SELECT
                br.name AS branch,
                sz.code AS size_code,
                bsp.retail_price_per_piece,
                bsp.wholesale_price_per_kg,
                bsp.is_active
            FROM branch_size_prices bsp
            JOIN branches br ON br.id = bsp.branch_id
            JOIN sizes sz ON sz.id = bsp.size_id
            {where_sql}
            ORDER BY br.name, sz.sort_order, sz.code
            """,
            tuple(params),
        )

        if price_rows:
            price_df = pd.DataFrame([dict(r) for r in price_rows])
            price_df["is_active"] = price_df["is_active"].map({1: "Yes", 0: "No"})
            st.dataframe(price_df, use_container_width=True, hide_index=True)
        else:
            st.caption("No fish branch prices configured for the selected filters.")

st.divider()

# -------------------------
# Generic branch product pricing
# -------------------------
st.subheader("Branch Product Pricing")
st.caption("Set pricing for packaging and service products by branch.")

if not is_admin:
    st.warning("Branch product pricing is restricted to Admin only.")
else:
    generic_products = [p for p in products if str(p["product_type"]) != "FISH"]

    if not branch_names or not generic_products:
        st.info("Branches or non-fish products are missing. Initialize the database first.")
    else:
        g1, g2, g3, g4 = st.columns(4)

        with g1:
            gp_branch_name = st.selectbox("Branch for product pricing", options=branch_names, key="gp_branch")
            gp_branch_id = next(int(b["id"]) for b in branches if b["name"] == gp_branch_name)

        with g2:
            gp_product_name = st.selectbox(
                "Product / Service",
                options=[str(p["name"]) for p in generic_products],
                key="gp_product_name",
            )
            gp_product = next(p for p in generic_products if str(p["name"]) == str(gp_product_name))
            gp_product_id = int(gp_product["id"])

        existing_gp = q(
            conn,
            """
            SELECT retail_price, wholesale_price, is_active
            FROM branch_product_prices
            WHERE branch_id=? AND product_id=?
            """,
            (int(gp_branch_id), int(gp_product_id)),
        )

        gp_default_retail = float(existing_gp[0]["retail_price"]) if existing_gp else 0.0
        gp_default_wholesale = float(existing_gp[0]["wholesale_price"]) if existing_gp else 0.0
        gp_default_active = bool(existing_gp[0]["is_active"]) if existing_gp else True

        with g3:
            gp_retail_price = st.number_input(
                "Retail price",
                min_value=0.0,
                value=gp_default_retail,
                step=1.0,
                key="gp_retail_price",
            )

        with g4:
            gp_wholesale_price = st.number_input(
                "Wholesale price",
                min_value=0.0,
                value=gp_default_wholesale,
                step=1.0,
                key="gp_wholesale_price",
            )

        gp_active = st.checkbox("Pricing active", value=gp_default_active, key="gp_active")

        if st.button("Save Branch Product Price", type="primary"):
            try:
                if float(gp_retail_price) < 0:
                    raise ValueError("Retail price cannot be negative.")
                if float(gp_wholesale_price) < 0:
                    raise ValueError("Wholesale price cannot be negative.")

                if existing_gp:
                    x(
                        conn,
                        """
                        UPDATE branch_product_prices
                        SET retail_price=?, wholesale_price=?, is_active=?
                        WHERE branch_id=? AND product_id=?
                        """,
                        (
                            float(gp_retail_price),
                            float(gp_wholesale_price),
                            1 if gp_active else 0,
                            int(gp_branch_id),
                            int(gp_product_id),
                        ),
                    )
                    st.success("Branch product price updated.")
                else:
                    x(
                        conn,
                        """
                        INSERT INTO branch_product_prices(
                            branch_id, product_id, retail_price, wholesale_price, is_active
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            int(gp_branch_id),
                            int(gp_product_id),
                            float(gp_retail_price),
                            float(gp_wholesale_price),
                            1 if gp_active else 0,
                        ),
                    )
                    st.success("Branch product price added.")

                st.rerun()
            except Exception as e:
                st.error(str(e))

        st.markdown("**Current packaging/service price matrix**")

        gp_filter1, gp_filter2 = st.columns(2)
        with gp_filter1:
            gp_price_filter_branch = st.selectbox(
                "Filter product matrix by branch",
                options=["All Branches"] + branch_names,
                key="gp_price_filter_branch",
            )
        with gp_filter2:
            gp_price_filter_type = st.selectbox(
                "Filter product matrix by type",
                options=["All Types", "PACKAGING", "SERVICE"],
                key="gp_price_filter_type",
            )

        gp_where = ["p.product_type <> 'FISH'"]
        gp_params: list[object] = []

        if gp_price_filter_branch != "All Branches":
            gp_where.append("br.name = ?")
            gp_params.append(gp_price_filter_branch)

        if gp_price_filter_type != "All Types":
            gp_where.append("p.product_type = ?")
            gp_params.append(gp_price_filter_type)

        gp_where_sql = "WHERE " + " AND ".join(gp_where)

        gp_rows = q(
            conn,
            f"""
            SELECT
                br.name AS branch,
                p.sku,
                p.name AS product_name,
                p.product_type,
                bpp.retail_price,
                bpp.wholesale_price,
                bpp.is_active
            FROM branch_product_prices bpp
            JOIN branches br ON br.id = bpp.branch_id
            JOIN products p ON p.id = bpp.product_id
            {gp_where_sql}
            ORDER BY br.name, p.product_type, p.name
            """,
            tuple(gp_params),
        )

        if gp_rows:
            gp_df = pd.DataFrame([dict(r) for r in gp_rows])
            gp_df["is_active"] = gp_df["is_active"].map({1: "Yes", 0: "No"})
            st.dataframe(gp_df, use_container_width=True, hide_index=True)
        else:
            st.caption("No branch product prices configured for the selected filters.")

st.divider()

# -------------------------
# Branch visibility rules
# -------------------------
st.subheader("Branch Visibility Rules")
st.caption("Control which branch is allowed to view another branch’s stock.")

if not is_admin:
    st.warning("Branch visibility rules are restricted to Admin only.")
else:
    if len(branch_names) < 2:
        st.info("At least two branches are needed to configure visibility rules.")
    else:
        bv1, bv2, bv3 = st.columns(3)

        with bv1:
            viewer_branch_name = st.selectbox(
                "Viewer branch",
                options=branch_names,
                key="viewer_branch_name",
            )
            viewer_branch_id = int(branch_id_by_name[viewer_branch_name])

        visible_options = [b for b in branch_names if b != viewer_branch_name]

        with bv2:
            visible_branch_name = st.selectbox(
                "Visible branch",
                options=visible_options,
                key="visible_branch_name",
            )
            visible_branch_id = int(branch_id_by_name[visible_branch_name])

        existing_visibility = q(
            conn,
            """
            SELECT id, is_active
            FROM branch_visibility_rules
            WHERE viewer_branch_id=? AND visible_branch_id=?
            """,
            (viewer_branch_id, visible_branch_id),
        )
        visibility_active_default = bool(existing_visibility[0]["is_active"]) if existing_visibility else True

        with bv3:
            visibility_active = st.checkbox(
                "Rule active",
                value=visibility_active_default,
                key="visibility_active",
            )

        if st.button("Save Visibility Rule", type="primary"):
            try:
                if viewer_branch_id == visible_branch_id:
                    raise ValueError("A branch cannot be set to view itself through this rule.")

                if existing_visibility:
                    x(
                        conn,
                        """
                        UPDATE branch_visibility_rules
                        SET is_active=?
                        WHERE id=?
                        """,
                        (
                            1 if visibility_active else 0,
                            int(existing_visibility[0]["id"]),
                        ),
                    )
                    st.success("Visibility rule updated.")
                else:
                    x(
                        conn,
                        """
                        INSERT INTO branch_visibility_rules(viewer_branch_id, visible_branch_id, is_active)
                        VALUES (?, ?, ?)
                        """,
                        (
                            int(viewer_branch_id),
                            int(visible_branch_id),
                            1 if visibility_active else 0,
                        ),
                    )
                    st.success("Visibility rule added.")

                st.rerun()
            except Exception as e:
                st.error(str(e))

        visibility_rows = q(
            conn,
            """
            SELECT
                vb.name AS viewer_branch,
                rb.name AS visible_branch,
                bvr.is_active
            FROM branch_visibility_rules bvr
            JOIN branches vb ON vb.id = bvr.viewer_branch_id
            JOIN branches rb ON rb.id = bvr.visible_branch_id
            ORDER BY vb.name, rb.name
            """
        )
        if visibility_rows:
            visibility_df = pd.DataFrame([dict(r) for r in visibility_rows])
            visibility_df["is_active"] = visibility_df["is_active"].map({1: "Yes", 0: "No"})
            st.dataframe(visibility_df, use_container_width=True, hide_index=True)
        else:
            st.caption("No branch visibility rules configured yet.")

st.divider()

# -------------------------
# Branch procurement rules
# -------------------------
st.subheader("Branch Procurement Rules")
st.caption("Control whether a branch purchases directly, receives transfers, and which branch is its default source.")

if not is_admin:
    st.warning("Branch procurement rules are restricted to Admin only.")
else:
    if not branch_names:
        st.info("No branches found. Initialize the database first.")
    else:
        pr1, pr2, pr3, pr4 = st.columns(4)

        with pr1:
            procurement_branch_name = st.selectbox(
                "Branch",
                options=branch_names,
                key="procurement_branch_name",
            )
            procurement_branch_id = int(branch_id_by_name[procurement_branch_name])

        existing_rule = q(
            conn,
            """
            SELECT
                id,
                can_purchase_direct,
                can_receive_transfer,
                default_source_branch_id,
                notes
            FROM branch_procurement_rules
            WHERE branch_id=?
            """,
            (procurement_branch_id,),
        )

        default_can_purchase = bool(existing_rule[0]["can_purchase_direct"]) if existing_rule else True
        default_can_receive = bool(existing_rule[0]["can_receive_transfer"]) if existing_rule else True
        default_source_branch_id = int(existing_rule[0]["default_source_branch_id"]) if existing_rule and existing_rule[0]["default_source_branch_id"] is not None else None
        default_notes = str(existing_rule[0]["notes"]) if existing_rule and existing_rule[0]["notes"] is not None else ""

        with pr2:
            can_purchase_direct = st.checkbox(
                "Can purchase direct",
                value=default_can_purchase,
                key="can_purchase_direct",
            )

        with pr3:
            can_receive_transfer = st.checkbox(
                "Can receive transfer",
                value=default_can_receive,
                key="can_receive_transfer",
            )

        source_options = ["None"] + [b for b in branch_names if b != procurement_branch_name]
        default_source_name = "None"
        if default_source_branch_id is not None:
            matched = next((b["name"] for b in branches if int(b["id"]) == int(default_source_branch_id)), None)
            if matched:
                default_source_name = str(matched)

        with pr4:
            default_source_branch_name = st.selectbox(
                "Default source branch",
                options=source_options,
                index=source_options.index(default_source_name) if default_source_name in source_options else 0,
                key="default_source_branch_name",
            )

        procurement_notes = st.text_input(
            "Notes (optional)",
            value=default_notes,
            key="procurement_notes",
        )

        if st.button("Save Procurement Rule", type="primary"):
            try:
                default_source_id = None
                if default_source_branch_name != "None":
                    default_source_id = int(branch_id_by_name[default_source_branch_name])

                if existing_rule:
                    x(
                        conn,
                        """
                        UPDATE branch_procurement_rules
                        SET can_purchase_direct=?, can_receive_transfer=?, default_source_branch_id=?, notes=?
                        WHERE id=?
                        """,
                        (
                            1 if can_purchase_direct else 0,
                            1 if can_receive_transfer else 0,
                            int(default_source_id) if default_source_id is not None else None,
                            procurement_notes.strip() or None,
                            int(existing_rule[0]["id"]),
                        ),
                    )
                    st.success("Procurement rule updated.")
                else:
                    x(
                        conn,
                        """
                        INSERT INTO branch_procurement_rules(
                            branch_id, can_purchase_direct, can_receive_transfer, default_source_branch_id, notes
                        )
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            int(procurement_branch_id),
                            1 if can_purchase_direct else 0,
                            1 if can_receive_transfer else 0,
                            int(default_source_id) if default_source_id is not None else None,
                            procurement_notes.strip() or None,
                        ),
                    )
                    st.success("Procurement rule added.")

                st.rerun()
            except Exception as e:
                st.error(str(e))

        procurement_rows = q(
            conn,
            """
            SELECT
                br.name AS branch,
                bpr.can_purchase_direct,
                bpr.can_receive_transfer,
                src.name AS default_source_branch,
                bpr.notes
            FROM branch_procurement_rules bpr
            JOIN branches br ON br.id = bpr.branch_id
            LEFT JOIN branches src ON src.id = bpr.default_source_branch_id
            ORDER BY br.name
            """
        )
        if procurement_rows:
            procurement_df = pd.DataFrame([dict(r) for r in procurement_rows])
            procurement_df["can_purchase_direct"] = procurement_df["can_purchase_direct"].map({1: "Yes", 0: "No"})
            procurement_df["can_receive_transfer"] = procurement_df["can_receive_transfer"].map({1: "Yes", 0: "No"})
            st.dataframe(procurement_df, use_container_width=True, hide_index=True)
        else:
            st.caption("No branch procurement rules configured yet.")

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
    UNION ALL SELECT 'product_categories', COUNT(*) FROM product_categories
    UNION ALL SELECT 'products', COUNT(*) FROM products
    UNION ALL SELECT 'branch_products', COUNT(*) FROM branch_products
    UNION ALL SELECT 'branch_product_prices', COUNT(*) FROM branch_product_prices
    UNION ALL SELECT 'suppliers', COUNT(*) FROM suppliers
    UNION ALL SELECT 'customers', COUNT(*) FROM customers
    UNION ALL SELECT 'branch_size_prices', COUNT(*) FROM branch_size_prices
    UNION ALL SELECT 'branch_visibility_rules', COUNT(*) FROM branch_visibility_rules
    UNION ALL SELECT 'branch_procurement_rules', COUNT(*) FROM branch_procurement_rules
    UNION ALL SELECT 'promos', COUNT(*) FROM promos
    UNION ALL SELECT 'branch_promos', COUNT(*) FROM branch_promos
    UNION ALL SELECT 'batches', COUNT(*) FROM batches
    UNION ALL SELECT 'sales', COUNT(*) FROM sales
    UNION ALL SELECT 'product_stock_movements', COUNT(*) FROM product_stock_movements
    UNION ALL SELECT 'stock_transfers', COUNT(*) FROM stock_transfers
    UNION ALL SELECT 'stock_transfer_lines', COUNT(*) FROM stock_transfer_lines
    UNION ALL SELECT 'product_transfers', COUNT(*) FROM product_transfers
    UNION ALL SELECT 'service_sales', COUNT(*) FROM service_sales
    UNION ALL SELECT 'inventory_adjustments', COUNT(*) FROM inventory_adjustments
    UNION ALL SELECT 'batch_closures', COUNT(*) FROM batch_closures
    """,
)
st.dataframe(pd.DataFrame([dict(r) for r in counts]), use_container_width=True, hide_index=True)