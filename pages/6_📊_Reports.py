from __future__ import annotations

import pandas as pd
import streamlit as st

from core.config import get_settings
from core.db import get_conn, ensure_schema, q


st.set_page_config(page_title="Reports", page_icon="📊", layout="wide")
st.title("📊 Reports")
st.caption(
    "Loss patterns, variance flags, gross margin, promo impact, customer insights, "
    "and product/service activity."
)

settings = get_settings()
conn = get_conn(settings.db_path)
ensure_schema(conn)

# -------------------------
# Shared filters
# -------------------------
branches = q(conn, "SELECT id, name FROM branches ORDER BY name")
branch_names = ["All"] + [b["name"] for b in branches]
selected_branch = st.selectbox("Filter by branch", options=branch_names, index=0)


def _branch_filter_by_alias(alias: str) -> tuple[str, tuple]:
    if selected_branch == "All":
        return "", tuple()
    br_id = next(int(b["id"]) for b in branches if b["name"] == selected_branch)
    return f" AND {alias}.id = ? ", (br_id,)


def _branch_filter_by_column(column_name: str) -> tuple[str, tuple]:
    if selected_branch == "All":
        return "", tuple()
    br_id = next(int(b["id"]) for b in branches if b["name"] == selected_branch)
    return f" AND {column_name} = ? ", (br_id,)


tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(
    [
        "Loss by Batch",
        "Wholesale Variance Flags",
        "Gross Margin + Promo Impact",
        "Customer Insights",
        "Products & Services",
        "Transfers & Branch Controls",
    ]
)

# -------------------------
# Tab 1: Loss by Batch
# -------------------------
with tab1:
    extra_where, extra_params = _branch_filter_by_alias("br")

    loss = q(
        conn,
        f"""
        SELECT
            b.batch_code,
            br.name AS branch,
            b.receipt_date,
            b.initial_kg,
            c.loss_kg,
            c.loss_pct,
            c.closed_ts
        FROM batch_closures c
        JOIN batches b ON b.id = c.batch_id
        JOIN branches br ON br.id = b.branch_id
        WHERE 1=1 {extra_where}
        ORDER BY c.id DESC
        """,
        extra_params if extra_params else None,
    )

    if not loss:
        st.info("No closed batches yet. Close some batches to see loss analytics.")
    else:
        df = pd.DataFrame([dict(r) for r in loss])

        for col in ["initial_kg", "loss_kg", "loss_pct"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        st.dataframe(df, use_container_width=True, hide_index=True)

        threshold = st.slider("Flag loss above (%)", min_value=0.0, max_value=30.0, value=10.0, step=0.5)
        outliers = df[df["loss_pct"].fillna(0) > float(threshold)]

        if not outliers.empty:
            st.warning(f"{len(outliers)} batch(es) exceed {threshold:.1f}% loss.")
            st.dataframe(outliers, use_container_width=True, hide_index=True)

        st.subheader("Batch loss percentage")
        chart_df = df[["batch_code", "loss_pct", "closed_ts", "receipt_date"]].copy()
        chart_df["sort_ts"] = chart_df["closed_ts"].fillna(chart_df["receipt_date"])
        chart_df = chart_df.sort_values("sort_ts").set_index("batch_code")[["loss_pct"]]
        st.line_chart(chart_df)

# -------------------------
# Tab 2: Wholesale Variance Flags
# -------------------------
with tab2:
    extra_where, extra_params = _branch_filter_by_alias("br")
    flagged_only = st.checkbox("Show flagged only", value=False)

    where_flagged = " AND s.variance_flag = 1 " if flagged_only else ""

    rows = q(
        conn,
        f"""
        SELECT
            s.sale_ts,
            br.name AS branch,
            b.batch_code,
            sz.code AS size_code,
            s.kg_sold,
            s.pcs_sold,
            s.pcs_suggested,
            s.variance_flag,
            COALESCE(c.display_name, s.customer) AS customer_name
        FROM sales s
        JOIN branches br ON br.id = s.branch_id
        JOIN batches b ON b.id = s.batch_id
        LEFT JOIN sizes sz ON sz.id = s.size_id
        LEFT JOIN customers c ON c.id = s.customer_id
        WHERE s.mode='WHOLESALE_KG'
        {where_flagged}
        {extra_where}
        ORDER BY s.id DESC
        LIMIT 200
        """,
        extra_params if extra_params else None,
    )

    if not rows:
        st.info("No wholesale sales yet.")
    else:
        df = pd.DataFrame([dict(r) for r in rows])
        flagged = df[df["variance_flag"] == 1] if "variance_flag" in df.columns else df.iloc[0:0]

        c1, c2 = st.columns(2)
        c1.metric("Wholesale sales (last 200)", f"{len(df)}")
        c2.metric("Variance-flagged", f"{len(flagged)}")

        st.dataframe(df, use_container_width=True, hide_index=True)

        if not flagged_only and not flagged.empty:
            st.warning("Variance-flagged records shown below:")
            st.dataframe(flagged, use_container_width=True, hide_index=True)

# -------------------------
# Tab 3: Gross Margin + Promo Impact
# -------------------------
with tab3:
    st.subheader("Sales vs COGS (batch buy price per kg)")
    st.caption("COGS = kg_sold × buy_price_per_kg. Promo discounts are shown separately for retail.")

    extra_where, extra_params = _branch_filter_by_alias("br")

    sales = q(
        conn,
        f"""
        SELECT
          s.sale_ts,
          br.name AS branch,
          b.batch_code,
          sz.code AS size_code,
          s.mode,
          COALESCE(c.display_name, s.customer) AS customer_name,
          s.kg_sold,
          s.pcs_sold,
          s.charged_pcs,
          s.free_pcs,
          s.promo_applied,
          s.promo_code,
          s.promo_discount_value,
          b.buy_price_per_kg,
          s.unit_price,
          s.total_price,
          ROUND(s.kg_sold * b.buy_price_per_kg, 2) AS cogs,
          CASE
            WHEN s.total_price IS NULL THEN NULL
            ELSE ROUND(s.total_price - (s.kg_sold * b.buy_price_per_kg), 2)
          END AS gross_margin
        FROM sales s
        JOIN batches b ON b.id = s.batch_id
        JOIN branches br ON br.id = s.branch_id
        LEFT JOIN sizes sz ON sz.id = s.size_id
        LEFT JOIN customers c ON c.id = s.customer_id
        WHERE 1=1 {extra_where}
        ORDER BY s.id DESC
        LIMIT 300
        """,
        extra_params if extra_params else None,
    )

    if not sales:
        st.info("No sales yet. Post some sales to view margin.")
    else:
        df = pd.DataFrame([dict(r) for r in sales])

        for col in [
            "kg_sold",
            "pcs_sold",
            "charged_pcs",
            "free_pcs",
            "promo_applied",
            "promo_discount_value",
            "buy_price_per_kg",
            "unit_price",
            "total_price",
            "cogs",
            "gross_margin",
        ]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        st.dataframe(df, use_container_width=True, hide_index=True)

        df_priced = df.dropna(subset=["total_price"]).copy()
        if not df_priced.empty:
            total_rev = float(df_priced["total_price"].fillna(0).sum())
            total_cogs = float(df_priced["cogs"].fillna(0).sum())
            total_margin = total_rev - total_cogs
            total_promo_discount = float(df_priced["promo_discount_value"].fillna(0).sum())
            promo_free_pcs = int(df_priced["free_pcs"].fillna(0).sum())
            margin_pct = (total_margin / total_rev * 100.0) if total_rev else 0.0

            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Revenue", f"{total_rev:,.2f}")
            c2.metric("COGS", f"{total_cogs:,.2f}")
            c3.metric("Gross Margin", f"{total_margin:,.2f}")
            c4.metric("Gross Margin %", f"{margin_pct:.1f}%")
            c5.metric("Promo Discount", f"{total_promo_discount:,.2f}")

            if total_promo_discount > 0 or promo_free_pcs > 0:
                st.info(f"Promo effect: {promo_free_pcs:,} free piece(s) granted • Discount value {total_promo_discount:,.2f}")

            promo_rows = df_priced[df_priced["promo_applied"].fillna(0) == 1].copy()
            if not promo_rows.empty:
                st.subheader("Promo activity")
                promo_summary = (
                    promo_rows.groupby(["branch", "promo_code"], dropna=False)
                    .agg(
                        retail_rows=("promo_applied", "count"),
                        free_pcs=("free_pcs", "sum"),
                        promo_discount_value=("promo_discount_value", "sum"),
                        revenue=("total_price", "sum"),
                    )
                    .reset_index()
                )
                st.dataframe(promo_summary, use_container_width=True, hide_index=True)
        else:
            st.info("No priced sales yet (total_price is empty). Add sales to compute margins.")

# -------------------------
# Tab 4: Customer Insights
# -------------------------
with tab4:
    st.subheader("Customer insights")
    st.caption("Branch-level customer mix and acquisition view.")

    extra_where_sales, extra_params_sales = _branch_filter_by_alias("br")
    extra_where_customers, extra_params_customers = _branch_filter_by_alias("br")

    customer_mix = q(
        conn,
        f"""
        SELECT
            br.name AS branch,
            c.category,
            COUNT(*) AS customers
        FROM customers c
        JOIN branches br ON br.id = c.branch_id
        WHERE c.is_active=1
        {extra_where_customers}
        GROUP BY br.name, c.category
        ORDER BY br.name, c.category
        """,
        extra_params_customers if extra_params_customers else None,
    )

    acquisition = q(
        conn,
        f"""
        SELECT
            br.name AS branch,
            COUNT(DISTINCT c.id) AS unique_customers,
            MIN(c.created_at) AS first_customer_created_at,
            MAX(c.created_at) AS latest_customer_created_at
        FROM customers c
        JOIN branches br ON br.id = c.branch_id
        WHERE c.is_active=1
        {extra_where_customers}
        GROUP BY br.name
        ORDER BY br.name
        """,
        extra_params_customers if extra_params_customers else None,
    )

    sales_by_customer = q(
        conn,
        f"""
        SELECT
            br.name AS branch,
            COALESCE(c.category, 'Unclassified') AS customer_category,
            COUNT(*) AS sales_rows,
            COUNT(DISTINCT s.customer_id) AS unique_customers_in_sales,
            ROUND(COALESCE(SUM(s.total_price), 0), 2) AS revenue
        FROM sales s
        JOIN branches br ON br.id = s.branch_id
        LEFT JOIN customers c ON c.id = s.customer_id
        WHERE 1=1
        {extra_where_sales}
        GROUP BY br.name, COALESCE(c.category, 'Unclassified')
        ORDER BY br.name, customer_category
        """,
        extra_params_sales if extra_params_sales else None,
    )

    if acquisition:
        st.markdown("**Customer acquisition by branch**")
        acq_df = pd.DataFrame([dict(r) for r in acquisition])
        st.dataframe(acq_df, use_container_width=True, hide_index=True)

        total_unique_customers = int(pd.to_numeric(acq_df["unique_customers"], errors="coerce").fillna(0).sum())
        st.metric("Total unique customers", f"{total_unique_customers:,}")
    else:
        st.info("No customer acquisition data yet.")

    if customer_mix:
        st.markdown("**Customer mix by category**")
        mix_df = pd.DataFrame([dict(r) for r in customer_mix])
        st.dataframe(mix_df, use_container_width=True, hide_index=True)

        pivot_mix = mix_df.pivot(index="branch", columns="category", values="customers").fillna(0)
        if not pivot_mix.empty:
            st.bar_chart(pivot_mix)
    else:
        st.caption("No customer category data yet.")

    if sales_by_customer:
        st.markdown("**Sales by customer category**")
        sales_cat_df = pd.DataFrame([dict(r) for r in sales_by_customer])
        st.dataframe(sales_cat_df, use_container_width=True, hide_index=True)
    else:
        st.caption("No customer-linked sales yet.")

# -------------------------
# Tab 5: Products & Services
# -------------------------
with tab5:
    st.subheader("Products & services")
    st.caption("Non-fish activity, packaging stock movement, and service revenue.")

    extra_where_services, extra_params_services = _branch_filter_by_alias("br")
    extra_where_products, extra_params_products = _branch_filter_by_alias("br")

    service_rows = q(
        conn,
        f"""
        SELECT
            ss.service_ts,
            br.name AS branch,
            p.sku,
            p.name AS service_name,
            COALESCE(c.display_name, 'Walk-in / Unlinked') AS customer_name,
            ss.quantity,
            ss.unit_price,
            ss.total_price,
            ss.sale_group_code,
            ss.notes
        FROM service_sales ss
        JOIN branches br ON br.id = ss.branch_id
        JOIN products p ON p.id = ss.product_id
        LEFT JOIN customers c ON c.id = ss.customer_id
        WHERE 1=1
        {extra_where_services}
        ORDER BY ss.id DESC
        LIMIT 200
        """,
        extra_params_services if extra_params_services else None,
    )

    product_movement_rows = q(
        conn,
        f"""
        SELECT
            psm.ts,
            br.name AS branch,
            p.sku,
            p.name AS product_name,
            p.product_type,
            psm.movement_type,
            psm.qty_delta,
            psm.unit_cost,
            psm.reference_no,
            psm.notes
        FROM product_stock_movements psm
        JOIN branches br ON br.id = psm.branch_id
        JOIN products p ON p.id = psm.product_id
        WHERE 1=1
        {extra_where_products}
        ORDER BY psm.id DESC
        LIMIT 300
        """,
        extra_params_products if extra_params_products else None,
    )

    service_summary_rows = q(
        conn,
        f"""
        SELECT
            br.name AS branch,
            p.name AS service_name,
            COUNT(*) AS service_rows,
            ROUND(COALESCE(SUM(ss.quantity), 0), 2) AS total_quantity,
            ROUND(COALESCE(SUM(ss.total_price), 0), 2) AS revenue
        FROM service_sales ss
        JOIN branches br ON br.id = ss.branch_id
        JOIN products p ON p.id = ss.product_id
        WHERE 1=1
        {extra_where_services}
        GROUP BY br.name, p.name
        ORDER BY br.name, p.name
        """,
        extra_params_services if extra_params_services else None,
    )

    product_stock_summary_rows = q(
        conn,
        f"""
        SELECT
            br.name AS branch,
            p.name AS product_name,
            p.product_type,
            ROUND(COALESCE(SUM(psm.qty_delta), 0), 2) AS net_qty,
            ROUND(COALESCE(SUM(COALESCE(psm.qty_delta, 0) * COALESCE(psm.unit_cost, 0)), 0), 2) AS gross_value_basis
        FROM product_stock_movements psm
        JOIN branches br ON br.id = psm.branch_id
        JOIN products p ON p.id = psm.product_id
        WHERE 1=1
        {extra_where_products}
        GROUP BY br.name, p.name, p.product_type
        ORDER BY br.name, p.product_type, p.name
        """,
        extra_params_products if extra_params_products else None,
    )

    if service_summary_rows:
        st.markdown("**Service sales summary**")
        svc_sum_df = pd.DataFrame([dict(r) for r in service_summary_rows])
        st.dataframe(svc_sum_df, use_container_width=True, hide_index=True)

        total_service_revenue = float(pd.to_numeric(svc_sum_df["revenue"], errors="coerce").fillna(0).sum())
        st.metric("Total service revenue", f"{total_service_revenue:,.2f}")
    else:
        st.caption("No service sales yet.")

    if service_rows:
        st.markdown("**Recent service sales**")
        svc_df = pd.DataFrame([dict(r) for r in service_rows])
        st.dataframe(svc_df, use_container_width=True, hide_index=True)

    if product_stock_summary_rows:
        st.markdown("**Packaging / non-fish stock summary**")
        prod_sum_df = pd.DataFrame([dict(r) for r in product_stock_summary_rows])
        st.dataframe(prod_sum_df, use_container_width=True, hide_index=True)
    else:
        st.caption("No non-fish product stock movement yet.")

    if product_movement_rows:
        st.markdown("**Recent packaging / non-fish stock movements**")
        prod_mov_df = pd.DataFrame([dict(r) for r in product_movement_rows])
        st.dataframe(prod_mov_df, use_container_width=True, hide_index=True)

# -------------------------
# Tab 6: Transfers & Branch Controls
# -------------------------
with tab6:
    st.subheader("Transfers & branch controls")
    st.caption("Track internal transfers and the branch rules that govern visibility and procurement.")

    extra_where_from, extra_params_from = _branch_filter_by_column("st.from_branch_id")
    extra_where_to, extra_params_to = _branch_filter_by_column("st.to_branch_id")
    extra_where_product_from, extra_params_product_from = _branch_filter_by_column("pt.from_branch_id")
    extra_where_product_to, extra_params_product_to = _branch_filter_by_column("pt.to_branch_id")

    # Fish stock transfers
    fish_transfers = q(
        conn,
        f"""
        SELECT
            st.transfer_ts,
            st.transfer_code,
            fb.name AS from_branch,
            tb.name AS to_branch,
            st.status,
            st.notes,
            st.created_by,
            COUNT(stl.id) AS lines_count,
            COALESCE(SUM(stl.pieces), 0) AS pieces_transferred,
            ROUND(COALESCE(SUM(stl.kg), 0), 3) AS kg_transferred
        FROM stock_transfers st
        JOIN branches fb ON fb.id = st.from_branch_id
        JOIN branches tb ON tb.id = st.to_branch_id
        LEFT JOIN stock_transfer_lines stl ON stl.transfer_id = st.id
        WHERE 1=1
        {extra_where_from}
        GROUP BY
            st.id, st.transfer_ts, st.transfer_code, fb.name, tb.name,
            st.status, st.notes, st.created_by
        ORDER BY st.id DESC
        LIMIT 200
        """,
        extra_params_from if extra_params_from else None,
    )

    fish_transfer_lines = q(
        conn,
        f"""
        SELECT
            st.transfer_ts,
            st.transfer_code,
            fb.name AS from_branch,
            tb.name AS to_branch,
            b.batch_code AS source_batch_code,
            sz.code AS size_code,
            stl.pieces,
            ROUND(stl.kg, 3) AS kg,
            ROUND(stl.avg_kg_per_piece, 4) AS avg_kg_per_piece,
            ROUND(stl.unit_cost_per_kg, 2) AS unit_cost_per_kg
        FROM stock_transfer_lines stl
        JOIN stock_transfers st ON st.id = stl.transfer_id
        JOIN branches fb ON fb.id = st.from_branch_id
        JOIN branches tb ON tb.id = st.to_branch_id
        JOIN batches b ON b.id = stl.from_batch_id
        JOIN sizes sz ON sz.id = stl.size_id
        WHERE 1=1
        {extra_where_from}
        ORDER BY stl.id DESC
        LIMIT 300
        """,
        extra_params_from if extra_params_from else None,
    )

    # Product transfers
    product_transfers = q(
        conn,
        f"""
        SELECT
            pt.transfer_ts,
            pt.transfer_code,
            fb.name AS from_branch,
            tb.name AS to_branch,
            p.sku,
            p.name AS product_name,
            p.product_type,
            pt.qty,
            pt.unit_cost,
            pt.status,
            pt.notes,
            pt.created_by
        FROM product_transfers pt
        JOIN branches fb ON fb.id = pt.from_branch_id
        JOIN branches tb ON tb.id = pt.to_branch_id
        JOIN products p ON p.id = pt.product_id
        WHERE 1=1
        {extra_where_product_from}
        ORDER BY pt.id DESC
        LIMIT 200
        """,
        extra_params_product_from if extra_params_product_from else None,
    )

    # Visibility rules
    visibility_rules = q(
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

    # Procurement rules
    procurement_rules = q(
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

    # Metrics
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Fish transfers", f"{len(fish_transfers)}")
    m2.metric("Product transfers", f"{len(product_transfers)}")

    total_fish_transfer_kg = 0.0
    if fish_transfers:
        total_fish_transfer_kg = float(
            pd.to_numeric(pd.DataFrame([dict(r) for r in fish_transfers])["kg_transferred"], errors="coerce")
            .fillna(0)
            .sum()
        )
    m3.metric("Fish transfer kg", f"{total_fish_transfer_kg:,.3f}")

    active_visibility_rules = 0
    if visibility_rules:
        vis_df_metric = pd.DataFrame([dict(r) for r in visibility_rules])
        active_visibility_rules = int(pd.to_numeric(vis_df_metric["is_active"], errors="coerce").fillna(0).sum())
    m4.metric("Active visibility rules", f"{active_visibility_rules}")

    st.divider()

    if fish_transfers:
        st.markdown("**Fish stock transfers**")
        fish_transfers_df = pd.DataFrame([dict(r) for r in fish_transfers])
        st.dataframe(fish_transfers_df, use_container_width=True, hide_index=True)
    else:
        st.caption("No fish stock transfers yet.")

    if fish_transfer_lines:
        st.markdown("**Fish stock transfer lines**")
        fish_transfer_lines_df = pd.DataFrame([dict(r) for r in fish_transfer_lines])
        st.dataframe(fish_transfer_lines_df, use_container_width=True, hide_index=True)

    if product_transfers:
        st.markdown("**Product transfers**")
        product_transfers_df = pd.DataFrame([dict(r) for r in product_transfers])
        st.dataframe(product_transfers_df, use_container_width=True, hide_index=True)
    else:
        st.caption("No product transfers yet.")

    if visibility_rules:
        st.markdown("**Branch visibility rules**")
        visibility_df = pd.DataFrame([dict(r) for r in visibility_rules])
        visibility_df["is_active"] = visibility_df["is_active"].map({1: "Yes", 0: "No"})
        st.dataframe(visibility_df, use_container_width=True, hide_index=True)
    else:
        st.caption("No branch visibility rules configured yet.")

    if procurement_rules:
        st.markdown("**Branch procurement rules**")
        procurement_df = pd.DataFrame([dict(r) for r in procurement_rules])
        procurement_df["can_purchase_direct"] = procurement_df["can_purchase_direct"].map({1: "Yes", 0: "No"})
        procurement_df["can_receive_transfer"] = procurement_df["can_receive_transfer"].map({1: "Yes", 0: "No"})
        st.dataframe(procurement_df, use_container_width=True, hide_index=True)
    else:
        st.caption("No branch procurement rules configured yet.")
