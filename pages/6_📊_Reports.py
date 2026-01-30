from __future__ import annotations

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

from core.config import get_settings
from core.db import get_conn, ensure_schema, q


st.set_page_config(page_title="Reports", page_icon="📊", layout="wide")
st.title("📊 Reports (Loss patterns, variance flags, batch performance)")
st.caption("Starter analytics to support loss monitoring, variance controls, and gross margin (COGS from supplier buy price).")

settings = get_settings()
conn = get_conn(settings.db_path)
ensure_schema(conn)

tab1, tab2, tab3 = st.tabs(["Loss by Batch", "Wholesale Variance Flags", "Gross Margin (Sales vs COGS)"])

with tab1:
    loss = q(
        conn,
        """
        SELECT b.batch_code, br.name AS branch, b.receipt_date,
               b.initial_kg, c.loss_kg, c.loss_pct, c.closed_ts
        FROM batch_closures c
        JOIN batches b ON b.id = c.batch_id
        JOIN branches br ON br.id = b.branch_id
        ORDER BY c.id DESC
        """,
    )
    if not loss:
        st.info("No closed batches yet. Close some batches to see loss analytics.")
    else:
        df = pd.DataFrame([dict(r) for r in loss])
        st.dataframe(df, use_container_width=True, hide_index=True)

        threshold = st.slider("Flag loss above (%)", min_value=0.0, max_value=30.0, value=10.0, step=0.5)
        outliers = df[df["loss_pct"] > threshold]
        if not outliers.empty:
            st.warning(f"{len(outliers)} batch(es) exceed {threshold:.1f}% loss.")
            st.dataframe(outliers, use_container_width=True, hide_index=True)

        fig = plt.figure()
        plt.plot(df["batch_code"], df["loss_pct"])
        plt.xticks(rotation=45, ha="right")
        plt.xlabel("Batch")
        plt.ylabel("Loss (%)")
        plt.title("Batch loss percentage")
        st.pyplot(fig, clear_figure=True)

with tab2:
    rows = q(
        conn,
        """
        SELECT s.sale_ts, br.name AS branch, b.batch_code,
               s.kg_sold, s.pcs_sold, s.pcs_suggested, s.variance_flag, s.customer
        FROM sales s
        JOIN branches br ON br.id = s.branch_id
        JOIN batches b ON b.id = s.batch_id
        WHERE s.mode='WHOLESALE_KG'
        ORDER BY s.id DESC
        LIMIT 200
        """,
    )
    if not rows:
        st.info("No wholesale sales yet.")
    else:
        df = pd.DataFrame([dict(r) for r in rows])
        flagged = df[df["variance_flag"] == 1]

        c1, c2 = st.columns(2)
        c1.metric("Wholesale sales (last 200)", f"{len(df)}")
        c2.metric("Variance-flagged", f"{len(flagged)}")

        st.dataframe(df, use_container_width=True, hide_index=True)

        if not flagged.empty:
            st.warning("Variance-flagged records shown below:")
            st.dataframe(flagged, use_container_width=True, hide_index=True)

with tab3:
    st.subheader("Sales vs COGS (batch buy price per kg)")
    st.caption("COGS = kg_sold × buy_price_per_kg (since each sale is tied to a batch).")

    sales = q(
        conn,
        """
        SELECT
          s.sale_ts,
          br.name AS branch,
          b.batch_code,
          s.mode,
          s.customer,
          s.kg_sold,
          s.pcs_sold,
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
        ORDER BY s.id DESC
        LIMIT 300
        """,
    )

    if not sales:
        st.info("No sales yet. Post some sales to view margin.")
    else:
        df = pd.DataFrame([dict(r) for r in sales])
        st.dataframe(df, use_container_width=True, hide_index=True)

        # Quick summary KPIs (only where total_price exists)
        df_priced = df.dropna(subset=["total_price"])
        if not df_priced.empty:
            total_rev = float(df_priced["total_price"].sum())
            total_cogs = float(df_priced["cogs"].sum())
            total_margin = total_rev - total_cogs
            margin_pct = (total_margin / total_rev * 100.0) if total_rev else 0.0

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Revenue (priced sales)", f"{total_rev:,.2f}")
            c2.metric("COGS", f"{total_cogs:,.2f}")
            c3.metric("Gross Margin", f"{total_margin:,.2f}")
            c4.metric("Gross Margin %", f"{margin_pct:.1f}%")
        else:
            st.info("No priced sales yet (total_price is empty). Add unit price in Sales page to compute margins.")
