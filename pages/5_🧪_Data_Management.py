from __future__ import annotations

import streamlit as st
import pandas as pd

from core.config import get_settings, persist_data_dir
from core.db import get_conn, ensure_schema, q
from core.services.demo_data import load_demo_data, wipe_all, upsert_reference_data


st.set_page_config(page_title="Data Management", page_icon="🧪", layout="wide")
st.title("🧪 Data Management (Demo mode)")
st.caption("Initialize database, load/reset demo data, and configure where data is stored (no hard-coded paths).")

settings = get_settings()
conn = get_conn(settings.db_path)
ensure_schema(conn)

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
st.subheader("Data preview")

counts = q(
    conn,
    """
    SELECT 'branches' AS table_name, COUNT(*) AS n FROM branches
    UNION ALL SELECT 'sizes', COUNT(*) FROM sizes
    UNION ALL SELECT 'batches', COUNT(*) FROM batches
    UNION ALL SELECT 'sales', COUNT(*) FROM sales
    UNION ALL SELECT 'inventory_adjustments', COUNT(*) FROM inventory_adjustments
    UNION ALL SELECT 'batch_closures', COUNT(*) FROM batch_closures
    """,
)
st.dataframe(pd.DataFrame([dict(r) for r in counts]), use_container_width=True, hide_index=True)
