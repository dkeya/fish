from __future__ import annotations

import streamlit as st

from core.config import get_settings
from core.db import get_conn, ensure_schema
from core.services.demo_data import upsert_reference_data

st.set_page_config(page_title="Fish ERP (Demo)", page_icon="🐟", layout="wide")

st.title("🐟 Fish ERP — Demo Scaffold")
st.caption("Batch-based fish inventory with catch-weight (pieces + kg) logic, sales modes, and shrinkage/loss variance.")

settings = get_settings()
conn = get_conn(settings.db_path)
ensure_schema(conn)
upsert_reference_data(conn)

with st.sidebar:
    st.subheader("Environment")
    st.write(f"**Data directory:** `{settings.data_dir}`")
    st.write(f"**Database:** `{settings.db_path.name}`")

st.info(
    "Use the left sidebar navigation. Start with **🧪 Data Management** to load demo data, then try **Production Run**, **Sales**, and **Batch Close & Loss**.",
    icon="ℹ️",
)
