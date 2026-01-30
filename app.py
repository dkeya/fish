from __future__ import annotations

import streamlit as st

st.set_page_config(page_title="Fish ERP (Demo)", page_icon="🐟", layout="wide")

pages = [
    st.Page("home.py", title="Home", icon="🏠"),
    st.Page("pages/1_📥_Batch_Receipt.py", title="Production Run", icon="🏭"),
    st.Page("pages/2_📦_Inventory.py", title="Inventory", icon="📦"),
    st.Page("pages/3_🛒_Sales.py", title="Sales", icon="🛒"),
    st.Page("pages/4_✅_Batch_Close_&_Loss.py", title="Batch Close & Loss", icon="✅"),
    st.Page("pages/5_🧪_Data_Management.py", title="Data Management", icon="🧪"),
    st.Page("pages/6_📊_Reports.py", title="Reports", icon="📊"),
]

st.navigation(pages).run()
