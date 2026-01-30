# Fish ERP (Streamlit) — Demo Scaffold

A runnable Streamlit scaffold for a **fish batch ERP** (catch-weight style) with:
- Batch receipt as “production run”
- Dual units of measure (**pieces + kg**) linked by a **batch average**
- Retail sales (enter pieces; system derives kg)
- Wholesale sales (enter kg; pieces counted/confirmed; system derives suggested pieces)
- Batch closure that computes **handling/water-loss shrinkage** (loss variance)

## Quick start

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate

pip install -r requirements.txt
streamlit run app.py
```

## No hard-coded paths

By default, the app stores its database in:
- `~/.fish_erp_demo/app.db`

Override with an environment variable:
- `FISH_ERP_DATA_DIR=/path/to/your/data`

Or use **🧪 Data Management** page to set/persist the data directory.
