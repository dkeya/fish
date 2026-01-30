SCHEMA_SQL = r"""
-- Branches
CREATE TABLE IF NOT EXISTS branches (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE
);

-- Sizes (Size 2, Size 3, etc.)
CREATE TABLE IF NOT EXISTS sizes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  code TEXT NOT NULL UNIQUE,
  description TEXT,
  sort_order INTEGER NOT NULL DEFAULT 0
);

-- Batches (one receipt = one batch)
CREATE TABLE IF NOT EXISTS batches (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_code TEXT NOT NULL UNIQUE,
  receipt_date TEXT NOT NULL,            -- ISO date
  branch_id INTEGER NOT NULL,
  supplier TEXT,
  notes TEXT,

  -- Supplier buying price (per kg) for valuation & gross margin
  buy_price_per_kg REAL NOT NULL DEFAULT 0,

  initial_pieces INTEGER NOT NULL,
  initial_kg REAL NOT NULL,
  batch_avg_kg_per_piece REAL NOT NULL,  -- (initial_kg / initial_pieces) fingerprint

  status TEXT NOT NULL DEFAULT 'OPEN',   -- OPEN / CLOSED
  closed_at TEXT,

  FOREIGN KEY (branch_id) REFERENCES branches(id)
);

-- Batch sub-entries by size (optional granularity)
CREATE TABLE IF NOT EXISTS batch_lines (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id INTEGER NOT NULL,
  size_id INTEGER NOT NULL,
  pieces INTEGER NOT NULL,
  kg REAL NOT NULL,
  avg_kg_per_piece REAL NOT NULL,
  FOREIGN KEY (batch_id) REFERENCES batches(id) ON DELETE CASCADE,
  FOREIGN KEY (size_id) REFERENCES sizes(id)
);

-- Sales (two modes)
CREATE TABLE IF NOT EXISTS sales (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sale_ts TEXT NOT NULL,                 -- ISO datetime
  branch_id INTEGER NOT NULL,
  mode TEXT NOT NULL,                    -- RETAIL_PCS / WHOLESALE_KG
  customer TEXT,

  batch_id INTEGER NOT NULL,
  size_id INTEGER,                       -- optional for reporting by size
  pcs_sold INTEGER NOT NULL,
  kg_sold REAL NOT NULL,

  unit_price REAL,                       -- optional (price per KG or per Piece depending on price_basis)
  price_basis TEXT NOT NULL DEFAULT 'PER_KG',  -- PER_KG / PER_PIECE
  total_price REAL,                      -- computed when unit_price is provided

  pcs_suggested INTEGER,                 -- for WHOLESALE_KG suggested pieces
  variance_flag INTEGER NOT NULL DEFAULT 0,

  FOREIGN KEY (branch_id) REFERENCES branches(id),
  FOREIGN KEY (batch_id) REFERENCES batches(id),
  FOREIGN KEY (size_id) REFERENCES sizes(id)
);

-- Inventory adjustments (stocktake, write-off, etc.)
CREATE TABLE IF NOT EXISTS inventory_adjustments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  batch_id INTEGER NOT NULL,
  reason TEXT NOT NULL,
  pcs_delta INTEGER NOT NULL,
  kg_delta REAL NOT NULL,
  notes TEXT,
  FOREIGN KEY (batch_id) REFERENCES batches(id) ON DELETE CASCADE
);

-- Batch closure (loss/shrinkage)
CREATE TABLE IF NOT EXISTS batch_closures (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id INTEGER NOT NULL UNIQUE,
  closed_ts TEXT NOT NULL,
  loss_kg REAL NOT NULL,
  loss_pct REAL NOT NULL,
  notes TEXT,
  FOREIGN KEY (batch_id) REFERENCES batches(id) ON DELETE CASCADE
);
"""
