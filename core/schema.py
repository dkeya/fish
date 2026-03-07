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

-- Suppliers (pre-configured master list)
CREATE TABLE IF NOT EXISTS suppliers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  contact_person TEXT,
  phone TEXT,
  is_active INTEGER NOT NULL DEFAULT 1
);

-- Customers (quick-create friendly; must have phone OR house number)
CREATE TABLE IF NOT EXISTS customers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  branch_id INTEGER NOT NULL,
  display_name TEXT NOT NULL,
  category TEXT NOT NULL DEFAULT 'Walk-in',   -- Individual / Market reseller / Restaurant / Walk-in
  phone TEXT UNIQUE,
  house_number TEXT UNIQUE,
  notes TEXT,
  is_active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  CHECK (
    TRIM(COALESCE(phone, '')) <> '' OR
    TRIM(COALESCE(house_number, '')) <> ''
  ),
  FOREIGN KEY (branch_id) REFERENCES branches(id)
);

-- Branch-specific prices by size
CREATE TABLE IF NOT EXISTS branch_size_prices (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  branch_id INTEGER NOT NULL,
  size_id INTEGER NOT NULL,
  retail_price_per_piece REAL NOT NULL DEFAULT 0,
  wholesale_price_per_kg REAL NOT NULL DEFAULT 0,
  is_active INTEGER NOT NULL DEFAULT 1,
  UNIQUE (branch_id, size_id),
  FOREIGN KEY (branch_id) REFERENCES branches(id),
  FOREIGN KEY (size_id) REFERENCES sizes(id)
);

-- Promo definitions (configurable templates)
CREATE TABLE IF NOT EXISTS promos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  code TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  buy_qty INTEGER NOT NULL,              -- e.g. 2 in Buy 2 Get 1
  free_qty INTEGER NOT NULL,             -- e.g. 1 in Buy 2 Get 1
  applies_mode TEXT NOT NULL DEFAULT 'RETAIL_PCS',
  is_active INTEGER NOT NULL DEFAULT 1
);

-- Promo assignment per branch
CREATE TABLE IF NOT EXISTS branch_promos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  branch_id INTEGER NOT NULL,
  promo_id INTEGER NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 1,
  UNIQUE (branch_id, promo_id),
  FOREIGN KEY (branch_id) REFERENCES branches(id),
  FOREIGN KEY (promo_id) REFERENCES promos(id)
);

-- Batches (one receipt = one batch)
CREATE TABLE IF NOT EXISTS batches (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_code TEXT NOT NULL UNIQUE,
  receipt_date TEXT NOT NULL,            -- ISO date
  branch_id INTEGER NOT NULL,
  supplier TEXT,                         -- keep for backward compatibility
  supplier_id INTEGER,                   -- master supplier reference
  notes TEXT,

  -- Supplier buying price (per kg) for valuation & gross margin
  buy_price_per_kg REAL NOT NULL DEFAULT 0,

  initial_pieces INTEGER NOT NULL,
  initial_kg REAL NOT NULL,
  batch_avg_kg_per_piece REAL NOT NULL,  -- (initial_kg / initial_pieces) fingerprint

  status TEXT NOT NULL DEFAULT 'OPEN',   -- OPEN / CLOSED
  closed_at TEXT,

  FOREIGN KEY (branch_id) REFERENCES branches(id),
  FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
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
  customer TEXT,                         -- keep for backward compatibility
  customer_id INTEGER,                   -- customer master reference

  batch_id INTEGER NOT NULL,
  size_id INTEGER,                       -- optional for reporting by size
  pcs_sold INTEGER NOT NULL,
  kg_sold REAL NOT NULL,

  unit_price REAL,                       -- price per KG or per Piece depending on price_basis
  price_basis TEXT NOT NULL DEFAULT 'PER_KG',  -- PER_KG / PER_PIECE
  total_price REAL,                      -- charged total

  pcs_suggested INTEGER,                 -- for WHOLESALE_KG suggested pieces
  variance_flag INTEGER NOT NULL DEFAULT 0,

  -- Promo tracking (mainly retail)
  promo_applied INTEGER NOT NULL DEFAULT 0,
  promo_code TEXT,
  promo_name TEXT,
  promo_buy_qty INTEGER,
  promo_free_qty INTEGER,
  charged_pcs INTEGER,                   -- pieces actually charged
  free_pcs INTEGER NOT NULL DEFAULT 0,   -- pieces free under promo
  promo_discount_value REAL,             -- free_pcs * unit_price for retail promos

  FOREIGN KEY (branch_id) REFERENCES branches(id),
  FOREIGN KEY (customer_id) REFERENCES customers(id),
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