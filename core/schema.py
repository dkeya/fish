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

-- =========================
-- Product / Service Structure
-- =========================

-- Product categories
CREATE TABLE IF NOT EXISTS product_categories (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  code TEXT NOT NULL UNIQUE,               -- FISH / PACKAGING / SERVICE
  name TEXT NOT NULL UNIQUE,
  description TEXT,
  is_active INTEGER NOT NULL DEFAULT 1
);

-- Product master
CREATE TABLE IF NOT EXISTS products (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sku TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  category_id INTEGER NOT NULL,
  product_type TEXT NOT NULL,             -- FISH / PACKAGING / SERVICE
  stock_uom TEXT NOT NULL DEFAULT 'UNIT', -- KG / PIECE / UNIT / SERVICE
  tracks_stock INTEGER NOT NULL DEFAULT 1,
  uses_batch_fifo INTEGER NOT NULL DEFAULT 0,
  uses_size_dimension INTEGER NOT NULL DEFAULT 0,
  requires_piece_entry INTEGER NOT NULL DEFAULT 0,
  requires_weight_entry INTEGER NOT NULL DEFAULT 0,
  service_non_stock INTEGER NOT NULL DEFAULT 0,
  default_notes TEXT,
  is_active INTEGER NOT NULL DEFAULT 1,
  FOREIGN KEY (category_id) REFERENCES product_categories(id)
);

-- Optional branch-level product enablement
CREATE TABLE IF NOT EXISTS branch_products (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  branch_id INTEGER NOT NULL,
  product_id INTEGER NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 1,
  UNIQUE (branch_id, product_id),
  FOREIGN KEY (branch_id) REFERENCES branches(id),
  FOREIGN KEY (product_id) REFERENCES products(id)
);

-- Branch-specific pricing for generic products/services
CREATE TABLE IF NOT EXISTS branch_product_prices (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  branch_id INTEGER NOT NULL,
  product_id INTEGER NOT NULL,
  retail_price REAL NOT NULL DEFAULT 0,
  wholesale_price REAL NOT NULL DEFAULT 0,
  is_active INTEGER NOT NULL DEFAULT 1,
  UNIQUE (branch_id, product_id),
  FOREIGN KEY (branch_id) REFERENCES branches(id),
  FOREIGN KEY (product_id) REFERENCES products(id)
);

-- Branch-specific prices by fish size (current working fish pricing logic)
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

-- =========================
-- Branch Access / Procurement Rules
-- =========================

-- Which branches are allowed to see stock from other branches
CREATE TABLE IF NOT EXISTS branch_visibility_rules (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  viewer_branch_id INTEGER NOT NULL,
  visible_branch_id INTEGER NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 1,
  UNIQUE (viewer_branch_id, visible_branch_id),
  FOREIGN KEY (viewer_branch_id) REFERENCES branches(id),
  FOREIGN KEY (visible_branch_id) REFERENCES branches(id)
);

-- Procurement / replenishment control by branch
CREATE TABLE IF NOT EXISTS branch_procurement_rules (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  branch_id INTEGER NOT NULL UNIQUE,
  can_purchase_direct INTEGER NOT NULL DEFAULT 1,
  can_receive_transfer INTEGER NOT NULL DEFAULT 1,
  default_source_branch_id INTEGER,
  notes TEXT,
  FOREIGN KEY (branch_id) REFERENCES branches(id),
  FOREIGN KEY (default_source_branch_id) REFERENCES branches(id)
);

-- =========================
-- Fish Batch / Inventory Structure
-- =========================

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

-- Batch sub-entries by size
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

-- Fish stock transfers (header)
CREATE TABLE IF NOT EXISTS stock_transfers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  transfer_code TEXT NOT NULL UNIQUE,
  transfer_ts TEXT NOT NULL,
  from_branch_id INTEGER NOT NULL,
  to_branch_id INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'POSTED',     -- DRAFT / POSTED / CANCELLED / RECEIVED
  notes TEXT,
  created_by TEXT,
  FOREIGN KEY (from_branch_id) REFERENCES branches(id),
  FOREIGN KEY (to_branch_id) REFERENCES branches(id)
);

-- Fish stock transfers (lines by batch/size)
CREATE TABLE IF NOT EXISTS stock_transfer_lines (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  transfer_id INTEGER NOT NULL,
  from_batch_id INTEGER NOT NULL,
  size_id INTEGER NOT NULL,
  pieces INTEGER NOT NULL,
  kg REAL NOT NULL,
  avg_kg_per_piece REAL NOT NULL,
  unit_cost_per_kg REAL,
  FOREIGN KEY (transfer_id) REFERENCES stock_transfers(id) ON DELETE CASCADE,
  FOREIGN KEY (from_batch_id) REFERENCES batches(id),
  FOREIGN KEY (size_id) REFERENCES sizes(id)
);

-- Generic non-fish stock movements (for packaging and similar items)
CREATE TABLE IF NOT EXISTS product_stock_movements (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  branch_id INTEGER NOT NULL,
  product_id INTEGER NOT NULL,
  movement_type TEXT NOT NULL,           -- STOCK_IN / SALE / ADJUSTMENT / TRANSFER_IN / TRANSFER_OUT / WRITE_OFF
  qty_delta REAL NOT NULL,
  unit_cost REAL,
  reference_no TEXT,
  notes TEXT,
  FOREIGN KEY (branch_id) REFERENCES branches(id),
  FOREIGN KEY (product_id) REFERENCES products(id)
);

-- Generic product transfers (packaging etc.)
CREATE TABLE IF NOT EXISTS product_transfers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  transfer_code TEXT NOT NULL UNIQUE,
  transfer_ts TEXT NOT NULL,
  from_branch_id INTEGER NOT NULL,
  to_branch_id INTEGER NOT NULL,
  product_id INTEGER NOT NULL,
  qty REAL NOT NULL,
  unit_cost REAL,
  status TEXT NOT NULL DEFAULT 'POSTED',     -- DRAFT / POSTED / CANCELLED / RECEIVED
  notes TEXT,
  created_by TEXT,
  FOREIGN KEY (from_branch_id) REFERENCES branches(id),
  FOREIGN KEY (to_branch_id) REFERENCES branches(id),
  FOREIGN KEY (product_id) REFERENCES products(id)
);

-- =========================
-- Sales
-- =========================

-- Sales (fish-focused today, but now future-ready for grouped carts and products/services)
CREATE TABLE IF NOT EXISTS sales (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sale_ts TEXT NOT NULL,                 -- ISO datetime
  sale_group_code TEXT,                  -- for grouping multiple cart lines into one sale/invoice
  branch_id INTEGER NOT NULL,
  mode TEXT NOT NULL,                    -- RETAIL_PCS / WHOLESALE_KG / PRODUCT / SERVICE
  customer TEXT,                         -- keep for backward compatibility
  customer_id INTEGER,                   -- customer master reference

  batch_id INTEGER NOT NULL,
  size_id INTEGER,                       -- optional for fish size reporting
  product_id INTEGER,                    -- for future packaging/service/fish master linkage

  pcs_sold INTEGER NOT NULL,
  kg_sold REAL NOT NULL,

  unit_price REAL,                       -- price per KG or per Piece depending on price_basis
  price_basis TEXT NOT NULL DEFAULT 'PER_KG',  -- PER_KG / PER_PIECE / PER_UNIT / PER_SERVICE
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
  FOREIGN KEY (size_id) REFERENCES sizes(id),
  FOREIGN KEY (product_id) REFERENCES products(id)
);

-- Dedicated service transaction table (future-safe; service does not affect fish stock)
CREATE TABLE IF NOT EXISTS service_sales (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  service_ts TEXT NOT NULL,
  sale_group_code TEXT,
  branch_id INTEGER NOT NULL,
  customer_id INTEGER,
  product_id INTEGER NOT NULL,           -- product_type = SERVICE
  quantity REAL NOT NULL DEFAULT 1,
  unit_price REAL NOT NULL DEFAULT 0,
  total_price REAL NOT NULL DEFAULT 0,
  notes TEXT,
  FOREIGN KEY (branch_id) REFERENCES branches(id),
  FOREIGN KEY (customer_id) REFERENCES customers(id),
  FOREIGN KEY (product_id) REFERENCES products(id)
);

-- Inventory adjustments (fish stocktake, write-off, etc.)
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