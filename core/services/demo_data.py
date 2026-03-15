from __future__ import annotations

import random
from datetime import date, timedelta

from core.db import q, x, ensure_schema
from core.services.batches import create_batches_from_purchase, BatchLineInput
from core.services.sales import create_retail_sale_fifo, create_wholesale_sale_fifo


DEFAULT_BRANCHES = ["Main Branch", "Branch B", "Branch C"]

DEFAULT_SIZES = [
    ("SIZE_2", "Size 2", 2),
    ("SIZE_3", "Size 3", 3),
    ("SIZE_4", "Size 4", 4),
    ("SIZE_5", "Size 5", 5),
]

DEFAULT_SUPPLIERS = [
    ("Lake Supplier", "Omondi", "0700000001"),
    ("Nairobi Fresh Fish", "Achieng", "0700000002"),
    ("Depot Bulk Supply", "Kamau", "0700000003"),
]

DEFAULT_CUSTOMER_CATEGORIES = [
    "Individual",
    "Market reseller",
    "Restaurant",
    "Walk-in",
]

DEFAULT_PROMOS = [
    ("BUY2GET1", "Buy 2 Get 1 Free", 2, 1, "RETAIL_PCS"),
    ("BUY3GET1", "Buy 3 Get 1 Free", 3, 1, "RETAIL_PCS"),
]

# Sample preset prices per fish size (applied per branch)
DEFAULT_PRICE_MATRIX = {
    "SIZE_2": {"retail_price_per_piece": 390.0, "wholesale_price_per_kg": 350.0},
    "SIZE_3": {"retail_price_per_piece": 420.0, "wholesale_price_per_kg": 370.0},
    "SIZE_4": {"retail_price_per_piece": 460.0, "wholesale_price_per_kg": 390.0},
    "SIZE_5": {"retail_price_per_piece": 500.0, "wholesale_price_per_kg": 420.0},
}

DEFAULT_PRODUCT_CATEGORIES = [
    ("FISH", "Fish", "Fish items handled using size/batch FIFO logic"),
    ("PACKAGING", "Packaging", "Packaging and consumables"),
    ("SERVICE", "Service", "Non-stock service items like frying"),
]

DEFAULT_PRODUCTS = [
    {
        "sku": "FISH_GENERIC",
        "name": "Fish",
        "category_code": "FISH",
        "product_type": "FISH",
        "stock_uom": "KG",
        "tracks_stock": 1,
        "uses_batch_fifo": 1,
        "uses_size_dimension": 1,
        "requires_piece_entry": 1,
        "requires_weight_entry": 1,
        "service_non_stock": 0,
    },
    {
        "sku": "PKG_POLY_SMALL",
        "name": "Small Packaging Bag",
        "category_code": "PACKAGING",
        "product_type": "PACKAGING",
        "stock_uom": "UNIT",
        "tracks_stock": 1,
        "uses_batch_fifo": 0,
        "uses_size_dimension": 0,
        "requires_piece_entry": 0,
        "requires_weight_entry": 0,
        "service_non_stock": 0,
    },
    {
        "sku": "PKG_POLY_LARGE",
        "name": "Large Packaging Bag",
        "category_code": "PACKAGING",
        "product_type": "PACKAGING",
        "stock_uom": "UNIT",
        "tracks_stock": 1,
        "uses_batch_fifo": 0,
        "uses_size_dimension": 0,
        "requires_piece_entry": 0,
        "requires_weight_entry": 0,
        "service_non_stock": 0,
    },
    {
        "sku": "SRV_FRYING",
        "name": "Frying Service",
        "category_code": "SERVICE",
        "product_type": "SERVICE",
        "stock_uom": "SERVICE",
        "tracks_stock": 0,
        "uses_batch_fifo": 0,
        "uses_size_dimension": 0,
        "requires_piece_entry": 0,
        "requires_weight_entry": 0,
        "service_non_stock": 1,
    },
]

DEFAULT_BRANCH_PRODUCT_PRICES = {
    "PKG_POLY_SMALL": {"retail_price": 10.0, "wholesale_price": 8.0},
    "PKG_POLY_LARGE": {"retail_price": 20.0, "wholesale_price": 16.0},
    "SRV_FRYING": {"retail_price": 150.0, "wholesale_price": 150.0},
}

# Approved cross-branch visibility demo
# Meaning: viewer branch can see visible branch stock
DEFAULT_BRANCH_VISIBILITY_RULES = [
    ("Branch B", "Main Branch"),
    ("Main Branch", "Branch B"),
]

# Branch purchasing / replenishment control demo
DEFAULT_BRANCH_PROCUREMENT_RULES = [
    {
        "branch_name": "Main Branch",
        "can_purchase_direct": 1,
        "can_receive_transfer": 1,
        "default_source_branch_name": None,
        "notes": "Main Branch can purchase directly and also transfer stock out/in.",
    },
    {
        "branch_name": "Branch B",
        "can_purchase_direct": 0,
        "can_receive_transfer": 1,
        "default_source_branch_name": "Main Branch",
        "notes": "Branch B should receive stock from Main Branch by default.",
    },
    {
        "branch_name": "Branch C",
        "can_purchase_direct": 1,
        "can_receive_transfer": 1,
        "default_source_branch_name": "Main Branch",
        "notes": "Branch C can purchase directly but may also receive from Main Branch.",
    },
]


def _generate_transfer_code(prefix: str, n: int) -> str:
    return f"{prefix}-{date.today().strftime('%Y%m%d')}-{n:03d}"


def upsert_reference_data(conn) -> None:
    ensure_schema(conn)

    # Branches
    for name in DEFAULT_BRANCHES:
        x(conn, "INSERT OR IGNORE INTO branches(name) VALUES (?)", (name,))

    # Sizes
    for code, desc, order in DEFAULT_SIZES:
        x(
            conn,
            "INSERT OR IGNORE INTO sizes(code, description, sort_order) VALUES (?, ?, ?)",
            (code, desc, int(order)),
        )

    # Suppliers
    for name, contact_person, phone in DEFAULT_SUPPLIERS:
        x(
            conn,
            """
            INSERT OR IGNORE INTO suppliers(name, contact_person, phone, is_active)
            VALUES (?, ?, ?, 1)
            """,
            (name, contact_person, phone),
        )

    # Product categories
    for code, name, description in DEFAULT_PRODUCT_CATEGORIES:
        x(
            conn,
            """
            INSERT OR IGNORE INTO product_categories(code, name, description, is_active)
            VALUES (?, ?, ?, 1)
            """,
            (code, name, description),
        )

    category_rows = q(conn, "SELECT id, code FROM product_categories")
    category_id_by_code = {str(r["code"]): int(r["id"]) for r in category_rows}

    # Products
    for p in DEFAULT_PRODUCTS:
        category_id = category_id_by_code.get(str(p["category_code"]))
        if not category_id:
            continue

        x(
            conn,
            """
            INSERT OR IGNORE INTO products(
                sku, name, category_id, product_type, stock_uom,
                tracks_stock, uses_batch_fifo, uses_size_dimension,
                requires_piece_entry, requires_weight_entry,
                service_non_stock, default_notes, is_active
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                str(p["sku"]),
                str(p["name"]),
                int(category_id),
                str(p["product_type"]),
                str(p["stock_uom"]),
                int(p["tracks_stock"]),
                int(p["uses_batch_fifo"]),
                int(p["uses_size_dimension"]),
                int(p["requires_piece_entry"]),
                int(p["requires_weight_entry"]),
                int(p["service_non_stock"]),
                None,
            ),
        )

    # Promos
    for code, name, buy_qty, free_qty, applies_mode in DEFAULT_PROMOS:
        x(
            conn,
            """
            INSERT OR IGNORE INTO promos(code, name, buy_qty, free_qty, applies_mode, is_active)
            VALUES (?, ?, ?, ?, ?, 1)
            """,
            (code, name, int(buy_qty), int(free_qty), applies_mode),
        )

    # Lookup branches, sizes, and products
    branches = q(conn, "SELECT id, name FROM branches ORDER BY id")
    sizes = q(conn, "SELECT id, code FROM sizes ORDER BY sort_order")
    products = q(conn, "SELECT id, sku FROM products ORDER BY id")
    product_id_by_sku = {str(r["sku"]): int(r["id"]) for r in products}
    branch_id_by_name = {str(r["name"]): int(r["id"]) for r in branches}

    # Prices by branch + fish size
    for br in branches:
        for sz in sizes:
            code = str(sz["code"])
            price_row = DEFAULT_PRICE_MATRIX.get(code)
            if not price_row:
                continue

            x(
                conn,
                """
                INSERT OR IGNORE INTO branch_size_prices(
                    branch_id, size_id, retail_price_per_piece, wholesale_price_per_kg, is_active
                ) VALUES (?, ?, ?, ?, 1)
                """,
                (
                    int(br["id"]),
                    int(sz["id"]),
                    float(price_row["retail_price_per_piece"]),
                    float(price_row["wholesale_price_per_kg"]),
                ),
            )

    # Branch products + branch product prices
    for br in branches:
        for sku, product_id in product_id_by_sku.items():
            x(
                conn,
                """
                INSERT OR IGNORE INTO branch_products(branch_id, product_id, is_active)
                VALUES (?, ?, 1)
                """,
                (int(br["id"]), int(product_id)),
            )

            price_row = DEFAULT_BRANCH_PRODUCT_PRICES.get(sku)
            if price_row:
                x(
                    conn,
                    """
                    INSERT OR IGNORE INTO branch_product_prices(
                        branch_id, product_id, retail_price, wholesale_price, is_active
                    ) VALUES (?, ?, ?, ?, 1)
                    """,
                    (
                        int(br["id"]),
                        int(product_id),
                        float(price_row["retail_price"]),
                        float(price_row["wholesale_price"]),
                    ),
                )

    # Branch visibility rules
    for viewer_branch_name, visible_branch_name in DEFAULT_BRANCH_VISIBILITY_RULES:
        viewer_branch_id = branch_id_by_name.get(str(viewer_branch_name))
        visible_branch_id = branch_id_by_name.get(str(visible_branch_name))
        if viewer_branch_id and visible_branch_id:
            x(
                conn,
                """
                INSERT OR IGNORE INTO branch_visibility_rules(
                    viewer_branch_id, visible_branch_id, is_active
                )
                VALUES (?, ?, 1)
                """,
                (int(viewer_branch_id), int(visible_branch_id)),
            )

    # Branch procurement rules
    for rule in DEFAULT_BRANCH_PROCUREMENT_RULES:
        branch_id = branch_id_by_name.get(str(rule["branch_name"]))
        default_source_branch_id = None
        default_source_branch_name = rule.get("default_source_branch_name")
        if default_source_branch_name:
            default_source_branch_id = branch_id_by_name.get(str(default_source_branch_name))

        if branch_id:
            x(
                conn,
                """
                INSERT OR IGNORE INTO branch_procurement_rules(
                    branch_id, can_purchase_direct, can_receive_transfer, default_source_branch_id, notes
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    int(branch_id),
                    int(rule["can_purchase_direct"]),
                    int(rule["can_receive_transfer"]),
                    int(default_source_branch_id) if default_source_branch_id is not None else None,
                    str(rule["notes"]) if rule.get("notes") else None,
                ),
            )

    # Activate BUY2GET1 for Main Branch only as demo default
    main_branch = q(conn, "SELECT id FROM branches WHERE name='Main Branch'")
    buy2 = q(conn, "SELECT id FROM promos WHERE code='BUY2GET1'")
    if main_branch and buy2:
        x(
            conn,
            """
            INSERT OR IGNORE INTO branch_promos(branch_id, promo_id, is_active)
            VALUES (?, ?, 1)
            """,
            (int(main_branch[0]["id"]), int(buy2[0]["id"])),
        )

    # Sample customers (phone primary, else house number)
    branch_b = q(conn, "SELECT id FROM branches WHERE name='Branch B'")
    branch_c = q(conn, "SELECT id FROM branches WHERE name='Branch C'")
    main_branch = q(conn, "SELECT id FROM branches WHERE name='Main Branch'")

    sample_customers = []
    if branch_b:
        sample_customers.extend(
            [
                (int(branch_b[0]["id"]), "Walk-in Branch B", "Walk-in", "0711111111", None, None),
                (int(branch_b[0]["id"]), "Mama Mboga B", "Market reseller", "0711111112", None, "Regular reseller"),
            ]
        )
    if branch_c:
        sample_customers.extend(
            [
                (int(branch_c[0]["id"]), "Blue Nile Restaurant", "Restaurant", "0722222221", None, None),
                (int(branch_c[0]["id"]), "House C-14", "Individual", None, "C-14", "No phone recorded"),
            ]
        )
    if main_branch:
        sample_customers.extend(
            [
                (int(main_branch[0]["id"]), "Walk-in Nairobi", "Walk-in", "0733333331", None, None),
                (int(main_branch[0]["id"]), "Market Trader Nairobi", "Market reseller", "0733333332", None, None),
            ]
        )

    for branch_id, display_name, category, phone, house_number, notes in sample_customers:
        x(
            conn,
            """
            INSERT OR IGNORE INTO customers(
                branch_id, display_name, category, phone, house_number, notes, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, 1)
            """,
            (branch_id, display_name, category, phone, house_number, notes),
        )


def wipe_all(conn) -> None:
    # Keep schema, delete data (order matters for FKs)
    for t in [
        "service_sales",
        "product_transfers",
        "stock_transfer_lines",
        "stock_transfers",
        "product_stock_movements",
        "branch_procurement_rules",
        "branch_visibility_rules",
        "branch_promos",
        "promos",
        "branch_product_prices",
        "branch_products",
        "products",
        "product_categories",
        "branch_size_prices",
        "customers",
        "suppliers",
        "batch_closures",
        "inventory_adjustments",
        "sales",
        "batch_lines",
        "batches",
        "sizes",
        "branches",
    ]:
        conn.execute(f"DELETE FROM {t};")
    conn.commit()


def load_demo_data(conn, *, seed: int = 7) -> None:
    random.seed(seed)
    upsert_reference_data(conn)

    branches = q(conn, "SELECT * FROM branches ORDER BY id")
    sizes = q(conn, "SELECT * FROM sizes ORDER BY sort_order")
    suppliers = q(conn, "SELECT * FROM suppliers WHERE is_active=1 ORDER BY id")
    customers = q(conn, "SELECT * FROM customers WHERE is_active=1 ORDER BY id")
    products = q(conn, "SELECT id, sku, tracks_stock, service_non_stock FROM products WHERE is_active=1 ORDER BY id")

    # Create demo stock-ins:
    # one purchase can generate multiple size-specific batches
    base_date = date.today() - timedelta(days=4)

    for i in range(4):
        br = random.choice(branches)
        supplier = random.choice(suppliers) if suppliers else None
        receipt_date = (base_date + timedelta(days=i)).isoformat()

        lines: list[BatchLineInput] = []
        for s in sizes:
            pcs = random.randint(40, 120)
            avg = random.uniform(0.28, 0.55) + (0.02 * (int(s["sort_order"]) - 2))
            kg = round(pcs * avg, 3)

            buy_price = round(random.uniform(180, 260), 2)

            lines.append(
                BatchLineInput(
                    size_id=int(s["id"]),
                    pieces=int(pcs),
                    kg=float(kg),
                    buy_price_per_kg=float(buy_price),
                )
            )

        create_batches_from_purchase(
            conn,
            receipt_date=receipt_date,
            branch_id=int(br["id"]),
            supplier=(str(supplier["name"]) if supplier else "Lake Supplier"),
            notes="Demo stock-in",
            lines=lines,
        )

    # Demo packaging stock movement
    packaging_products = [p for p in products if str(p["sku"]).startswith("PKG_")]
    for br in branches:
        for p in packaging_products:
            qty_in = random.randint(100, 300)
            unit_cost = round(random.uniform(5, 15), 2)
            x(
                conn,
                """
                INSERT INTO product_stock_movements(
                    ts, branch_id, product_id, movement_type, qty_delta, unit_cost, reference_no, notes
                )
                VALUES (CURRENT_TIMESTAMP, ?, ?, 'STOCK_IN', ?, ?, ?, ?)
                """,
                (
                    int(br["id"]),
                    int(p["id"]),
                    float(qty_in),
                    float(unit_cost),
                    f"PKG-DEMO-{int(br['id'])}-{int(p['id'])}",
                    "Demo packaging stock-in",
                ),
            )

    # Demo service sales
    frying_service = next((p for p in products if str(p["sku"]) == "SRV_FRYING"), None)
    if frying_service:
        for br in branches:
            branch_customers = [c for c in customers if int(c["branch_id"]) == int(br["id"])]
            svc_customer = random.choice(branch_customers) if branch_customers else None
            qty = random.randint(2, 8)
            unit_price = 150.0
            total_price = round(qty * unit_price, 2)

            x(
                conn,
                """
                INSERT INTO service_sales(
                    service_ts, sale_group_code, branch_id, customer_id, product_id,
                    quantity, unit_price, total_price, notes
                )
                VALUES (CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"SVC-DEMO-{int(br['id'])}",
                    int(br["id"]),
                    int(svc_customer["id"]) if svc_customer else None,
                    int(frying_service["id"]),
                    float(qty),
                    float(unit_price),
                    float(total_price),
                    "Demo frying service sale",
                ),
            )

    # Refresh customers after stock-in in case DB was just initialized
    customers = q(conn, "SELECT * FROM customers WHERE is_active=1 ORDER BY id")

    # Create demo fish sales using FIFO by branch+size
    branch_rows = q(conn, "SELECT id, name FROM branches ORDER BY id")
    size_rows = q(conn, "SELECT id, code FROM sizes ORDER BY sort_order")

    for br in branch_rows:
        branch_customers = [c for c in customers if int(c["branch_id"]) == int(br["id"])]

        for sz in size_rows[:2]:
            price_cfg = DEFAULT_PRICE_MATRIX.get(str(sz["code"]), {})
            retail_price = price_cfg.get("retail_price_per_piece")
            wholesale_price = price_cfg.get("wholesale_price_per_kg")

            # Retail demo sale
            try:
                retail_pcs = random.randint(5, 15)
                actual_avg = random.uniform(0.30, 0.60)
                retail_kg_actual = round(retail_pcs * actual_avg, 3)

                retail_customer = random.choice(branch_customers) if branch_customers else None

                create_retail_sale_fifo(
                    conn,
                    branch_id=int(br["id"]),
                    size_id=int(sz["id"]),
                    customer=(str(retail_customer["display_name"]) if retail_customer else "Walk-in"),
                    customer_id=(int(retail_customer["id"]) if retail_customer else None),
                    pcs_sold=int(retail_pcs),
                    kg_sold_actual=float(retail_kg_actual),
                    unit_price=float(retail_price) if retail_price is not None else None,
                    allow_negative_stock=True,
                )
            except Exception:
                pass

            # Wholesale demo sale
            try:
                kg = round(random.uniform(12, 35), 3)
                pcs_counted = random.randint(20, 90)

                wholesale_customer = random.choice(branch_customers) if branch_customers else None

                create_wholesale_sale_fifo(
                    conn,
                    branch_id=int(br["id"]),
                    size_id=int(sz["id"]),
                    customer=(str(wholesale_customer["display_name"]) if wholesale_customer else "Wholesale Customer"),
                    customer_id=(int(wholesale_customer["id"]) if wholesale_customer else None),
                    kg_sold=float(kg),
                    pcs_counted=int(pcs_counted),
                    tolerance_pcs=2,
                    unit_price=float(wholesale_price) if wholesale_price is not None else None,
                    allow_negative_stock=True,
                )
            except Exception:
                pass

    # Demo fish stock transfer: Main Branch -> Branch B
    main_branch = next((b for b in branches if str(b["name"]) == "Main Branch"), None)
    branch_b = next((b for b in branches if str(b["name"]) == "Branch B"), None)
    if main_branch and branch_b and size_rows:
        source_batches = q(
            conn,
            """
            SELECT
                b.id AS batch_id,
                b.batch_code,
                b.buy_price_per_kg,
                bl.size_id,
                bl.pieces,
                bl.kg,
                bl.avg_kg_per_piece
            FROM batches b
            JOIN batch_lines bl ON bl.batch_id = b.id
            WHERE b.branch_id=?
              AND b.status='OPEN'
            ORDER BY b.receipt_date, b.id
            """,
            (int(main_branch["id"]),),
        )

        if source_batches:
            transfer_header_id = x(
                conn,
                """
                INSERT OR IGNORE INTO stock_transfers(
                    transfer_code, transfer_ts, from_branch_id, to_branch_id, status, notes, created_by
                )
                VALUES (?, CURRENT_TIMESTAMP, ?, ?, 'POSTED', ?, ?)
                """,
                (
                    _generate_transfer_code("TRF", 1),
                    int(main_branch["id"]),
                    int(branch_b["id"]),
                    "Demo fish transfer from Main Branch to Branch B",
                    "demo_loader",
                ),
            )

            lines_added = 0
            for row in source_batches[:2]:
                move_pcs = min(8, int(row["pieces"]))
                if move_pcs <= 0:
                    continue
                move_kg = round(float(move_pcs) * float(row["avg_kg_per_piece"]), 3)

                x(
                    conn,
                    """
                    INSERT INTO stock_transfer_lines(
                        transfer_id, from_batch_id, size_id, pieces, kg, avg_kg_per_piece, unit_cost_per_kg
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(transfer_header_id),
                        int(row["batch_id"]),
                        int(row["size_id"]),
                        int(move_pcs),
                        float(move_kg),
                        float(row["avg_kg_per_piece"]),
                        float(row["buy_price_per_kg"]),
                    ),
                )

                # Destination receives new fish batch
                create_batches_from_purchase(
                    conn,
                    receipt_date=date.today().isoformat(),
                    branch_id=int(branch_b["id"]),
                    supplier="Internal Transfer",
                    notes=f"Demo transfer receipt from {main_branch['name']} ({row['batch_code']})",
                    lines=[
                        BatchLineInput(
                            size_id=int(row["size_id"]),
                            pieces=int(move_pcs),
                            kg=float(move_kg),
                            buy_price_per_kg=float(row["buy_price_per_kg"]),
                        )
                    ],
                )
                lines_added += 1

            if lines_added == 0:
                conn.execute("DELETE FROM stock_transfers WHERE id=?", (int(transfer_header_id),))
                conn.commit()

    # Demo packaging product transfer: Main Branch -> Branch B
    packaging_small = next((p for p in products if str(p["sku"]) == "PKG_POLY_SMALL"), None)
    if main_branch and branch_b and packaging_small:
        x(
            conn,
            """
            INSERT OR IGNORE INTO product_transfers(
                transfer_code, transfer_ts, from_branch_id, to_branch_id, product_id,
                qty, unit_cost, status, notes, created_by
            )
            VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, 'POSTED', ?, ?)
            """,
            (
                _generate_transfer_code("PTR", 1),
                int(main_branch["id"]),
                int(branch_b["id"]),
                int(packaging_small["id"]),
                25.0,
                8.5,
                "Demo packaging transfer from Main Branch to Branch B",
                "demo_loader",
            ),
        )

        x(
            conn,
            """
            INSERT INTO product_stock_movements(
                ts, branch_id, product_id, movement_type, qty_delta, unit_cost, reference_no, notes
            )
            VALUES (CURRENT_TIMESTAMP, ?, ?, 'TRANSFER_OUT', ?, ?, ?, ?)
            """,
            (
                int(main_branch["id"]),
                int(packaging_small["id"]),
                -25.0,
                8.5,
                "PTR-DEMO-001",
                "Demo packaging transfer out",
            ),
        )

        x(
            conn,
            """
            INSERT INTO product_stock_movements(
                ts, branch_id, product_id, movement_type, qty_delta, unit_cost, reference_no, notes
            )
            VALUES (CURRENT_TIMESTAMP, ?, ?, 'TRANSFER_IN', ?, ?, ?, ?)
            """,
            (
                int(branch_b["id"]),
                int(packaging_small["id"]),
                25.0,
                8.5,
                "PTR-DEMO-001",
                "Demo packaging transfer in",
            ),
        )
