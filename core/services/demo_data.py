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

# Sample preset prices per size (applied per branch)
DEFAULT_PRICE_MATRIX = {
    "SIZE_2": {"retail_price_per_piece": 390.0, "wholesale_price_per_kg": 350.0},
    "SIZE_3": {"retail_price_per_piece": 420.0, "wholesale_price_per_kg": 370.0},
    "SIZE_4": {"retail_price_per_piece": 460.0, "wholesale_price_per_kg": 390.0},
    "SIZE_5": {"retail_price_per_piece": 500.0, "wholesale_price_per_kg": 420.0},
}


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

    # Prices by branch+size
    branches = q(conn, "SELECT id, name FROM branches ORDER BY id")
    sizes = q(conn, "SELECT id, code FROM sizes ORDER BY sort_order")

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
        "branch_promos",
        "promos",
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

    # Refresh customers after stock-in in case DB was just initialized
    customers = q(conn, "SELECT * FROM customers WHERE is_active=1 ORDER BY id")

    # Create demo sales using FIFO by branch+size
    branch_rows = q(conn, "SELECT id, name FROM branches ORDER BY id")
    size_rows = q(conn, "SELECT id, code FROM sizes ORDER BY sort_order")

    for br in branch_rows:
        branch_customers = [c for c in customers if int(c["branch_id"]) == int(br["id"])]

        for sz in size_rows[:2]:  # a couple of sizes per branch
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
                )
            except Exception:
                pass