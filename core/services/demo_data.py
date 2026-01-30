from __future__ import annotations

import random
from datetime import date, timedelta

from core.db import q, x, ensure_schema
from core.services.batches import create_batch, BatchLineInput
from core.services.sales import create_retail_sale, create_wholesale_sale


DEFAULT_BRANCHES = ["Main Branch", "Branch B", "Branch C"]
DEFAULT_SIZES = [
    ("SIZE_2", "Size 2", 2),
    ("SIZE_3", "Size 3", 3),
    ("SIZE_4", "Size 4", 4),
    ("SIZE_5", "Size 5", 5),
]


def upsert_reference_data(conn) -> None:
    ensure_schema(conn)

    for name in DEFAULT_BRANCHES:
        x(conn, "INSERT OR IGNORE INTO branches(name) VALUES (?)", (name,))

    for code, desc, order in DEFAULT_SIZES:
        x(conn, "INSERT OR IGNORE INTO sizes(code, description, sort_order) VALUES (?, ?, ?)", (code, desc, int(order)))


def wipe_all(conn) -> None:
    # Keep schema, delete data (order matters for FKs).
    for t in ["batch_closures", "inventory_adjustments", "sales", "batch_lines", "batches", "sizes", "branches"]:
        conn.execute(f"DELETE FROM {t};")
    conn.commit()


def load_demo_data(conn, *, seed: int = 7) -> None:
    random.seed(seed)
    upsert_reference_data(conn)

    branches = q(conn, "SELECT * FROM branches ORDER BY id")
    sizes = q(conn, "SELECT * FROM sizes ORDER BY sort_order")

    # Create 6 demo batches across branches
    base_date = date.today() - timedelta(days=3)
    for i in range(6):
        br = random.choice(branches)
        receipt_date = (base_date + timedelta(days=i % 3)).isoformat()
        batch_code = f"RCPT-{receipt_date.replace('-', '')}-{i+1:03d}"

        lines = []
        for s in sizes:
            pcs = random.randint(40, 120)
            avg = random.uniform(0.28, 0.55) + (0.02 * (int(s["sort_order"]) - 2))
            kg = round(pcs * avg, 3)
            lines.append(BatchLineInput(size_id=int(s["id"]), pieces=int(pcs), kg=float(kg)))

        create_batch(
            conn,
            batch_code=batch_code,
            receipt_date=receipt_date,
            branch_id=int(br["id"]),
            supplier="Lake Supplier",
            notes="Demo receipt batch",
            lines=lines,
        )

    # Add a few sales
    batches = q(conn, "SELECT * FROM batches WHERE status='OPEN' ORDER BY id DESC")
    for b in batches[:4]:
        br_id = int(b["branch_id"])
        batch_id = int(b["id"])

        create_retail_sale(
            conn,
            branch_id=br_id,
            batch_id=batch_id,
            size_id=None,
            customer="Walk-in",
            pcs_sold=random.randint(30, 120),
            unit_price=420.0,
        )

        kg = round(random.uniform(20, 80), 3)
        avg = float(b["batch_avg_kg_per_piece"])
        suggested = int(round(kg / avg))
        counted = max(1, suggested + random.choice([-3, -1, 0, 1, 2, 4]))

        create_wholesale_sale(
            conn,
            branch_id=br_id,
            batch_id=batch_id,
            size_id=None,
            customer="Wholesale Customer",
            kg_sold=kg,
            pcs_counted=counted,
            tolerance_pcs=2,
            unit_price=390.0,
        )
