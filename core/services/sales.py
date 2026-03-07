from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from core.db import q, x
from core.utils import iso_now, safe_div


@dataclass
class SaleResult:
    sale_id: int
    pcs_sold: int
    kg_sold: float
    variance_flag: int
    pcs_suggested: Optional[int]
    customer_id: Optional[int] = None
    promo_applied: int = 0
    charged_pcs: Optional[int] = None
    free_pcs: int = 0
    promo_discount_value: Optional[float] = None


def _normalize_customer(customer: Optional[str]) -> Optional[str]:
    if customer is None:
        return None
    s = str(customer).strip()
    return s if s else None


def _safe_int_or_none(v: Optional[int]) -> Optional[int]:
    if v is None:
        return None
    return int(v)


def _get_batch_avg(conn, batch_id: int) -> float:
    rows = q(conn, "SELECT batch_avg_kg_per_piece FROM batches WHERE id=?", (int(batch_id),))
    if not rows:
        raise ValueError("Batch not found.")

    avg = rows[0]["batch_avg_kg_per_piece"]
    if avg is None:
        raise ValueError("Batch average kg/pc is missing.")

    avg_f = float(avg)
    if avg_f <= 0:
        raise ValueError("Batch average kg/pc must be > 0.")
    return avg_f


def _normalize_price_basis(price_basis: Optional[str]) -> str:
    if not price_basis:
        return "PER_KG"
    pb = str(price_basis).strip().upper()
    if pb in {"PER_KG", "PER_PIECE"}:
        return pb
    raise ValueError("Invalid price_basis. Use 'PER_KG' or 'PER_PIECE'.")


def _compute_total_price(
    unit_price: Optional[float],
    price_basis: str,
    kg_sold: float,
    pcs_sold: int,
) -> Optional[float]:
    if unit_price is None:
        return None
    try:
        up = float(unit_price)
    except Exception:
        raise ValueError("Unit price must be a number.")
    if up <= 0:
        return None

    if price_basis == "PER_PIECE":
        return round(int(pcs_sold) * up, 2)
    return round(float(kg_sold) * up, 2)


def _normalize_total_price(total_price: Optional[float]) -> Optional[float]:
    if total_price is None:
        return None
    try:
        tp = float(total_price)
    except Exception:
        return None
    if tp < 0:
        return None
    return tp


def _batch_line_on_hand(conn, batch_id: int, size_id: int) -> dict:
    bl = q(
        conn,
        """
        SELECT pieces, kg
        FROM batch_lines
        WHERE batch_id=? AND size_id=?
        """,
        (int(batch_id), int(size_id)),
    )
    if not bl:
        return {"pcs": 0, "kg": 0.0}

    init_pcs = int(bl[0]["pieces"])
    init_kg = float(bl[0]["kg"])

    sold = q(
        conn,
        """
        SELECT
          COALESCE(SUM(pcs_sold),0) AS pcs,
          COALESCE(SUM(kg_sold),0) AS kg
        FROM sales
        WHERE batch_id=? AND size_id=?
        """,
        (int(batch_id), int(size_id)),
    )[0]

    pcs = init_pcs - int(sold["pcs"])
    kg = init_kg - float(sold["kg"])
    return {"pcs": pcs, "kg": kg}


def _fifo_batches_for_size(conn, *, branch_id: int, size_id: int) -> list[dict]:
    rows = q(
        conn,
        """
        SELECT
          b.id AS batch_id,
          b.batch_code,
          b.receipt_date,
          b.branch_id,
          b.buy_price_per_kg,
          b.batch_avg_kg_per_piece
        FROM batches b
        JOIN batch_lines bl ON bl.batch_id = b.id
        WHERE b.status='OPEN'
          AND b.branch_id=?
          AND bl.size_id=?
        ORDER BY b.receipt_date ASC, b.id ASC
        """,
        (int(branch_id), int(size_id)),
    )

    out: list[dict] = []
    for r in rows:
        onhand = _batch_line_on_hand(conn, int(r["batch_id"]), int(size_id))
        if onhand["pcs"] > 0 and onhand["kg"] > 0:
            out.append(
                {
                    "batch_id": int(r["batch_id"]),
                    "batch_code": str(r["batch_code"]),
                    "receipt_date": str(r["receipt_date"]),
                    "avg_kg_per_piece": float(r["batch_avg_kg_per_piece"]),
                    "pcs_on_hand": int(onhand["pcs"]),
                    "kg_on_hand": float(onhand["kg"]),
                }
            )
    return out


def get_branch_size_prices(conn, *, branch_id: int, size_id: int) -> dict:
    rows = q(
        conn,
        """
        SELECT retail_price_per_piece, wholesale_price_per_kg
        FROM branch_size_prices
        WHERE branch_id=? AND size_id=? AND is_active=1
        """,
        (int(branch_id), int(size_id)),
    )
    if not rows:
        raise ValueError("No active price configuration found for this branch and size.")
    return {
        "retail_price_per_piece": float(rows[0]["retail_price_per_piece"]),
        "wholesale_price_per_kg": float(rows[0]["wholesale_price_per_kg"]),
    }


def get_active_branch_retail_promo(conn, *, branch_id: int) -> Optional[dict]:
    rows = q(
        conn,
        """
        SELECT
            p.id AS promo_id,
            p.code,
            p.name,
            p.buy_qty,
            p.free_qty,
            p.applies_mode
        FROM branch_promos bp
        JOIN promos p ON p.id = bp.promo_id
        WHERE bp.branch_id=?
          AND bp.is_active=1
          AND p.is_active=1
          AND p.applies_mode='RETAIL_PCS'
        ORDER BY bp.id DESC
        LIMIT 1
        """,
        (int(branch_id),),
    )
    if not rows:
        return None

    r = rows[0]
    return {
        "promo_id": int(r["promo_id"]),
        "code": str(r["code"]),
        "name": str(r["name"]),
        "buy_qty": int(r["buy_qty"]),
        "free_qty": int(r["free_qty"]),
        "applies_mode": str(r["applies_mode"]),
    }


def calculate_retail_promo_summary(
    *,
    total_pcs: int,
    unit_price: float,
    promo: Optional[dict],
) -> dict:
    total_pcs = int(total_pcs)
    unit_price = float(unit_price)

    if total_pcs <= 0:
        return {
            "promo_applied": 0,
            "charged_pcs": 0,
            "free_pcs": 0,
            "total_price": 0.0,
            "promo_discount_value": 0.0,
        }

    if not promo:
        total_price = round(total_pcs * unit_price, 2)
        return {
            "promo_applied": 0,
            "charged_pcs": total_pcs,
            "free_pcs": 0,
            "total_price": total_price,
            "promo_discount_value": 0.0,
        }

    buy_qty = int(promo["buy_qty"])
    free_qty = int(promo["free_qty"])
    group_size = buy_qty + free_qty

    if buy_qty <= 0 or free_qty <= 0 or group_size <= 0:
        total_price = round(total_pcs * unit_price, 2)
        return {
            "promo_applied": 0,
            "charged_pcs": total_pcs,
            "free_pcs": 0,
            "total_price": total_price,
            "promo_discount_value": 0.0,
        }

    full_groups = total_pcs // group_size
    free_pcs = full_groups * free_qty
    charged_pcs = total_pcs - free_pcs
    promo_discount_value = round(free_pcs * unit_price, 2)
    total_price = round(charged_pcs * unit_price, 2)

    return {
        "promo_applied": 1 if free_pcs > 0 else 0,
        "charged_pcs": int(charged_pcs),
        "free_pcs": int(free_pcs),
        "total_price": float(total_price),
        "promo_discount_value": float(promo_discount_value),
    }


def _free_pcs_in_interval(start_pos: int, end_pos: int, *, buy_qty: int, free_qty: int) -> int:
    """
    Sequence-based free-piece allocation across FIFO rows.
    Example Buy 2 Get 1:
      positions 1,2 charged
      position 3 free
      positions 4,5 charged
      position 6 free
    """
    if end_pos < start_pos:
        return 0

    group_size = int(buy_qty) + int(free_qty)
    if group_size <= 0 or buy_qty <= 0 or free_qty <= 0:
        return 0

    free_count = 0
    for pos in range(int(start_pos), int(end_pos) + 1):
        pos_in_group = ((pos - 1) % group_size) + 1
        if pos_in_group > int(buy_qty):
            free_count += 1
    return int(free_count)


# -------------------------
# Base single-batch writers
# -------------------------

def create_retail_sale(
    conn,
    *,
    branch_id: int,
    batch_id: int,
    size_id: Optional[int],
    customer: Optional[str],
    customer_id: Optional[int] = None,
    pcs_sold: int,
    unit_price: Optional[float],
    price_basis: str = "PER_PIECE",
    total_price: Optional[float] = None,
    kg_sold_override: Optional[float] = None,
    variance_flag: int = 0,
    promo_applied: int = 0,
    promo_code: Optional[str] = None,
    promo_name: Optional[str] = None,
    promo_buy_qty: Optional[int] = None,
    promo_free_qty: Optional[int] = None,
    charged_pcs: Optional[int] = None,
    free_pcs: int = 0,
    promo_discount_value: Optional[float] = None,
) -> SaleResult:
    if int(pcs_sold) <= 0:
        raise ValueError("Pieces sold must be > 0.")

    price_basis = _normalize_price_basis(price_basis)

    if kg_sold_override is not None:
        kg_sold = float(kg_sold_override)
        if kg_sold <= 0:
            raise ValueError("Kg sold must be > 0.")
    else:
        avg = _get_batch_avg(conn, int(batch_id))
        kg_sold = float(int(pcs_sold)) * avg

    tp = _normalize_total_price(total_price)
    if tp is None:
        charge_qty = int(charged_pcs) if charged_pcs is not None else int(pcs_sold)
        tp = _compute_total_price(unit_price, price_basis, kg_sold, charge_qty)

    sale_id = x(
        conn,
        """
        INSERT INTO sales (
            sale_ts, branch_id, mode, customer, customer_id, batch_id, size_id,
            pcs_sold, kg_sold,
            unit_price, price_basis, total_price,
            pcs_suggested, variance_flag,
            promo_applied, promo_code, promo_name, promo_buy_qty, promo_free_qty,
            charged_pcs, free_pcs, promo_discount_value
        ) VALUES (?, ?, 'RETAIL_PCS', ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            iso_now(),
            int(branch_id),
            _normalize_customer(customer),
            _safe_int_or_none(customer_id),
            int(batch_id),
            _safe_int_or_none(size_id),
            int(pcs_sold),
            float(kg_sold),
            float(unit_price) if unit_price is not None else None,
            str(price_basis),
            float(tp) if tp is not None else None,
            int(variance_flag),
            int(promo_applied),
            promo_code,
            promo_name,
            _safe_int_or_none(promo_buy_qty),
            _safe_int_or_none(promo_free_qty),
            _safe_int_or_none(charged_pcs),
            int(free_pcs),
            float(promo_discount_value) if promo_discount_value is not None else None,
        ),
    )

    return SaleResult(
        sale_id=int(sale_id),
        pcs_sold=int(pcs_sold),
        kg_sold=float(kg_sold),
        variance_flag=int(variance_flag),
        pcs_suggested=None,
        customer_id=_safe_int_or_none(customer_id),
        promo_applied=int(promo_applied),
        charged_pcs=_safe_int_or_none(charged_pcs),
        free_pcs=int(free_pcs),
        promo_discount_value=float(promo_discount_value) if promo_discount_value is not None else None,
    )


def create_wholesale_sale(
    conn,
    *,
    branch_id: int,
    batch_id: int,
    size_id: Optional[int],
    customer: Optional[str],
    customer_id: Optional[int] = None,
    kg_sold: float,
    pcs_counted: int,
    tolerance_pcs: int = 2,
    unit_price: Optional[float] = None,
    price_basis: str = "PER_KG",
    total_price: Optional[float] = None,
) -> SaleResult:
    if float(kg_sold) <= 0:
        raise ValueError("Kg sold must be > 0.")
    if int(pcs_counted) <= 0:
        raise ValueError("Counted pieces must be > 0.")

    tol = max(0, int(tolerance_pcs))
    price_basis = _normalize_price_basis(price_basis)

    avg = _get_batch_avg(conn, int(batch_id))
    pcs_suggested = int(round(safe_div(float(kg_sold), avg)))
    variance_flag = 1 if abs(int(pcs_counted) - int(pcs_suggested)) > tol else 0

    tp = _normalize_total_price(total_price)
    if tp is None:
        tp = _compute_total_price(unit_price, price_basis, float(kg_sold), int(pcs_counted))

    sale_id = x(
        conn,
        """
        INSERT INTO sales (
            sale_ts, branch_id, mode, customer, customer_id, batch_id, size_id,
            pcs_sold, kg_sold,
            unit_price, price_basis, total_price,
            pcs_suggested, variance_flag,
            promo_applied, charged_pcs, free_pcs, promo_discount_value
        ) VALUES (?, ?, 'WHOLESALE_KG', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 0, NULL)
        """,
        (
            iso_now(),
            int(branch_id),
            _normalize_customer(customer),
            _safe_int_or_none(customer_id),
            int(batch_id),
            _safe_int_or_none(size_id),
            int(pcs_counted),
            float(kg_sold),
            float(unit_price) if unit_price is not None else None,
            str(price_basis),
            float(tp) if tp is not None else None,
            int(pcs_suggested),
            int(variance_flag),
            int(pcs_counted),
        ),
    )

    return SaleResult(
        sale_id=int(sale_id),
        pcs_sold=int(pcs_counted),
        kg_sold=float(kg_sold),
        variance_flag=int(variance_flag),
        pcs_suggested=int(pcs_suggested),
        customer_id=_safe_int_or_none(customer_id),
        promo_applied=0,
        charged_pcs=int(pcs_counted),
        free_pcs=0,
        promo_discount_value=None,
    )


# -------------------------
# FIFO wrappers (AUTO)
# -------------------------

def create_retail_sale_fifo(
    conn,
    *,
    branch_id: int,
    size_id: int,
    customer: Optional[str],
    customer_id: Optional[int] = None,
    pcs_sold: int,
    kg_sold_actual: float,
    unit_price: Optional[float],
    tolerance_weight_ratio: float = 0.20,
) -> list[SaleResult]:
    """
    Retail:
    - pricing is PER_PIECE
    - salesperson inputs pieces + actual kg
    - actual kg is allocated proportionally across FIFO rows
    - active branch promo (if any) is automatically applied
    """
    if int(pcs_sold) <= 0:
        raise ValueError("Pieces sold must be > 0.")
    if float(kg_sold_actual) <= 0:
        raise ValueError("Kg sold must be > 0.")
    if unit_price is None:
        raise ValueError("Retail unit price is required.")

    fifo = _fifo_batches_for_size(conn, branch_id=int(branch_id), size_id=int(size_id))
    if not fifo:
        raise ValueError("No stock available for this size (FIFO).")

    remaining_pcs = int(pcs_sold)
    allocations: list[tuple[dict, int, float]] = []  # batch, pcs, expected_kg

    for b in fifo:
        if remaining_pcs <= 0:
            break

        take_pcs = min(remaining_pcs, int(b["pcs_on_hand"]))
        if take_pcs <= 0:
            continue

        expected_kg = float(take_pcs) * float(b["avg_kg_per_piece"])
        allocations.append((b, int(take_pcs), float(expected_kg)))
        remaining_pcs -= int(take_pcs)

    if remaining_pcs > 0:
        raise ValueError("Not enough pieces on hand across FIFO batches for this size.")

    total_expected_kg = sum(exp_kg for _, _, exp_kg in allocations)
    if total_expected_kg <= 0:
        raise ValueError("Expected kg could not be computed for FIFO allocations.")

    promo = get_active_branch_retail_promo(conn, branch_id=int(branch_id))
    promo_summary = calculate_retail_promo_summary(
        total_pcs=int(pcs_sold),
        unit_price=float(unit_price),
        promo=promo,
    )

    results: list[SaleResult] = []
    kg_remaining = float(kg_sold_actual)

    # Allocate promo free/charged pieces by FIFO sequence positions
    cursor_start = 1

    total_free_alloc = 0
    total_charged_alloc = 0
    total_discount_alloc = 0.0
    total_price_alloc = 0.0

    for i, (b, take_pcs, expected_kg) in enumerate(allocations):
        if i < len(allocations) - 1:
            actual_alloc_kg = round((expected_kg / total_expected_kg) * float(kg_sold_actual), 3)
            max_allowed = kg_remaining - 0.001 * (len(allocations) - i - 1)
            actual_alloc_kg = max(0.001, min(actual_alloc_kg, max_allowed))
        else:
            actual_alloc_kg = round(kg_remaining, 3)

        kg_remaining = round(kg_remaining - actual_alloc_kg, 3)

        ratio_diff = abs(actual_alloc_kg - expected_kg) / expected_kg if expected_kg > 0 else 0.0
        variance_flag = 1 if ratio_diff > float(tolerance_weight_ratio) else 0

        row_free_pcs = 0
        row_charged_pcs = int(take_pcs)
        row_discount = 0.0
        row_total_price = float(take_pcs) * float(unit_price)
        row_promo_applied = 0

        if promo_summary["promo_applied"] == 1 and promo is not None:
            row_start = int(cursor_start)
            row_end = int(cursor_start + take_pcs - 1)
            row_free_pcs = _free_pcs_in_interval(
                row_start,
                row_end,
                buy_qty=int(promo["buy_qty"]),
                free_qty=int(promo["free_qty"]),
            )
            row_charged_pcs = int(take_pcs) - int(row_free_pcs)
            row_discount = round(float(row_free_pcs) * float(unit_price), 2)
            row_total_price = round(float(row_charged_pcs) * float(unit_price), 2)
            row_promo_applied = 1 if row_free_pcs > 0 else 0
            cursor_start = row_end + 1

            total_free_alloc += int(row_free_pcs)
            total_charged_alloc += int(row_charged_pcs)
            total_discount_alloc = round(total_discount_alloc + row_discount, 2)
            total_price_alloc = round(total_price_alloc + row_total_price, 2)

        res = create_retail_sale(
            conn,
            branch_id=int(branch_id),
            batch_id=int(b["batch_id"]),
            size_id=int(size_id),
            customer=customer,
            customer_id=customer_id,
            pcs_sold=int(take_pcs),
            unit_price=float(unit_price),
            price_basis="PER_PIECE",
            total_price=float(row_total_price),
            kg_sold_override=float(actual_alloc_kg),
            variance_flag=int(variance_flag),
            promo_applied=int(row_promo_applied),
            promo_code=(promo["code"] if row_promo_applied and promo else None),
            promo_name=(promo["name"] if row_promo_applied and promo else None),
            promo_buy_qty=(int(promo["buy_qty"]) if row_promo_applied and promo else None),
            promo_free_qty=(int(promo["free_qty"]) if row_promo_applied and promo else None),
            charged_pcs=int(row_charged_pcs),
            free_pcs=int(row_free_pcs),
            promo_discount_value=float(row_discount) if row_promo_applied else None,
        )
        results.append(res)

    # sanity adjustment for no-promo case
    if promo_summary["promo_applied"] == 0:
        # rows were already written as full charge rows
        pass

    return results


def create_wholesale_sale_fifo(
    conn,
    *,
    branch_id: int,
    size_id: int,
    customer: Optional[str],
    customer_id: Optional[int] = None,
    kg_sold: float,
    pcs_counted: int,
    tolerance_pcs: int = 2,
    unit_price: Optional[float] = None,
) -> list[SaleResult]:
    """
    Wholesale:
    - pricing is PER_KG
    - salesperson inputs kg + counted pieces
    - counted pieces are allocated across FIFO rows in a controlled way
    """
    if float(kg_sold) <= 0:
        raise ValueError("Kg sold must be > 0.")
    if int(pcs_counted) <= 0:
        raise ValueError("Counted pieces must be > 0.")

    tol = max(0, int(tolerance_pcs))

    fifo = _fifo_batches_for_size(conn, branch_id=int(branch_id), size_id=int(size_id))
    if not fifo:
        raise ValueError("No stock available for this size (FIFO).")

    remaining_kg = float(kg_sold)
    allocations: list[tuple[dict, float]] = []

    for b in fifo:
        if remaining_kg <= 1e-9:
            break
        take_kg = min(remaining_kg, float(b["kg_on_hand"]))
        if take_kg <= 1e-9:
            continue
        allocations.append((b, float(take_kg)))
        remaining_kg -= float(take_kg)

    if remaining_kg > 1e-6:
        raise ValueError("Not enough kg on hand across FIFO batches for this size.")

    if int(pcs_counted) < len(allocations):
        raise ValueError(
            f"This sale spans {len(allocations)} FIFO batch(es), but counted pieces is {pcs_counted}. "
            "Re-check size selection, stock, or count again."
        )

    total_alloc_kg = sum(kg for _, kg in allocations) or 0.0
    results: list[SaleResult] = []

    pcs_remaining = int(pcs_counted)
    kg_remaining_total = float(total_alloc_kg)

    for i, (b, take_kg) in enumerate(allocations):
        avg = float(b["avg_kg_per_piece"])
        pcs_suggested = int(round(safe_div(float(take_kg), avg)))

        if i < len(allocations) - 1:
            min_needed_for_rest = (len(allocations) - i - 1)
            raw = (float(take_kg) / kg_remaining_total) * pcs_remaining if kg_remaining_total > 0 else 1.0
            pcs_alloc = int(round(raw))
            pcs_alloc = max(1, pcs_alloc)
            pcs_alloc = min(pcs_alloc, pcs_remaining - min_needed_for_rest)
        else:
            pcs_alloc = pcs_remaining

        pcs_remaining -= int(pcs_alloc)
        kg_remaining_total -= float(take_kg)

        variance_flag = 1 if abs(int(pcs_alloc) - int(pcs_suggested)) > tol else 0
        total_price = _compute_total_price(unit_price, "PER_KG", float(take_kg), int(pcs_alloc))

        sale_id = x(
            conn,
            """
            INSERT INTO sales (
                sale_ts, branch_id, mode, customer, customer_id, batch_id, size_id,
                pcs_sold, kg_sold,
                unit_price, price_basis, total_price,
                pcs_suggested, variance_flag,
                promo_applied, charged_pcs, free_pcs, promo_discount_value
            ) VALUES (?, ?, 'WHOLESALE_KG', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 0, NULL)
            """,
            (
                iso_now(),
                int(branch_id),
                _normalize_customer(customer),
                _safe_int_or_none(customer_id),
                int(b["batch_id"]),
                int(size_id),
                int(pcs_alloc),
                float(take_kg),
                float(unit_price) if unit_price is not None else None,
                "PER_KG",
                float(total_price) if total_price is not None else None,
                int(pcs_suggested),
                int(variance_flag),
                int(pcs_alloc),
            ),
        )

        results.append(
            SaleResult(
                sale_id=int(sale_id),
                pcs_sold=int(pcs_alloc),
                kg_sold=float(take_kg),
                variance_flag=int(variance_flag),
                pcs_suggested=int(pcs_suggested),
                customer_id=_safe_int_or_none(customer_id),
                promo_applied=0,
                charged_pcs=int(pcs_alloc),
                free_pcs=0,
                promo_discount_value=None,
            )
        )

    return results