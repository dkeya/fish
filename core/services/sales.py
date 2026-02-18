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

    # sqlite3.Row supports dict-like indexing, not .get()
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
    if tp <= 0:
        return None
    return tp


def _batch_line_on_hand(conn, batch_id: int, size_id: int) -> dict:
    """
    Size-level on-hand for FIFO depletion.
    Uses batch_lines as initial, subtracts sales attributed to batch+size.
    """
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
    """
    Oldest-first open batches for a branch+size, with avg kg/pc and on-hand.
    """
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
    pcs_sold: int,
    unit_price: Optional[float],
    price_basis: str = "PER_KG",
    total_price: Optional[float] = None,
) -> SaleResult:
    if int(pcs_sold) <= 0:
        raise ValueError("Pieces sold must be > 0.")

    price_basis = _normalize_price_basis(price_basis)

    avg = _get_batch_avg(conn, int(batch_id))
    kg_sold = float(int(pcs_sold)) * avg

    tp = _normalize_total_price(total_price)
    if tp is None:
        tp = _compute_total_price(unit_price, price_basis, kg_sold, int(pcs_sold))

    sale_id = x(
        conn,
        """
        INSERT INTO sales (
            sale_ts, branch_id, mode, customer, batch_id, size_id,
            pcs_sold, kg_sold,
            unit_price, price_basis, total_price,
            pcs_suggested, variance_flag
        ) VALUES (?, ?, 'RETAIL_PCS', ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0)
        """,
        (
            iso_now(),
            int(branch_id),
            _normalize_customer(customer),
            int(batch_id),
            _safe_int_or_none(size_id),
            int(pcs_sold),
            float(kg_sold),
            float(unit_price) if unit_price is not None else None,
            str(price_basis),
            float(tp) if tp is not None else None,
        ),
    )

    return SaleResult(
        sale_id=int(sale_id),
        pcs_sold=int(pcs_sold),
        kg_sold=float(kg_sold),
        variance_flag=0,
        pcs_suggested=None,
    )


def create_wholesale_sale(
    conn,
    *,
    branch_id: int,
    batch_id: int,
    size_id: Optional[int],
    customer: Optional[str],
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
            sale_ts, branch_id, mode, customer, batch_id, size_id,
            pcs_sold, kg_sold,
            unit_price, price_basis, total_price,
            pcs_suggested, variance_flag
        ) VALUES (?, ?, 'WHOLESALE_KG', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            iso_now(),
            int(branch_id),
            _normalize_customer(customer),
            int(batch_id),
            _safe_int_or_none(size_id),
            int(pcs_counted),
            float(kg_sold),
            float(unit_price) if unit_price is not None else None,
            str(price_basis),
            float(tp) if tp is not None else None,
            int(pcs_suggested),
            int(variance_flag),
        ),
    )

    return SaleResult(
        sale_id=int(sale_id),
        pcs_sold=int(pcs_counted),
        kg_sold=float(kg_sold),
        variance_flag=int(variance_flag),
        pcs_suggested=int(pcs_suggested),
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
    pcs_sold: int,
    unit_price: Optional[float],
    price_basis: str = "PER_KG",
) -> list[SaleResult]:
    """
    Auto-deplete FIFO batches for the given branch+size.
    Splits across batches if needed.
    """
    if int(pcs_sold) <= 0:
        raise ValueError("Pieces sold must be > 0.")

    price_basis = _normalize_price_basis(price_basis)

    fifo = _fifo_batches_for_size(conn, branch_id=int(branch_id), size_id=int(size_id))
    if not fifo:
        raise ValueError("No stock available for this size (FIFO).")

    remaining_pcs = int(pcs_sold)
    results: list[SaleResult] = []

    for b in fifo:
        if remaining_pcs <= 0:
            break

        take_pcs = min(remaining_pcs, int(b["pcs_on_hand"]))
        if take_pcs <= 0:
            continue

        # Use this batch's avg (safer than recomputing elsewhere)
        avg = float(b["avg_kg_per_piece"])
        kg = float(take_pcs) * avg
        total_price = _compute_total_price(unit_price, price_basis, kg, int(take_pcs))

        res = create_retail_sale(
            conn,
            branch_id=int(branch_id),
            batch_id=int(b["batch_id"]),
            size_id=int(size_id),
            customer=customer,
            pcs_sold=int(take_pcs),
            unit_price=unit_price,
            price_basis=price_basis,
            total_price=total_price,
        )
        results.append(res)
        remaining_pcs -= int(take_pcs)

    if remaining_pcs > 0:
        raise ValueError("Not enough pieces on hand across FIFO batches for this size.")

    return results


def create_wholesale_sale_fifo(
    conn,
    *,
    branch_id: int,
    size_id: int,
    customer: Optional[str],
    kg_sold: float,
    pcs_counted: int,
    tolerance_pcs: int = 2,
    unit_price: Optional[float] = None,
    price_basis: str = "PER_KG",
) -> list[SaleResult]:
    """
    Auto-deplete FIFO batches for the given branch+size based on kg_sold.
    Splits across batches if needed.

    Since a single sale may span multiple batches, we allocate the counted pieces
    across FIFO batches in a controlled way (never producing 0-piece rows).
    """
    if float(kg_sold) <= 0:
        raise ValueError("Kg sold must be > 0.")
    if int(pcs_counted) <= 0:
        raise ValueError("Counted pieces must be > 0.")

    tol = max(0, int(tolerance_pcs))
    price_basis = _normalize_price_basis(price_basis)

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

    # Guard: cannot allocate fewer counted pieces than number of batches used
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

        # Controlled allocation: ensure each batch row gets at least 1 piece
        if i < len(allocations) - 1:
            min_needed_for_rest = (len(allocations) - i - 1)
            raw = (float(take_kg) / kg_remaining_total) * pcs_remaining if kg_remaining_total > 0 else 1.0
            pcs_alloc = int(round(raw))

            # enforce bounds: >=1 and leave enough for remaining rows
            pcs_alloc = max(1, pcs_alloc)
            pcs_alloc = min(pcs_alloc, pcs_remaining - min_needed_for_rest)
        else:
            pcs_alloc = pcs_remaining  # last gets the remainder

        pcs_remaining -= int(pcs_alloc)
        kg_remaining_total -= float(take_kg)

        variance_flag = 1 if abs(int(pcs_alloc) - int(pcs_suggested)) > tol else 0
        total_price = _compute_total_price(unit_price, price_basis, float(take_kg), int(pcs_alloc))

        sale_id = x(
            conn,
            """
            INSERT INTO sales (
                sale_ts, branch_id, mode, customer, batch_id, size_id,
                pcs_sold, kg_sold,
                unit_price, price_basis, total_price,
                pcs_suggested, variance_flag
            ) VALUES (?, ?, 'WHOLESALE_KG', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                iso_now(),
                int(branch_id),
                _normalize_customer(customer),
                int(b["batch_id"]),
                int(size_id),
                int(pcs_alloc),
                float(take_kg),
                float(unit_price) if unit_price is not None else None,
                str(price_basis),
                float(total_price) if total_price is not None else None,
                int(pcs_suggested),
                int(variance_flag),
            ),
        )

        results.append(
            SaleResult(
                sale_id=int(sale_id),
                pcs_sold=int(pcs_alloc),
                kg_sold=float(take_kg),
                variance_flag=int(variance_flag),
                pcs_suggested=int(pcs_suggested),
            )
        )

    return results
