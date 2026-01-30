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


def _get_batch_avg(conn, batch_id: int) -> float:
    b = q(conn, "SELECT batch_avg_kg_per_piece FROM batches WHERE id=?", (batch_id,))
    if not b:
        raise ValueError("Batch not found.")
    return float(b[0]["batch_avg_kg_per_piece"])


def _normalize_price_basis(price_basis: Optional[str]) -> str:
    if not price_basis:
        return "PER_KG"
    pb = str(price_basis).strip().upper()
    if pb in {"PER_KG", "PER_PIECE"}:
        return pb
    raise ValueError("Invalid price_basis. Use 'PER_KG' or 'PER_PIECE'.")


def _compute_total_price(unit_price: Optional[float], price_basis: str, kg_sold: float, pcs_sold: int) -> Optional[float]:
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
    # default PER_KG
    return round(float(kg_sold) * up, 2)


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
    if pcs_sold <= 0:
        raise ValueError("Pieces sold must be > 0.")

    price_basis = _normalize_price_basis(price_basis)

    avg = _get_batch_avg(conn, batch_id)
    kg_sold = float(pcs_sold) * avg

    # If UI already computed total_price, trust it; otherwise compute consistently.
    if total_price is None:
        total_price = _compute_total_price(unit_price, price_basis, kg_sold, int(pcs_sold))
    else:
        total_price = float(total_price)

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
            customer,
            int(batch_id),
            size_id,
            int(pcs_sold),
            float(kg_sold),
            float(unit_price) if unit_price is not None else None,
            str(price_basis),
            float(total_price) if total_price is not None else None,
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
    if kg_sold <= 0:
        raise ValueError("Kg sold must be > 0.")
    if pcs_counted <= 0:
        raise ValueError("Counted pieces must be > 0.")

    price_basis = _normalize_price_basis(price_basis)

    avg = _get_batch_avg(conn, batch_id)
    pcs_suggested = int(round(safe_div(float(kg_sold), avg)))
    variance_flag = 1 if abs(int(pcs_counted) - int(pcs_suggested)) > int(tolerance_pcs) else 0

    # If UI already computed total_price, trust it; otherwise compute consistently.
    if total_price is None:
        total_price = _compute_total_price(unit_price, price_basis, float(kg_sold), int(pcs_counted))
    else:
        total_price = float(total_price)

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
            customer,
            int(batch_id),
            size_id,
            int(pcs_counted),
            float(kg_sold),
            float(unit_price) if unit_price is not None else None,
            str(price_basis),
            float(total_price) if total_price is not None else None,
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
