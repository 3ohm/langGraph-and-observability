"""Invoices for paid orders."""

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, model_validator


class InvoiceLine(BaseModel):
    sku: str
    quantity: int
    unit_price: Decimal


class Invoice(BaseModel):
    order_id: str
    customer_id: str
    email: str
    lines: list[InvoiceLine]

    @model_validator(mode="before")
    @classmethod
    def _flatten_customer(cls, data: dict[str, Any]) -> dict[str, Any]:
        customer = data["customer"]
        return {**data, "customer_id": customer["id"], "email": customer["email"]}

    @property
    def total(self) -> Decimal:
        return sum((line.unit_price * line.quantity for line in self.lines), Decimal(0))


def build_invoice(order: dict[str, Any]) -> Invoice:
    """The invoice of a paid order, from its orders.paid event."""
    return Invoice.model_validate(order)
