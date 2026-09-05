"""Money arithmetic. Pure functions, no I/O - the piece worth pinning down."""

from decimal import ROUND_HALF_UP, Decimal

# Sub-cent precision: rounding every transaction to whole cents makes the
# per-user total drift once you sum millions of rows, so amounts are stored
# with four decimal places and only formatted to cents for display.
USD_QUANTUM = Decimal("0.0001")


def quantize_usd(value: Decimal) -> Decimal:
    """Round a USD amount to the storage precision, half away from zero."""
    return value.quantize(USD_QUANTUM, rounding=ROUND_HALF_UP)


def convert_to_usd(amount: Decimal, rate: Decimal) -> Decimal:
    """Convert `amount` to USD using `rate` (USD per 1 unit of currency).

    Rounding is HALF_UP because that is what finance people expect when they
    check a total by hand; Python's default (HALF_EVEN) would surprise them.
    Both arguments must be `Decimal`: accepting a float here is how rounding
    bugs get into money.
    """
    if not isinstance(amount, Decimal) or not isinstance(rate, Decimal):
        raise TypeError("amount and rate must be Decimal to avoid float error")
    if not amount.is_finite() or not rate.is_finite():
        raise ValueError("amount and rate must be finite")
    if rate <= 0:
        raise ValueError("rate must be positive")
    return quantize_usd(amount * rate)
