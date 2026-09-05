"""Read endpoints: per-user summary and paginated transaction history."""

from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException, Path, Query, status

from app.dependencies import RepositoryDep, SettingsDep
from app.schemas import TransactionOut, TransactionPage, UserSummary

router = APIRouter(prefix="/users", tags=["users"])

UserIdPath = Path(..., min_length=1, max_length=128, description="Producer's user id")


def _as_utc(value: datetime | None) -> datetime | None:
    """Treat a naive query bound as UTC so it compares against timestamptz."""
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=UTC)


@router.get(
    "/{user_id}/summary",
    response_model=UserSummary,
    summary="Total USD and transaction count for a user",
)
async def get_user_summary(repo: RepositoryDep, user_id: str = UserIdPath) -> UserSummary:
    """Aggregate every stored transaction of one user.

    An unknown user is not a 404: it is a user with zero transactions, and
    the caller wants `{total_usd: 0, transaction_count: 0}` rather than an
    error to special-case.
    """
    summary = await repo.get_summary(user_id)
    return UserSummary(
        user_id=user_id,
        total_usd=summary.total_usd,
        transaction_count=summary.transaction_count,
    )


@router.get(
    "/{user_id}/transactions",
    response_model=TransactionPage,
    summary="Paginated, optionally time-ranged list of a user's transactions",
)
async def list_user_transactions(
    repo: RepositoryDep,
    settings: SettingsDep,
    user_id: str = UserIdPath,
    date_from: datetime | None = Query(
        None, alias="from", description="Inclusive lower bound on `timestamp` (ISO 8601)"
    ),
    date_to: datetime | None = Query(
        None, alias="to", description="Inclusive upper bound on `timestamp` (ISO 8601)"
    ),
    limit: int | None = Query(None, ge=1, description="Page size"),
    offset: int = Query(0, ge=0, description="Rows to skip"),
) -> TransactionPage:
    """Return one page of transactions, newest first."""
    date_from = _as_utc(date_from)
    date_to = _as_utc(date_to)
    if date_from and date_to and date_from > date_to:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="`from` must not be later than `to`",
        )

    page_size = min(limit or settings.api_page_size, settings.api_max_page_size)

    total = await repo.count_transactions(user_id, date_from, date_to)
    rows = await repo.list_transactions(
        user_id, date_from, date_to, limit=page_size, offset=offset
    )

    return TransactionPage(
        items=[TransactionOut.model_validate(row) for row in rows],
        total=total,
        limit=page_size,
        offset=offset,
        has_more=offset + len(rows) < total,
    )
