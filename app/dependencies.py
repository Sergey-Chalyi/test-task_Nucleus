"""FastAPI dependency providers."""

from typing import Annotated

from fastapi import Depends
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings, get_settings
from app.db import get_session
from app.queue import EventQueue
from app.redis_client import get_redis
from app.repository import TransactionRepository


def get_settings_dep() -> Settings:
    return get_settings()


def get_redis_dep() -> Redis:
    return get_redis()


def get_queue(
    redis: Annotated[Redis, Depends(get_redis_dep)],
    settings: Annotated[Settings, Depends(get_settings_dep)],
) -> EventQueue:
    return EventQueue(redis, settings)


def get_repository(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> TransactionRepository:
    return TransactionRepository(session)


SettingsDep = Annotated[Settings, Depends(get_settings_dep)]
RedisDep = Annotated[Redis, Depends(get_redis_dep)]
QueueDep = Annotated[EventQueue, Depends(get_queue)]
RepositoryDep = Annotated[TransactionRepository, Depends(get_repository)]
