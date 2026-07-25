"""Shared SQLAlchemy declarative base."""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Provide shared SQLAlchemy model metadata.

    Args:
        None.

    Returns:
        A declarative base class.

    Raises:
        None.
    """
