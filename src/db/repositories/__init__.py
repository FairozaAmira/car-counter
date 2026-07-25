"""Database repository exports."""

from src.db.repositories.traffic_analysis import (
    AnalysisResultRepository,
    SqlAlchemyAnalysisResultRepository,
)

__all__ = ["AnalysisResultRepository", "SqlAlchemyAnalysisResultRepository"]
