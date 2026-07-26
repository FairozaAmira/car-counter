"""Use camelCase response-aligned analysis columns.

Revision ID: 20260726_0002
Revises: 20260726_0001
Create Date: 26-07-2026
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260726_0002"
down_revision: str | None = "20260726_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Rename metadata and unnest the persisted response payload."""
    op.alter_column("traffic_analysis_results", "analysis_kind", new_column_name="analysisKind")
    op.alter_column("traffic_analysis_results", "created_at", new_column_name="createdAt")
    op.alter_column("traffic_analysis_results", "time_taken", new_column_name="timeTaken")
    op.alter_column(
        "traffic_analysis_results",
        "response_payload",
        new_column_name="responsePayload",
    )
    op.add_column(
        "traffic_analysis_results",
        sa.Column("totalCars", sa.Integer(), nullable=True),
    )
    op.add_column(
        "traffic_analysis_results",
        sa.Column("dailyTotals", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "traffic_analysis_results",
        sa.Column("topHalfHours", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "traffic_analysis_results",
        sa.Column("leastCarsPeriodStart", sa.Text(), nullable=True),
    )
    op.add_column(
        "traffic_analysis_results",
        sa.Column("leastCarsPeriodEnd", sa.Text(), nullable=True),
    )
    op.add_column(
        "traffic_analysis_results",
        sa.Column("leastCarsPeriodTotalCars", sa.Integer(), nullable=True),
    )
    op.add_column(
        "traffic_analysis_results",
        sa.Column(
            "leastCarsPeriodRecords",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.add_column(
        "traffic_analysis_results",
        sa.Column("items", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.execute(
        sa.text(
            """
            UPDATE traffic_analysis_results
            SET
                "totalCars" = ("responsePayload" ->> 'totalCars')::integer,
                "dailyTotals" = "responsePayload" -> 'dailyTotals',
                "topHalfHours" = "responsePayload" -> 'topHalfHours',
                "leastCarsPeriodStart" = "responsePayload" -> 'leastCarsPeriod' ->> 'start',
                "leastCarsPeriodEnd" = "responsePayload" -> 'leastCarsPeriod' ->> 'end',
                "leastCarsPeriodTotalCars" =
                    ("responsePayload" -> 'leastCarsPeriod' ->> 'totalCars')::integer,
                "leastCarsPeriodRecords" = "responsePayload" -> 'leastCarsPeriod' -> 'records',
                "items" = "responsePayload" -> 'items'
            """
        )
    )
    op.drop_column("traffic_analysis_results", "responsePayload")


def downgrade() -> None:
    """Rebuild the legacy nested response payload and snake_case columns."""
    op.add_column(
        "traffic_analysis_results",
        sa.Column(
            "responsePayload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.execute(
        sa.text(
            """
            UPDATE traffic_analysis_results
            SET "responsePayload" =
                CASE
                    WHEN "analysisKind" = 'batch' THEN
                        jsonb_build_object(
                            'id', id,
                            'createdAt', to_char("createdAt", 'DD-MM-YYYY'),
                            'timeTaken', "timeTaken",
                            'items', items
                        )
                    ELSE
                        jsonb_build_object(
                            'id', id,
                            'createdAt', to_char("createdAt", 'DD-MM-YYYY'),
                            'timeTaken', "timeTaken",
                            'totalCars', "totalCars",
                            'dailyTotals', "dailyTotals",
                            'topHalfHours', "topHalfHours",
                            'leastCarsPeriod', jsonb_build_object(
                                'start', "leastCarsPeriodStart",
                                'end', "leastCarsPeriodEnd",
                                'totalCars', "leastCarsPeriodTotalCars",
                                'records', "leastCarsPeriodRecords"
                            )
                        )
                END
            """
        )
    )
    op.alter_column("traffic_analysis_results", "responsePayload", nullable=False)
    for columnName in (
        "items",
        "leastCarsPeriodRecords",
        "leastCarsPeriodTotalCars",
        "leastCarsPeriodEnd",
        "leastCarsPeriodStart",
        "topHalfHours",
        "dailyTotals",
        "totalCars",
    ):
        op.drop_column("traffic_analysis_results", columnName)
    op.alter_column(
        "traffic_analysis_results",
        "responsePayload",
        new_column_name="response_payload",
    )
    op.alter_column("traffic_analysis_results", "timeTaken", new_column_name="time_taken")
    op.alter_column("traffic_analysis_results", "createdAt", new_column_name="created_at")
    op.alter_column("traffic_analysis_results", "analysisKind", new_column_name="analysis_kind")
