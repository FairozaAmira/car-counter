"""Create traffic analysis results.

Revision ID: 20260726_0001
Revises:
Create Date: 26-07-2026
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260726_0001"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the table used to persist complete POST API results."""
    op.create_table(
        "traffic_analysis_results",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("analysis_kind", sa.String(length=16), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("time_taken", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column(
            "response_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.CheckConstraint(
            "analysis_kind IN ('single', 'batch')",
            name="ck_traffic_analysis_results_kind",
        ),
        sa.CheckConstraint(
            "time_taken >= 0",
            name="ck_traffic_analysis_results_time_taken",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_traffic_analysis_results_kind_created_at",
        "traffic_analysis_results",
        ["analysis_kind", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    """Drop the traffic analysis result table and its indexes."""
    op.drop_index(
        "ix_traffic_analysis_results_kind_created_at",
        table_name="traffic_analysis_results",
    )
    op.drop_table("traffic_analysis_results")
