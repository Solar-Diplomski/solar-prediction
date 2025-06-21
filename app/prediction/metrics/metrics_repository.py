import logging
from typing import List
from app.config.database import db_manager

logger = logging.getLogger(__name__)


class MetricsRepository:
    async def get_horizon_metric_types(self) -> List[str]:
        """
        Fetch available horizon metric types from the database enum.
        """
        query = """
            SELECT unnest(enum_range(NULL::horizon_metric_type))::text AS metric_type
            ORDER BY metric_type
        """

        try:
            rows = await db_manager.execute(query)
            return [row["metric_type"] for row in rows]
        except Exception as e:
            logger.error(f"Failed to fetch horizon metric types: {e}")
            raise
