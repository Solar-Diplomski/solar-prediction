import logging
from typing import List
from datetime import datetime
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

    async def get_cycle_metric_types(self) -> List[str]:
        """
        Fetch available cycle metric types from the database enum.
        """
        query = """
            SELECT unnest(enum_range(NULL::cycle_metric_type))::text AS metric_type
            ORDER BY metric_type
        """

        try:
            rows = await db_manager.execute(query)
            return [row["metric_type"] for row in rows]
        except Exception as e:
            logger.error(f"Failed to fetch cycle metric types: {e}")
            raise

    async def get_horizon_metrics(self, model_id: int) -> List[dict]:
        """
        Fetch horizon metrics for a specific model.

        Args:
            model_id: The model ID to fetch metrics for

        Returns:
            List of dictionaries containing metric_type, horizon, and value
        """
        query = """
            SELECT metric_type::text, horizon, value
            FROM horizon_metrics
            WHERE model_id = $1
        """

        try:
            rows = await db_manager.execute(query, model_id)
            return rows
        except Exception as e:
            logger.error(f"Failed to fetch horizon metrics for model {model_id}: {e}")
            raise

    async def get_cycle_metrics(
        self, model_id: int, start_time: datetime, end_time: datetime
    ) -> List[dict]:
        """
        Fetch cycle metrics for a specific model within a time range.

        Args:
            model_id: The model ID to fetch metrics for
            start_time: Start time filter
            end_time: End time filter

        Returns:
            List of dictionaries containing time_of_forecast, metric_type, and value
        """
        query = """
            SELECT time_of_forecast, metric_type::text, value
            FROM cycle_metrics
            WHERE model_id = $1 
            AND time_of_forecast >= $2 
            AND time_of_forecast <= $3
            ORDER BY time_of_forecast
        """

        try:
            rows = await db_manager.execute(query, model_id, start_time, end_time)
            return rows
        except Exception as e:
            logger.error(f"Failed to fetch cycle metrics for model {model_id}: {e}")
            raise
