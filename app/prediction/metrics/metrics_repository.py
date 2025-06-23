import logging
from typing import List, Tuple
from datetime import datetime
from app.config.database import db_manager

logger = logging.getLogger(__name__)


class MetricsRepository:

    async def get_horizon_metric_types(self) -> List[str]:
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
        self, model_id: int, start_date: datetime, end_date: datetime
    ) -> List[dict]:
        query = """
            SELECT time_of_forecast, metric_type::text, value
            FROM cycle_metrics
            WHERE model_id = $1 
            AND time_of_forecast >= $2 
            AND time_of_forecast <= $3
            ORDER BY time_of_forecast
        """

        try:
            rows = await db_manager.execute(query, model_id, start_date, end_date)
            return rows
        except Exception as e:
            logger.error(f"Failed to fetch cycle metrics for model {model_id}: {e}")
            raise

    async def save_horizon_metrics(
        self, metrics_data: List[Tuple[int, str, float, float]]
    ) -> None:
        if not metrics_data:
            return

        query = """
            INSERT INTO horizon_metrics (model_id, metric_type, horizon, value)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (model_id, metric_type, horizon) 
            DO UPDATE SET value = EXCLUDED.value
        """

        try:
            await db_manager.execute_many(query, metrics_data)
            logger.info(f"Successfully saved {len(metrics_data)} horizon metrics")
        except Exception as e:
            logger.error(f"Failed to save horizon metrics: {e}")
            raise

    async def get_predictions_and_readings_for_model(
        self, model_id: int, plant_id: int
    ) -> List[dict]:
        """
        Fetch predictions and corresponding actual readings for a specific model.

        Args:
            model_id: The model ID
            plant_id: The power plant ID

        Returns:
            List of dictionaries containing prediction and actual reading pairs
        """
        query = """
            SELECT 
                pp.prediction_time,
                pp.predicted_power,
                pp.horizon,
                pr.power_w as actual_power
            FROM power_predictions pp
            INNER JOIN power_readings pr ON pp.prediction_time = pr.timestamp 
                AND pr.plant_id = $2
            WHERE pp.model_id = $1
                AND pp.predicted_power IS NOT NULL
                AND pr.power_w IS NOT NULL
                AND pp.horizon IN (0.25, 1, 6, 24, 48, 72)
            ORDER BY pp.prediction_time
        """

        try:
            rows = await db_manager.execute(query, model_id, plant_id)
            return rows
        except Exception as e:
            logger.error(
                f"Failed to fetch predictions and readings for model {model_id}: {e}"
            )
            raise

    async def save_cycle_metrics(
        self, metrics_data: List[Tuple[datetime, int, str, float]]
    ) -> None:
        if not metrics_data:
            return

        query = """
            INSERT INTO cycle_metrics (time_of_forecast, model_id, metric_type, value)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (time_of_forecast, model_id, metric_type) 
            DO UPDATE SET value = EXCLUDED.value
        """

        try:
            await db_manager.execute_many(query, metrics_data)
            logger.info(f"Successfully saved {len(metrics_data)} cycle metrics")
        except Exception as e:
            logger.error(f"Failed to save cycle metrics: {e}")
            raise

    async def get_predictions_and_readings_by_cycle(
        self, model_id: int, plant_id: int
    ) -> List[dict]:
        """
        Fetch predictions and corresponding actual readings grouped by forecast cycle (created_at).

        Args:
            model_id: The model ID
            plant_id: The power plant ID

        Returns:
            List of dictionaries containing prediction and actual reading pairs with cycle info
        """
        query = """
            SELECT 
                pp.created_at as time_of_forecast,
                pp.prediction_time,
                pp.predicted_power,
                pr.power_w as actual_power
            FROM power_predictions pp
            INNER JOIN power_readings pr ON pp.prediction_time = pr.timestamp 
                AND pr.plant_id = $2
            WHERE pp.model_id = $1
                AND pp.predicted_power IS NOT NULL
                AND pr.power_w IS NOT NULL
            ORDER BY pp.created_at, pp.prediction_time
        """

        try:
            rows = await db_manager.execute(query, model_id, plant_id)
            return rows
        except Exception as e:
            logger.error(
                f"Failed to fetch predictions and readings by cycle for model {model_id}: {e}"
            )
            raise
