import logging
from typing import List
from decimal import Decimal
from app.prediction.metrics.metrics_repository import MetricsRepository
from app.prediction.metrics.metrics_models import HorizonMetric

logger = logging.getLogger(__name__)


class MetricsService:
    def __init__(self, metrics_repository: MetricsRepository):
        self._metrics_repository = metrics_repository

    async def get_horizon_metric_types(self) -> List[str]:
        """
        Get available horizon metric types.

        Returns:
            List[str]: List of horizon metric type names
        """
        logger.info("Fetching horizon metric types")

        try:
            metric_types = await self._metrics_repository.get_horizon_metric_types()
            logger.info(f"Retrieved {len(metric_types)} horizon metric types")
            return metric_types
        except Exception as e:
            logger.error(f"Error fetching horizon metric types: {e}")
            raise

    async def get_cycle_metric_types(self) -> List[str]:
        """
        Get available cycle metric types.

        Returns:
            List[str]: List of cycle metric type names
        """
        logger.info("Fetching cycle metric types")

        try:
            metric_types = await self._metrics_repository.get_cycle_metric_types()
            logger.info(f"Retrieved {len(metric_types)} cycle metric types")
            return metric_types
        except Exception as e:
            logger.error(f"Error fetching cycle metric types: {e}")
            raise

    async def get_horizon_metrics(self, model_id: int) -> List[HorizonMetric]:
        """
        Get horizon metrics for a specific model.

        Args:
            model_id: The model ID to fetch metrics for

        Returns:
            List[HorizonMetric]: List of horizon metrics
        """
        logger.info(f"Fetching horizon metrics for model {model_id}")

        try:
            rows = await self._metrics_repository.get_horizon_metrics(model_id)
            metrics = [
                HorizonMetric(
                    metric_type=row["metric_type"],
                    horizon=Decimal(str(row["horizon"])),
                    value=Decimal(str(row["value"])),
                )
                for row in rows
            ]

            logger.info(
                f"Retrieved {len(metrics)} horizon metrics for model {model_id}"
            )
            return metrics
        except Exception as e:
            logger.error(f"Error fetching horizon metrics for model {model_id}: {e}")
            raise
