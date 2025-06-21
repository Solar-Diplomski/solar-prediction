import logging
from typing import List
from app.prediction.metrics.metrics_repository import MetricsRepository

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
