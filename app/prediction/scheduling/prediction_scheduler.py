import logging
from typing import Dict, Any
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from app.prediction.prediction_service import PredictionService
from app.prediction.scheduling.scheduler_config import create_prediction_scheduler

logger = logging.getLogger(__name__)


class PredictionScheduler:
    def __init__(self, prediction_service: PredictionService):
        self.prediction_service = prediction_service
        self.scheduler: AsyncIOScheduler = create_prediction_scheduler()
        self._is_running = False

    async def start(self) -> None:
        if self._is_running:
            logger.warning("Prediction scheduler is already running")
            return

        try:
            self.scheduler.start()
            self._setup_prediction_jobs()
            self._is_running = True
            logger.info("Prediction scheduler started successfully")

        except Exception as e:
            logger.error(f"Failed to start prediction scheduler: {e}")
            raise

    async def stop(self) -> None:
        if not self._is_running:
            logger.debug("Prediction scheduler is not running")
            return

        try:
            logger.info("Stopping prediction scheduler...")
            # wait=True ensures running jobs complete before shutdown
            self.scheduler.shutdown(wait=True)
            self._is_running = False
            logger.info("Prediction scheduler stopped successfully")
        except Exception as e:
            logger.error(f"Error stopping prediction scheduler: {e}")
            raise

    def _setup_prediction_jobs(self) -> None:
        self.scheduler.add_job(
            func=self._execute_predictions,
            trigger="cron",
            hour="0,6,12,18",  # Run at 00:00, 06:00, 12:00, 18:00
            minute=0,
            id="prediction_generation",
            replace_existing=True,
            name="Generate Solar Power Predictions",
        )
        logger.info(
            "Prediction generation job scheduled for 00:00, 06:00, 12:00, 18:00"
        )

    async def _execute_predictions(self) -> None:
        job_id = "prediction_generation"
        try:
            logger.info(f"Starting scheduled task: {job_id}")
            self.prediction_service.predict()
            logger.info(f"Scheduled task completed successfully: {job_id}")

        except Exception as e:
            logger.error(f"Scheduled task failed: {job_id} - {e}")

    def get_status(self) -> Dict[str, Any]:
        if not self._is_running:
            return {"running": False, "jobs": [], "message": "Scheduler is not running"}

        jobs = []
        for job in self.scheduler.get_jobs():
            job_info = {
                "id": job.id,
                "name": job.name,
                "next_run": (
                    job.next_run_time.isoformat() if job.next_run_time else None
                ),
                "trigger": str(job.trigger),
                "pending": job.pending,
            }
            jobs.append(job_info)

        return {"running": self._is_running, "jobs": jobs, "total_jobs": len(jobs)}
