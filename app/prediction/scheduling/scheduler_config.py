from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.asyncio import AsyncIOExecutor
import logging

logger = logging.getLogger(__name__)


def create_prediction_scheduler() -> AsyncIOScheduler:
    jobstores = {"default": MemoryJobStore()}

    executors = {"default": AsyncIOExecutor()}

    job_defaults = {
        "coalesce": False,  # Don't combine multiple missed runs
        "max_instances": 1,  # Only one instance of each job at a time
        "misfire_grace_time": 60,  # Allow 1 minute grace period for missed jobs
    }

    scheduler = AsyncIOScheduler(
        jobstores=jobstores, executors=executors, job_defaults=job_defaults
    )

    logger.info("Prediction scheduler configured with memory job store")
    return scheduler
