import asyncio
import logging
import threading
from typing import List

from demand_link.demand_link.submit_job import Submission
from demand_link.demand_link.utils import split_jobs

logger = logging.getLogger(__name__)


class WorkerManager:
    def __init__(
        self, jobs: List, endpoint: str, rate_limit: int, worker_num: int
    ):
        self.jobs = jobs
        self.endpoint = endpoint
        self.rate_limit = rate_limit
        self.worker_num = worker_num

    async def start(self):
        job_slices = split_jobs(self.jobs, self.worker_num)

        threads = []

        for i, job_batch in enumerate(job_slices):
            thread = threading.Thread(
                target=self._run_worker_thread,
                args=(job_batch, i),
                name=f"SubmissionThread-{i}",
            )
            thread.start()
            threads.append(thread)

        for t in threads:
            t.join()

        logger.info(
            "Completed running all submission job. "
            "All worker thread are completed."
        )

    def _run_worker_thread(self, jobs_list, thread_id):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        logger.info(f"-> Worker {thread_id} starting jobs: {jobs_list}")
        try:
            loop.run_until_complete(
                self._submission_worker(jobs_list, thread_id)
            )
        except Exception as e:
            logger.error(f"[Thread-{thread_id}] Worker crashed: {e}")
        finally:
            loop.close()
            logger.info(f"[Thread-{thread_id}] Worker stopped.")

    async def _submission_worker(self, jobs_list, thread_id):
        queue = asyncio.Queue()

        # Use sentinel to signal shutdown
        jobs_list.append("STOP")
        for job in jobs_list:
            await queue.put(job)

        submission = Submission(
            dsp_endpoint=self.endpoint, rate_limit=self.rate_limit
        )

        logger.info(
            f"[Thread-{thread_id}] Starting async submission worker..."
        )
        try:
            await submission.submit_job(queue)
            await queue.join()
        finally:
            logger.info(f"[Thread-{thread_id}] Worker completed cleanup.")
