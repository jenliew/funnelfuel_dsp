import asyncio
import logging

from scripts.mock_dsp_api import AdGroup

from demand_link.demand_link.constant import (
    DEFAULT_RATE_LIMIT,
    DEFAULT_URL_API_STR,
    MAX_RETRIES,
)
from demand_link.demand_link.exception import SubmissionError
from demand_link.demand_link.notifier import Notifier

logger = logging.getLogger(__name__)


class Submission:
    def __init__(
        self,
        session,
        dsp_endpoint=DEFAULT_URL_API_STR,
        rate_limit=DEFAULT_RATE_LIMIT,
    ) -> None:
        self.notifier = Notifier(session, dsp_endpoint, rate_limit)

    async def process_campaign_job(self, job):
        campaign_response = await self.notifier.post_entity(
            "campaigns", job.dict()
        )
        campaign_id = campaign_response.get("campaign_id", None)

        if not campaign_id:
            raise SubmissionError("Fail get campaign_id")

        if not await self.notifier.poll_status("campaigns", campaign_id):
            raise SubmissionError("Fail poll status from DSP API")

        for group in job.ad_groups:
            # Overwrite campaign_id return by DSP API
            group.campaign_id = campaign_id
            await self._submit_ad_group(group)

    async def _submit_ad_group(self, group_data: AdGroup):
        ad_group_response = await self.notifier.post_entity(
            "ad-groups", group_data.dict()
        )
        ad_group_id_str = ad_group_response.get("ad_group_id", None)

        if not ad_group_id_str:
            raise SubmissionError("Failed to create ad_group")

        if not await self.notifier.poll_status("ad-groups", ad_group_id_str):
            raise SubmissionError("Fail pool status from DSP API")

        for ad in group_data.ads:
            await self._submit_ad(ad)

    async def _submit_ad(self, ad_data):
        logger.info(f"-> Uploading ad {ad_data.id} to DSP endpoint now.")
        ad_response = await self.notifier.post_entity("ads", ad_data.dict())
        ad_id = ad_response.get("ad_id", None)

        if not ad_id:
            raise SubmissionError("Failed to create/update ad.")

        if not await self.notifier.poll_status("ads", ad_id):
            raise SubmissionError("Fail pool status from DSP API")

    async def submit_job(self, queue: asyncio.Queue):
        logger.info("Starting pick up jobs from queue.")

        while True:
            job = await queue.get()

            try:
                logger.info("Processing campaign job...")
                await self.process_campaign_job(job)
            except SubmissionError as e:
                logger.error(f"Exception: {e}")
                if job.retries >= MAX_RETRIES:
                    logger.error(
                        f"Dropping job {job.id} after {MAX_RETRIES} retries"
                    )

                    await self.complete_task(False, job.id)

                    queue.task_done()  # mark it as done — we give up

                else:
                    job.retries += 1
                    logger.info(
                        f"🔁 Attempting retry {job.retries} for job {job.id}"
                    )
                    await asyncio.sleep(2**job.retries)

                    await queue.put(job)
                    queue.task_done()  # mark current attempt as complete
            else:
                logger.info(
                    f"Submission Job {job.id} completed successfully "
                    f"after {job.retries} retries"
                )
                await self.complete_task(True, job.id)
                queue.task_done()

    async def complete_task(self, is_success, job_id):

        logger.info(
            f"The submission job {job_id} is completed. is_success: "
            f"{is_success}."
        )
        # THis function can be extend to upload the auditlog or any
        # update the status..
