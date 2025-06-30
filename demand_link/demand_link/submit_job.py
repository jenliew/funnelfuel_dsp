import asyncio
import logging

import aiohttp

from demand_link.demand_link.constant import (
    DEFAULT_RATE_LIMIT,
    DEFAULT_URL_API_STR,
    MAX_RETRIES,
)
from demand_link.demand_link.data_model import AdGroup
from demand_link.demand_link.exception import SubmissionError
from demand_link.demand_link.notifier import Notifier

logger = logging.getLogger(__name__)


class Submission:
    def __init__(
        self,
        dsp_endpoint=DEFAULT_URL_API_STR,
        rate_limit=DEFAULT_RATE_LIMIT,
    ) -> None:
        self.session = None
        self.notifier = None
        self.endpoint = dsp_endpoint
        self.rate_limit = rate_limit

    async def setup_session(self):
        if not self.session:
            logger.info("Setting up session for aiohttp.clientsession now.")
            self.session = aiohttp.ClientSession()
            self.notifier = Notifier(
                self.session, self.endpoint, self.rate_limit
            )
        else:
            logger.info("Re-using existing session.")

    async def process_campaign_job(self, job):

        if not self.notifier:
            raise SubmissionError("Notifier object is null.")

        campaign_response = await self.notifier.post_entity(
            "campaigns", job.dict()
        )
        # Expecting the DSP endpoint will return the campaign_id
        # as successful request.
        campaign_id = campaign_response.get("campaign_id", None)

        if not campaign_id:
            raise SubmissionError("Fail get campaign_id")

        if not await self.notifier.poll_status("campaigns", campaign_id):
            raise SubmissionError("Fail poll status from DSP API")

        # Proceed next step, make the Ad_Groups request to the DSP
        # endpoint.
        for group in job.ad_groups:
            # Overwrite campaign_id return by DSP API
            group.campaign_id = campaign_id
            await self._submit_ad_group(group)

    async def _submit_ad_group(self, group_data: AdGroup):

        if not self.notifier:
            raise SubmissionError("Notifier object is null.")

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

        if not self.notifier:
            raise SubmissionError("Notifier object is null.")

        ad_response = await self.notifier.post_entity("ads", ad_data.dict())
        ad_id = ad_response.get("ad_id", None)

        if not ad_id:
            raise SubmissionError("Failed to create/update ad.")

        if not await self.notifier.poll_status("ads", ad_id):
            raise SubmissionError("Fail pool status from DSP API")

    async def submit_job(self, queue: asyncio.Queue):
        logger.info("Starting job submission worker.")
        await self.setup_session()

        while True:
            job = await queue.get()

            if job == "STOP":
                logger.info(f"Queue is empty. Breaking the loop. job:{job}")
                queue.task_done()
                break

            try:
                logger.info(f"Processing campaign job: {job}")
                await self.process_campaign_job(job)
            except SubmissionError as e:
                logger.error(f"Submission failed: {e}")
                await self._handle_failed_job(job, queue)
            else:
                logger.info(
                    f"Job {job.id} completed after {job.retries} retries."
                )
                queue.task_done()
                await self.complete_task(True, job.id)

        logger.info("Queue worker is completed. closing down gracefully.")
        if self.session and not self.session.closed:
            await self.session.close()
            del self.session
            del self.notifier
            self.session = None
            self.notifier = None
            logger.debug("As par of clean up, close off the session. ")

    async def _handle_failed_job(self, job, queue: asyncio.Queue):

        if job.retries >= MAX_RETRIES:
            logger.error(f"Dropping job {job.id} after {MAX_RETRIES} retries.")
            await self.complete_task(False, job.id)
        else:
            job.retries += 1
            logger.info(f"Retrying job {job.id}, attempt {job.retries}.")
            await asyncio.sleep(2**job.retries)
            await queue.put(job)
        queue.task_done()

    async def complete_task(self, is_success, job_id):

        logger.info(
            f"The submission job {job_id} is completed. is_success: "
            f"{is_success}."
        )

        # THis function can be extend to upload the auditlog or any
        # update the status..
