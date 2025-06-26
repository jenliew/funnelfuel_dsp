import asyncio

from demand_link.demand_link.constant import (
    MAX_RETRIES,
)
from demand_link.demand_link.exception import SubmissionError
from demand_link.demand_link.notifier import Notifier
from scripts.mock_dsp_api import AdGroup

# TODO: need to handle error condition and retries.


class Submission:
    def __init__(self) -> None:
        self.notifier = Notifier()

    async def process_campaign_job(self, job):
        campaign_response = await self.notifier.post_entity("campaigns", job.dict())
        campaign_id = campaign_response.get("campaign_id", None)

        if not campaign_id:
            raise SubmissionError("Fail get campaign_id")

        if not await self.notifier.poll_status("campaigns", campaign_id):
            raise SubmissionError("Fail pool status from DSP API")
            # TODO: need to handle this scenario

        for group in job.ad_groups:
            # Overwrite campaign_id return by DSP API
            group.campaing_id = campaign_id
            await self._submit_ad_group(group)

    async def _submit_ad_group(self, group_data: AdGroup):

        group_response = await self.notifier.post_entity("ad-groups", group_data.dict())
        group_id = group_response.get("ad_group_id", None)

        if not group_id:
            raise SubmissionError("Failed to create ad_group")

        if not await self.notifier.poll_status("ad-groups", group_id):
            raise SubmissionError("Fail pool status from DSP API")

        for ad in group_data.ads:
            await self._submit_ad(ad)

    async def _submit_ad(self, ad_data):
        print(f"-> Uploading ad {ad_data.id}")
        ad_response = await self.notifier.post_entity("ads", ad_data.dict())
        ad_id = ad_response.get("ad_id", None)

        if not ad_id:
            raise SubmissionError("Failed to create/update ad.")

        if not await self.notifier.poll_status("ads", ad_id):
            return SubmissionError("Fail pool status from DSP API")

    async def submit_job(self, queue: asyncio.Queue):
        print("🚀 Starting DSP job submissions")

        while True:
            job = await queue.get()
            try:
                print("🛠️ Processing campaign job...")
                await self.process_campaign_job(job)
            except SubmissionError as e:
                print(f"⚠️ Exception: {e}")

                if job.retries == MAX_RETRIES:
                    raise SubmissionError(f"Failed process job error:{e}")
                else:
                    job.retries += 1
                    await asyncio.sleep(1)  # small backoff before retry

                    await queue.put(job)
                    print("Attempting retries {retries}".format(retries=job.retries))

            finally:
                print("✅ Job complete")
                queue.task_done()
