import asyncio
import json
import aiohttp
from aiolimiter import AsyncLimiter

from demand_link.demand_link.constant import API_REQUEST, RATE_LIMIT, URL_API_STR
from demand_link.demand_link.exception import SubmissionError
from scripts.mock_dsp_api import AdGroup

# TODO: need to handle error condition and retries.


class Submission:
    def __init__(self) -> None:
        self.api_url = f"http://{URL_API_STR}"
        self.rate_limiter = AsyncLimiter(RATE_LIMIT, 60)

    async def _request_with_limiter(self, method, url, **kwargs):
        async with self.rate_limiter:
            async with aiohttp.ClientSession() as session:
                api_method = session.post if method == API_REQUEST.POST else session.get
                async with api_method(url, **kwargs) as response:
                    print(response.status)
                    text = await response.text()
                    print(f"-> raw_response from {url}: {text}")
                    return json.loads(text)

    async def poll_status(self, entity_type, entity_id):
        url = f"{self.api_url}/{entity_type}/{entity_id}/status"
        while True:
            data = await self._request_with_limiter(API_REQUEST.GET, url)
            status = data.get("status", None)

            if status == "success":
                print(f"{entity_type.capitalize()} {entity_id} succeeded.")
                return True
            elif status == "failed":
                print(
                    f"{entity_type.capitalize()} {entity_id} failed: {data.get('error')}"
                )
                return False

            await asyncio.sleep(2)

    async def post_entity(self, entity_type, data):
        url = f"{self.api_url}/{entity_type}"

        return await self._request_with_limiter(API_REQUEST.POST, url, json=data)

    async def process_campaign_job(self, job):

        campaign_response = await self.post_entity("campaigns", job.dict())
        campaign_id = campaign_response.get("campaign_id", None)

        if not campaign_id:
            raise SubmissionError("Fail get campaign_id")

        if not await self.poll_status("campaigns", campaign_id):
            print("failed poll status for campaign job")
            raise SubmissionError("Fail pool status from DSP API")
            # TODO: need to handle this scenario
            return

        for group in job.ad_groups:
            # Overwrite campaign_id return by DSP API
            group.campaing_id = campaign_id
            await self._submit_ad_group(group)

    async def _submit_ad_group(self, group_data: AdGroup):

        group_response = await self.post_entity("ad-groups", group_data.dict())
        group_id = group_response.get("ad_group_id", None)

        if not group_id:
            raise SubmissionError("Failed to create ad_group")

        if not await self.poll_status("ad-groups", group_id):
            raise SubmissionError("Fail pool status from DSP API")

        for ad in group_data.ads:
            await self._submit_ad(ad)

    async def _submit_ad(self, ad_data):
        print(f"-> Uploading ad {ad_data.id}")
        ad_response = await self.post_entity("ads", ad_data.dict())
        ad_id = ad_response.get("ad_id", None)

        if not ad_id:
            raise SubmissionError("Failed to create/update ad.")

        if not await self.poll_status("ads", ad_id):
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
                raise SubmissionError(f"Failed process job error:{e}")
            finally:
                print("✅ Job complete")
                queue.task_done()
