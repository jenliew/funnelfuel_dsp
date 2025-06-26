import asyncio
import json
import re
import aiohttp
from aiolimiter import AsyncLimiter

from demand_link.demand_link.constant import RATE_LIMIT, URL_API_STR
from scripts.mock_dsp_api import AdGroup

# TODO: need to handle error condition and retries.


class Submission:
    def __init__(self) -> None:
        self.api_url = f"http://{URL_API_STR}"
        self.rate_limiter = AsyncLimiter(RATE_LIMIT, 60)

    async def _request_with_limiter(self, method, url, **kwargs):
        async with self.rate_limiter:
            async with method(url, **kwargs) as response:
                print(response.status)
                text = await response.text()
                print(f"-> raw_response from {url}: {text}")
                return json.loads(text)

    async def poll_status(self, session, entity_type, entity_id):
        url = f"{self.api_url}/{entity_type}/{entity_id}/status"
        while True:
            data = await self._request_with_limiter(session.get, url)
            status = data.get("status")

            if status == "success":
                print(f"{entity_type.capitalize()} {entity_id} succeeded.")
                return True
            elif status == "failed":
                print(
                    f"{entity_type.capitalize()} {entity_id} failed: {data.get('error')}"
                )
                return False

            await asyncio.sleep(2)

    async def post_entity(self, session, entity_type, data):
        url = f"{self.api_url}/{entity_type}"
        return await self._request_with_limiter(session.post, url, json=data)

    async def process_campaign_job(self, session, job):

        campaign_response = await self.post_entity(session, "campaigns", job.dict())
        campaign_id = campaign_response.get("campaign_id", None)

        if not campaign_id:
            print("Error: campaign_id missing")
            return

        if not await self.poll_status(session, "campaigns", campaign_id):
            return

        for group in job.ad_groups:
            # Overwrite campaign_id return by DSP API
            group.campaing_id = campaign_id
            await self._submit_ad_group(session, group)

    async def _submit_ad_group(self, session, group_data: AdGroup):

        group_response = await self.post_entity(session, "ad-groups", group_data.dict())
        group_id = group_response.get("ad_group_id", None)

        if not group_id:
            raise asyncio.InvalidStateError("Failed to create ad_group")

        if not await self.poll_status(session, "ad-groups", group_id):
            return

        for ad in group_data.ads:
            await self._submit_ad(session, ad)

    async def _submit_ad(self, session, ad_data):
        print(f"-> Uploading ad {ad_data.id}")
        ad_response = await self.post_entity(session, "ads", ad_data.dict())
        ad_id = ad_response.get("ad_id", None)

        if not ad_id or not await self.poll_status(session, "ads", ad_id):
            return

    async def submit_job(self, queue: asyncio.Queue):
        print("🚀 Starting DSP job submissions")
        async with aiohttp.ClientSession() as session:
            while True:
                job = await queue.get()
                try:
                    print("🛠️ Processing campaign job...")
                    await self.process_campaign_job(session, job)
                except Exception as e:
                    print(f"⚠️ Exception: {e}")
                finally:
                    print("✅ Job complete")
                    queue.task_done()
