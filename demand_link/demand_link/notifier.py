import asyncio
import json
from typing import Dict
import aiohttp
from aiolimiter import AsyncLimiter

from demand_link.demand_link.constant import API_REQUEST, RATE_LIMIT, URL_API_STR
from demand_link.demand_link.exception import SubmissionError


class Notifier:
    def __init__(self) -> None:
        self.api_url = f"http://{URL_API_STR}"
        self.rate_limiter = AsyncLimiter(RATE_LIMIT, 60)

    async def request_with_limiter(self, method: API_REQUEST, url: str, **kwargs):
        async with self.rate_limiter:
            async with aiohttp.ClientSession() as session:
                api_method = session.post if method == API_REQUEST.POST else session.get
                async with api_method(url, **kwargs) as response:
                    print(response.status)
                    text = await response.text()
                    print(f"-> raw_response from {url}: {text}")
                    return json.loads(text)

    async def poll_status(self, entity_type: str, entity_id: str):
        try:
            url = f"{self.api_url}/{entity_type}/{entity_id}/status"
            while True:
                data = await self.request_with_limiter(API_REQUEST.GET, url)
                if data:
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
        except aiohttp.ClientConnectionError as e:
            raise SubmissionError(f"ClientConnection exception: {e}")

    async def post_entity(self, entity_type: str, data: Dict):

        try:
            url = f"{self.api_url}/{entity_type}"

            return await self.request_with_limiter(API_REQUEST.POST, url, json=data)
        except aiohttp.ClientConnectionError as e:
            raise SubmissionError(f"ClientConnection exception: {e}")
