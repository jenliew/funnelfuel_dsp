import asyncio
import json
import logging
from typing import Dict

import aiohttp
from aiolimiter import AsyncLimiter

from demand_link.demand_link.constant import (
    API_REQUEST,
    DEFAULT_RATE_LIMIT,
    DEFAULT_URL_API_STR,
    HTTP_TIMEOUT,
)
from demand_link.demand_link.exception import SubmissionError

logger = logging.getLogger(__name__)


class Notifier:
    def __init__(
        self, endpoint=DEFAULT_URL_API_STR, rate_limit=DEFAULT_RATE_LIMIT
    ) -> None:
        self.api_url = f"http://{endpoint}"
        self.rate_limiter = AsyncLimiter(rate_limit, 60)

    async def request_with_limiter(
        self, method: API_REQUEST, url: str, **kwargs
    ):
        async with self.rate_limiter:
            async with aiohttp.ClientSession() as session:
                api_method = (
                    session.post if method == API_REQUEST.POST else session.get
                )
                async with api_method(
                    url, timeout=HTTP_TIMEOUT, **kwargs
                ) as response:
                    logging.debug(response.status)
                    text = await response.text()
                    logging.info(f"-> raw_response from {url}: {text}")
                    return json.loads(text)

    async def poll_status(self, entity_type: str, entity_id: str):
        try:
            url = f"{self.api_url}/{entity_type}/{entity_id}/status"
            while True:
                data = await self.request_with_limiter(API_REQUEST.GET, url)
                if data:
                    status = data.get("status", None)

                    entity_type_str = entity_type.capitalize()
                    if status == "success":
                        logging.info(
                            f"{entity_type_str} {entity_id} succeeded."
                        )
                        return True
                    elif status == "failed":
                        logging.info(
                            f"{entity_type_str} {entity_id} "
                            f" failed: {data.get('error')}"
                        )
                        return False

                await asyncio.sleep(2)
        except aiohttp.ClientConnectionError as e:
            raise SubmissionError(f"ClientConnection exception: {e}")

    async def post_entity(self, entity_type: str, data: Dict):
        try:
            url = f"{self.api_url}/{entity_type}"

            return await self.request_with_limiter(
                API_REQUEST.POST, url, json=data
            )
        except aiohttp.ClientConnectionError as e:
            raise SubmissionError(f"ClientConnection exception: {e}")
