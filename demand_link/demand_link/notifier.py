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
    MAX_RETRIES_POLL,
)
from demand_link.demand_link.exception import SubmissionError

logger = logging.getLogger(__name__)


class Notifier:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        endpoint: str = DEFAULT_URL_API_STR,
        rate_limit: int = DEFAULT_RATE_LIMIT,
    ) -> None:
        self.api_url = f"http://{endpoint}"
        self.rate_limiter = AsyncLimiter(rate_limit, 60)

        self._http_session = session
        if not self._http_session:
            logger.warning("ClientSession (self._http_session) is null.")

        self.auth_dsp = None  # This is to authaiohttp.BasicAuth (if need to)

        self.api_header = None  # This is for API key.

    async def request_with_limiter(
        self, method: API_REQUEST, url: str, **kwargs
    ):
        async with self.rate_limiter:
            try:
                # async with self._http_session as session:
                api_method = (
                    self._http_session.post
                    if method == API_REQUEST.POST
                    else self._http_session.get
                )
                async with api_method(
                    url,
                    headers=None if not self.api_header else self.api_header,
                    auth=(
                        self.auth_dsp if not self.auth_dsp else self.auth_dsp
                    ),
                    **kwargs,
                ) as response:
                    logger.debug(f"Response received:{response.status}")
                    text = await response.text()
                    logger.info(f"Raw response from {url}: {text}")

                    # TODO:
                    # Can be check the return code, if the endpoint require
                    # API key/authentication. Possible need to retries
                    # and re-request to the endpoint.
                    return json.loads(text)

            except aiohttp.ClientResponseError as e:
                logger.error(f"ClientResponse Error: {e}")
                raise SubmissionError(f"ClientRespons Error:{e}")
            except aiohttp.ClientConnectionError as e:
                logger.error(f" Connection error: {e}")
                raise SubmissionError(f"ClientConnection error: {e}")

            except asyncio.TimeoutError:
                logger.error(f"Timeout reached for request to {url}")
                raise SubmissionError(f"Request to {url} timed out")

            except json.JSONDecodeError:
                logger.error(f"Failed to parse JSON from {url}")
                raise SubmissionError(f"Invalid JSON response from {url}")

            except Exception as e:
                logger.exception("Unexpected error during request")
                raise SubmissionError(f"Unexpected error: {e}")

    async def poll_status(self, entity_type: str, entity_id: str):
        retries = 0
        url = f"{self.api_url}/{entity_type}/{entity_id}/status"

        while retries <= MAX_RETRIES_POLL:
            try:
                data = await self.request_with_limiter(API_REQUEST.GET, url)
                if data:
                    status = data.get("status", None)

                    entity_type_str = entity_type.capitalize()
                    if status == "success":
                        logger.info(
                            f"Poll status - {entity_type_str} {entity_id} "
                            f"succeeded."
                        )
                        return True
                    elif status == "failed":
                        logger.info(
                            f"Poll status - {entity_type_str} {entity_id} "
                            f" failed: {data.get('error')}"
                        )
                        return False
                retries += 1
                await asyncio.sleep(2)

            except aiohttp.ServerTimeoutError as e:
                raise SubmissionError(f"DSP Server connection timeout:{e}")
            except aiohttp.ClientConnectionError as e:
                raise SubmissionError(f"ClientConnection exception: {e}")

    async def post_entity(self, entity_type: str, data: Dict):
        try:
            url = f"{self.api_url}/{entity_type}"

            # At this moment, it will pass in a standard Dict json
            # format to make the request.
            # This is where possible package the format of the data
            # according to the endpoint API.
            return await self.request_with_limiter(
                API_REQUEST.POST, url, json=data
            )
        except aiohttp.ClientConnectionError as e:
            raise SubmissionError(f"ClientConnection exception: {e}")
