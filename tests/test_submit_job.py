import asyncio
import logging
from unittest.mock import AsyncMock

import pytest
from pytest import MonkeyPatch

from demand_link.demand_link.submit_job import Submission

logger = logging.getLogger(__name__)


class MockAd:
    def __init__(self, id="ad_001", type="banner"):
        self.id = id
        self.type = type
        self.creative_url = "http://creative"
        self.click_url = "http://click"
        self.status = "pending"

    def dict(self):
        return self.__dict__


class MockAdGroup:
    def __init__(self, id="ag_001"):
        self.id = id
        self.name = "Ad Group Test"
        self.bid = 2.5
        self.targeting = {
            "ages": "18-24;25-34",
            "interests": "fashion;shoes",
            "geo": "UK;IE",
        }
        self.ads = [MockAd()]

    def dict(self):
        return self.__dict__


class MockCampaignJob:
    def __init__(self):
        self.id = "cmp_001"
        self.name = "Campaign Test"
        self.budget = 15000
        self.start_date = "2025-07-01"
        self.end_date = "2025-08-15"
        self.objective = "engagement"
        self.ad_groups = [MockAdGroup()]
        self.retries = 0

    def dict(self):
        return self.__dict__


@pytest.mark.asyncio
async def test_successful_job(
    submission: Submission, monkeypatch: MonkeyPatch, queue: asyncio.Queue
):
    submission.notifier.post_entity = AsyncMock(
        side_effect=[
            {"campaign_id": "camp_123"},
            {"ad_group_id": "group_456"},
            {"ad_id": "ad_789"},
        ]
    )
    submission.notifier.poll_status = AsyncMock(side_effect=[True, True, True])
    await queue.put(MockCampaignJob())

    task = asyncio.create_task(submission.submit_job(queue))
    await queue.join()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_campaign_creation_failure(
    submission: Submission, monkeypatch: MonkeyPatch, queue: asyncio.Queue
):
    monkeypatch.setattr("demand_link.demand_link.submit_job.MAX_RETRIES", 1)

    submission.notifier.post_entity = AsyncMock(
        side_effect=[{"campaign_id": None}, {"campaign_id": None}]
    )
    submission.notifier.poll_status = AsyncMock(side_effect=[])
    await queue.put(MockCampaignJob())

    task = asyncio.create_task(submission.submit_job(queue))
    await queue.join()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert submission.notifier.poll_status.call_count == 0


@pytest.mark.asyncio
async def test_campaign_poll_failure(
    submission: Submission, monkeypatch: MonkeyPatch, queue: asyncio.Queue
):
    monkeypatch.setattr("demand_link.demand_link.submit_job.MAX_RETRIES", 0)

    submission.notifier.post_entity = AsyncMock(
        side_effect=[{"campaign_id": "camp_123"}]
    )
    submission.notifier.poll_status = AsyncMock(side_effect=[False])
    await queue.put(MockCampaignJob())

    task = asyncio.create_task(submission.submit_job(queue))
    await queue.join()
    task.cancel()

    assert submission.notifier.poll_status.call_count == 1


@pytest.mark.asyncio
async def test_ad_group_creation_failure(
    submission, monkeypatch, queue: asyncio.Queue
):
    monkeypatch.setattr("demand_link.demand_link.submit_job.MAX_RETRIES", 0)
    submission.notifier.post_entity = AsyncMock(
        side_effect=[
            {"campaign_id": "camp_123"},
            {None: None},
        ]
    )
    submission.notifier.poll_status = AsyncMock(side_effect=[True])
    await queue.put(MockCampaignJob())

    task = asyncio.create_task(submission.submit_job(queue))
    await queue.join()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert submission.notifier.poll_status.call_count == 1


@pytest.mark.asyncio
async def test_ad_poll_failure(submission, monkeypatch, queue: asyncio.Queue):
    submission.notifier.post_entity = AsyncMock(
        side_effect=[
            {"campaign_id": "camp_123"},
            {"ad_group_id": "group_456"},
            {"ad_id": "ad_789"},
        ]
    )
    submission.notifier.poll_status = AsyncMock(
        side_effect=[True, True, False]
    )
    await queue.put(MockCampaignJob())

    task = asyncio.create_task(submission.submit_job(queue))
    await queue.join()
    task.cancel()

    assert submission.notifier.poll_status.call_count == 3


@pytest.mark.asyncio
async def test_ad_id_missing(
    submission: Submission, monkeypatch: MonkeyPatch, queue: asyncio.Queue
):
    monkeypatch.setattr("demand_link.demand_link.submit_job.MAX_RETRIES", 0)
    submission.notifier.post_entity = AsyncMock(
        side_effect=[
            {"campaign_id": "camp_123"},
            {"ad_group_id": "group_456"},
            {},
        ]
    )
    submission.notifier.poll_status = AsyncMock(
        side_effect=[True, True, False]
    )
    await queue.put(MockCampaignJob())

    task = asyncio.create_task(submission.submit_job(queue))
    await queue.join()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_retries_failure(
    monkeypatch: MonkeyPatch, submission: Submission, queue: asyncio.Queue
):
    monkeypatch.setattr("demand_link.demand_link.submit_job.MAX_RETRIES", 2)
    response_status = [False, True, True, True]
    responses = [
        {"campaign_id": "camp_123"},
        {"campaign_id": "camp_123"},
        {"ad_group_id": "group_456"},
        {"ad_id": "ad_789"},
    ]

    submission.notifier.post_entity = AsyncMock(side_effect=responses)
    submission.notifier.poll_status = AsyncMock(side_effect=response_status)

    await queue.put(MockCampaignJob())
    task = asyncio.create_task(submission.submit_job(queue))
    await queue.join()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert submission.notifier.poll_status.call_count == 4
