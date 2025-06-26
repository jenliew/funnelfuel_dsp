import asyncio
import pytest
from pytest import MonkeyPatch
from unittest.mock import AsyncMock
from demand_link.demand_link.exception import SubmissionError
from demand_link.demand_link.submit_job import Submission
from tests.conftest import MockCampaignJob


async def run_job_with_mocks(
    submission: Submission,
    monkeypatch: MonkeyPatch,
    responses: list,
    response_status: list,
    queue: asyncio.Queue,
):
    submission.notifier.post_entity = AsyncMock(side_effect=responses)
    submission.notifier.poll_status = AsyncMock(side_effect=response_status)
    await queue.put(MockCampaignJob())

    task = asyncio.create_task(submission.submit_job(queue))
    await queue.join()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_successful_job(submission, monkeypatch, queue):
    await run_job_with_mocks(
        submission,
        monkeypatch,
        [
            {"campaign_id": "camp_123"},
            {"ad_group_id": "group_456"},
            {"ad_id": "ad_789"},
        ],
        [True, True, True],
        queue,
    )


@pytest.mark.asyncio
async def test_campaign_creation_failure(submission, monkeypatch, queue):
    monkeypatch.setattr("demand_link.demand_link.submit_job.MAX_RETRIES", 1)
    with pytest.raises(
        SubmissionError, match="Failed process job error:Fail get campaign_id"
    ):
        await run_job_with_mocks(
            submission,
            monkeypatch,
            [{"campaign_id": None}, {"campaign_id": None}],
            [False, False],
            queue,
        )


@pytest.mark.asyncio
async def test_campaign_poll_failure(submission, monkeypatch, queue):
    monkeypatch.setattr("demand_link.demand_link.submit_job.MAX_RETRIES", 0)
    with pytest.raises(
        SubmissionError, match="Failed process job error:Fail pool status from DSP API"
    ):
        await run_job_with_mocks(
            submission,
            monkeypatch,
            [{"campaign_id": "camp_123"}],
            [False],
            queue,
        )


@pytest.mark.asyncio
async def test_ad_group_creation_failure(submission, monkeypatch, queue):
    monkeypatch.setattr("demand_link.demand_link.submit_job.MAX_RETRIES", 0)
    with pytest.raises(
        SubmissionError, match="Failed process job error:Failed to create ad_group"
    ):
        await run_job_with_mocks(
            submission,
            monkeypatch,
            [
                {"campaign_id": "camp_123"},
                {None: None},
            ],
            [True],
            queue,
        )


@pytest.mark.asyncio
async def test_ad_poll_failure(submission, monkeypatch, queue):
    await run_job_with_mocks(
        submission,
        monkeypatch,
        [
            {"campaign_id": "camp_123"},
            {"ad_group_id": "group_456"},
            {"ad_id": "ad_789"},
        ],
        [True, True, False],
        queue,
    )


@pytest.mark.asyncio
async def test_ad_id_missing(submission, monkeypatch, queue):
    monkeypatch.setattr("demand_link.demand_link.submit_job.MAX_RETRIES", 0)
    with pytest.raises(
        SubmissionError, match="Failed process job error:Failed to create/update ad"
    ):
        await run_job_with_mocks(
            submission,
            monkeypatch,
            [
                {"campaign_id": "camp_123"},
                {"ad_group_id": "group_456"},
                {},
            ],
            [True, True, False],
            queue,
        )


@pytest.mark.asyncio
async def test_retries_failure(monkeypatch: MonkeyPatch):
    monkeypatch.setattr("demand_link.demand_link.submit_job.MAX_RETRIES", 2)
    submission = Submission()
    response_status = [False, True, True, True]
    responses = [
        {"campaign_id": "camp_123"},
        {"campaign_id": "camp_123"},
        {"ad_group_id": "group_456"},
        {"ad_id": "ad_789"},
    ]

    submission.notifier.post_entity = AsyncMock(side_effect=responses)
    submission.notifier.poll_status = AsyncMock(side_effect=response_status)

    queue = asyncio.Queue()
    await queue.put(MockCampaignJob())
    task = asyncio.create_task(submission.submit_job(queue))
    await queue.join()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert submission.notifier.poll_status.call_count == 4
