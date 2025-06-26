import asyncio
import pytest
from pytest import MonkeyPatch
from demand_link.demand_link.exception import SubmissionError
from demand_link.demand_link.submit_job import Submission
from tests.conftest import MockCampaignJob
from unittest.mock import AsyncMock


@pytest.mark.asyncio
async def run_single_job(
    submission: Submission, monkeypatch, responses, response_status
):

    async def mocked_post_entity(method, mock_repsonse_dict):
        for pattern, value in responses.items():
            if pattern == method:
                return value
        return {}

    monkeypatch.setattr(
        submission.notifier,
        "post_entity",
        mocked_post_entity,
    )

    submission.notifier.poll_status = AsyncMock(side_effect=response_status)

    queue = asyncio.Queue()
    await queue.put(MockCampaignJob())

    task = asyncio.create_task(submission.submit_job(queue))
    await queue.join()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_successful_job(monkeypatch: MonkeyPatch):
    submission = Submission()
    await run_single_job(
        submission,
        monkeypatch,
        {
            "campaigns": {"campaign_id": "camp_123"},
            "ad-groups": {"ad_group_id": "group_456"},
            "ads": {"ad_id": "ad_789"},
        },
        [True, True, True],
    )


@pytest.mark.asyncio
async def test_campaign_creation_failure(monkeypatch: MonkeyPatch):
    submission = Submission()

    with pytest.raises(
        SubmissionError, match="Failed process job error:Fail get campaign_id"
    ):
        await run_single_job(
            submission,
            monkeypatch,
            {
                "campaigns": {},  # no campaign_id
            },
            [],
        )  # Should print error and skip rest


@pytest.mark.asyncio
async def test_campaign_poll_failure(monkeypatch: MonkeyPatch):
    submission = Submission()

    with pytest.raises(
        SubmissionError, match="Failed process job error:Fail pool status from DSP API"
    ):
        await run_single_job(
            submission,
            monkeypatch,
            {
                "campaigns": {"campaign_id": "camp_123"},
            },
            [False],
        )


@pytest.mark.asyncio
async def test_ad_group_creation_failure(monkeypatch: MonkeyPatch):
    submission = Submission()

    with pytest.raises(
        SubmissionError, match="Failed process job error:Failed to create ad_group"
    ) as e:
        await run_single_job(
            submission,
            monkeypatch,
            {
                "campaigns": {"campaign_id": "camp_123"},
                "ad-groups": {None: None},
            },
            [True],
        )


@pytest.mark.asyncio
async def test_ad_poll_failure(monkeypatch: MonkeyPatch):
    submission = Submission()
    await run_single_job(
        submission,
        monkeypatch,
        {
            "campaigns": {"campaign_id": "camp_123"},
            "ad-groups": {"ad_group_id": "group_456"},
            "ads": {"ad_id": "ad_789"},
        },
        [True, True, False],
    )


@pytest.mark.asyncio
async def test_campaign_pool_wait_for_success(monkeypatch: MonkeyPatch):
    submission = Submission()

    await run_single_job(
        submission,
        monkeypatch,
        {
            "campaigns": {"campaign_id": "camp_1123"},
            "ad-groups": {"ad_group_id": "group_456"},
            "ads": {"ad_id": "ad_789"},
        },
        {
            "campaigns/camp_1123/status": [
                {"status": None},
                {"status": None},
                {"status": "success"},
            ],
            "ad-groups/group_456/status": {"status": "success"},
            "ads/ad_789/status": {"status": "failed", "error": "Creative rejected"},
        },
    )


@pytest.mark.asyncio
async def test_ad_id_missing(monkeypatch: MonkeyPatch):
    submission = Submission()

    with pytest.raises(
        SubmissionError, match="Failed process job error:Failed to create/update ad"
    ):
        await run_single_job(
            submission,
            monkeypatch,
            {
                "campaigns": {"campaign_id": "camp_123"},
                "ad-groups": {"ad_group_id": "group_456"},
                "ads": {},  # missing ad_id
            },
            {
                "campaigns/camp_123/status": {"status": "success"},
                "ad-groups/group_456/status": {"status": "success"},
            },
        )
