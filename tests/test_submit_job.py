import asyncio
import pytest
from demand_link.demand_link.exception import SubmissionError
from demand_link.demand_link.submit_job import Submission
from tests.conftest import MockCampaignJob


@pytest.mark.asyncio
async def run_single_job(submission: Submission, monkeypatch, responses):

    print(responses)

    async def mocked_request_with_limiter(method, url, **kwargs):
        url_cleaned = url.replace("http://127.0.0.1:8000/", "")
        for pattern, value in responses.items():
            if pattern == url_cleaned:
                if isinstance(value, list) and value:
                    return value.pop(0)
                return value
        return {}

    monkeypatch.setattr(
        submission, "_request_with_limiter", mocked_request_with_limiter
    )

    queue = asyncio.Queue()
    await queue.put(MockCampaignJob())

    task = asyncio.create_task(submission.submit_job(queue))
    await queue.join()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_successful_job(monkeypatch):
    submission = Submission()
    await run_single_job(
        submission,
        monkeypatch,
        {
            "campaigns": {"campaign_id": "camp_123"},
            "campaigns/camp_123/status": {"status": "success"},
            "ad-groups": {"ad_group_id": "group_456"},
            "ad-groups/group_456/status": {"status": "success"},
            "ads": {"ad_id": "ad_789"},
            "ads/ad_789/status": {"status": "success"},
        },
    )


@pytest.mark.asyncio
async def test_campaign_creation_failure(monkeypatch):
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
        )  # Should print error and skip rest


@pytest.mark.asyncio
async def test_campaign_poll_failure(monkeypatch):
    submission = Submission()

    with pytest.raises(
        SubmissionError, match="Failed process job error:Fail pool status from DSP API"
    ):
        await run_single_job(
            submission,
            monkeypatch,
            {
                "campaigns": {"campaign_id": "camp_123"},
                "campaigns/camp_123/status": {
                    "status": "failed",
                    "error": "Invalid IP",
                },
            },
        )


@pytest.mark.asyncio
async def test_ad_group_creation_failure(monkeypatch):
    submission = Submission()

    with pytest.raises(
        SubmissionError, match="Failed process job error:Failed to create ad_group"
    ) as e:
        await run_single_job(
            submission,
            monkeypatch,
            {
                "campaigns": {"campaign_id": "camp_123"},
                "campaigns/camp_123/status": {"status": "success"},
                "ad-groups": {None: None},
            },
        )


@pytest.mark.asyncio
async def test_ad_poll_failure(monkeypatch: pytest.MonkeyPatch):
    submission = Submission()
    await run_single_job(
        submission,
        monkeypatch,
        {
            "campaigns": {"campaign_id": "camp_123"},
            "campaigns/camp_123/status": {"status": "success"},
            "ad-groups": {"ad_group_id": "group_456"},
            "ad-groups/group_456/status": {"status": "success"},
            "ads": {"ad_id": "ad_789"},
            "ads/ad_789/status": {"status": "failed", "error": "Creative rejected"},
        },
    )


@pytest.mark.asyncio
async def test_campaign_pool_wait_for_success(monkeypatch: pytest.MonkeyPatch):
    submission = Submission()

    await run_single_job(
        submission,
        monkeypatch,
        {
            "campaigns": {"campaign_id": "camp_1123"},
            "campaigns/camp_1123/status": [
                {"status": None},
                {"status": None},
                {"status": "success"},
            ],
            "ad-groups": {"ad_group_id": "group_456"},
            "ad-groups/group_456/status": {"status": "success"},
            "ads": {"ad_id": "ad_789"},
            "ads/ad_789/status": {"status": "failed", "error": "Creative rejected"},
        },
    )


@pytest.mark.asyncio
async def test_ad_id_missing(monkeypatch):
    submission = Submission()

    with pytest.raises(
        SubmissionError, match="Failed process job error:Failed to create/update ad"
    ):
        await run_single_job(
            submission,
            monkeypatch,
            {
                "campaigns": {"campaign_id": "camp_123"},
                "campaigns/camp_123/status": {"status": "success"},
                "ad-groups": {"ad_group_id": "group_456"},
                "ad-groups/group_456/status": {"status": "success"},
                "ads": {},  # missing ad_id
            },
        )
