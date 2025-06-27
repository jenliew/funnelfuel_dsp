from unittest.mock import AsyncMock

import pytest

from demand_link.demand_link.notifier import Notifier


@pytest.mark.asyncio
async def test_poll_status_success(monkeypatch, mock_client_session):
    notifier = Notifier(mock_client_session)

    # Mock _request_with_limiter to return success immediately
    monkeypatch.setattr(
        notifier,
        "request_with_limiter",
        AsyncMock(return_value={"status": "success"}),
    )

    result = await notifier.poll_status("campaigns", "abc123")
    assert result is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response_status, count",
    [
        (
            [
                None,
                None,
                {"status": "success"},
            ],
            3,
        ),
        ([None, None, None, {"status": "success"}], 4),
    ],
)
async def test_delayed_poll_status(
    mock_client_session, monkeypatch, response_status, count
):
    entity_type = "campaign"

    notifier = Notifier(mock_client_session)
    notifier.request_with_limiter = AsyncMock(side_effect=response_status)

    await notifier.poll_status(entity_type, "mocked_campaign_id")

    assert notifier.request_with_limiter.call_count == count


@pytest.mark.asyncio
async def test_poll_status_failure(monkeypatch, mock_client_session):
    notifier = Notifier(mock_client_session)

    monkeypatch.setattr(
        notifier,
        "request_with_limiter",
        AsyncMock(
            return_value={"status": "failed", "error": "Something went wrong"}
        ),
    )

    result = await notifier.poll_status("ads", "xyz789")
    assert result is False


@pytest.mark.asyncio
async def test_post_entity(monkeypatch, mock_client_session):
    notifier = Notifier(mock_client_session)

    mock_response = {"ad_id": "ad123"}
    monkeypatch.setattr(
        notifier, "request_with_limiter", AsyncMock(return_value=mock_response)
    )

    result = await notifier.post_entity("ads", {"id": "ad123"})
    assert result == mock_response
