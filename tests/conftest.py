import asyncio

import pytest

from demand_link.demand_link.submit_job import Submission


@pytest.fixture
def submission():
    return Submission()


@pytest.fixture
def queue():
    return asyncio.Queue()


@pytest.fixture
def test_campaign_dict():
    return {
        "campaign_id": "cmp_2025_004",
        "campaign_name": "Autumn Styles 2024",
        "campaign_budget": 21000,
        "start_date": "2024-09-01",
        "end_date": "2024-10-15",
        "objective": "video_views",
    }


@pytest.fixture
def ad_groups_dict():
    return {
        "id": "test_ag_1001",
        "name": "Adults - Test",
        "bid": 2.0,
        "targeting_ages": "35-44;45-64",
        "targeting_interests": "fashion;streetwear",
        "targeting_geo": "UK;IE",
        "ads": [
            {
                "id": "ad_test_6001",
                "type": "video",
                "creative_url": (
                    "https://cdn.test.com/creatives" "/autumn_style_city.jpg"
                ),
                "click_url": ("https://shop.test.com/city-autumn"),
                "status": "new",
            }
        ],
    }
