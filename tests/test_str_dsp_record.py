import pytest

from demand_link.demand_link.record import Record
from demand_link.demand_link.utils import convert_str_dsp_record


@pytest.mark.parametrize(
    "test_data, num_camp_obj, num_ad_groups",
    [
        (
            [
                {
                    "campaign_id": "cmp_2025_001",
                    "campaign_name": "Autumn Styles 2024",
                    "campaign_budget": "21000",
                    "start_date": "2024-09-01",
                    "end_date": "2024-10-15",
                    "objective": "video_views",
                    "ad_group_id": "adffs",
                    "ad_group_name": "ad_group_004",
                    "bid": "12344bid",
                    "targeting_ages": "43",
                    "targeting_interests": "fashion",
                    "targeting_geo": "UK;EU",
                    "ad_id": "ad_iad34",
                    "ad_type": "video",
                    "creative_url": "https://cdn.example.com/creatives/test",
                    "click_url": "https://shop.example.com/city-autumn",
                    "ad_status": "new",
                },
                {
                    "campaign_id": "cmp_2025_004",
                    "campaign_name": "Autumn Styles 2024",
                    "campaign_budget": 21000,
                    "start_date": "2024-09-01",
                    "end_date": "2024-10-15",
                    "objective": "video_views",
                    "ad_groups": [
                        {
                            "id": "ag_1003",
                            "name": "Adults - City",
                            "bid": 4.0,
                            "targeting_ages": "35-44;45-64",
                            "targeting_interests": "fashion;streetwear",
                            "targeting_geo": "UK;IE",
                            "ads": [
                                {
                                    "id": "ad_6001",
                                    "type": "video",
                                    "creative_url": (
                                        "https://cdn.example.com/creatives"
                                        "/autumn_style_city.jpg"
                                    ),
                                    "click_url": (
                                        "https://shop.example.com/city-autumn"
                                    ),
                                    "status": "new",
                                },
                                {
                                    "id": "ad_6002",
                                    "type": "paper",
                                    "creative_url": (
                                        "https://cdn.example.com/creatives/"
                                        "/testautumn_style_city.jpg"
                                    ),
                                    "click_url": (
                                        "https://shop.example.com/urban"
                                    ),
                                    "status": "new",
                                },
                            ],
                        }
                    ],
                },
            ],
            2,
            1,
        ),
        ([], 0, 0),
    ],
)
def test_valid_csv_data(test_data, num_camp_obj, num_ad_groups):

    record = Record()
    for i in test_data:
        convert_str_dsp_record(record, i)

    count_campaign_object = len(record.campaign_record)
    assert count_campaign_object == num_camp_obj

    if count_campaign_object > 0:
        assert len(record.campaign_record[0].ad_groups) == num_ad_groups
