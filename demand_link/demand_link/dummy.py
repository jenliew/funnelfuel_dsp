import logging

from demand_link.demand_link.import_data import (
    convert_str_dsp_record,
)
from demand_link.demand_link.record import Record

logging.basicConfig(
    level=logging.INFO,  # or DEBUG for more verbosity
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

record = Record()
test_data = {
    "campaign_id": "cmp_2025_004",
    "campaign_name": "Autumn Styles 2024",
    "campaign_budget": 21000,
    "start_date": "2024-09-01",
    "end_date": "2024-10-15",
    "objective": "video_views",
    "ad_group_id": "adffs",
    "ad_group_name": "2e3sd",
    "bid": "12344bid",
    "targeting_ages": "43",
    "targeting_interests": "sport teast",
    "targeting_geo": "UK;EU",
    "ad_id": "ad_iad34",
    "ad_type": "video",
    "creative_url": "http://asda",
    "click_url": "https://shop.example.com/city-autumn",
    "ad_status": "new",
}

result = convert_str_dsp_record(record, test_data)

print(result)


test_data2 = {
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
                    "click_url": "https://shop.example.com/city-autumn",
                    "status": "new",
                }
            ],
        }
    ],
}

result = convert_str_dsp_record(record, test_data2)
print(result)
