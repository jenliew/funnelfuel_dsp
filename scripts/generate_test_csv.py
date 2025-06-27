import csv

# Sample merged data
creative_url_str = "https://cdn.example.com/creatives/summer_style_urban.jpg"
data = [
    {
        "campaign_id": "cmp_2025_001",
        "campaign_name": "Summer Styles 2025",
        "campaign_budget": 15000,
        "start_date": "2025-07-01",
        "end_date": "2025-08-15",
        "objective": "engagement",
        "ad_groups": [
            {
                "id": "ag_1001",
                "name": "Young Adults - Urban",
                "bid": 2.5,
                "targeting_ages": "18-24;25-34",
                "targeting_interests": "fashion;streetwear",
                "targeting_geo": "UK;IE",
                "ads": [
                    {
                        "id": "ad_5001",
                        "type": "banner",
                        "creative_url": creative_url_str,
                        "click_url": "https://shop.example.com/urban-summer",
                        "status": "pending",
                    }
                ],
            }
        ],
    },
    {
        "campaign_id": "cmp_2025_002",
        "campaign_name": "Summer Styles 2025",
        "campaign_budget": 15000,
        "start_date": "2025-07-01",
        "end_date": "2025-08-15",
        "objective": "engagement",
        "ad_groups": [
            {
                "id": "ag_1001",
                "name": "Young Adults - Urban",
                "bid": 2.5,
                "targeting_ages": "18-24;25-34",
                "targeting_interests": "fashion;streetwear",
                "targeting_geo": "UK;IE",
                "ads": [
                    {
                        "id": "ad_5002",
                        "type": "video",
                        "creative_url": (
                            "https://cdn.example.com/"
                            "videos/urban_sizzle.mp4"
                        ),
                        "click_url": ("https://shop.example.com/urban-summer"),
                        "status": "pending",
                    }
                ],
            }
        ],
    },
    {
        "campaign_id": "cmp_2025_003",
        "campaign_name": "Summer Styles 2025",
        "campaign_budget": 15000,
        "start_date": "2025-07-01",
        "end_date": "2025-08-15",
        "objective": "engagement",
        "ad_groups": [
            {
                "id": "ag_1002",
                "name": "Parents - Back to School",
                "bid": 1.8,
                "targeting_ages": "35-50",
                "targeting_interests": "kids fashion;back to school",
                "targeting_geo": "UK",
                "ads": [
                    {
                        "id": "ad_5003",
                        "type": "native",
                        "creative_url": (
                            "https://cdn.example.com/creatives/"
                            "back_to_school_native.jpg"
                        ),
                        "click_url": "https://shop.example.com/back-to-school",
                        "status": "pending",
                    }
                ],
            }
        ],
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
                        "click_url": "https://shop.example.com/city-autumn",
                        "status": "new",
                    }
                ],
            }
        ],
    },
    {
        "campaign_id": "cmp_2024_005",
        "campaign_name": "Autumn Styles 2024",
        "campaign_budget": 35000,
        "start_date": "2024-07-01",
        "end_date": "2024-08-15",
        "objective": "video_views",
        "ad_groups": [
            {
                "id": "ag_1004",
                "name": "Young Adults - Urban",
                "bid": 5.5,
                "targeting_ages": "18-24;25-34",
                "targeting_interests": "fashion;streetwear",
                "targeting_geo": "UK;IE",
                "ads": [
                    {
                        "id": "ad_6002",
                        "type": "ctv",
                        "creative_url": (
                            "https://cdn.example.com/creatives/"
                            "autumn_style_urban.jpg"
                        ),
                        "click_url": "https://shop.example.com/urban-autumn",
                        "status": "pending",
                    }
                ],
            }
        ],
    },
    {
        "campaign_id": "cmp_2025_multi_01",
        "campaign_name": "Autumn Sale Push",
        "campaign_budget": 20000,
        "start_date": "2025-09-01",
        "end_date": "2025-10-15",
        "objective": "conversions",
        "ad_groups": [
            {
                "id": "ag_3001",
                "name": "Urban Millennials",
                "bid": 2.8,
                "targeting_ages": "25-34",
                "targeting_interests": "fashion;tech",
                "targeting_geo": "UK;DE",
                "ads": [
                    {
                        "id": "ad_9001",
                        "type": "banner",
                        "creative_url": (
                            "https://cdn.example.com/banners/"
                            "millennials_banner.jpg"
                        ),
                        "click_url": (
                            "https://shop.example.com/autumn-millennials"
                        ),
                        "status": "pending",
                    },
                    {
                        "id": "ad_9002",
                        "type": "video",
                        "creative_url": (
                            "https://cdn.example.com/videos/"
                            "millennials_ad.mp4"
                        ),
                        "click_url": (
                            "https://shop.example.com/autumn-millennials"
                        ),
                        "status": "pending",
                    },
                ],
            },
            {
                "id": "ag_3002",
                "name": "Budget-Conscious Families",
                "bid": 1.7,
                "targeting_ages": "35-50",
                "targeting_interests": "family;discount shopping",
                "targeting_geo": "UK;NL",
                "ads": [
                    {
                        "id": "ad_9003",
                        "type": "native",
                        "creative_url": (
                            "https://cdn.example.com/native/"
                            "family_savings_native.jpg"
                        ),
                        "click_url": (
                            "https://shop.example.com/family-autumn-deals"
                        ),
                        "status": "pending",
                    }
                ],
            },
            {
                "id": "ag_3003",
                "name": "Campus Trendsetters",
                "bid": 2.3,
                "targeting_ages": "18-24",
                "targeting_interests": "college life;fashion",
                "targeting_geo": "UK",
                "ads": [
                    {
                        "id": "ad_9004",
                        "type": "carousel",
                        "creative_url": (
                            "https://cdn.example.com/carousel/"
                            "back_to_uni_set.jpg"
                        ),
                        "click_url": ("https://shop.example.com/campus-sale"),
                        "status": "pending",
                    }
                ],
            },
        ],
    },
]

# Output file name
filename = "dsp_merged_data.csv"

# Write to CSV
with open(filename, mode="w", newline="", encoding="utf-8") as csvfile:
    fieldnames = [
        "campaign_id",
        "campaign_name",
        "campaign_budget",
        "start_date",
        "end_date",
        "objective",
        "ad_group_id",
        "ad_group_name",
        "bid",
        "targeting_ages",
        "targeting_interests",
        "targeting_geo",
        "ad_id",
        "ad_type",
        "creative_url",
        "click_url",
        "ad_status",
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for campaign_job in data:
        for ag in campaign_job["ad_groups"]:
            for ad in ag["ads"]:
                print(f"ads: {ad}")
                writer.writerow(
                    {
                        "campaign_id": campaign_job["campaign_id"],
                        "campaign_name": campaign_job["campaign_name"],
                        "campaign_budget": campaign_job["campaign_budget"],
                        "start_date": campaign_job["start_date"],
                        "end_date": campaign_job["end_date"],
                        "objective": campaign_job["objective"],
                        "ad_group_id": ag["id"],
                        "ad_group_name": ag["name"],
                        "bid": ag["bid"],
                        "targeting_ages": ag["targeting_ages"],
                        "targeting_interests": ag["targeting_interests"],
                        "targeting_geo": ag["targeting_geo"],
                        "ad_id": ad["id"],
                        "ad_type": ad["type"],
                        "creative_url": ad["creative_url"],
                        "click_url": ad["click_url"],
                        "ad_status": ad["status"],
                    }
                )


print(f"CSV file '{filename}' has been created.")
