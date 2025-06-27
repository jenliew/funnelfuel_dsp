from demand_link.demand_link.record import Record


def test_valid_csv_campaign_data(test_campaign_dict, ad_groups_dict):

    msg = Record()

    test_ad_groups_dict = {
        "id": "test_ag_1002",
        "name": "Adults - Test2",
        "bid": 24.0,
        "targeting_ages": "35-44;45-64",
        "targeting_interests": "fashion;travel",
        "targeting_geo": "UK;US",
        "ads": [
            {
                "id": "ad_test_6501",
                "type": "banner",
                "creative_url": (
                    "https://cdn1.test.com/creatives"
                    "/autumn_style_urband.jpg"
                ),
                "click_url": ("https://shop.test.com/city-autumn"),
                "status": "new",
            }
        ],
    }

    msg.add_campaign_record(test_campaign_dict)
    msg.insert_ad_groups(
        test_campaign_dict["campaign_id"],
        test_ad_groups_dict,
    )

    assert len(msg.campaign_record) == 1
    assert len(msg.campaign_record[0].ad_groups) == 1

    msg.insert_ad_groups(test_campaign_dict["campaign_id"], ad_groups_dict)

    assert len(msg.campaign_record) == 1
    assert len(msg.campaign_record[0].ad_groups) == 2
