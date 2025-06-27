import logging

from demand_link.demand_link.constant import (
    AD_GROUP_KEY_MAPPING,
    ADS_KEY_MAPPING,
    CAMPAIGN_KEYS,
)
from demand_link.demand_link.record import SubmissionError

logger = logging.getLogger(__name__)


def convert_str_dsp_record(record, input_data):

    grouped_ad_group_data = {}
    ads_group_data = {}

    campaign_dict = {k: v for k, v in input_data.items() if k in CAMPAIGN_KEYS}

    ad_groups_dict_list = input_data.get("ad_groups", [])
    ads_dict_list = []

    if not record:
        raise SubmissionError("Record object didn't create properly.")

    record.add_campaign_record(campaign_dict)
    if not ad_groups_dict_list:

        for k, v in input_data.items():
            for key, mapped_key in AD_GROUP_KEY_MAPPING.items():
                if key in k:
                    grouped_ad_group_data[mapped_key] = v

            for ad_key, ad_mapped_key in ADS_KEY_MAPPING.items():
                if ad_key in k:
                    ads_group_data[ad_mapped_key] = v

        ads_dict_list.append(ads_group_data)
        grouped_ad_group_data["ads"] = ads_dict_list
        ad_groups_dict_list.append(grouped_ad_group_data)

    else:
        for ads_item in ad_groups_dict_list:
            ads_dict_list += ads_item.get("ads", [])

    for ads_group_items in ad_groups_dict_list:
        record.insert_ad_groups(
            campaign_dict["campaign_id"],
            ads_group_items,
        )

    return
