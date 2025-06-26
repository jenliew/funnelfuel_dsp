from csv import DictReader

from demand_link.demand_link.data_model import Ad, AdGroup, Campaign, DSPRecord


def regroup_records(records):
    campaigns_by_id = {}

    for record in records:
        # Get or create campaign
        if record.campaign_id not in campaigns_by_id:
            campaigns_by_id[record.campaign_id] = Campaign(
                id=record.campaign_id,
                name=record.campaign_name,
                budget=record.campaign_budget,
                start_date=record.start_date,
                end_date=record.end_date,
                objective=record.objective,
            )

        campaign = campaigns_by_id[record.campaign_id]

        # Get or create ad group
        existing_ad_group = next(
            (ag for ag in campaign.ad_groups if ag.campaign_id == record.ad_group_id),
            None,
        )
        if not existing_ad_group:
            target_dict = {
                "ages": record.targeting_ages,
                "interests": record.targeting_interests,
                "geo": record.targeting_geo,
            }
            existing_ad_group = AdGroup(
                id=record.ad_group_id,
                campaign_id=record.campaign_id,
                name=record.ad_group_name,
                bid=record.bid,
                targeting=target_dict,
            )
            campaign.ad_groups.append(existing_ad_group)

        # Add ad
        existing_ad_group.ads.append(
            Ad(
                id=record.ad_id,
                type=record.ad_type,
                creative_url=record.creative_url,
                click_url=record.click_url,
                status=record.ad_status,
            )
        )

    return list(campaigns_by_id.values())


def import_dsp_data(input: str):

    list_campaign = []

    with open(input) as input_obj:
        csv_dict_reader = DictReader(input_obj)

        for i in csv_dict_reader:
            record = DSPRecord(
                campaign_id=i["campaign_id"],
                campaign_name=i["campaign_name"],
                campaign_budget=int(i["campaign_budget"]),
                start_date=i["start_date"],
                end_date=i["end_date"],
                objective=i["objective"],
                ad_group_id=i["ad_group_id"],
                ad_group_name=i["ad_group_name"],
                bid=float(i["bid"]),
                targeting_ages=i["targeting_ages"],
                targeting_interests=i["targeting_interests"],
                targeting_geo=i["targeting_geo"],
                ad_id=i["ad_id"],
                ad_type=i["ad_type"],
                creative_url=i["creative_url"],
                click_url=i["click_url"],
                ad_status=i["ad_status"],
            )

            list_campaign.append(record)

    result = regroup_records(list_campaign)

    return result
