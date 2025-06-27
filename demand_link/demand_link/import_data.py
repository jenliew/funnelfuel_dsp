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
            (
                ag
                for ag in campaign.ad_groups
                if ag.campaign_id == record.ad_group_id
            ),
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
            list_campaign.append(convert_str_dsp_record(i))

    result = regroup_records(list_campaign)

    return result


def convert_str_dsp_record(input):
    return DSPRecord(
        campaign_id=input["campaign_id"],
        campaign_name=input["campaign_name"],
        campaign_budget=int(input["campaign_budget"]),
        start_date=input["start_date"],
        end_date=input["end_date"],
        objective=input["objective"],
        ad_group_id=input["ad_group_id"],
        ad_group_name=input["ad_group_name"],
        bid=float(input["bid"]),
        targeting_ages=input["targeting_ages"],
        targeting_interests=input["targeting_interests"],
        targeting_geo=input["targeting_geo"],
        ad_id=input["ad_id"],
        ad_type=input["ad_type"],
        creative_url=input["creative_url"],
        click_url=input["click_url"],
        ad_status=input["ad_status"],
    )
