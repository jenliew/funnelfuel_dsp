import logging
from typing import Dict

from demand_link.demand_link.data_model import Ad, AdGroup, Campaign
from demand_link.demand_link.exception import SubmissionError

logger = logging.getLogger(__name__)


class Record:
    def __init__(self) -> None:
        logger.info("Creating Campaign Message")
        self.campaign_record = []

    def find_existing_record(self, campaign_id):
        for record in self.campaign_record:
            if record.id == campaign_id:
                return record
            else:
                logger.warning(f"Not found campaign_id:{campaign_id}")
        return None

    def add_campaign_record(self, data):
        campaign_record = self.find_existing_record(data["campaign_id"])

        if not campaign_record:
            campaign_data = Campaign(
                id=data["campaign_id"],
                name=data["campaign_name"],
                budget=int(data["campaign_budget"]),
                start_date=data["start_date"],
                end_date=data["end_date"],
                objective=data["objective"],
            )
            self.campaign_record.append(campaign_data)

    def check_campagain_ad_group(self, campaign_id: str, ad_group_id: str):

        campaign_record = self.find_existing_record(campaign_id)
        if not campaign_record:
            logger.error(f"Not found {campaign_id} record")
            return None
        return next(
            (
                ad_grp
                for ad_grp in campaign_record.ad_groups
                if ad_grp.id == ad_group_id
            ),
            None,
        )

    def insert_ad_groups(self, campaign_id: str, ad_group_dict: Dict):

        campaign_record = self.find_existing_record(campaign_id)
        if not campaign_record:
            logger.error(f"Not found {campaign_id} record")
            return None

        ad_group_id = ad_group_dict["id"]
        if not self.check_campagain_ad_group(campaign_id, ad_group_id):
            logger.info("Insert a new ad-group to the campaign object.")

            existing_ad_group = AdGroup(
                id=ad_group_id,
                campaign_id=campaign_record.id,
                name=ad_group_dict["name"],
                bid=ad_group_dict["bid"],
                targeting={
                    "ages": ad_group_dict["targeting_ages"],
                    "interests": ad_group_dict["targeting_interests"],
                    "geo": ad_group_dict["targeting_geo"],
                },
            )

            campaign_record.ad_groups.append(existing_ad_group)
        else:
            logger.info(
                f"Ad_Group:{ad_group_id} existed in the campaign data."
            )

        for ads_group in campaign_record.ad_groups:
            if ad_group_id == ads_group.id:
                ad_list = ad_group_dict.get("ads", [])
                for ad in ad_list:
                    self.campaign_ads_list(campaign_id, ad_group_id, ad)

    def campaign_ads_list(
        self, campaign_id, ad_groups_id: str, ads_dict: Dict
    ):

        existing_ad_groups = self.check_campagain_ad_group(
            campaign_id, ad_groups_id
        )

        if not existing_ad_groups:
            raise SubmissionError(
                f"Not found ad_groups_id {ad_groups_id} in campaign data"
            )
        else:
            logger.info(f"-> ads_dict:{ads_dict}")
            existing_ad_groups.ads.append(
                Ad(
                    id=ads_dict["id"],
                    type=ads_dict["type"],
                    creative_url=ads_dict["creative_url"],
                    click_url=ads_dict["click_url"],
                    status=ads_dict["status"],
                )
            )
