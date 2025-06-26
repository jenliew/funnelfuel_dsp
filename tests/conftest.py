import asyncio
import pytest

from demand_link.demand_link.submit_job import Submission


class MockAd:
    def __init__(self, id="ad_001", type="banner"):
        self.id = id
        self.type = type
        self.creative_url = "http://creative"
        self.click_url = "http://click"
        self.status = "pending"

    def dict(self):
        return self.__dict__


class MockAdGroup:
    def __init__(self, id="ag_001"):
        self.id = id
        self.name = "Ad Group Test"
        self.bid = 2.5
        self.targeting = {
            "ages": "18-24;25-34",
            "interests": "fashion;shoes",
            "geo": "UK;IE",
        }
        self.ads = [MockAd()]

    def dict(self):
        return self.__dict__


class MockCampaignJob:
    def __init__(self):
        self.id = "cmp_001"
        self.name = "Campaign Test"
        self.budget = 15000
        self.start_date = "2025-07-01"
        self.end_date = "2025-08-15"
        self.objective = "engagement"
        self.ad_groups = [MockAdGroup()]

    def dict(self):
        return self.__dict__
