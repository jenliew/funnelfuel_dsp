from dataclasses import asdict, dataclass, field
from typing import List


@dataclass
class Ad:
    id: str
    type: str
    creative_url: str
    click_url: str
    status: str

    def dict(self):
        return {k: v for k, v in asdict(self).items()}


@dataclass
class AdGroup:
    id: str
    campaign_id: str
    name: str
    bid: float
    targeting: dict
    ads: List[Ad] = field(default_factory=list)

    def dict(self):
        return {k: v for k, v in asdict(self).items()}


@dataclass
class Campaign:
    id: str
    name: str
    budget: float
    start_date: str
    end_date: str
    objective: str
    ad_groups: List[AdGroup] = field(default_factory=list)
    retries: int = 0

    def dict(self):
        return {k: v for k, v in asdict(self).items()}


@dataclass
class DSPRecord:
    campaign_id: str
    campaign_name: str
    campaign_budget: float
    start_date: str
    end_date: str
    objective: str

    # Ad Group-level fields
    ad_group_id: str
    ad_group_name: str
    bid: float
    targeting_ages: str  # e.g., "18-24;25-34"
    targeting_interests: str
    targeting_geo: str

    # Ad-level fields
    ad_id: str
    ad_type: str
    creative_url: str
    click_url: str
    ad_status: str
