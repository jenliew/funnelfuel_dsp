import asyncio
import logging
import random
import sys
from uuid import uuid4

from fastapi import Body, FastAPI, HTTPException

from demand_link.demand_link.data_model import Ad, AdGroup, Campaign

app = FastAPI()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "%(asctime)s [%(processName)s: %(process)d] "
    "[%(threadName)s: %(thread)d] [%(levelname)s] %(name)s: %(message)s"
)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)

logger.addHandler(stream_handler)

logger.info("API is starting up")


# In-memory store to track campaign statuses
campaign_db = {}


@app.post("/campaigns", status_code=202)
async def create_campaign(data: Campaign = Body(...)):
    campaign_id = str(uuid4())

    logger.info(f"--> getting requestion: {data}")

    campaign_db[campaign_id] = {}
    campaign_db[campaign_id]["id"] = data.id
    campaign_db[campaign_id]["name"] = data.name
    campaign_db[campaign_id]["budget"] = data.budget
    campaign_db[campaign_id]["start_date"] = data.start_date
    campaign_db[campaign_id]["end_date"] = data.end_date
    campaign_db[campaign_id]["objective"] = data.objective
    campaign_db[campaign_id]["status"] = "success"

    # Simulate processing in background
    asyncio.create_task(process_campaign(campaign_id))

    return {"campaign_id": campaign_id}


@app.get("/campaigns/{campaign_id}/status")
async def get_campaign_status(campaign_id: str):
    logger.info(f"-> received campaign status:{campaign_id}")
    campaign = campaign_db.get(campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    logger.info(f"--> campaign:{campaign['id']}")
    status = campaign["status"]
    result = {"status": status}

    if status == "failed":
        result["error"] = campaign["fail_reason"]

    return result


@app.post("/ad-groups")
async def create_ad_groups(data: AdGroup):
    logger.info(f"Create ad-groups: status: {data}")
    ad_group = {}
    ad_group[data.id] = data.id

    result = {"ad_group_id": data.id}

    return result


@app.get("/ad-groups/{ad_group_id}/status")
async def get_ad_groups_status(ad_group_id: str):
    logger.info(f"-> get ad-group status:{ad_group_id}")

    return {"status": "success"}


@app.post("/ads")
async def create_ad(data: Ad):
    logger.info("Create ad status:")
    result = {"ad_id": data.id}

    return result


@app.get("/ads/{ad_id}/status")
async def get_ad_status(ad_id: str):
    logger.info(f"-> get ad status:{ad_id}")

    return {"status": "success"}


async def process_campaign(campaign_id):
    await asyncio.sleep(random.randint(2, 5))  # simulate delay

    campaign = campaign_db[campaign_id]
    if "fail_reason" in campaign:
        campaign["status"] = "failed"
    else:
        logger.info(f"Successfully create campaign {campaign_id}")
        campaign["status"] = "success"
