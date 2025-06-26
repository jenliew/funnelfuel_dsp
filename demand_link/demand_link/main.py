from asyncio import Queue
import asyncio
from aiolimiter import AsyncLimiter
from demand_link.demand_link.constant import RATE_LIMIT, URL_API_STR
from demand_link.demand_link.import_data import import_dsp_data
from demand_link.demand_link.submit_job import Submission


async def parse_campaign_job(list_campaign):
    queue = Queue()
    for campaign in list_campaign:
        await queue.put(campaign)

    job_submission = Submission()

    workers = [
        asyncio.create_task(job_submission.submit_job(queue)) for _ in range(3)
    ]  # 3 workers

    await queue.join()

    for w in workers:
        w.cancel()


if __name__ == "__main__":
    print("Hello Funnel Fuel")

    try:
        list_campaign = import_dsp_data("/home/jenny/funnel_fuel/dsp_merged_data.csv")

        asyncio.run(parse_campaign_job(list_campaign))

    except Exception as e:
        print(f"exception caught: {e}")
