import argparse
import asyncio
import logging
import os
import traceback
from asyncio import Queue

from demand_link.demand_link.constant import (
    DEFAULT_RATE_LIMIT,
    DEFAULT_URL_API_STR,
)
from demand_link.demand_link.import_data import import_dsp_data
from demand_link.demand_link.submit_job import Submission


async def parse_campaign_job(list_campaign, endpoint, rate_limit):
    queue = Queue()
    for campaign in list_campaign:
        await queue.put(campaign)

    job_submission = Submission(dsp_endpoint=endpoint, rate_limit=rate_limit)

    workers = [
        asyncio.create_task(job_submission.submit_job(queue)) for _ in range(3)
    ]  # 3 workers

    await queue.join()

    for w in workers:
        w.cancel()


def main():
    parser = intialise_scripts()

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log),  # or DEBUG for more verbosity
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )

    # Optionally create a named logger for your module
    logger = logging.getLogger("dsp_submission")

    if args.file:
        input_file = args.file
        if not os.path.isfile(input_file):
            raise IOError(f"{input_file} not existed")

        data_campaign = import_dsp_data(input_file)
        asyncio.run(
            parse_campaign_job(data_campaign, args.dsp, args.rate_limit)
        )

    else:
        logger.error("error. missing file parameter")


def intialise_scripts():
    parser = argparse.ArgumentParser(description="Submission Campaign data")

    parser.add_argument(
        "--log",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Enabled logging",
        default="INFO",
    )
    parser.add_argument(
        "--file", type=str, help="Full path for the CSV file of campaign data"
    )
    parser.add_argument(
        "--rate-limit", type=int, help="Rate Limit", default=DEFAULT_RATE_LIMIT
    )
    parser.add_argument(
        "--dsp",
        type=str,
        help="The URL of the DSP API endpoint.",
        default=DEFAULT_URL_API_STR,
    )

    return parser


if __name__ == "__main__":
    try:
        main()

    except Exception as e:
        print(traceback.format_exc())
        print(f"exception caught: {e}")
