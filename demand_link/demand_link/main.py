import argparse
import asyncio
import json
import logging
import os
import traceback
from asyncio import Queue
from csv import DictReader

import aiohttp

from demand_link.demand_link.constant import (
    DEFAULT_RATE_LIMIT,
    DEFAULT_URL_API_STR,
    DEFAULT_WORKER_NUM,
    HTTP_TIMEOUT,
)
from demand_link.demand_link.import_data import (
    convert_str_dsp_record,
)
from demand_link.demand_link.record import Record
from demand_link.demand_link.submit_job import Submission


async def parse_campaign_job(list_campaign, endpoint, rate_limit, worker_num):
    queue = Queue()
    for campaign in list_campaign:
        await queue.put(campaign)

    logging.info("Creating session.")
    client_timeout = aiohttp.ClientTimeout(
        total=None, sock_connect=HTTP_TIMEOUT, sock_read=HTTP_TIMEOUT
    )
    async with aiohttp.ClientSession(timeout=client_timeout) as session:

        job_submission = Submission(
            session, dsp_endpoint=endpoint, rate_limit=rate_limit
        )

        workers = [
            asyncio.create_task(job_submission.submit_job(queue))
            for _ in range(worker_num)
        ]  # worker_num workers

        await queue.join()

        for w in workers:
            w.cancel()


def configure_logging(log_level: str):
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
    return logging.getLogger("dsp_submission")


def process_file_input(file_path: str, record, logger):
    if not os.path.isfile(file_path):
        logger.error(f"Input file not found: {file_path}")
        raise FileNotFoundError(f"{file_path} does not exist.")

    with open(file_path) as input_obj:
        for row in DictReader(input_obj):
            convert_str_dsp_record(record, row)


def process_line_input(line_str: str, record):
    try:
        data = json.loads(line_str)
        convert_str_dsp_record(record, data)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON input for --line: {e}")


def main():
    parser = intialise_scripts()
    args = parser.parse_args()

    logger = configure_logging(args.log)

    record = Record()

    if args.file:
        process_file_input(args.file, record, logger)
    elif args.line:
        process_line_input(args.line, record)
    else:
        logger.error(
            "Missing --file or --line argument. No campaign data provided."
        )
        return

    if record.campaign_record:
        asyncio.run(
            parse_campaign_job(
                record.campaign_record, args.dsp, args.rate_limit, args.worker
            )
        )


def intialise_scripts():
    parser = argparse.ArgumentParser(description="Submission Campaign data")

    parser.add_argument(
        "--log",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Enabled logging",
        default="INFO",
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--line", type=str, help="Dict of campaign data to process"
    )

    group.add_argument(
        "--file", type=str, help="Full path for the CSV file of campaign data"
    )
    parser.add_argument(
        "--rate-limit",
        type=int,
        dest="rate_limit",
        help="Rate Limit to restrict the request limit to the DSP Endpoint",
        default=DEFAULT_RATE_LIMIT,
    )
    parser.add_argument(
        "--dsp",
        type=str,
        help="The URL of the DSP API endpoint.",
        default=DEFAULT_URL_API_STR,
    )
    parser.add_argument(
        "--worker",
        type=int,
        help="The number of worker to the queue..",
        default=DEFAULT_WORKER_NUM,
    )

    return parser


if __name__ == "__main__":
    try:
        main()

    except Exception as e:
        print(traceback.format_exc())
        print(f"exception caught: {e}")
