import argparse
import asyncio
import json
import logging
import os
import traceback
from csv import DictReader

from demand_link.demand_link.constant import (
    DEFAULT_RATE_LIMIT,
    DEFAULT_URL_API_STR,
    DEFAULT_WORKER_NUM,
)
from demand_link.demand_link.exception import SubmissionError
from demand_link.demand_link.operational import WorkerManager
from demand_link.demand_link.record import Record
from demand_link.demand_link.utils import (
    convert_str_dsp_record,
)


def configure_logging(log_level: str):
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format=(
            "%(asctime)s - %(levelname)s - %(threadName)s "
            "- %(name)s - %(message)s"
        ),
    )
    return logging.getLogger("dsp_submission")


def process_file_input(file_path: str, record, logger):
    if not os.path.isfile(file_path):
        logger.error(f"Input file not found: {file_path}")
        raise FileNotFoundError(f"{file_path} does not exist.")

    with open(file_path) as input_obj:
        for row in DictReader(input_obj):
            try:
                convert_str_dsp_record(record, row)
            except SubmissionError as e:
                raise e


def process_line_input(line_str: str, record):
    try:
        data = json.loads(line_str)
        convert_str_dsp_record(record, data)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON input for --line: {e}")
    except SubmissionError as e:
        raise e


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

    logger.info(
        "Successsfully process input data. Preparing "
        "to submit the jobs to the queue."
    )

    if record.campaign_record:
        operator_worker = WorkerManager(
            record.campaign_record, args.endpoint, args.rate_limit, args.worker
        )

        asyncio.run(operator_worker.start())


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
        "--endpoint",
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
