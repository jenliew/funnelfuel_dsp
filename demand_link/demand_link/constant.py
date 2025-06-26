from enum import Enum


RATE_LIMIT = 40  # Max requests per minute
URL_API_STR = "127.0.0.1:8000"


class API_REQUEST(Enum):
    GET = 1
    POST = 2
