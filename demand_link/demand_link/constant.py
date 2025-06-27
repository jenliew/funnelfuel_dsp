from enum import Enum

DEFAULT_RATE_LIMIT = 40  # Max requests per minute
DEFAULT_URL_API_STR = "127.0.0.1:8000"
MAX_RETRIES = 3
HTTP_TIMEOUT = 30  # Timeout 30 seconds


class API_REQUEST(Enum):
    GET = 1
    POST = 2
