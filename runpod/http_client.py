"""
HTTP Client abstractions
"""

import os
import logging

import requests
from aiohttp import ClientSession, ClientTimeout, TCPConnector, ClientResponseError

from .cli.groups.config.functions import get_credentials
from .user_agent import USER_AGENT
from .serverless.aiohttp_client_logger import HTTPClientLogger


class TooManyRequests(ClientResponseError):
    pass


def get_auth_header():
    """
    Produce a header dict with the `Authorization` key derived from
    credentials.get("api_key") OR os.getenv('RUNPOD_AI_API_KEY')
    """
    if credentials := get_credentials():
        auth = credentials.get("api_key", "")
    else:
        auth = os.getenv("RUNPOD_AI_API_KEY", "")

    return {
        "Content-Type": "application/json",
        "Authorization": auth,
        "User-Agent": USER_AGENT,
    }


def AsyncClientSession(*args, **kwargs):  # pylint: disable=invalid-name
    """
    Deprecation from aiohttp.ClientSession forbids inheritance.
    This is now a factory method
    """
    logging.basicConfig(level=logging.DEBUG)
    http_logger = HTTPClientLogger(
        request_headers='ALWAYS',
        request_body='ALWAYS',
        response_headers='ALWAYS',
        response_body='ALWAYS'
    )
    return ClientSession(
        trace_configs=[http_logger],
        connector=TCPConnector(limit=0),
        headers=get_auth_header(),
        timeout=ClientTimeout(600, ceil_threshold=400),
        *args,
        **kwargs,
    )


class SyncClientSession(requests.Session):
    """
    Inherits requests.Session to override `request()` method for tracing
    """
    pass