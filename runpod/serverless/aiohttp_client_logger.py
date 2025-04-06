import logging
import time
from typing import Optional

import aiohttp.log
from runpod.serverless.modules.rp_logger import RunPodLogger

log = RunPodLogger()


def shorten(data: bytes, max_len: int, placeholder: bytes = b'') -> bytes:
    if max_len and len(data) > max_len:
        data = data[:max_len] + placeholder

    return data


class HTTPClientLogger(aiohttp.TraceConfig):
    """
    aiohttp http request logger.
    """

    def __init__(
        self,
        *args,
        request_headers: Optional[str] = None,
        request_body: Optional[str] = None,
        request_body_max_len: int = 0,
        response_headers: Optional[str] = None,
        response_body: Optional[str] = None,
        response_body_max_len: int = 20000,
        request_loglevel=logging.DEBUG,
        content_loglevel=logging.DEBUG,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._request_headers = request_headers
        self._request_body = request_body
        self._request_body_max_len = request_body_max_len
        self._response_headers = response_headers
        self._response_body = response_body
        self._response_body_max_len = response_body_max_len
        self._request_loglevel = request_loglevel
        self._content_loglevel = content_loglevel

        self.on_request_start.append(self.prepare)

        self.on_request_end.append(self.store_request_info)
        self.on_request_chunk_sent.append(self.store_request_body)
        self.on_request_end.append(self.log_request)

        self.on_request_end.append(self.store_response_info)
        self.on_response_chunk_received.append(self.store_response_body)
        self.on_response_chunk_received.append(self.log_response)

    async def prepare(self, session, trace_config_ctx, params):
        trace_config_ctx.started_at = time.time()
        trace_config_ctx.request_chunks = []
        trace_config_ctx.response_chunks = []

    async def store_request_info(self, session, trace_config_ctx, params):
        request = params.response.request_info

        trace_config_ctx.method = request.method
        trace_config_ctx.url = request.url
        trace_config_ctx.request_headers = request.headers

    async def store_request_body(self, session, trace_config_ctx, params):
        if self._request_body:
            trace_config_ctx.request_chunks.append(params.chunk)

    async def log_request(self, session, trace_config_ctx, params):
        log.log("%s %s" % (trace_config_ctx.method, trace_config_ctx.url), "ERROR")

    async def store_response_info(self, session, trace_config_ctx, params):
        trace_config_ctx.status = params.response.status
        trace_config_ctx.reason = params.response.reason
        trace_config_ctx.response_headers = params.response.headers

    async def store_response_body(self, session, trace_config_ctx, params):
        if self._response_body:
            trace_config_ctx.response_chunks.append(params.chunk)

    async def log_response(self, session, trace_config_ctx, params):
        log.log(
            "%s %s - %s %.3f sec" % (
                trace_config_ctx.method,
                trace_config_ctx.url,
                trace_config_ctx.status,
                time.time() - trace_config_ctx.started_at
            ), "ERROR"
        )
        is_error_status = trace_config_ctx.status >= 400

        if self._request_headers == 'ALWAYS' or self._request_headers == 'ON_ERROR' and is_error_status:
            log.log(
                "%s %s - %s\nreq headers:\n%r" % (
                trace_config_ctx.method, trace_config_ctx.url, trace_config_ctx.status,
                trace_config_ctx.request_headers,
                ), "ERROR"
            )

        if self._request_body == 'ALWAYS' or self._request_body == 'ON_ERROR' and is_error_status:
            log.log(
                "%s %s - %s\nreq body:\n%r" % (
                trace_config_ctx.method, trace_config_ctx.url, trace_config_ctx.status,
                shorten(b''.join(trace_config_ctx.request_chunks), self._request_body_max_len, b'[...]')), "ERROR"
            )

        if self._response_headers == 'ALWAYS' or self._response_headers == 'ON_ERROR' and is_error_status:
            log.log(
                "%s %s - %s\nresp headers:\n%r" % (
                trace_config_ctx.method, trace_config_ctx.url, trace_config_ctx.status,
                trace_config_ctx.response_headers), "ERROR"
            )

        if self._response_body == 'ALWAYS' or self._response_body == 'ON_ERROR' and is_error_status:
            log.log(
                "%s %s - %s\nresp body:\n%r" % (
                trace_config_ctx.method, trace_config_ctx.url, trace_config_ctx.status,
                shorten(b''.join(trace_config_ctx.response_chunks), self._response_body_max_len, b'[...]')), "ERROR"
            )
