"""Small HTTP helpers for talking to stations."""

from __future__ import annotations

import asyncio

import aiohttp

USER_AGENT = "radioproxy/1.0"
FETCH_TIMEOUT = aiohttp.ClientTimeout(total=20, connect=8)
# A live playlist is small and normally answered at once, so a request that hangs
# is given up on early and asked again.
PLAYLIST_TIMEOUT = aiohttp.ClientTimeout(total=6, connect=4)


class HttpError(Exception):
    def __init__(self, status: int, url: str) -> None:
        super().__init__(f"HTTP {status}")
        self.status = status
        self.url = url


def why(exc: BaseException) -> str:
    """An error in a few plain words for the log, never with the address in it.

    aiohttp's own messages quote the whole address, and a signed address carries
    its key. Our own errors (HlsError and the like) are already plain text.
    """
    if isinstance(exc, HttpError):
        return f"HTTP {exc.status}"
    if isinstance(exc, getattr(aiohttp, "ConnectionTimeoutError", ())):
        return "no connection in time"
    if isinstance(exc, asyncio.TimeoutError):
        return "no answer in time"
    if isinstance(exc, aiohttp.ClientConnectorError):
        return "could not connect"
    if isinstance(exc, aiohttp.ServerDisconnectedError):
        return "the station closed the connection"
    if isinstance(exc, aiohttp.ClientPayloadError):
        return "the answer was cut short"
    if isinstance(exc, aiohttp.ClientError):
        return type(exc).__name__
    return str(exc) or type(exc).__name__


async def fetch(session: aiohttp.ClientSession, url: str, *, text: bool = False, timeout: aiohttp.ClientTimeout = FETCH_TIMEOUT):
    """GET a whole (small) resource. Returns (final url after redirects, body)."""
    async with session.get(url, timeout=timeout) as resp:
        if resp.status >= 400:
            raise HttpError(resp.status, url)
        body = await (resp.text(errors="replace") if text else resp.read())
        return str(resp.url), body
