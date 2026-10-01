"""Small HTTP helpers for talking to stations."""

from __future__ import annotations

import aiohttp

USER_AGENT = "radioproxy/1.0"
FETCH_TIMEOUT = aiohttp.ClientTimeout(total=20, connect=8)


class HttpError(Exception):
    def __init__(self, status: int, url: str) -> None:
        super().__init__(f"HTTP {status}")
        self.status = status
        self.url = url


async def fetch(session: aiohttp.ClientSession, url: str, *, text: bool = False):
    """GET a whole (small) resource. Returns (final url after redirects, body)."""
    async with session.get(url, timeout=FETCH_TIMEOUT) as resp:
        if resp.status >= 400:
            raise HttpError(resp.status, url)
        body = await (resp.text(errors="replace") if text else resp.read())
        return str(resp.url), body
