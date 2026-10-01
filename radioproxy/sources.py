"""Work out what a station address is and open it as one plain audio stream.

  HLS playlist           -> HlsStream (AAC unwrapped to ADTS)
  M3U / PLS playlist     -> the first entry that works
  anything else          -> passed straight through (Icecast/Shoutcast, without
                            its inline metadata because we never ask for it)
"""

from __future__ import annotations

import logging
import re
import time
from typing import AsyncIterator, Awaitable, Callable
from urllib.parse import urljoin, urlsplit

import aiohttp

from .hls import HlsStream
from .http import FETCH_TIMEOUT, HttpError

logger = logging.getLogger("radioproxy")

STREAM_TIMEOUT = aiohttp.ClientTimeout(total=None, connect=8, sock_read=30)
_PLAYLIST_TYPES = ("mpegurl", "scpls", "pls+xml")
_PLAYLIST_EXTS = (".m3u8", ".m3u", ".pls")
_MAX_PLAYLIST_BYTES = 1_000_000
_MAX_DEPTH = 3

TUNEIN_URL = (
    "https://opml.radiotime.com/Tune.ashx"
    "?id={station}&partnerId=RadioTime&version=5.38&listenId=1"
    "&formats=mp3,aac,ogg,hls&type=station&render=json"
)
TUNEIN_ID_RE = re.compile(r"^[sgpt]\d+$")
# A TuneIn answer is a signed address good for hours. Reusing a recent one saves
# the lookup on every connect, which is most of a station's start-up time.
TUNEIN_REUSE_S = 600.0
_tunein_recent: dict[str, tuple[float, str]] = {}


class SourceError(Exception):
    pass


async def tunein_address(session: aiohttp.ClientSession, station: str, *, fresh: bool = False) -> str:
    """Where a TuneIn station streams from (a signed address).

    A recent answer is reused unless fresh is set, which a caller does when the
    address it was given has stopped working.
    """
    recent = _tunein_recent.get(station)
    if recent and not fresh and time.monotonic() - recent[0] < TUNEIN_REUSE_S:
        return recent[1]
    async with session.get(TUNEIN_URL.format(station=station), timeout=FETCH_TIMEOUT) as resp:
        if resp.status >= 400:
            raise SourceError(f"TuneIn lookup failed (HTTP {resp.status})")
        body = (await resp.json(content_type=None) or {}).get("body") or []
    entries = [
        e for e in body
        if isinstance(e, dict) and (e.get("url") or "").strip() and "notcompatible" not in e["url"].lower()
    ]
    if not entries:
        raise SourceError(f"TuneIn has no playable stream for {station}")

    def bitrate(e: dict) -> int:
        try:
            return int(e.get("bitrate") or 0)
        except (TypeError, ValueError):
            return 0

    url = max(entries, key=bitrate)["url"].strip()
    _tunein_recent[station] = (time.monotonic(), url)
    return url


class PassthroughStream:
    """A station that already is a plain stream: hand its bytes on untouched."""

    def __init__(self, resp: aiohttp.ClientResponse) -> None:
        self._resp = resp
        self._first = b""
        self.content_type = resp.headers.get("Content-Type", "audio/mpeg").split(";", 1)[0].strip()
        self.description = f"direct {self.content_type}"

    async def open(self) -> None:
        self._first = await self._resp.content.readany()
        if not self._first:
            raise SourceError("station sent no audio")

    async def chunks(self) -> AsyncIterator[bytes]:
        yield self._first
        self._first = b""
        async for chunk in self._resp.content.iter_any():
            yield chunk

    async def close(self) -> None:
        self._resp.close()


def _playlist_entries(text: str, base: str) -> list[str]:
    if text.lstrip().lower().startswith("[playlist]"):
        found = re.findall(r"(?im)^\s*file\d+\s*=\s*(\S+)", text)
    else:
        found = [line.strip() for line in text.splitlines() if line.strip() and not line.startswith("#")]
    return [urljoin(base, u) for u in found if urljoin(base, u).lower().startswith(("http://", "https://"))]


async def open_source(
    session: aiohttp.ClientSession,
    url: str,
    label: str,
    refresh: Callable[[], Awaitable[str]] | None = None,
    depth: int = 0,
):
    """Open url as a stream object: .content_type, .description, .chunks(), .close()."""
    resp = await session.get(url, timeout=STREAM_TIMEOUT)
    try:
        if resp.status >= 400:
            raise HttpError(resp.status, url)
        ctype = resp.headers.get("Content-Type", "").lower()
        path = urlsplit(str(resp.url)).path.lower()
        if not (any(t in ctype for t in _PLAYLIST_TYPES) or path.endswith(_PLAYLIST_EXTS)):
            stream = PassthroughStream(resp)
            await stream.open()
            return stream

        text = (await resp.content.read(_MAX_PLAYLIST_BYTES)).decode("utf-8", errors="replace")
        final_url = str(resp.url)
    except BaseException:
        resp.close()
        raise
    resp.close()

    if "#EXT-X-" in text:
        stream = HlsStream(session, final_url, text, label, refresh)
        await stream.open()
        return stream

    # An ordinary playlist of alternative stream addresses: first one that works.
    if depth >= _MAX_DEPTH:
        raise SourceError("playlists nested too deeply")
    entries = _playlist_entries(text, final_url)
    if not entries:
        raise SourceError("playlist contains no stream addresses")
    last: Exception | None = None
    for entry in entries:
        try:
            return await open_source(session, entry, label, None, depth + 1)
        except Exception as exc:  # try the next alternative
            last = exc
            logger.info("%s playlist entry failed (%s): %s", label, exc, entry)
    raise SourceError(f"no playlist entry worked: {last}")
