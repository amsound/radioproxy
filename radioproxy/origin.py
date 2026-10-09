"""A station's own HLS, served from here under an address that never changes.

  /hls/<tunein id>/index.m3u8    the station's live playlist, its segment lines pointing back here
  /hls/<tunein id>/seg/<name>    one segment (or the opening "map" segment), bytes untouched

This is for a player that plays HLS itself. A TuneIn station's addresses are
signed and run out (Apple's: the playlist address every six hours, the segment
addresses on six-hourly boundaries), and a player given one directly stops when
it does. Here the player only ever sees the two addresses above. Behind them
the station's playlist is re-read every couple of seconds, its newest segments
are downloaded as they appear and kept in memory, and when an address is
refused TuneIn is asked for a new one and the playlist is re-read until the
station hands out current signatures. The audio is not converted or re-timed.

There is one of these per station, shared by whoever asks. The first request
starts it and it stops after IDLE_S without one.
"""

from __future__ import annotations

import asyncio
import collections
import logging
import re
import time
from typing import Callable
from urllib.parse import urljoin, urlsplit

import aiohttp

from .hls import HlsError, _refused, is_master, parse_master, parse_media, pick_variant
from .http import HttpError, fetch
from .sources import tunein_address

logger = logging.getLogger("radioproxy")

PLAYLIST_TYPE = "application/vnd.apple.mpegurl"
IDLE_S = 90.0            # no request for this long: stop following the station
REFRESH_S = 2.0          # how often the station's playlist is re-read
READ_AHEAD = 4           # the newest segments listed are downloaded before anyone asks
KEEP_SEGMENTS = 8        # segments kept in memory (about 0.5 MB each on Apple's station)
STALE_S = 60.0           # a playlist the station last gave this long ago is no longer handed out
FIRST_WAIT_S = 15.0      # how long a request waits for the first playlist
# After a refusal and a new address, how long to keep re-reading the playlist for
# a segment address the station accepts (see RESIGN_WAIT_S in hls.py).
RESIGN_WAIT_S = 30.0
RESIGN_POLL_S = 2.0

_URI_RE = re.compile(r'(URI=")([^"]+)(")')
_NAME_RE = re.compile(r"[A-Za-z0-9._~-]{1,200}")
_SEGMENT_TYPES = {
    ".mp4": "audio/mp4", ".m4s": "audio/mp4", ".m4a": "audio/mp4", ".cmfa": "audio/mp4",
    ".ts": "video/mp2t", ".aac": "audio/aac", ".mp3": "audio/mpeg",
}


def segment_type(name: str) -> str:
    return _SEGMENT_TYPES.get("." + name.rsplit(".", 1)[-1].lower(), "application/octet-stream")


def _plain(url: str) -> str:
    """An address without its query, which is where the signature is: safe to log."""
    return url.split("?", 1)[0]


class OriginError(Exception):
    pass


class HlsOrigin:
    def __init__(self, session: aiohttp.ClientSession, station: str, on_stop: Callable[["HlsOrigin"], None]) -> None:
        self.station = station
        self.label = f"[{station} hls]"
        self._session = session
        self._on_stop = on_stop
        self._media_url: str | None = None
        self._generation = 0                 # goes up with every look-up
        self._lookup_lock = asyncio.Lock()   # one look-up at a time
        self.lookups = 0
        self.description = "HLS"
        self._body: bytes | None = None      # the playlist as handed out
        self._body_at = 0.0                  # when the station last gave it
        self._have_body = asyncio.Event()
        self._addresses: collections.OrderedDict[str, str] = collections.OrderedDict()   # name -> current address
        self._order: list[str] = []          # media segments in the latest playlist, oldest first
        self._listed: set[str] = set()       # every name in the latest playlist (opening segment too)
        self._maps: dict[str, bytes | None] = {}   # opening segments: kept for good once fetched
        self._cache: collections.OrderedDict[str, bytes] = collections.OrderedDict()
        self._locks: dict[str, asyncio.Lock] = {}
        self._first_seq = -1
        self._unread_since: float | None = None
        self._started = time.monotonic()
        self._used = self._started
        self.playlists = self.segments = 0
        self.recent: collections.deque[str] = collections.deque(maxlen=40)   # the latest requests, for /status
        self._task = asyncio.ensure_future(self._run())

    # ---- for the server ----

    async def playlist(self, peer: str) -> bytes:
        self._used = time.monotonic()
        if self._body is None:
            try:
                await asyncio.wait_for(self._have_body.wait(), FIRST_WAIT_S)
            except asyncio.TimeoutError:
                raise OriginError("the station gave no playlist") from None
        if time.monotonic() - self._body_at > STALE_S:
            raise OriginError(f"the station's playlist has not been readable for {STALE_S:g}s")
        self.playlists += 1
        self._note(peer, f"playlist ({self._order[0]} to {self._order[-1]})" if self._order else "playlist (empty)")
        return self._body

    async def segment(self, name: str, peer: str) -> bytes | None:
        """A segment's bytes, or None if the station does not (or no longer does) list it."""
        self._used = time.monotonic()
        started = time.monotonic()
        held = name in self._cache or self._maps.get(name) is not None
        data = await self._get(name) if name in self._addresses else None
        if data is None:
            self._note(peer, f"{name}: not available")
            return None
        self.segments += 1
        self._note(peer, f"{name}: {len(data)} B, " + ("from memory" if held else f"fetched in {time.monotonic() - started:.1f}s"))
        return data

    def status(self) -> dict:
        now = time.monotonic()
        return {
            "station": self.station,
            "stream": self.description,
            "running_s": round(now - self._started),
            "last_request_s_ago": round(now - self._used, 1),
            "playlist_age_s": None if self._body is None else round(now - self._body_at, 1),
            "lookups": self.lookups,
            "playlists_served": self.playlists,
            "segments_served": self.segments,
            "segments_in_memory": len(self._cache),
            "recent": list(self.recent),
        }

    async def close(self) -> None:
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)

    def _note(self, peer: str, what: str) -> None:
        self.recent.append(f"{time.strftime('%H:%M:%S')} {peer} {what}")

    # ---- following the station ----

    async def _run(self) -> None:
        reason = "radioproxy shutting down"
        try:
            while time.monotonic() - self._used < IDLE_S:
                try:
                    await self._refresh()
                    await self._read_ahead()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._unreadable(exc)
                await asyncio.sleep(REFRESH_S)
            reason = f"no requests for {IDLE_S:g}s"
        finally:
            self._on_stop(self)
            logger.info(
                "%s stopped (%s) after %.0fs: %d playlists and %d segments served, %d look-ups",
                self.label, reason, time.monotonic() - self._started, self.playlists, self.segments, self.lookups,
            )

    def _unreadable(self, exc: Exception) -> None:
        now = time.monotonic()
        if self._unread_since is None:
            self._unread_since = now
            logger.warning("%s station could not be read (%s); handing out what is in hand", self.label,
                           f"HTTP {exc.status}" if isinstance(exc, HttpError) else exc or type(exc).__name__)

    async def _look_up(self, seen: int, *, fresh: bool) -> None:
        """Ask TuneIn where the station is (unless another task just did)."""
        async with self._lookup_lock:
            if self._generation != seen:
                return
            url, text = await fetch(self._session, await tunein_address(self._session, self.station, fresh=fresh), text=True)
            if is_master(text):
                variant = pick_variant(parse_master(text, url))
                url = variant.url
                self.description = f"HLS {variant.codecs or 'aac'} {variant.bandwidth // 1000}k, served as it comes"
            elif "#EXTINF" not in text:
                raise OriginError("this station is not HLS")
            self._media_url = url
            self._generation += 1
            self.lookups += 1
        if self.lookups == 1:
            logger.info("%s started: %s from %s", self.label, self.description, _plain(url))
        else:
            logger.info("%s station looked up again (look-up #%d): %s", self.label, self.lookups, _plain(url))

    async def _refresh(self) -> None:
        """Read the station's playlist, with a new address first if this one is refused."""
        if self._media_url is None:
            await self._look_up(self._generation, fresh=False)
        seen = self._generation
        try:
            url, text = await fetch(self._session, self._media_url, text=True)
        except HttpError as exc:
            if not _refused(exc.status):
                raise
            await self._look_up(seen, fresh=True)
            url, text = await fetch(self._session, self._media_url, text=True)
        self._take(text, url)

    def _take(self, text: str, base: str) -> None:
        """Keep the playlist with every segment (and its opening segment) pointed back here."""
        parse_media(text, base)   # refuses an encrypted station
        seen: dict[str, str] = {}
        order: list[str] = []
        maps: list[str] = []
        first_seq = -1

        def local(address: str) -> str:
            address = urljoin(base, address)
            name = urlsplit(address).path.rsplit("/", 1)[-1]   # the same whichever signature it carries
            if not _NAME_RE.fullmatch(name):
                raise HlsError("a segment name the proxy cannot serve")
            seen[name] = address
            return name

        out = []
        for line in text.splitlines():
            line = line.strip()
            if line.startswith("#EXT-X-MEDIA-SEQUENCE:"):
                first_seq = int(line.split(":", 1)[1])
            elif line.startswith("#EXT-X-MAP"):
                uri = _URI_RE.search(line)
                if uri:
                    maps.append(local(uri[2]))
                    line = line[:uri.start(2)] + f"seg/{maps[-1]}" + line[uri.end(2):]
            elif line and not line.startswith("#"):
                order.append(local(line))
                line = f"seg/{order[-1]}"
            out.append(line)
        if not order:
            raise HlsError("playlist has no segments")
        if first_seq < self._first_seq:
            logger.warning("%s station's segment numbering went back (%d after %d); a player may jump",
                           self.label, first_seq, self._first_seq)
        self._first_seq = first_seq
        for name, address in seen.items():
            self._addresses[name] = address
            self._addresses.move_to_end(name)
        while len(self._addresses) > 4 * max(KEEP_SEGMENTS, len(seen)):
            gone, _ = self._addresses.popitem(last=False)
            self._locks.pop(gone, None)
        for name in maps:
            self._maps.setdefault(name, None)
        self._order, self._listed = order, set(seen)
        self._body, self._body_at = ("\n".join(out) + "\n").encode(), time.monotonic()
        self._have_body.set()
        if self._unread_since is not None:
            logger.info("%s station readable again after %.0fs", self.label, time.monotonic() - self._unread_since)
            self._unread_since = None

    async def _read_ahead(self) -> None:
        for name in [n for n, data in self._maps.items() if data is None] + self._order[-READ_AHEAD:]:
            if name not in self._cache and self._maps.get(name) is None:
                await self._get(name)

    async def _get(self, name: str) -> bytes | None:
        """A segment from memory, or downloaded (once, however many ask at the same moment)."""
        async with self._locks.setdefault(name, asyncio.Lock()):
            data = self._maps.get(name) or self._cache.get(name)
            if data is not None:
                return data
            data = await self._download(name)
            if data is None:
                return None
            if name in self._maps:
                self._maps[name] = data
            else:
                self._cache[name] = data
                while len(self._cache) > KEEP_SEGMENTS:
                    self._cache.popitem(last=False)
            return data

    async def _download(self, name: str) -> bytes | None:
        """Fetch a segment, waiting out a re-signing if its address is refused.

        None if the station no longer lists it, or it was still refused at the end.
        """
        started = time.monotonic()
        refused: int | None = None
        failures = 0
        while True:
            address, seen = self._addresses.get(name), self._generation
            if address is None:
                return None
            try:
                data = (await fetch(self._session, address))[1]
            except (aiohttp.ClientError, asyncio.TimeoutError):
                failures += 1
                if failures >= 3:
                    raise
                await asyncio.sleep(0.5 * failures)
                continue
            except HttpError as exc:
                if exc.status == 404:
                    return None
                if not _refused(exc.status):
                    raise
                if time.monotonic() - started >= RESIGN_WAIT_S:
                    logger.warning("%s segment %s still refused (HTTP %d) after %.0fs", self.label, name, exc.status, RESIGN_WAIT_S)
                    return None
                if refused is None:
                    await self._look_up(seen, fresh=True)   # one new address for a new signature
                else:
                    await asyncio.sleep(RESIGN_POLL_S)      # the station is still handing out the old one
                refused = exc.status
                try:
                    await self._refresh()
                except (HttpError, aiohttp.ClientError, asyncio.TimeoutError, HlsError):
                    continue   # keep the playlist in hand and try again
                if name not in self._listed:
                    return None   # the station has moved past it
                continue
            if refused:
                logger.info("%s segment %s refused (HTTP %d), fetched freshly signed after %.1fs",
                            self.label, name, refused, time.monotonic() - started)
            return data
