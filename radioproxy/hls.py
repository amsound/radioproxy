"""HLS client: follow a live playlist and hand back plain ADTS audio, segment by segment."""

from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass
from typing import AsyncIterator, Awaitable, Callable
from urllib.parse import urljoin

import aiohttp

from . import mp4
from .adts import SAMPLE_RATES, AacError, AdtsFramer, frame_len, strip_id3
from .http import HttpError, fetch
from .ts import TsDemuxer, TsError, looks_like_ts

logger = logging.getLogger("radioproxy")

# Start up to this many segments back from live (fewer on a short playlist).
#
# The first ones are sent at once: the listener's head start. Three segments
# where the playlist allows, as the HLS spec asks players to hold: ~12 s on 4 s
# segments, ~48 s on Apple's 16 s ones.
#
# The last RESERVE_SEGMENTS are not: from there on audio is fed out at real time
# in small slices, so radioproxy always has a segment in hand and the connection
# is never silent while the station gets round to publishing its next one. A
# silent connection makes some players hang up (Audio Pro/LinkPlay: after 15 s,
# and Apple publishes 16 s segments, irregularly).
START_SEGMENTS_BACK = 5
RESERVE_SEGMENTS = 2
SLICE_S = 0.5
RELOAD_S = 1.0   # how often to look for the next segment once the current one is sent
# A live playlist that stays broken this long ends the stream.
GIVE_UP_AFTER_S = 60.0
# After a new address, how long to keep asking for a playlist whose segments are
# signed afresh. Apple's segment signatures all run out together on a six-hourly
# boundary, and for a few seconds after it the playlists it serves still carry
# the old ones. Shorter than the listener's head start, so it never hears a gap.
RESIGN_WAIT_S = 30.0
RESIGN_POLL_S = 2.0

_ATTR_RE = re.compile(r'([A-Z0-9-]+)=("[^"]*"|[^,]*)')
_AAC_LC = "mp4a.40.2"
_AAC_CODECS = ("mp4a.40.2", "mp4a.40.5", "mp4a.40.29")


def _refused(status: int) -> bool:
    """A 4xx answer: the address itself is turned down (signed addresses end this way)."""
    return 400 <= status < 500 and status not in (408, 429)


class HlsError(Exception):
    pass


def _attrs(line: str) -> dict[str, str]:
    return {k: v.strip('"') for k, v in _ATTR_RE.findall(line.split(":", 1)[1])}


@dataclass(frozen=True)
class Variant:
    url: str
    bandwidth: int
    codecs: str


@dataclass(frozen=True)
class Segment:
    seq: int
    url: str
    duration: float
    map_url: str | None
    discontinuity: bool = False       # the station flagged a break before this segment
    program_time: str | None = None   # broadcast clock time, when the station gives it


@dataclass(frozen=True)
class ChunkInfo:
    """About the piece of audio a stream has just handed over."""

    segment: int | None = None
    duration: float | None = None
    download_s: float | None = None
    program_time: str | None = None


@dataclass(frozen=True)
class MediaPlaylist:
    target: float
    segments: list[Segment]
    ended: bool


def is_master(text: str) -> bool:
    return "#EXT-X-STREAM-INF" in text


def parse_master(text: str, base: str) -> list[Variant]:
    variants = []
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for i, line in enumerate(lines):
        if line.startswith("#EXT-X-STREAM-INF") and i + 1 < len(lines) and not lines[i + 1].startswith("#"):
            a = _attrs(line)
            variants.append(Variant(urljoin(base, lines[i + 1]), int(a.get("BANDWIDTH") or 0), a.get("CODECS", "")))
    return variants


def pick_variant(variants: list[Variant]) -> Variant:
    """The highest-bitrate AAC-LC version; failing that, the highest AAC of any kind."""
    if not variants:
        raise HlsError("master playlist lists no streams")
    audio = [v for v in variants if not v.codecs or any(c in v.codecs for c in _AAC_CODECS)]
    if not audio:
        raise HlsError(f"no AAC stream on offer (codecs: {sorted({v.codecs for v in variants})})")
    lc = [v for v in audio if _AAC_LC in v.codecs]
    return max(lc or audio, key=lambda v: v.bandwidth)


def parse_media(text: str, base: str) -> MediaPlaylist:
    target, seq, duration, map_url, ended = 0.0, 0, 0.0, None, False
    discontinuity, program_time = False, None
    segments: list[Segment] = []
    for line in (raw.strip() for raw in text.splitlines()):
        if not line:
            continue
        if line.startswith("#EXT-X-TARGETDURATION:"):
            target = float(line.split(":", 1)[1])
        elif line.startswith("#EXT-X-MEDIA-SEQUENCE:"):
            seq = int(line.split(":", 1)[1])
        elif line.startswith("#EXTINF:"):
            duration = float(line.split(":", 1)[1].split(",", 1)[0])
        elif line.startswith("#EXT-X-MAP:"):
            map_url = urljoin(base, _attrs(line)["URI"])
        elif line.startswith("#EXT-X-KEY:"):
            if _attrs(line).get("METHOD", "NONE") != "NONE":
                raise HlsError("stream is encrypted")
        elif line.startswith("#EXT-X-ENDLIST"):
            ended = True
        elif line.startswith("#EXT-X-DISCONTINUITY") and not line.startswith("#EXT-X-DISCONTINUITY-SEQUENCE"):
            discontinuity = True
        elif line.startswith("#EXT-X-PROGRAM-DATE-TIME:"):
            program_time = line.split(":", 1)[1]
        elif not line.startswith("#"):
            segments.append(Segment(seq, urljoin(base, line), duration, map_url, discontinuity, program_time))
            seq += 1
            discontinuity, program_time = False, None
    return MediaPlaylist(target or 6.0, segments, ended)


def _slices(data: bytes, seconds: float):
    """Cut ADTS audio at frame boundaries: (offset in seconds, bytes) for each ~seconds of it."""
    rate = SAMPLE_RATES[(data[2] >> 2) & 0x0F]
    per_slice = max(1, int(seconds * rate / 1024))
    i, start, frames, done, n = 0, 0, 0, 0, len(data)
    while i + 7 <= n:
        length = frame_len(data, i)
        if not length or i + length > n:
            break
        i += length
        frames += 1
        if frames == per_slice:
            yield done * 1024 / rate, data[start:i]
            done += frames
            start, frames = i, 0
    if start < n:
        yield done * 1024 / rate, data[start:]


class HlsStream:
    """One listener's view of an HLS station, as ADTS bytes."""

    content_type = "audio/aac"

    def __init__(
        self,
        session: aiohttp.ClientSession,
        url: str,
        text: str,
        label: str,
        refresh: Callable[[], Awaitable[str]] | None = None,
    ) -> None:
        self._session = session
        self._label = label
        self._refresh = refresh        # gets a fresh address when a signed one expires
        self._first_url = url
        self._first_text = text
        self._media_url = url
        self._next_seq = 0
        self._pending: list[Segment] = []
        self._first: bytes = b""
        self._target = 6.0
        self._init: mp4.Init | None = None
        self._init_url: str | None = None
        self._ts = TsDemuxer()
        self._framer = AdtsFramer()
        self._download_s = 0.0
        self._renewed: dict[int, Segment] = {}   # segments as listed under the newest signed address
        self._due: float | None = None   # when the next paced segment's audio is due to start going out
        self.description = ""
        self.info = ChunkInfo()
        # Called with a line of text when something notable happens to the stream.
        self.note: Callable[[str], None] = lambda text: logger.info("%s %s", label, text)

    async def open(self) -> None:
        playlist = await self._select(self._first_url, self._first_text)
        if not playlist.segments:
            raise HlsError("playlist has no segments")
        back = min(START_SEGMENTS_BACK, max(1, len(playlist.segments) - 1))
        start = playlist.segments[-back:]
        self._burst = max(1, len(start) - RESERVE_SEGMENTS)
        self._next_seq = start[0].seq
        self._pending = list(start)
        # Convert the first segment now, so a stream we can't handle fails before
        # the listener is told "200 OK".
        first = self._pending.pop(0)
        self._first = await self._convert(first)
        self._first_info = self._info(first)
        head_start = sum(s.duration for s in start[:self._burst])
        self.description += f", {playlist.target:g}s segments, {head_start:.0f}s head start"

    async def _select(self, url: str, text: str) -> MediaPlaylist:
        """Settle on a media playlist (choosing a quality if given a master)."""
        if is_master(text):
            variant = pick_variant(parse_master(text, url))
            self._media_url, text = await fetch(self._session, variant.url, text=True)
            self.description = f"HLS {variant.codecs or 'aac'} {variant.bandwidth // 1000}k"
        else:
            self._media_url = url
            self.description = "HLS"
        playlist = parse_media(text, self._media_url)
        self._target = playlist.target
        return playlist

    async def chunks(self) -> AsyncIterator[bytes]:
        self.info = self._first_info
        yield self._first
        self._first = b""
        burst = self._burst - 1   # the first segment has just gone
        for segment in self._pending[:burst]:
            data = await self._convert(segment)
            self.info = self._info(segment)
            yield data
        for segment in self._pending[burst:]:
            async for piece in self._paced(segment):
                yield piece
        self._pending = []

        failing_since: float | None = None
        while True:
            try:
                playlist = await self._reload()
                failing_since = None
            except (HttpError, aiohttp.ClientError, asyncio.TimeoutError, HlsError) as exc:
                now = time.monotonic()
                failing_since = failing_since or now
                if now - failing_since > GIVE_UP_AFTER_S:
                    raise HlsError(f"playlist unavailable for {GIVE_UP_AFTER_S:g}s: {exc}") from exc
                self.note(f"playlist reload failed ({exc}); retrying")
                await asyncio.sleep(2.0)
                continue

            new = [s for s in playlist.segments if s.seq >= self._next_seq]
            if playlist.segments and playlist.segments[0].seq > self._next_seq:
                self.note(f"fell behind: skipped {playlist.segments[0].seq - self._next_seq} segments")
            for segment in new:
                async for piece in self._paced(segment):
                    yield piece
            if playlist.ended and not new:
                return
            if not new:
                await asyncio.sleep(RELOAD_S)

    async def _paced(self, segment: Segment) -> AsyncIterator[bytes]:
        """One segment's audio, released at real time against a running schedule.

        The schedule is absolute, so time spent downloading or waiting for the
        station is made up rather than accumulating. A segment that is already
        more than its own length late is sent at once.
        """
        data = await self._convert(segment)
        if not data:
            return
        self.info = self._info(segment)
        now = time.monotonic()
        start = now if self._due is None else max(self._due, now - segment.duration)
        self._due = start + segment.duration
        for offset_s, piece in _slices(data, SLICE_S):
            delay = start + offset_s - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            yield piece

    async def _reload(self) -> MediaPlaylist:
        try:
            _, text = await fetch(self._session, self._media_url, text=True)
        except HttpError as exc:
            if self._refresh is None or not _refused(exc.status):
                raise
            return await self._renew(exc.status)
        return parse_media(text, self._media_url)

    async def _resigned(self, segment: Segment, refused: HttpError) -> tuple[Segment, bytes]:
        """A segment's address was turned down: get a new address, then the same
        segment from a playlist whose signatures are current.

        Right after Apple's signatures run out, even a new address can list the
        old ones for a few seconds, so the playlist is asked again until a
        segment is accepted or RESIGN_WAIT_S passes.
        """
        give_up = time.monotonic() + RESIGN_WAIT_S
        try:
            playlist = await self._renew(refused.status)
        except (HttpError, aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise HlsError(f"segment download failed: {refused}; no new address: {exc}") from exc
        tries = 0
        while True:
            current = next((s for s in playlist.segments if s.seq >= segment.seq), None)
            if current is not None:
                try:
                    data = (await fetch(self._session, current.url))[1]
                    if tries:
                        self.note(f"segment {current.seq} accepted after {tries} more playlist reloads")
                    return current, data
                except (HttpError, aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    refused = exc if isinstance(exc, HttpError) else refused
                    last = exc
            else:
                last = HlsError(f"segment {segment.seq} no longer listed")
            if time.monotonic() + RESIGN_POLL_S > give_up:
                raise HlsError(f"segment download failed for {RESIGN_WAIT_S:g}s after a new address: {last}")
            await asyncio.sleep(RESIGN_POLL_S)
            tries += 1
            try:
                _, text = await fetch(self._session, self._media_url, text=True)
                playlist = parse_media(text, self._media_url)
                self._renewed = {s.seq: s for s in playlist.segments}
            except (HttpError, aiohttp.ClientError, asyncio.TimeoutError, HlsError):
                pass   # keep the playlist we have and try again

    async def _renew(self, status: int) -> MediaPlaylist:
        """A signed address ran out: get a new one and carry on from the same segment."""
        self.note(f"address expired (HTTP {status}); fetching a new one")
        url = await self._refresh()
        url, text = await fetch(self._session, url, text=True)
        playlist = await self._select(url, text)
        # Segments already queued still carry the old signature; these replace them.
        self._renewed = {s.seq: s for s in playlist.segments}
        return playlist

    async def _convert(self, segment: Segment) -> bytes:
        """Download one segment and return its audio as whole ADTS frames."""
        started = time.monotonic()
        segment = self._renewed.get(segment.seq, segment)
        try:
            data = await self._download(segment.url)
        except HttpError as exc:
            # The segment's own address was turned down: the playlist's signature has
            # run out. Take the same segment from a freshly signed playlist.
            segment, data = await self._resigned(segment, exc)
        took = self._download_s = time.monotonic() - started
        if segment.duration and took > segment.duration:
            self.note(f"slow segment {segment.seq}: {took:.1f}s to download {segment.duration:.0f}s of audio")
        if segment.discontinuity:
            self.note(f"station flagged a discontinuity before segment {segment.seq}")
        self._next_seq = segment.seq + 1
        try:
            if segment.map_url:
                if segment.map_url != self._init_url:
                    init = mp4.parse_init(await self._download(segment.map_url, renewable=False))
                    if self._init is not None and init.config != self._init.config:
                        self.note(
                            f"station changed its audio description at segment {segment.seq}: "
                            f"{self._init.config} -> {init.config}"
                        )
                    self._init = init
                    self._init_url = segment.map_url
                return mp4.to_adts(data, self._init)
            if looks_like_ts(data):
                return self._framer.feed(self._ts.feed(data))
            return self._framer.feed(strip_id3(data))
        except (mp4.Mp4Error, TsError, AacError) as exc:
            raise HlsError(f"cannot read segment {segment.seq}: {exc}") from exc

    def _info(self, segment: Segment) -> ChunkInfo:
        return ChunkInfo(segment.seq, segment.duration, self._download_s, segment.program_time)

    async def _download(self, url: str, *, renewable: bool = True) -> bytes:
        """Fetch one file, trying three times. A refusal of a signed address is
        raised as HttpError at once (retrying it cannot help) for the caller to renew."""
        last: Exception | None = None
        for attempt in range(3):
            try:
                return (await fetch(self._session, url))[1]
            except (HttpError, aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if (renewable and self._refresh is not None
                        and isinstance(exc, HttpError) and _refused(exc.status)):
                    raise
                last = exc
                await asyncio.sleep(0.5 * (attempt + 1))
        raise HlsError(f"segment download failed: {last}")

    async def close(self) -> None:
        pass
