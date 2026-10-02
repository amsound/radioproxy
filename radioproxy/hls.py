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
from .adts import AacError, AdtsFramer, strip_id3
from .http import HttpError, fetch
from .ts import TsDemuxer, TsError, looks_like_ts

logger = logging.getLogger("radioproxy")

# Start this many segments back from live, as the HLS spec asks of players. It is
# the listener's head start: ~12 s on 4 s segments, ~48 s on Apple's 16 s ones.
START_SEGMENTS_BACK = 3
RELOAD_MIN_S = 1.0
RELOAD_MAX_S = 6.0
# A live playlist that stays broken this long ends the stream.
GIVE_UP_AFTER_S = 60.0

_ATTR_RE = re.compile(r'([A-Z0-9-]+)=("[^"]*"|[^,]*)')
_AAC_LC = "mp4a.40.2"
_AAC_CODECS = ("mp4a.40.2", "mp4a.40.5", "mp4a.40.29")


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
        self.description = ""
        self.info = ChunkInfo()
        # Called with a line of text when something notable happens to the stream.
        self.note: Callable[[str], None] = lambda text: logger.info("%s %s", label, text)

    async def open(self) -> None:
        playlist = await self._select(self._first_url, self._first_text)
        if not playlist.segments:
            raise HlsError("playlist has no segments")
        start = playlist.segments[-START_SEGMENTS_BACK:]
        self._next_seq = start[0].seq
        self._pending = list(start)
        # Convert the first segment now, so a stream we can't handle fails before
        # the listener is told "200 OK".
        first = self._pending.pop(0)
        self._first = await self._convert(first)
        self._first_info = self._info(first)
        head_start = sum(s.duration for s in start)
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
        for segment in self._pending:
            data = await self._convert(segment)
            self.info = self._info(segment)
            yield data
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
                data = await self._convert(segment)
                if data:
                    self.info = self._info(segment)
                    yield data
            if playlist.ended and not new:
                return
            await asyncio.sleep(min(RELOAD_MAX_S, max(RELOAD_MIN_S, self._target / 2)))

    async def _reload(self) -> MediaPlaylist:
        try:
            _, text = await fetch(self._session, self._media_url, text=True)
        except HttpError as exc:
            if self._refresh is None or exc.status not in (401, 403, 404, 410):
                raise
            # A signed address ran out: get a new one and carry on from the same segment.
            self.note(f"address expired (HTTP {exc.status}); fetching a new one")
            url = await self._refresh()
            url, text = await fetch(self._session, url, text=True)
            return await self._select(url, text)
        return parse_media(text, self._media_url)

    async def _convert(self, segment: Segment) -> bytes:
        """Download one segment and return its audio as whole ADTS frames."""
        started = time.monotonic()
        data = await self._download(segment.url)
        took = self._download_s = time.monotonic() - started
        if segment.duration and took > segment.duration:
            self.note(f"slow segment {segment.seq}: {took:.1f}s to download {segment.duration:.0f}s of audio")
        if segment.discontinuity:
            self.note(f"station flagged a discontinuity before segment {segment.seq}")
        self._next_seq = segment.seq + 1
        try:
            if segment.map_url:
                if segment.map_url != self._init_url:
                    init = mp4.parse_init(await self._download(segment.map_url))
                    if self._init is not None:
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

    async def _download(self, url: str) -> bytes:
        last: Exception | None = None
        for attempt in range(3):
            try:
                return (await fetch(self._session, url))[1]
            except (HttpError, aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last = exc
                await asyncio.sleep(0.5 * (attempt + 1))
        raise HlsError(f"segment download failed: {last}")

    async def close(self) -> None:
        pass
