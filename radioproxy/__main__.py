"""radioproxy server.

  GET /stream/<http|https>/<host>/<path>?<query>   any station address
  GET /tunein/<station id>                          TuneIn station, e.g. /tunein/s345724
  ...?out=mp4                                       on either: send an fMP4 station as MP4 instead of AAC
  GET /status                                       who is listening and how it is going (JSON)
  GET /health

The reply is the station as one plain, never-ending HTTP audio stream. The
audio itself is never re-encoded.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import json
import logging
import os
import signal
import sys
import time

import aiohttp

from .hls import HEAD_START_SEGMENTS, RESERVE_SEGMENTS
from .http import USER_AGENT
from .listener import DATA_DIR, Listener
from .origin import PLAYLIST_TYPE, HlsOrigin, segment_type
from .sources import TUNEIN_ID_RE, open_source, tunein_address

logger = logging.getLogger("radioproxy")

PORT = int(os.environ.get("PORT", "8010"))


def _burst_seconds() -> float | None:
    """BURST_SECONDS: seconds of an HLS station sent at once on connect, then real time.

    The same setting, with the same meaning, as the restreamer's. Unset (or not
    a number) leaves the default of three segments.
    """
    raw = os.environ.get("BURST_SECONDS", "").strip()
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        logger.warning("BURST_SECONDS=%r is not a number; using the default head start", raw)
        return None


BURST_S = _burst_seconds()
REQUEST_TIMEOUT_S = 10.0
OPEN_TIMEOUT_S = 30.0
_request_ids = itertools.count(1)
_STARTED = time.monotonic()


def _build_date() -> str:
    try:
        with open("/app/BUILD_DATE", encoding="utf-8") as f:
            return f.read().strip() or "unknown"
    except OSError:
        return "unknown"


async def _reply(writer: asyncio.StreamWriter, status: str, body: str, ctype: str = "text/plain; charset=utf-8") -> None:
    data = (body + "\n").encode()
    writer.write(
        f"HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\n"
        f"Content-Length: {len(data)}\r\nConnection: close\r\n\r\n".encode() + data
    )
    await writer.drain()


# A player's own web page fetches the playlist and segments (Cast does), so they must be readable from any origin.
_CORS = (
    "Access-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, HEAD, OPTIONS\r\n"
    "Access-Control-Allow-Headers: *\r\nAccess-Control-Expose-Headers: Content-Length\r\n"
)


async def _reply_bytes(writer: asyncio.StreamWriter, data: bytes, ctype: str, *, head_only: bool = False) -> None:
    writer.write(
        f"HTTP/1.1 200 OK\r\nServer: radioproxy\r\nContent-Type: {ctype}\r\nContent-Length: {len(data)}\r\n"
        f"Cache-Control: no-cache, no-store\r\n{_CORS}Connection: close\r\n\r\n".encode() + (b"" if head_only else data)
    )
    await writer.drain()


def _hls_target(target: str) -> tuple[str, str | None] | None:
    """(tunein id, segment name or None for the playlist) for an /hls/ request, or None if it isn't one."""
    parts = target.split("?", 1)[0].split("/")
    if len(parts) < 4 or parts[1] != "hls" or not TUNEIN_ID_RE.match(parts[2].lower()):
        return None
    if parts[3:] == ["index.m3u8"]:
        return parts[2].lower(), None
    if len(parts) == 5 and parts[3] == "seg" and parts[4]:
        return parts[2].lower(), parts[4]
    return None


def _station(target: str) -> tuple[str, str | None, str, str] | None:
    """(label, tunein id or None, address, output) for a request target, or None if it isn't a stream.

    output is "mp4" if the request carries out=mp4, else "adts". That one
    parameter is ours; the rest of the query belongs to the station and is
    passed on untouched.
    """
    path, _, query = target.partition("?")
    pairs = query.split("&") if query else []
    output = "mp4" if "out=mp4" in pairs else "adts"
    query = "&".join(pair for pair in pairs if pair != "out=mp4")
    parts = path.split("/", 3)
    if len(parts) == 3 and parts[1] == "tunein" and TUNEIN_ID_RE.match(parts[2].lower()):
        return parts[2].lower(), parts[2].lower(), "", output
    if len(parts) == 4 and parts[1] == "stream" and parts[2] in ("http", "https") and parts[3]:
        address = f"{parts[2]}://{parts[3]}" + (f"?{query}" if query else "")
        return parts[3].split("/", 1)[0], None, address, output
    return None


class Server:
    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None
        self._listeners: dict[int, Listener] = {}
        self._origins: dict[str, HlsOrigin] = {}   # stations being served as HLS, by TuneIn id

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await self._handle(reader, writer)
        except (ConnectionError, asyncio.IncompleteReadError, asyncio.TimeoutError):
            pass
        except Exception:
            logger.exception("request failed")
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), REQUEST_TIMEOUT_S)
        request_line = head.split(b"\r\n", 1)[0].decode("latin-1")
        try:
            method, target, _ = request_line.split(" ", 2)
        except ValueError:
            return await _reply(writer, "400 Bad Request", "bad request")
        hls = _hls_target(target)
        if hls is not None and method in ("GET", "HEAD", "OPTIONS"):
            return await self._serve_hls(writer, method, *hls)
        if method not in ("GET", "HEAD"):
            return await _reply(writer, "405 Method Not Allowed", "GET only")
        if target.split("?", 1)[0] == "/health":
            return await _reply(writer, "200 OK", "ok")
        if target.split("?", 1)[0] == "/status":
            # The top-level fields match the restreamer's /status, so one look says what each is running.
            body = json.dumps(
                {
                    "service": "radioproxy",
                    "built": _build_date(),
                    "uptime_s": round(time.monotonic() - _STARTED),
                    "burst_s": BURST_S,   # null: the default, HEAD_START_SEGMENTS segments
                    "hls_reserve_segments": RESERVE_SEGMENTS,
                    "captures": DATA_DIR,
                    "listeners": [x.status() for x in self._listeners.values()],
                    "hls": [x.status() for x in self._origins.values()],
                },
                indent=2,
            )
            return await _reply(writer, "200 OK", body, "application/json")

        station = _station(target)
        if station is None:
            return await _reply(
                writer, "404 Not Found",
                "use /stream/<http|https>/<host>/<path>?<query> or /tunein/<station id> (add out=mp4 for MP4), "
                "or /hls/<station id>/index.m3u8 for the station's own HLS",
            )
        name, tunein_id, address, output = station
        peer = (writer.get_extra_info("peername") or ("?",))[0]
        label = f"[{name} #{next(_request_ids)} <- {peer}]"

        async def renew() -> str:
            return await tunein_address(self._session, tunein_id, fresh=True)

        async def open_station():
            if not tunein_id:
                return await open_source(self._session, address, label, output=output, burst_s=BURST_S)
            try:
                return await open_source(
                    self._session, await tunein_address(self._session, tunein_id), label, renew,
                    output=output, burst_s=BURST_S,
                )
            except Exception:
                # A reused address may have gone stale: look it up again once.
                return await open_source(self._session, await renew(), label, renew, output=output, burst_s=BURST_S)

        try:
            stream = await asyncio.wait_for(open_station(), OPEN_TIMEOUT_S)
        except Exception as exc:
            logger.warning("%s could not open: %s", label, exc or type(exc).__name__)
            return await _reply(writer, "502 Bad Gateway", f"could not open station: {exc or type(exc).__name__}")

        listener = Listener(label, name, peer, stream, writer.get_extra_info("socket"))
        stream.note = listener.note
        self._listeners[id(listener)] = listener
        reason = "listener hung up"
        logger.info("%s connected: %s", label, stream.description)
        try:
            writer.write(
                f"HTTP/1.1 200 OK\r\nServer: radioproxy\r\nContent-Type: {stream.content_type}\r\n"
                "Cache-Control: no-cache, no-store\r\nConnection: close\r\n\r\n".encode()
            )
            await writer.drain()
            if method == "HEAD":
                return
            # The listener sends nothing more, so any read ending means it hung up.
            gone = asyncio.ensure_future(reader.read())
            pump = asyncio.ensure_future(self._pump(stream, writer, listener))
            done, pending = await asyncio.wait({gone, pump}, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            if gone in done:
                gone.exception()   # a reset connection is just the listener leaving
            if pump in done:
                reason = "station ended"
                pump.result()      # re-raises if the station side failed
        except (ConnectionError, asyncio.CancelledError):
            pass
        except Exception as exc:
            reason = f"stream failed: {exc}"
            listener.note(reason)
        finally:
            self._listeners.pop(id(listener), None)
            await stream.close()
            saved = None if method == "HEAD" else listener.save(reason)
            logger.info(
                "%s disconnected (%s) after %s%s",
                label, reason, listener.summary(), f"; capture {saved}" if saved else "",
            )

    def _origin_stopped(self, origin: HlsOrigin) -> None:
        if self._origins.get(origin.station) is origin:
            del self._origins[origin.station]

    async def _serve_hls(self, writer: asyncio.StreamWriter, method: str, station: str, name: str | None) -> None:
        """The station's own HLS under an address that does not change (see origin.py)."""
        if method == "OPTIONS":
            writer.write(f"HTTP/1.1 204 No Content\r\n{_CORS}Content-Length: 0\r\nConnection: close\r\n\r\n".encode())
            return await writer.drain()
        peer = (writer.get_extra_info("peername") or ("?",))[0]
        origin = self._origins.get(station)
        if origin is None:
            if name is not None:
                return await _reply(writer, "404 Not Found", "ask for the playlist first")
            origin = self._origins[station] = HlsOrigin(self._session, station, self._origin_stopped)
        try:
            if name is None:
                return await _reply_bytes(writer, await origin.playlist(peer), PLAYLIST_TYPE, head_only=method == "HEAD")
            data = await origin.segment(name, peer)
        except Exception as exc:
            logger.warning("%s %s failed: %s", origin.label, name or "playlist", exc or type(exc).__name__)
            return await _reply(writer, "502 Bad Gateway", "the station could not be reached")
        if data is None:
            return await _reply(writer, "404 Not Found", "the station no longer has this segment")
        await _reply_bytes(writer, data, segment_type(name), head_only=method == "HEAD")

    @staticmethod
    async def _pump(stream, writer: asyncio.StreamWriter, listener: Listener) -> None:
        async for chunk in stream.chunks():
            writer.write(chunk)
            waiting = time.monotonic()
            await writer.drain()
            listener.record(chunk, stream.info, time.monotonic() - waiting)

    async def run(self) -> None:
        # Keep idle connections to stations open, so the next listener skips the TLS setup.
        self._session = aiohttp.ClientSession(
            headers={"User-Agent": USER_AGENT},
            connector=aiohttp.TCPConnector(keepalive_timeout=120, limit_per_host=16),
        )
        server = await asyncio.start_server(self.handle, "0.0.0.0", PORT, limit=64 * 1024)
        logger.info(
            "listening on :%d (built %s, python %s, HLS head start %s then real time)",
            PORT, _build_date(), sys.version.split()[0],
            f"{HEAD_START_SEGMENTS} segments" if BURST_S is None else f"{BURST_S:g}s",
        )
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
        async with server:
            await stop.wait()
        for origin in list(self._origins.values()):
            await origin.close()
        await self._session.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    asyncio.run(Server().run())


if __name__ == "__main__":
    main()
