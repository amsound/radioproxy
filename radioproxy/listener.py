"""What one listener was sent, kept so a failure can be examined afterwards.

For each listener this keeps the last few minutes of audio exactly as sent, a
timeline of each piece (with readings from the listener's own TCP connection),
and anything notable that happened. When the listener leaves, the audio and a
plain-text report are written to the data folder.
"""

from __future__ import annotations

import collections
import fcntl
import logging
import os
import socket
import struct
import termios
import time
from dataclasses import dataclass

from .adts import AdtsFramer, scan

logger = logging.getLogger("radioproxy")

DATA_DIR = os.environ.get("DATA_DIR", "/data")
CAPTURE_SECONDS = 300.0           # how much recent audio to keep per listener
CAPTURE_MAX_BYTES = 32 * 1024 * 1024
KEEP_CAPTURES = 12                # oldest captures are deleted beyond this
ROW_EVERY_S = 10.0                # timeline rows for a continuous (non-HLS) stream
_AAC_TYPES = ("audio/aac", "audio/aacp")
_EXTENSIONS = {
    "audio/aac": "aac", "audio/aacp": "aac", "audio/mp4": "mp4",
    "audio/mpeg": "mp3", "audio/ogg": "ogg", "audio/flac": "flac",
}


def _clock(t: float) -> str:
    return time.strftime("%H:%M:%S", time.localtime(t))


@dataclass
class Row:
    at: float
    segment: int | None
    audio_s: float | None
    size: int
    download_s: float | None
    blocked_s: float
    unsent: int | None
    rtt_ms: float | None
    retransmits: int | None
    program_time: str | None


class Listener:
    def __init__(self, label: str, station: str, peer: str, stream, sock) -> None:
        self.label = label
        self.station = station
        self.peer = peer
        self.description = stream.description
        self.content_type = stream.content_type
        self._stream = stream
        self._sock = sock
        self.started = time.time()
        self._started_mono = time.monotonic()
        self.sent = 0
        # Audio is counted from the ADTS frames themselves, or (MP4) as the stream reports it.
        self.audio_s: float | None = 0.0 if self.content_type in (*_AAC_TYPES, "audio/mp4") else None
        self.blocked_s = 0.0
        self._counter = AdtsFramer() if self.content_type in _AAC_TYPES else None
        self._format: tuple[int, int, int] | None = None
        self._audio: collections.deque[tuple[float, bytes]] = collections.deque()
        self._audio_bytes = 0
        self.rows: collections.deque[Row] = collections.deque(maxlen=400)
        self.events: list[tuple[float, str]] = []
        self._pending_row: Row | None = None

    # ---- while streaming ----

    def note(self, text: str) -> None:
        """Something notable: log it and keep it for the report."""
        logger.warning("%s %s", self.label, text)
        self.events.append((time.time(), text))

    def record(self, data: bytes, info, blocked_s: float) -> None:
        """One piece of audio has been handed to the listener's connection."""
        now = time.time()
        self.sent += len(data)
        self.blocked_s += blocked_s
        audio_s = None
        if self._counter is not None:
            frames, formats = scan(self._counter.feed(data))
            for fmt in formats:
                if self._format is not None and fmt != self._format:
                    self.note(f"audio format changed mid-stream: {self._describe(self._format)} -> {self._describe(fmt)}")
                self._format = fmt
            if self._format:
                audio_s = frames * 1024 / self._format[1]
                self.audio_s += audio_s
        elif self.audio_s is not None and info.piece_s is not None:
            audio_s = info.piece_s
            self.audio_s += audio_s

        self._audio.append((time.monotonic(), data))
        self._audio_bytes += len(data)
        horizon = time.monotonic() - CAPTURE_SECONDS
        while self._audio and (self._audio[0][0] < horizon or self._audio_bytes > CAPTURE_MAX_BYTES):
            self._audio_bytes -= len(self._audio.popleft()[1])

        unsent, rtt_ms, retransmits = self._tcp()
        row = Row(now, info.segment, audio_s, len(data), info.download_s, blocked_s, unsent, rtt_ms, retransmits, info.program_time)
        if info.segment is not None:
            last = self.rows[-1] if self.rows else None
            if last is not None and last.segment == info.segment:
                # Another slice of the same segment: one timeline row per segment.
                last.size += row.size
                last.blocked_s += row.blocked_s
                if last.audio_s is not None and row.audio_s is not None:
                    last.audio_s += row.audio_s
                last.unsent, last.rtt_ms, last.retransmits = unsent, rtt_ms, retransmits
            else:
                self.rows.append(row)
            return
        # A continuous stream arrives in many small pieces: one row per ROW_EVERY_S.
        pending = self._pending_row
        if pending is None:
            self._pending_row = row
        else:
            pending.size += row.size
            pending.blocked_s += row.blocked_s
            if pending.audio_s is not None and row.audio_s is not None:
                pending.audio_s += row.audio_s
            pending.unsent, pending.rtt_ms, pending.retransmits = unsent, rtt_ms, retransmits
            if now - pending.at >= ROW_EVERY_S:
                self.rows.append(pending)
                self._pending_row = None

    @staticmethod
    def _describe(fmt: tuple[int, int, int]) -> str:
        return f"AAC profile {fmt[0] + 1}, {fmt[1]} Hz, {fmt[2]} ch"

    def _tcp(self) -> tuple[int | None, float | None, int | None]:
        """From the listener's connection: bytes it has not yet accepted, round trip (ms), total retransmits."""
        try:
            fd = self._sock.fileno()
            unsent = struct.unpack("i", fcntl.ioctl(fd, termios.TIOCOUTQ, b"\0\0\0\0"))[0]
            info = struct.unpack("8B24I", self._sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_INFO, 104))
            return unsent, info[8 + 15] / 1000.0, info[8 + 23]
        except (OSError, AttributeError, struct.error, ValueError):
            return None, None, None

    # ---- status ----

    def lead_s(self) -> float | None:
        """Audio sent beyond real time since connecting: roughly what the player is holding."""
        if self.audio_s is None:
            return None
        return self.audio_s - (time.monotonic() - self._started_mono)

    def status(self) -> dict:
        unsent, rtt_ms, retransmits = self._tcp()
        lead = self.lead_s()
        last = self.rows[-1] if self.rows else None
        return {
            "station": self.station,
            "listener": self.peer,
            "stream": self.description,
            "connected_at": self.started,
            "connected_s": round(time.monotonic() - self._started_mono),
            "sent_mb": round(self.sent / 1e6, 1),
            "audio_sent_s": None if self.audio_s is None else round(self.audio_s),
            "listener_holding_s": None if lead is None else round(lead),
            "not_yet_accepted_bytes": unsent,
            "round_trip_ms": rtt_ms,
            "retransmits": retransmits,
            "blocked_s": round(self.blocked_s, 1),
            "last_segment": last.segment if last else None,
            "events": [f"{_clock(at)} {text}" for at, text in self.events[-20:]],
        }

    # ---- when the listener leaves ----

    def summary(self) -> str:
        unsent, rtt_ms, retransmits = self._tcp()
        parts = [f"{time.monotonic() - self._started_mono:.0f}s", f"{self.sent / 1e6:.1f} MB sent"]
        lead = self.lead_s()
        if lead is not None:
            parts.append(f"{self.audio_s:.0f}s of audio (listener holding ~{lead:.0f}s)")
        if unsent is not None:
            parts.append(f"{unsent} B not yet accepted, round trip {rtt_ms:.0f} ms, {retransmits} retransmits")
        return ", ".join(parts)

    def save(self, reason: str) -> str | None:
        """Write the recent audio and a report. Returns the audio file's path, or None if there is nowhere to write."""
        try:
            os.makedirs(DATA_DIR, exist_ok=True)
            stamp = time.strftime("%Y%m%d-%H%M%S")
            base = os.path.join(DATA_DIR, f"{stamp}-{self.station}")
            audio_path = f"{base}.{_EXTENSIONS.get(self.content_type, 'bin')}"
            with open(audio_path, "wb") as f:
                for data in self._playable():
                    f.write(data)
            with open(f"{base}.txt", "w", encoding="utf-8") as f:
                f.write(self._report(reason, os.path.basename(audio_path)))
            self._prune()
            return audio_path
        except OSError as exc:
            logger.info("%s capture not saved (%s)", self.label, exc)
            return None

    def _playable(self) -> list[bytes]:
        """The kept audio in a form a player can open.

        ADTS can be cut anywhere. MP4 cannot: the kept pieces usually begin
        part-way through a fragment, so those are dropped up to the next
        fragment, and the stream's description is put in front.
        """
        pieces = [data for _, data in self._audio]
        if self.content_type != "audio/mp4" or not pieces or pieces[0][4:8] == b"ftyp":
            return pieces
        for i, data in enumerate(pieces):
            if data[4:8] == b"moof":
                return [self._stream.capture_head, *pieces[i:]]
        return pieces

    def _report(self, reason: str, audio_name: str) -> str:
        if self._pending_row is not None:
            self.rows.append(self._pending_row)
        unsent, rtt_ms, retransmits = self._tcp()
        now = time.time()
        lead = self.lead_s()
        lines = [
            f"station:       {self.station}  ({self.description})",
            f"listener:      {self.peer}",
            f"connected:     {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.started))}",
            f"disconnected:  {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now))}  ({reason}) after {now - self.started:.0f} s",
            f"sent:          {self.sent / 1e6:.1f} MB"
            + ("" if self.audio_s is None else f" = {self.audio_s:.0f} s of audio"),
        ]
        if lead is not None:
            lines.append(
                f"player buffer: about {lead:.0f} s (audio sent beyond real time). If the player was playing"
                f" normally, it was about {lead:.0f} s before the end of everything sent."
            )
        if unsent is not None:
            lines.append(
                f"connection:    {unsent} B sent but not yet accepted by the listener, round trip {rtt_ms:.0f} ms,"
                f" {retransmits} packets retransmitted in total, {self.blocked_s:.1f} s spent waiting for it to take data"
            )
        kept_s = time.monotonic() - self._audio[0][0] if self._audio else 0
        lines += [
            f"audio file:    {audio_name}: the last {self._audio_bytes / 1e6:.1f} MB sent"
            f" (pieces handed over in the final {kept_s:.0f} s), exactly as the listener received it",
            "",
            "events:",
            *([f"  {_clock(at)}  {text}" for at, text in self.events] or ["  none"]),
            "",
            "timeline (most recent last):",
            "  time      segment     audio   bytes     download  waited   not-accepted  rtt     retrans  broadcast time",
        ]
        for r in list(self.rows)[-60:]:
            lines.append(
                f"  {_clock(r.at)}  {r.segment if r.segment is not None else '-':<10}  "
                f"{'' if r.audio_s is None else format(r.audio_s, '.1f') + 's':<6}  {r.size:<8}  "
                f"{'' if r.download_s is None else format(r.download_s, '.2f') + 's':<8}  {r.blocked_s:<6.2f}  "
                f"{'' if r.unsent is None else r.unsent:<12}  {'' if r.rtt_ms is None else format(r.rtt_ms, '.0f') + 'ms':<6}  "
                f"{'' if r.retransmits is None else r.retransmits:<7}  {r.program_time or ''}"
            )
        return "\n".join(lines) + "\n"

    @staticmethod
    def _prune() -> None:
        reports = sorted(f for f in os.listdir(DATA_DIR) if f.endswith(".txt"))
        for old in reports[:-KEEP_CAPTURES]:
            stem = old[:-4]
            for name in os.listdir(DATA_DIR):
                if name.startswith(stem + "."):
                    os.remove(os.path.join(DATA_DIR, name))
