"""MPEG-TS, as the BBC's and most other HLS radio uses: the AAC stream out.

A TS file is a run of 188-byte packets from several interleaved streams. The
audio is one of them, and already ADTS-framed inside its PES packets, so
unwrapping is: find the audio stream, join its packets, drop the PES headers.
"""

from __future__ import annotations

PACKET = 188
STREAM_TYPE_AAC_ADTS = 0x0F
_STREAM_TYPE_NAMES = {0x03: "MP3", 0x04: "MP3", 0x11: "AAC (LATM)", 0x81: "AC-3", 0x87: "E-AC-3"}


class TsError(ValueError):
    pass


def looks_like_ts(data: bytes) -> bool:
    return len(data) >= 2 * PACKET and data[0] == 0x47 and data[PACKET] == 0x47


class TsDemuxer:
    """Feed it TS segments in order; it returns the audio elementary stream."""

    def __init__(self) -> None:
        self._pmt_pid: int | None = None
        self._audio_pid: int | None = None

    def feed(self, data: bytes) -> bytes:
        if self._audio_pid is None:
            # Some stations put audio packets ahead of the tables that identify
            # them, so find the audio stream before taking any audio.
            self._scan(data, tables_only=True)
        return self._scan(data, tables_only=False)

    def _scan(self, data: bytes, *, tables_only: bool) -> bytes:
        out = bytearray()
        for pos in range(0, len(data) - PACKET + 1, PACKET):
            if data[pos] != 0x47:
                raise TsError("lost MPEG-TS packet sync")
            pid = ((data[pos + 1] & 0x1F) << 8) | data[pos + 2]
            start = bool(data[pos + 1] & 0x40)
            adaptation = (data[pos + 3] >> 4) & 0x3
            if not adaptation & 0x1:          # no payload
                continue
            p = pos + 4
            if adaptation & 0x2:
                p += 1 + data[p]
            end = pos + PACKET
            if p >= end:
                continue

            if pid == 0:
                if start:
                    self._parse_pat(data, p, end)
            elif pid == self._pmt_pid:
                if start:
                    self._parse_pmt(data, p, end)
            elif pid == self._audio_pid and not tables_only:
                if start:
                    if data[p:p + 3] != b"\x00\x00\x01":
                        raise TsError("bad PES header")
                    p += 9 + data[p + 8]
                if p < end:
                    out += data[p:end]
        return bytes(out)

    def _parse_pat(self, data: bytes, p: int, end: int) -> None:
        p += 1 + data[p]                       # pointer field
        section_end = min(end, p + 3 + (((data[p + 1] & 0x0F) << 8) | data[p + 2]) - 4)
        p += 8
        while p + 4 <= section_end:
            program = (data[p] << 8) | data[p + 1]
            if program:
                self._pmt_pid = ((data[p + 2] & 0x1F) << 8) | data[p + 3]
                return
            p += 4

    def _parse_pmt(self, data: bytes, p: int, end: int) -> None:
        p += 1 + data[p]
        section_end = min(end, p + 3 + (((data[p + 1] & 0x0F) << 8) | data[p + 2]) - 4)
        p += 12 + (((data[p + 10] & 0x0F) << 8) | data[p + 11])
        other = []
        while p + 5 <= section_end:
            stream_type = data[p]
            pid = ((data[p + 1] & 0x1F) << 8) | data[p + 2]
            if stream_type == STREAM_TYPE_AAC_ADTS:
                self._audio_pid = pid
                return
            other.append(_STREAM_TYPE_NAMES.get(stream_type, f"type 0x{stream_type:02x}"))
            p += 5 + (((data[p + 3] & 0x0F) << 8) | data[p + 4])
        if self._audio_pid is None:
            raise TsError(f"no AAC audio in this stream (found: {', '.join(other) or 'nothing'})")
