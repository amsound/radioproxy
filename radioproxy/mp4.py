"""Fragmented MP4 (CMAF), as Apple's HLS radio uses: raw AAC frames out.

An fMP4 stream is an init segment (describes the audio) followed by media
segments. A media segment is one or more moof+mdat pairs: moof holds a table of
frame sizes, mdat the frames themselves, back to back with no headers.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Iterator

from .adts import AacConfig, parse_audio_specific_config


class Mp4Error(ValueError):
    pass


def boxes(data: bytes, start: int = 0, end: int | None = None) -> Iterator[tuple[bytes, int, int]]:
    """Yield (type, payload_start, box_end) for each box in data[start:end]."""
    end = len(data) if end is None else end
    pos = start
    while pos + 8 <= end:
        size, kind = struct.unpack_from(">I4s", data, pos)
        header = 8
        if size == 1:
            if pos + 16 > end:
                raise Mp4Error("truncated box header")
            size = struct.unpack_from(">Q", data, pos + 8)[0]
            header = 16
        elif size == 0:
            size = end - pos
        if size < header or pos + size > end:
            raise Mp4Error(f"box {kind!r} runs past the end of the data")
        yield kind, pos + header, pos + size
        pos += size


def _find(data: bytes, path: tuple[bytes, ...], start: int = 0, end: int | None = None) -> tuple[int, int] | None:
    """Payload range of the first box at the nested path, or None."""
    for kind, p, e in boxes(data, start, end):
        if kind == path[0]:
            if len(path) == 1:
                return p, e
            found = _find(data, path[1:], p, e)
            if found:
                return found
    return None


@dataclass(frozen=True)
class Init:
    config: AacConfig
    default_sample_size: int  # from trex; a fragment may override it


def _read_descriptor_len(data: bytes, pos: int) -> tuple[int, int]:
    length = 0
    for _ in range(4):
        b = data[pos]
        pos += 1
        length = (length << 7) | (b & 0x7F)
        if not b & 0x80:
            break
    return length, pos


def _asc_from_esds(data: bytes, start: int, end: int) -> bytes:
    pos = start + 4  # version and flags
    while pos < end:
        tag = data[pos]
        length, pos = _read_descriptor_len(data, pos + 1)
        if tag == 0x03:      # ES_Descriptor: walk into it
            flags = data[pos + 2]
            pos += 3
            if flags & 0x80:
                pos += 2
            if flags & 0x40:
                pos += 1 + data[pos]
            if flags & 0x20:
                pos += 2
        elif tag == 0x04:    # DecoderConfigDescriptor: walk into it
            pos += 13
        elif tag == 0x05:    # DecoderSpecificInfo: the AudioSpecificConfig
            return data[pos:pos + length]
        else:
            pos += length
    raise Mp4Error("no AudioSpecificConfig in esds")


def parse_init(data: bytes) -> Init:
    """Read the audio description from an fMP4 init segment."""
    stsd = _find(data, (b"moov", b"trak", b"mdia", b"minf", b"stbl", b"stsd"))
    if not stsd:
        raise Mp4Error("init segment has no audio track description")
    config = None
    for kind, p, e in boxes(data, stsd[0] + 8, stsd[1]):   # skip version/flags + entry count
        if kind == b"enca":
            raise Mp4Error("stream is encrypted")
        if kind == b"mp4a":
            esds = _find(data, (b"esds",), p + 28, e)      # 28 = fixed audio sample entry fields
            if not esds:
                raise Mp4Error("mp4a entry has no esds")
            config = parse_audio_specific_config(_asc_from_esds(data, *esds))
            break
    if config is None:
        raise Mp4Error("init segment's audio is not AAC")
    trex = _find(data, (b"moov", b"mvex", b"trex"))
    default_size = struct.unpack_from(">I", data, trex[0] + 16)[0] if trex else 0
    return Init(config, default_size)


def frames(segment: bytes, init: Init) -> Iterator[bytes]:
    """Yield each raw AAC frame in an fMP4 media segment, in order."""
    pending: list[tuple[int | None, list[int]]] = []   # (absolute offset or None, sizes) per trun
    for kind, p, e in boxes(segment):
        if kind == b"moof":
            moof_start = p - 8
            pending = []
            traf = _find(segment, (b"traf",), p, e)
            if not traf:
                continue
            base = moof_start
            default_size = init.default_sample_size
            for tkind, tp, te in boxes(segment, *traf):
                if tkind == b"tfhd":
                    flags = struct.unpack_from(">I", segment, tp)[0] & 0xFFFFFF
                    pos = tp + 8
                    if flags & 0x000001:
                        base = struct.unpack_from(">Q", segment, pos)[0]
                        pos += 8
                    if flags & 0x000002:
                        pos += 4
                    if flags & 0x000008:
                        pos += 4
                    if flags & 0x000010:
                        default_size = struct.unpack_from(">I", segment, pos)[0]
                elif tkind == b"trun":
                    flags = struct.unpack_from(">I", segment, tp)[0] & 0xFFFFFF
                    count = struct.unpack_from(">I", segment, tp + 4)[0]
                    pos = tp + 8
                    offset = None
                    if flags & 0x000001:
                        offset = base + struct.unpack_from(">i", segment, pos)[0]
                        pos += 4
                    if flags & 0x000004:
                        pos += 4
                    per_sample = 4 * sum(bool(flags & f) for f in (0x100, 0x200, 0x400, 0x800))
                    if pos + count * per_sample > te:
                        raise Mp4Error("trun table runs past its box")
                    if flags & 0x000200:
                        skip = 4 if flags & 0x100 else 0
                        sizes = [struct.unpack_from(">I", segment, pos + i * per_sample + skip)[0] for i in range(count)]
                    else:
                        sizes = [default_size] * count
                    pending.append((offset, sizes))
        elif kind == b"mdat":
            pos = p
            for offset, sizes in pending:
                if offset is not None:
                    pos = offset
                for size in sizes:
                    if size <= 0 or pos + size > len(segment):
                        raise Mp4Error("frame table points outside the segment")
                    yield segment[pos:pos + size]
                    pos += size
            pending = []


def to_adts(segment: bytes, init: Init) -> bytes:
    """An fMP4 media segment as ADTS: every frame, each with its 7-byte header."""
    out = bytearray()
    header = init.config.header
    for frame in frames(segment, init):
        out += header(len(frame))
        out += frame
    return bytes(out)
