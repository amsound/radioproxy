"""AAC in ADTS framing: the plain stream format players expect."""

from __future__ import annotations

from dataclasses import dataclass

SAMPLE_RATES = (96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350)
HEADER_LEN = 7
AOT_AAC_LC = 2
AOT_SBR = 5   # HE-AAC
AOT_PS = 29   # HE-AAC v2


class AacError(ValueError):
    pass


@dataclass(frozen=True)
class AacConfig:
    """What an ADTS header needs to say about the stream."""

    object_type: int   # 2 = AAC-LC
    rate_index: int    # index into SAMPLE_RATES
    channels: int      # channel configuration (2 = stereo)

    @property
    def sample_rate(self) -> int:
        return SAMPLE_RATES[self.rate_index]

    def header(self, payload_len: int) -> bytes:
        """The 7-byte ADTS header for one raw AAC frame of payload_len bytes."""
        n = payload_len + HEADER_LEN
        if n > 0x1FFF:
            raise AacError(f"AAC frame too large for ADTS: {payload_len} bytes")
        return bytes((
            0xFF,
            0xF1,  # MPEG-4, layer 0, no CRC
            ((self.object_type - 1) << 6) | (self.rate_index << 2) | (self.channels >> 2),
            ((self.channels & 3) << 6) | (n >> 11),
            (n >> 3) & 0xFF,
            ((n & 7) << 5) | 0x1F,
            0xFC,
        ))


class _Bits:
    def __init__(self, data: bytes) -> None:
        self._v = int.from_bytes(data, "big")
        self._left = len(data) * 8

    def read(self, n: int) -> int:
        if n > self._left:
            raise AacError("AudioSpecificConfig is truncated")
        self._left -= n
        return (self._v >> self._left) & ((1 << n) - 1)


def parse_audio_specific_config(asc: bytes) -> AacConfig:
    """Read the MP4 AudioSpecificConfig (what an fMP4 stream carries instead of ADTS headers)."""
    bits = _Bits(asc)

    def object_type() -> int:
        aot = bits.read(5)
        return 32 + bits.read(6) if aot == 31 else aot

    def rate_index() -> int:
        idx = bits.read(4)
        if idx != 15:
            return idx
        rate = bits.read(24)
        if rate not in SAMPLE_RATES:
            raise AacError(f"non-standard sample rate {rate} Hz")
        return SAMPLE_RATES.index(rate)

    aot = object_type()
    idx = rate_index()
    channels = bits.read(4)
    if aot in (AOT_SBR, AOT_PS):
        # HE-AAC signalled explicitly: an AAC-LC core at the rate above. ADTS carries
        # the core; a player finds the SBR/PS extension inside the frames.
        rate_index()
        aot = object_type()
    if aot != AOT_AAC_LC:
        raise AacError(f"unsupported AAC object type {aot} (only AAC-LC and HE-AAC)")
    if idx >= len(SAMPLE_RATES) or not 1 <= channels <= 7:
        raise AacError("unsupported AAC sample rate or channel layout")
    return AacConfig(aot, idx, channels)


def frame_len(buf: bytes | bytearray, i: int) -> int:
    """Length of the ADTS frame starting at i, or 0 if there is no valid header there."""
    if buf[i] != 0xFF or (buf[i + 1] & 0xF6) != 0xF0:   # 12-bit sync, layer 00
        return 0
    if (buf[i + 2] >> 2) & 0x0F >= len(SAMPLE_RATES):
        return 0
    length = ((buf[i + 3] & 0x03) << 11) | (buf[i + 4] << 3) | (buf[i + 5] >> 5)
    return length if length >= HEADER_LEN else 0


class AdtsFramer:
    """Pass on whole ADTS frames only.

    A frame is released once all of it has arrived; anything that isn't a frame
    is skipped up to the next valid header. State carries across feeds, so a
    frame split between two HLS segments comes out whole.
    """

    def __init__(self) -> None:
        self._buf = bytearray()
        self.skipped = 0

    def feed(self, data: bytes) -> bytes:
        buf = self._buf
        buf += data
        out = bytearray()
        i, n = 0, len(buf)
        while n - i >= HEADER_LEN:
            length = frame_len(buf, i)
            if not length:
                i += 1
                self.skipped += 1
                continue
            if n - i < length:
                break
            out += buf[i:i + length]
            i += length
        del buf[:i]
        return bytes(out)


def strip_id3(data: bytes) -> bytes:
    """Drop ID3v2 tags from the front (HLS 'packed audio' segments start with one)."""
    while len(data) >= 10 and data[:3] == b"ID3":
        size = (data[6] & 0x7F) << 21 | (data[7] & 0x7F) << 14 | (data[8] & 0x7F) << 7 | (data[9] & 0x7F)
        data = data[10 + size + (10 if data[5] & 0x10 else 0):]
    return data
