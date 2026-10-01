# radioproxy

Turns a radio station into one plain, never-ending HTTP audio stream that any
player can take, without ever re-encoding the audio. No ffmpeg.

It exists for players that can't follow HLS themselves (or do it badly): they
ask radioproxy for the station, and radioproxy does the HLS work and hands back
ordinary AAC.

## Addresses

The station is in the address. Nothing else to configure.

| Address | Plays |
|---|---|
| `/stream/<http or https>/<host>/<path>?<query>` | Any station address. Scheme first, then the station's own host, path and query, untouched. |
| `/tunein/<station id>` | A TuneIn station, e.g. `/tunein/s345724`. Looked up fresh on every connect, and again whenever the signed address expires. |
| `/health` | Liveness check (used by the Docker healthcheck). |

```
http://pi:8010/tunein/s345724
http://pi:8010/stream/https/stream.radiofrance.fr/fip/fip.m3u8?id=radiofrance
http://pi:8010/stream/https/stream.radioparadise.com/aac-320
```

## What it does with a station

| The address turns out to be | radioproxy |
|---|---|
| **HLS** (`.m3u8`, master or media playlist) | Follows the playlist, downloads each segment once and sends the AAC inside as a plain ADTS stream. Fragmented MP4 (Apple), MPEG-TS (BBC, most others) and packed AAC segments. From a master playlist it takes the highest-bitrate AAC-LC version. |
| **M3U or PLS playlist** of stream addresses | Connects to the first entry that works. |
| **Plain stream** (Icecast, Shoutcast; MP3, AAC, anything) | Passes it straight through. Inline track metadata is never requested, so the audio arrives clean. |

The audio is never decoded or re-encoded: an HLS station is only unwrapped from
its container. The frames radioproxy sends are byte-for-byte the ones ffmpeg
would produce with `-c copy -f adts` (`tools/verify_against_ffmpeg.py` checks).

Not supported, on purpose: MPEG-DASH, encrypted HLS, HLS carrying anything but
AAC, transcoding.

## Behaviour

- **Head start.** An HLS listener starts three segments back from live, as the
  HLS spec asks of players, and receives those at once: about 12 s of audio on
  4 s segments, about 48 s on Apple's 16 s ones. After that each segment is
  passed on the moment it is downloaded. It follows the station; there is no
  setting.
- **Signed addresses.** When a TuneIn address expires mid-stream, radioproxy
  fetches a new one and carries on in the same response, from the next segment.
- **Whole frames only.** The listener never receives part of an AAC frame.
- **Reply.** `HTTP/1.1 200`, `Content-Type` of the audio (`audio/aac` for HLS),
  no length, ends when either side closes. A station that can't be opened gets
  `502` with the reason in plain text, before any audio.

## Run

```yaml
services:
  radioproxy:
    image: ghcr.io/amsound/radioproxy:latest
    container_name: radioproxy
    init: true
    ports:
      - "8010:8010"
    restart: unless-stopped
```

`PORT` (default `8010`) is the only setting.

GitHub Actions (`.github/workflows/image.yml`) builds the arm64 image on every
push to `main` and publishes `ghcr.io/amsound/radioproxy:latest`, plus
`:sha-<commit>` for rolling back. Build locally with `docker build -t radioproxy .`.

## Logs

```
[s345724 #3 <- 192.168.70.51] connected: HLS mp4a.40.2 281k, 16s segments, 48s head start
[s345724 #3 <- 192.168.70.51] slow segment 181234: 19.2s to download 16s of audio
[s345724 #3 <- 192.168.70.51] address expired (HTTP 403); fetching a new one
[s345724 #3 <- 192.168.70.51] disconnected after 21604s, 691.2 MB sent
```

One line when a listener connects (what was chosen) and one when it leaves.
In between, only trouble is logged: a slow or failed download, a skipped
segment, a renewed address.

## Layout

| File | Job |
|---|---|
| `radioproxy/__main__.py` | The HTTP server and address parsing |
| `radioproxy/sources.py` | Works out what an address is; TuneIn lookup; pass-through |
| `radioproxy/hls.py` | HLS client: playlists, quality choice, segment loop |
| `radioproxy/mp4.py` | Fragmented MP4 to raw AAC frames |
| `radioproxy/ts.py` | MPEG-TS to the AAC stream |
| `radioproxy/adts.py` | ADTS headers and whole-frame output |
