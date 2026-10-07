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
| either of the above, with `out=mp4` | The same station sent as fragmented MP4 instead of plain AAC (see below). |
| `/status` | Who is listening right now and how it is going (JSON). |
| `/health` | Liveness check (used by the Docker healthcheck). |

```
http://pi:8010/tunein/s345724
http://pi:8010/stream/https/stream.radiofrance.fr/fip/fip.m3u8?id=radiofrance
http://pi:8010/stream/https/stream.radioparadise.com/aac-320
http://pi:8010/tunein/s345724?out=mp4
```

### MP4 output (`out=mp4`)

Add `out=mp4` to the address (`?out=mp4`, or `&out=mp4` after a station's own
query) and an HLS station whose segments are fragmented MP4 (Apple's are) is
sent as one fragmented MP4 stream, `Content-Type: audio/mp4`, instead of ADTS.

- The audio and its tables are the station's own, untouched: each segment's
  `moof`+`mdat` pairs are passed on as they came, after the station's audio
  description (sent once, first).
- Dropped: the per-segment wrapping a player has no use for (`styp`, `sidx`, and
  the `emsg` boxes Apple fills with titles and artwork).
- Changed: one number per fragment, its start time, so the stream starts at 0
  for each listener instead of wherever the station's clock is (hundreds of days in).

Unlike ADTS, MP4 carries timestamps, so a player does not have to keep time by
counting frames. A station that is not fragmented MP4 (MPEG-TS, a plain stream)
is sent as usual and the log says so; `out=mp4` is ours and is not passed to the station.

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

- **Head start.** An HLS listener is sent a head start at once, and everything
  after it at real time in half-second slices. By default the head start is
  three segments, as the HLS spec asks players to hold: about 12 s of audio on
  4 s segments, about 48 s on Apple's 16 s ones. `BURST_SECONDS` makes it that
  many seconds instead. It is the same setting, with the same meaning, as the
  restreamer's.
- **Reserve.** A listener joins two segments further back than its head start,
  and those two are not sent at once. Radioproxy therefore always has audio in
  hand, and the connection is never silent while the station gets round to
  publishing its next segment (some players hang up on a silent connection).
  So a listener joins five segments back from live by default, and three on
  Apple's station with `BURST_SECONDS=15`, which is where the restreamer joins.
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
    environment:
      BURST_SECONDS: "90"                # optional: see Head start
    volumes:
      - ./radioproxy-data:/data
    restart: unless-stopped
```

Settings: `PORT` (default `8010`), and `BURST_SECONDS` (unset: a head start of three segments).
A small `BURST_SECONDS` leaves less margin at Apple's six-hourly re-signing, when
nothing can be sent for a few seconds (about 10 when measured) and the listener
plays from its head start. The data folder must be writable by the container's user
(`sudo chown 10001:10001 radioproxy-data`); without it radioproxy runs the same, just without captures.

GitHub Actions (`.github/workflows/image.yml`) builds the arm64 image on every
push to `main` and publishes `ghcr.io/amsound/radioproxy:latest`, plus
`:sha-<commit>` for rolling back. Build locally with `docker build -t radioproxy .`.

## Logs

```
[s345724 #3 <- 192.168.70.51] connected: HLS mp4a.40.2 281k, 16s segments, 48s head start
[s345724 #3 <- 192.168.70.51] slow segment 2648383: 19.2s to download 16s of audio
[s345724 #3 <- 192.168.70.51] address expired (HTTP 403); fetching a new one
[s345724 #3 <- 192.168.70.51] station changed its audio description at segment 2648391: ...
[s345724 #3 <- 192.168.70.51] audio format changed mid-stream: AAC profile 2, 48000 Hz, 2 ch -> ...
[s345724 #3 <- 192.168.70.51] disconnected (listener hung up) after 46317s, 1482.1 MB sent, 46380s of audio (listener holding ~63s), 0 B not yet accepted, round trip 4 ms, 31 retransmits; capture /data/20261002-075631-s345724.aac
```

One line when a listener connects (what was chosen) and one when it leaves.
In between, only notable things are logged: a slow or failed download, a
skipped segment, a renewed address, a discontinuity the station flagged, or a
change in the audio's format.

## When a listener drops

For each listener radioproxy keeps the last five minutes of audio exactly as
sent. When the listener leaves, it writes two files to `/data` (the newest 12
are kept):

- `<time>-<station>.aac` (`.mp4` for MP4 output): that audio. Decode it to see
  whether the data was sound: `ffmpeg -v error -i <file> -f null -` prints
  nothing for clean audio. An MP4 capture starts at the first whole fragment
  kept and ends where the listener left, usually part-way through a fragment,
  so ffmpeg reports `partial file` for the very end; anything earlier is real.
- `<time>-<station>.txt`: a report. When it connected and left and why, how
  much audio was sent, roughly how much the player was holding, and a timeline
  of each segment: when it was sent, how long it took to download, the
  station's broadcast clock time, and readings from the listener's own TCP
  connection (bytes it had not yet accepted, round-trip time, retransmissions,
  time spent waiting for it to take data).

"Listener holding" is audio sent minus time elapsed: about what the player has
buffered if it is playing normally. A player that failed was playing roughly
that far before the end of the audio file.

## Layout

| File | Job |
|---|---|
| `radioproxy/__main__.py` | The HTTP server and address parsing |
| `radioproxy/sources.py` | Works out what an address is; TuneIn lookup; pass-through |
| `radioproxy/hls.py` | HLS client: playlists, quality choice, segment loop |
| `radioproxy/mp4.py` | Fragmented MP4 to raw AAC frames, or to a continuous MP4 stream |
| `radioproxy/ts.py` | MPEG-TS to the AAC stream |
| `radioproxy/adts.py` | ADTS headers and whole-frame output |
| `radioproxy/listener.py` | Per-listener capture, timeline, connection readings, report |
