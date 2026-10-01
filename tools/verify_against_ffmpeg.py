"""Dev check: unwrap live segments with radioproxy's parsers and with ffmpeg; the AAC frames must match.

Needs ffmpeg on this machine (the container itself has none). Run from the repo root:
    python tools/verify_against_ffmpeg.py
"""
import json, re, subprocess, sys, urllib.request, urllib.parse
sys.path.insert(0, ".")
from radioproxy import mp4, ts, adts
UA = {"User-Agent": "VLC/3.0"}
def get(u):
    r = urllib.request.urlopen(urllib.request.Request(u, headers=UA), timeout=20); return r.geturl(), r.read()
def frames_of(b):
    out, i = [], 0
    while i + 7 <= len(b):
        n = adts.frame_len(b, i)
        if not n or i + n > len(b): break
        out.append(bytes(b[i:i+n])); i += n
    return out, len(b) - i
def ffmpeg_adts(path):
    return subprocess.run(["ffmpeg", "-v", "error", "-i", path, "-c:a", "copy", "-f", "adts", "-"], capture_output=True).stdout
def media_playlist(url, pick):
    base, body = get(url); text = body.decode()
    variants = re.findall(r"#EXT-X-STREAM-INF:([^\n]*)\n([^\n#]+)", text)
    if variants:
        v = pick(variants); print("   variant:", v[0][:110])
        base, body = get(urllib.parse.urljoin(base, v[1].strip())); text = body.decode()
    return base, text
def check(label, url, pick):
    print("==", label)
    base, text = media_playlist(url, pick)
    segs = [l.strip() for l in text.splitlines() if l.strip() and not l.startswith("#")]
    m = re.search(r'#EXT-X-MAP:URI="([^"]+)"', text)
    for n, seg in enumerate(segs[-2:]):
        _, data = get(urllib.parse.urljoin(base, seg))
        if m:
            _, init_b = get(urllib.parse.urljoin(base, m.group(1)))
            init = mp4.parse_init(init_b)
            mine = mp4.to_adts(data, init)
            open("ref.mp4", "wb").write(init_b + data); ref = ffmpeg_adts("ref.mp4")
            kind = f"fMP4 {init.config}"
        else:
            mine = adts.AdtsFramer().feed(ts.TsDemuxer().feed(data))
            open("ref.ts", "wb").write(data); ref = ffmpeg_adts("ref.ts")
            kind = "MPEG-TS"
        a, ra = frames_of(mine); b, rb = frames_of(ref)
        same = a == b
        print(f"   segment {n}: {kind}; {len(data)} B in -> mine {len(a)} frames ({len(mine)} B, {ra} leftover), ffmpeg {len(b)} frames ({len(ref)} B) -> {'IDENTICAL' if same else 'DIFFERENT'}")
        if not same:
            k = next((i for i, (x, y) in enumerate(zip(a, b)) if x != y), min(len(a), len(b)))
            print("      first difference at frame", k, "| mine hdr", a[k][:7].hex() if k < len(a) else None, "| ffmpeg hdr", b[k][:7].hex() if k < len(b) else None, "| payload same:", (a[k][7:] == b[k][7:]) if k < min(len(a), len(b)) else None)
def best_lc(vs):
    lc = [v for v in vs if "mp4a.40.2" in v[0]] or vs
    return max(lc, key=lambda v: int(re.search(r"BANDWIDTH=(\d+)", v[0]).group(1)))
def he(vs):
    h = [v for v in vs if "mp4a.40.5" in v[0]]
    return max(h, key=lambda v: int(re.search(r"BANDWIDTH=(\d+)", v[0]).group(1))) if h else best_lc(vs)
t = json.load(urllib.request.urlopen(urllib.request.Request("https://opml.radiotime.com/Tune.ashx?id=s345724&partnerId=RadioTime&version=5.38&listenId=1&formats=mp3,aac,ogg,hls&type=station&render=json", headers=UA), timeout=12))["body"]
apple = t[0]["url"]
check("Apple Music Hits, best AAC-LC", apple, best_lc)
check("Apple Music Hits, HE-AAC variant", apple, he)
check("BBC 6 Music", "http://as-hls-ww-live.akamaized.net/pool_81827798/live/ww/bbc_6music/bbc_6music.isml/bbc_6music-audio%3d320000.norewind.m3u8", best_lc)
check("FIP hifi", "https://stream.radiofrance.fr/fip/fip.m3u8?id=radiofrance", best_lc)
