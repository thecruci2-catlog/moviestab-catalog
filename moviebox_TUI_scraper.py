"""
MovieBox catalog scraper -> parallel sharded sync & ~10MB JSON chunks for MoviesTab.

Supports:
  - Parallel sharding (--shard-index 0 --total-shards 4) for fast GitHub Actions runs
  - Fresh per-run scraping (does not carry over old progress from prior runs)
  - Pre-classification into `movies`, `series`, `anime`, and `short_tv`
  - Normalized `genres` lists (Action, Adventure, Sci-Fi, etc.) + genre counts in `index.json`
  - Clean replacement of old `catalog_part_*.json` chunks + updated `index.json`
  - Batch cap (--max-titles 3000) so jobs stay well under GitHub's 6-hour timeout
  - Merge mode (--merge-only) to combine current run shard logs into ~10MB chunks
"""

import argparse
import base64
import glob
import hashlib
import hmac
import json
import os
import random
import re
import time
from urllib.error import URLError
from urllib.request import Request, urlopen

# ---------------------------------------------------------------- config ----

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(BASE_DIR, "output")
INDEX_FILE = os.path.join(OUT_DIR, "index.json")

# 10 MB per chunk for faster parallel mobile downloads and lower memory overhead
MAX_CHUNK_BYTES = 10 * 1024 * 1024

HOST_POOL = [
    "https://api6.aoneroom.com",
    "https://api5.aoneroom.com",
    "https://api4.aoneroom.com",
    "https://api4sg.aoneroom.com",
    "https://api3.aoneroom.com",
    "https://api6sg.aoneroom.com",
    "https://api.inmoviebox.com",
]

RETRY_STATUS = {403, 406, 407, 429, 500, 502, 503, 504}

SECRET_KEY = bytes([
    0xEF, 0xA8, 0x91, 0x97, 0x4E, 0xEC, 0xD3, 0x14,
    0x8D, 0xF6, 0x3A, 0xA6, 0x11, 0x60, 0x2D, 0xEF,
    0xD1, 0x01, 0x25, 0x9B, 0xA5, 0x21, 0x02, 0x2C,
    0x57, 0xAE, 0x05, 0x66, 0xBD, 0x8E,
])

STOCK_CERT_SHA256 = "03d4b741632d136bc23e2de4c6adcdb7712f1980b619c183221801730bbd0410"
BUILD_TAG_SALT = "oneroom"
STREAM_REFERER = "https://sportslive.wine"
SIGNATURE_BODY_MAX_BYTES = 102_400

TAB_IDS = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14"]

SEARCH_SEEDS = [
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
    "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
    "aa", "ab", "ac", "ad", "ae", "af", "ag", "ah", "ai", "aj", "ak", "al", "am",
    "an", "ao", "ap", "aq", "ar", "as", "at", "au", "av", "aw", "ax", "ay", "az",
    "ba", "be", "bi", "bl", "bo", "br", "bu", "by",
    "ca", "ce", "ch", "ci", "cl", "co", "cr", "cu",
    "da", "de", "di", "do", "dr", "du", "dy",
    "ea", "ec", "ed", "ef", "el", "em", "en", "ep", "er", "es", "et", "ev", "ex",
    "fa", "fe", "fi", "fl", "fo", "fr", "fu",
    "ga", "ge", "gh", "gi", "gl", "go", "gr", "gu",
    "ha", "he", "hi", "ho", "hu",
    "ia", "id", "ig", "il", "im", "in", "io", "ir", "is", "it",
    "ja", "je", "ji", "jo", "ju",
    "ka", "ke", "kh", "ki", "kn", "ko", "kr", "ku", "ky",
    "la", "le", "li", "lo", "lu", "ly",
    "ma", "me", "mi", "mo", "mu", "my",
    "na", "ne", "ni", "no", "nu",
    "oa", "ob", "oc", "od", "of", "oh", "oi", "ok", "ol", "om", "on", "oo", "op", "or", "os", "ot", "ou", "ov", "ow", "ox",
    "pa", "pe", "ph", "pi", "pl", "po", "pr", "pu",
    "qu",
    "ra", "re", "rh", "ri", "ro", "ru", "ry",
    "sa", "sc", "se", "sh", "si", "sk", "sl", "sm", "sn", "so", "sp", "sq", "st", "su", "sw", "sy",
    "ta", "te", "th", "ti", "to", "tr", "ts", "tu", "tw", "ty",
    "ua", "ub", "uc", "ul", "um", "un", "up", "ur", "us", "ut",
    "va", "ve", "vi", "vo", "vu",
    "wa", "we", "wh", "wi", "wo", "wr",
    "xi", "xu",
    "ya", "ye", "yi", "yo", "yu",
    "za", "ze", "zi", "zo",
    "the", "love", "war", "man", "dark", "night", "star", "india",
    "hindi", "tamil", "telugu", "malayalam", "kannada", "bengali", "marathi", "punjabi",
    "korean", "japanese", "chinese", "anime", "movie", "film", "series",
    "season", "part", "story", "life", "king", "queen", "boss", "world",
    "dead", "last", "one", "two", "three", "2026", "2025", "2024", "2023",
]

PER_PAGE = 20
MAX_SEARCH_PAGES = 100
REQUEST_DELAY = 0.10
MAX_HOMEPAGE_PAGES = 30


# ------------------------------------------------------------- signing -----

def md5_hex(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def client_token(ts_ms: int) -> str:
    ts_str = str(ts_ms)
    return f"{ts_str},{md5_hex(ts_str[::-1].encode())}"


def build_tag(ts_ms: int, rand_part: int) -> str:
    stamp = f"{ts_ms // 1000}{ts_ms % 1000:03d}000{rand_part}"
    digest = md5_hex(f"{STOCK_CERT_SHA256}{BUILD_TAG_SALT}{stamp}".encode())
    return f"{stamp}.{digest}"


def canonical_string(method, accept, content_type, url, body, ts_ms) -> str:
    from urllib.parse import urlparse, parse_qsl

    parsed = urlparse(url)
    path = parsed.path or "/"
    pairs = sorted(parse_qsl(parsed.query, keep_blank_values=True))
    canonical_url = f"{path}?{'&'.join(f'{k}={v}' for k, v in pairs)}" if pairs else path

    if body is not None:
        raw = body.encode()
        truncated = raw[:SIGNATURE_BODY_MAX_BYTES]
        body_hash = md5_hex(truncated)
        body_len = str(len(raw))
    else:
        body_hash = ""
        body_len = ""

    return "\n".join([
        method.upper(),
        accept or "",
        content_type or "",
        body_len,
        str(ts_ms),
        body_hash,
        canonical_url,
    ])


def tr_signature(method, accept, content_type, url, body, ts_ms) -> str:
    canon = canonical_string(method, accept, content_type, url, body, ts_ms)
    sig = hmac.new(SECRET_KEY, canon.encode(), hashlib.md5).digest()
    return f"{ts_ms}|2|{base64.b64encode(sig).decode()}"


def random_hex(n: int) -> str:
    return "".join(random.choice("0123456789abcdef") for _ in range(n))


def random_spoofed_ip() -> str:
    prefix = random.choice([
        "103.241", "49.36", "117.195", "106.198", "122.162",
        "157.32", "182.70", "103.58", "27.60", "59.90",
    ])
    return f"{prefix}.{random.randint(1, 253)}.{random.randint(1, 253)}"


ANDROID_VERSIONS = [
    ("9", "PQ3A.190605.03081104"),
    ("10", "QP1A.191005.007.A3"),
    ("11", "RP1A.200720.011"),
    ("12", "S1B.220414.015"),
    ("13", "TQ2A.230405.003"),
]
REDMI_DEVICES = [
    "23078RKD5C", "2201117TY", "2201117TG", "22101316G",
    "21121210G", "M2012K11AG", "M2007J20CG",
]
VERSION_CODES = [50020117, 50020118, 50020119, 50020120, 50020121]


def generate_identity():
    android, build_id = random.choice(ANDROID_VERSIONS)
    device = random.choice(REDMI_DEVICES)
    version_code = random.choice(VERSION_CODES)
    network = random.choice(["NETWORK_WIFI", "NETWORK_MOBILE"])
    tz = random.choice(["Asia/Kolkata", "Asia/Shanghai", "Asia/Tokyo",
                        "America/New_York", "Europe/London"])
    gaid = "-".join([random_hex(8), random_hex(4), random_hex(4),
                     random_hex(4), random_hex(12)])
    device_id = random_hex(32)

    ua = (f"com.community.oneroom/{version_code} (Linux; U; Android {android}; "
          f"en_US; {device}; Build/{build_id}; Cronet/135.0.7012.3)")

    client_info = {
        "package_name": "com.community.oneroom",
        "version_name": "4.0.01.0813.03",
        "version_code": version_code,
        "os": "android",
        "os_version": android,
        "install_ch": "ps",
        "device_id": device_id,
        "install_store": "ps",
        "gaid": gaid,
        "brand": "Redmi",
        "model": device,
        "system_language": "en",
        "net": network,
        "region": "US",
        "timezone": tz,
        "sp_code": "40401",
        "X-Play-Mode": "2",
    }
    return ua, client_info


# -------------------------------------------------------------- client -----

class MovieBox:
    def __init__(self):
        self.ua, self.client_info = generate_identity()
        self.ip = random_spoofed_ip()
        self.token = None
        self.base_idx = 0
        try:
            import requests as _requests
            self._requests = _requests
            self.sess = _requests.Session()
        except ImportError:
            self._requests = None
            self.sess = None

    def _signed_headers(self, method, url, body, token=None):
        ts = int(time.time() * 1000)
        accept = "application/json"
        ctype = "application/json"

        tag = build_tag(ts, random.randrange(0, 1000))
        info = dict(self.client_info)
        info["X-Client-Build"] = tag

        headers = {
            "User-Agent": self.ua,
            "Accept": accept,
            "Content-Type": ctype,
            "Connection": "keep-alive",
            "x-client-token": client_token(ts),
            "x-tr-signature": tr_signature(method, accept, ctype, url, body, ts),
            "x-client-build": tag,
            "x-client-info": json.dumps(info, separators=(",", ":")),
            "x-client-status": "0",
            "x-forwarded-for": self.ip,
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _http_call(self, method, url, headers, body):
        data_bytes = body.encode() if body else None
        if self.sess is not None:
            resp = self.sess.request(method, url, headers=headers, data=data_bytes, timeout=15)
            return resp.status_code, resp.headers, resp.text
        req = Request(url, data=data_bytes, headers=headers, method=method)
        try:
            with urlopen(req, timeout=15) as r:
                return r.status, r.headers, r.read().decode("utf-8", errors="replace")
        except URLError as exc:
            code = getattr(exc, "code", None)
            if code:
                return int(code), {}, ""
            raise

    def _raw_request(self, method, path, body=None, token=None):
        start = self.base_idx
        backoff = 0.05

        for i in range(len(HOST_POOL)):
            idx = (start + i) % len(HOST_POOL)
            base = HOST_POOL[idx]
            url = base + path

            try:
                headers = self._signed_headers(method, url, body, token)
                status, resp_headers, text = self._http_call(method, url, headers, body)
            except Exception as exc:
                log(f"  host {idx} net error: {exc}")
                self.base_idx = (idx + 1) % len(HOST_POOL)
                continue

            if status in RETRY_STATUS:
                self.base_idx = (idx + 1) % len(HOST_POOL)
                if status == 429:
                    ra = resp_headers.get("Retry-After", "0") if resp_headers else "0"
                    try:
                        backoff = min(float(ra), 3.0)
                    except ValueError:
                        backoff = 0.4
                time.sleep(backoff)
                continue

            if status >= 400:
                log(f"  host {idx} status {status}")
                continue

            self.base_idx = idx
            try:
                payload = json.loads(text)
            except ValueError:
                log(f"  host {idx} bad json")
                continue

            return payload.get("data", payload) if isinstance(payload, dict) else payload

        return None

    def _request(self, method, path, body=None, _retry=True):
        if not self.token:
            if not self.login():
                return None
        result = self._raw_request(method, path, body, self.token)
        if result is None and _retry:
            log("  session might be dead, re-logging in")
            self.token = None
            if self.login():
                return self._raw_request(method, path, body, self.token)
        return result

    def get(self, path):
        return self._request("GET", path)

    def post(self, path, payload):
        return self._request("POST", path, json.dumps(payload, separators=(",", ":")))

    def login(self):
        for attempt in range(3):
            val = self._raw_request(
                "POST", "/wefeed-mobile-bff/user-api/visitor-login", "{}", None
            )
            if isinstance(val, dict) and val.get("token"):
                self.token = val["token"]
                log("  logged in as visitor")
                return True
            log(f"  login attempt {attempt + 1} failed")
            time.sleep(1.5)
        return False

    def refresh_session(self):
        self.token = None
        self.ip = random_spoofed_ip()
        return self.login()

    def homepage(self, tab_id, page=1):
        return self.get(f"/wefeed-mobile-bff/tab-operating?page={page}&tabId={tab_id}&version=")

    def search(self, keyword, page=1):
        return self.post("/wefeed-mobile-bff/subject-api/search/v2", {
            "keyword": keyword,
            "page": page,
            "perPage": PER_PAGE,
            "subjectType": 0,
        })

    def details(self, subject_id):
        data = self.get(f"/wefeed-mobile-bff/subject-api/get?subjectId={subject_id}")
        if isinstance(data, dict):
            stype = data.get("subjectType") or data.get("stype") or 1
            if int(stype) == 2:
                seasons = self.get(
                    f"/wefeed-mobile-bff/subject-api/season-info?subjectId={subject_id}"
                )
                if seasons:
                    data["seasons"] = seasons
        return data

    def play_info(self, subject_id, season=0, episode=0):
        if season == 0 and episode == 0:
            path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subject_id}"
        else:
            path = (f"/wefeed-mobile-bff/subject-api/play-info/v2"
                    f"?subjectId={subject_id}&se={season}&ep={episode}")
        return self.get(path)

    def resources(self, subject_id, season=0, episode=0, page=1, per_page=20):
        if season == 0 and episode == 0:
            path = (f"/wefeed-mobile-bff/subject-api/resource"
                    f"?subjectId={subject_id}&page={page}&perPage={per_page}")
        else:
            path = (f"/wefeed-mobile-bff/subject-api/resource"
                    f"?subjectId={subject_id}&se={season}&ep={episode}"
                    f"&page={page}&perPage={per_page}")
        return self.get(path)


# ------------------------------------------------------------ helpers ------

def log(msg):
    print(msg, flush=True)


NOTICE_MARKERS = [
    "1c7de0bd3393702d9191801f15f88f8d",
    "9a0461bc39da389663bf3dbb17091d3f",
    "b164fbfb4347792950bdfbfb563d39d9",
    "/notice.mp4",
]


def is_notice_url(url: str) -> bool:
    if not url:
        return True
    low = url.lower()
    if any(m in low for m in NOTICE_MARKERS):
        return True
    if "macdn.aoneroom.com" in low and "/other/" in low:
        return True
    return False


def b64_decode_loose(s: str):
    s = s.strip()
    s = s.replace("-", "+").replace("_", "/").replace("~", "/")
    pad = (4 - len(s) % 4) % 4
    if pad:
        s += "=" * pad
    try:
        return base64.b64decode(s)
    except Exception:
        return None


def to_dash_mpd(url: str):
    base = url.rstrip("*").rstrip("/")
    if base.startswith(("http://", "https://")) and "/dash/" in base:
        return base + "/index.mpd"
    return None


def manifest_from_cookie(cookie: str):
    if not cookie:
        return None
    for part in cookie.split(";"):
        part = part.strip()
        if not part:
            continue

        if "urlprefix=" in part:
            idx = part.find("urlprefix=")
            chunk = part[idx + len("urlprefix="):]
            token = chunk.split(":")[0].strip()
            decoded = b64_decode_loose(token)
            if decoded:
                try:
                    mpd = to_dash_mpd(decoded.decode("utf-8"))
                    if mpd:
                        return mpd
                except UnicodeDecodeError:
                    pass
            if chunk.startswith(("http://", "https://")):
                mpd = to_dash_mpd(chunk.split(":sign")[0])
                if mpd:
                    return mpd

        if part.startswith("CloudFront-Policy="):
            raw = part[len("CloudFront-Policy="):].strip()
            decoded = b64_decode_loose(raw)
            if not decoded:
                continue
            try:
                policy = json.loads(decoded)
            except ValueError:
                continue
            stmts = policy.get("Statement") or []
            if stmts:
                res = stmts[0].get("Resource")
                if isinstance(res, str):
                    mpd = to_dash_mpd(res)
                    if mpd:
                        return mpd
    return None


def clean_cookie(cookie: str) -> str:
    parts = [p.strip() for p in cookie.split(";") if p.strip()]
    return "; ".join(parts)


def extract_year(raw):
    if not raw or not isinstance(raw, str):
        return None
    for i in range(len(raw) - 3):
        chunk = raw[i:i + 4]
        if chunk.isdigit() and 1900 <= int(chunk) <= 2099:
            return chunk
    return None


GENRE_CANONICAL = {
    "sci fi": "Sci-Fi",
    "scifi": "Sci-Fi",
    "sci-fi": "Sci-Fi",
    "science fiction": "Sci-Fi",
}


def normalize_genres(raw_genres):
    """Return a clean, deduplicated list of genre labels (e.g. Action, Adventure, Sci-Fi)."""
    if not raw_genres:
        return None

    parts = []
    if isinstance(raw_genres, list):
        for g in raw_genres:
            if isinstance(g, dict):
                val = g.get("name") or g.get("genreName") or g.get("title") or g.get("label")
                if val:
                    parts.append(str(val))
            elif g is not None:
                parts.append(str(g))
    elif isinstance(raw_genres, str):
        parts.append(raw_genres)
    else:
        return None

    cleaned = []
    seen = set()
    for chunk in parts:
        for token in re.split(r"[,/|]+", chunk):
            label = re.sub(r"\s+", " ", token).strip(" -·•")
            if not label:
                continue
            canon = GENRE_CANONICAL.get(label.lower(), label)
            key = canon.lower()
            if key not in seen:
                seen.add(key)
                cleaned.append(canon)

    return cleaned or None


def quality_label(resolution):
    if resolution is None:
        return None
    s = str(resolution).strip()
    if s.isdigit():
        return f"{s}p"
    return s or None


def detect_format(url: str):
    if not url:
        return None
    low = url.lower()
    if ".m3u8" in low:
        return "m3u8"
    if "/dash/" in low or ".mpd" in low:
        return "mpd"
    if ".mp4" in low:
        return "mp4"
    return "unknown"


def extract_raw_stream_url(st: dict) -> str:
    if not isinstance(st, dict):
        return ""
    for key in ("url", "playUrl", "videoUrl", "dashUrl", "hlsUrl", "m3u8Url", "mp4Url", "resourceLink", "downloadUrl"):
        val = st.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return ""


def add_stream(bucket, item, source_link_hint=None, default_headers=None, season=None, episode=None, prepend=False):
    if not isinstance(item, dict):
        return
    link = extract_raw_stream_url(item)
    if link and is_notice_url(link):
        link = None

    resolution = item.get("resolution") or item.get("quality")
    if resolution is None and source_link_hint:
        m = re.search(r"(?<!\d)(\d{3,4})[pP]", str(source_link_hint))
        if m:
            resolution = m.group(1)

    entry = {
        "quality": quality_label(resolution),
        "url": link,
        "format": detect_format(link) if link else None,
        "codec": item.get("codecName") or item.get("codec"),
        "size": item.get("size"),
        "season": season if season is not None else (item.get("se") or item.get("season")),
        "episode": episode if episode is not None else (item.get("ep") or item.get("episode")),
    }
    if default_headers and link:
        entry["headers"] = dict(default_headers)
    if not link:
        entry["note"] = "no direct url in resource listing"
    if prepend:
        bucket.insert(0, entry)
    else:
        bucket.append(entry)


def subjects_from_search(payload):
    if not isinstance(payload, dict):
        return []
    results = payload.get("results")
    if isinstance(results, list) and results:
        subjects = results[0].get("subjects")
        if isinstance(subjects, list):
            return subjects
    for key in ("list", "subjects", "items"):
        val = payload.get(key)
        if isinstance(val, list):
            return val
    return []


def subjects_from_homepage(payload):
    if not isinstance(payload, dict):
        return []
    out = []
    groups = payload.get("items")
    if not isinstance(groups, list):
        groups = payload if isinstance(payload, list) else []
    for group in groups:
        if not isinstance(group, dict):
            continue
        for subj in (group.get("subjects") or []):
            if isinstance(subj, dict):
                out.append(subj)
        banner = (group.get("banner") or {}).get("banners") or []
        for b in banner:
            if isinstance(b, dict) and isinstance(b.get("subject"), dict):
                out.append(b["subject"])
        custom = (group.get("customData") or {}).get("items") or []
        for c in custom:
            if isinstance(c, dict) and isinstance(c.get("subject"), dict):
                out.append(c["subject"])
    return out


def classify_category(entry: dict, raw_stype=None) -> str:
    """
    Classify item into 'short_tv', 'anime', 'series', or 'movies' so Short TV
    never leaks into Movies or Series tabs.
    """
    streams = entry.get("streams") or []
    episodes = entry.get("episodes") or []
    genres = [str(g).lower() for g in (entry.get("genres") or [])]
    title_low = (entry.get("title") or "").lower()
    desc_low = (entry.get("description") or "").lower()

    if raw_stype is not None:
        try:
            if int(raw_stype) not in (1, 2):
                return "short_tv"
        except (TypeError, ValueError):
            pass

    if any("short" in g or "micro" in g or "reel" in g for g in genres):
        return "short_tv"

    if entry.get("type") == "movie" and not episodes:
        if any((s.get("episode") or 0) > 0 for s in streams):
            return "short_tv"

    dur_str = str(entry.get("duration") or "")
    dur_match = re.search(r"(\d+)", dur_str)
    if dur_match:
        mins = int(dur_match.group(1))
        if 0 < mins <= 8 and (len(episodes) >= 10 or any((s.get("episode") or 0) > 0 for s in streams)):
            return "short_tv"

    if any("anime" in g for g in genres) or "anime" in title_low or "anime" in desc_low:
        return "anime"

    if entry.get("type") == "series" or len(episodes) > 0:
        return "series"

    return "movies"


def subject_to_entry(subject, moviebox, with_streams=True):
    sid = subject.get("subjectId") or subject.get("id")
    if sid is None:
        return None
    sid = str(sid)

    stype = subject.get("subjectType") or subject.get("stype") or 1
    is_series = int(stype) == 2

    cover = None
    if isinstance(subject.get("cover"), dict):
        cover = subject["cover"].get("url")
    cover = cover or subject.get("coverUrl") or subject.get("poster") or subject.get("pic")

    raw_genres = subject.get("genre") if subject.get("genre") is not None else subject.get("genres")

    entry = {
        "id": sid,
        "title": subject.get("title") or subject.get("name") or "Unknown",
        "type": "series" if is_series else "movie",
        "category": "series" if is_series else "movies",
        "year": extract_year(subject.get("releaseDate") or subject.get("year")
                             or subject.get("releaseInfo")),
        "poster": cover,
        "description": subject.get("description") or subject.get("intro"),
        "rating": subject.get("imdbRatingValue") or subject.get("rating"),
        "director": subject.get("director"),
        "cast": subject.get("stars"),
        "genres": normalize_genres(raw_genres),
        "duration": subject.get("duration"),
        "season_count": subject.get("season"),
        "streams": [],
        "episodes": [],
        "dubs": [],
    }

    if not with_streams:
        entry["category"] = classify_category(entry, stype)
        return entry

    details = moviebox.details(sid)
    if isinstance(details, dict):
        for key in ("description", "director", "stars", "duration"):
            if not entry.get(key) and details.get(key):
                entry[key] = details[key]
        if not entry.get("rating") and details.get("imdbRatingValue"):
            entry["rating"] = details["imdbRatingValue"]
        if not entry.get("genres"):
            det_genres = details.get("genre") if details.get("genre") is not None else details.get("genres")
            entry["genres"] = normalize_genres(det_genres)

        dubs = details.get("dubs")
        if isinstance(dubs, list):
            entry["dubs"] = [
                {
                    "subject_id": str(d.get("subjectId") or d.get("id") or ""),
                    "language": d.get("lanName") or d.get("language") or d.get("lang"),
                    "label": d.get("title") or d.get("name") or d.get("lanName"),
                }
                for d in dubs if isinstance(d, dict)
            ]

        for det in details.get("resourceDetectors") or []:
            if not isinstance(det, dict):
                continue
            rlist = det.get("resolutionList") or []
            if rlist:
                for item in rlist:
                    add_stream(entry["streams"], item)
            else:
                add_stream(entry["streams"], {
                    "resourceLink": det.get("downloadUrl"),
                    "resourceId": det.get("resourceId"),
                    "size": det.get("firstSize"),
                    "codecName": det.get("codecName"),
                    "se": 0,
                    "ep": 0,
                }, source_link_hint=det.get("resourceLink"))

        season_payload = details.get("seasons")
        seasons_list = None
        if isinstance(season_payload, dict):
            seasons_list = season_payload.get("seasons")
        elif isinstance(season_payload, list):
            seasons_list = season_payload
        if isinstance(seasons_list, list):
            for s in seasons_list:
                if not isinstance(s, dict):
                    continue
                se = int(s.get("se") or 1)
                numbers = []
                eps = s.get("episodeNumbers")
                if isinstance(eps, list) and eps:
                    for e in eps:
                        try:
                            numbers.append(int(e))
                        except (TypeError, ValueError):
                            pass
                else:
                    max_ep = int(s.get("maxEp") or 0)
                    numbers = list(range(1, max_ep + 1))
                for number in numbers:
                    entry["episodes"].append({
                        "season": se,
                        "episode": number,
                        "streams": [],
                    })

    def harvest(payload, season=0, episode=0) -> bool:
        found_any = False
        if not isinstance(payload, dict):
            return False
        data = payload.get("data", payload)
        if not isinstance(data, dict):
            return False

        raw_streams = data.get("streams")
        if not isinstance(raw_streams, list) or not raw_streams:
            for alt_key in ("list", "playList", "items"):
                alt_val = data.get(alt_key)
                if isinstance(alt_val, list) and alt_val:
                    raw_streams = alt_val
                    break
        if not isinstance(raw_streams, list):
            raw_streams = []

        for st in raw_streams:
            if not isinstance(st, dict):
                continue
            cookie = st.get("signCookie") or ""
            playable = manifest_from_cookie(cookie)
            if not playable:
                raw_url = extract_raw_stream_url(st)
                playable = None if is_notice_url(raw_url) else (raw_url or None)
            if not playable:
                continue

            found_any = True
            hdrs = {"Referer": STREAM_REFERER, "User-Agent": moviebox.ua}
            if cookie:
                hdrs["Cookie"] = clean_cookie(cookie)

            res_str = st.get("resolutions") or data.get("displayResolutions") or st.get("resolution") or "1080"
            resolutions = []
            for r in str(res_str).split(","):
                r = r.strip()
                if r.isdigit():
                    resolutions.append(int(r))
            if not resolutions:
                resolutions = [1080]
            resolutions = sorted(set(resolutions), reverse=True)

            for res in resolutions:
                stream_obj = {
                    "quality": f"{res}p",
                    "url": playable,
                    "format": detect_format(playable),
                    "codec": st.get("codecName") or st.get("codec"),
                    "size": st.get("size"),
                    "headers": hdrs,
                    "season": season or None,
                    "episode": episode or None,
                }
                if season and episode:
                    matched = False
                    for ep in entry["episodes"]:
                        if ep["season"] == season and ep["episode"] == episode:
                            ep["streams"].append(dict(stream_obj))
                            matched = True
                            break
                    if not matched:
                        entry["episodes"].append({
                            "season": season,
                            "episode": episode,
                            "streams": [dict(stream_obj)],
                        })
                else:
                    entry["streams"].append(stream_obj)

        return found_any

    def attach_episode_resources(se_num: int, ep_num: int):
        res_ep = moviebox.resources(sid, se_num, ep_num)
        time.sleep(REQUEST_DELAY)
        if not isinstance(res_ep, dict):
            return
        hdrs = {"Referer": STREAM_REFERER, "User-Agent": moviebox.ua}
        for item in res_ep.get("list") or []:
            if not isinstance(item, dict):
                continue
            link = extract_raw_stream_url(item)
            if not link or is_notice_url(link):
                continue
            stream_obj = {
                "quality": quality_label(item.get("resolution") or item.get("quality")),
                "url": link,
                "format": detect_format(link),
                "codec": item.get("codecName") or item.get("codec"),
                "size": item.get("size"),
                "headers": hdrs,
                "season": se_num,
                "episode": ep_num,
            }
            for ep in entry["episodes"]:
                if ep["season"] == se_num and ep["episode"] == ep_num:
                    ep["streams"].insert(0, stream_obj)
                    break

    if is_series and entry["episodes"]:
        for ep in entry["episodes"]:
            payload = moviebox.play_info(sid, ep["season"], ep["episode"])
            time.sleep(REQUEST_DELAY)
            if not harvest(payload, ep["season"], ep["episode"]) and ep["episode"] >= 3:
                moviebox.refresh_session()
                payload = moviebox.play_info(sid, ep["season"], ep["episode"])
                time.sleep(REQUEST_DELAY)
                harvest(payload, ep["season"], ep["episode"])
            attach_episode_resources(ep["season"], ep["episode"])
    elif is_series:
        miss_streak = 0
        for ep_num in range(1, 31):
            payload = moviebox.play_info(sid, 1, ep_num)
            time.sleep(REQUEST_DELAY)
            if harvest(payload, 1, ep_num):
                miss_streak = 0
                attach_episode_resources(1, ep_num)
            else:
                miss_streak += 1
                if miss_streak >= 3:
                    break
        if not entry["episodes"]:
            payload = moviebox.play_info(sid)
            time.sleep(REQUEST_DELAY)
            harvest(payload)
    else:
        payload = moviebox.play_info(sid)
        time.sleep(REQUEST_DELAY)
        if not harvest(payload):
            moviebox.refresh_session()
            payload = moviebox.play_info(sid)
            time.sleep(REQUEST_DELAY)
            harvest(payload)

    res_payload = moviebox.resources(sid)
    time.sleep(REQUEST_DELAY)
    if isinstance(res_payload, dict):
        hdrs = {"Referer": STREAM_REFERER, "User-Agent": moviebox.ua}
        for item in res_payload.get("list") or []:
            add_stream(entry["streams"], item, default_headers=hdrs, prepend=True)

    def dedup_stream_list(stream_list):
        seen = set()
        uniq = []
        for s in stream_list:
            key = (s.get("url"), s.get("quality"), s.get("format"), s.get("season"), s.get("episode"))
            if not s.get("url") or key in seen:
                continue
            seen.add(key)
            uniq.append(s)
        return uniq

    entry["streams"] = dedup_stream_list(entry["streams"])
    for ep in entry["episodes"]:
        ep["streams"] = dedup_stream_list(ep["streams"])
    entry["category"] = classify_category(entry, stype)

    return entry


# ------------------------------------------- expiration & progress io ------

def get_progress_path(shard_index: int, total_shards: int) -> str:
    if total_shards > 1:
        return os.path.join(OUT_DIR, f"moviebox_progress_shard_{shard_index}.jsonl")
    return os.path.join(OUT_DIR, "moviebox_progress.jsonl")


def load_progress(progress_file: str) -> dict:
    done = {}
    if not os.path.exists(progress_file):
        return done
    with open(progress_file, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            sid = rec.get("id")
            if sid:
                rec["genres"] = normalize_genres(rec.get("genres"))
                rec["category"] = classify_category(rec)
                done[str(sid)] = rec
    return done


def load_all_merged_progress() -> dict:
    """Load and deduplicate current-run shard progress files only."""
    merged = {}
    patterns = [
        os.path.join(OUT_DIR, "moviebox_progress.jsonl"),
        os.path.join(OUT_DIR, "moviebox_progress_shard_*.jsonl"),
    ]
    files = []
    for pat in patterns:
        files.extend(sorted(glob.glob(pat)))

    for pfile in files:
        shard_data = load_progress(pfile)
        for sid, rec in shard_data.items():
            rec["genres"] = normalize_genres(rec.get("genres"))
            rec["category"] = classify_category(rec)
            merged[sid] = rec
    return merged


def compact_progress(progress_file: str, done: dict):
    os.makedirs(OUT_DIR, exist_ok=True)
    tmp = progress_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        for entry in done.values():
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    os.replace(tmp, progress_file)


def append_progress(progress_file: str, entry: dict):
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(progress_file, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------- main -----

def write_catalog(done: dict):
    """Replace old `catalog_part_*.json` chunks with fresh ones and update `index.json`."""
    os.makedirs(OUT_DIR, exist_ok=True)

    for fname in os.listdir(OUT_DIR):
        if fname.startswith("catalog_part_") and fname.endswith(".json"):
            os.remove(os.path.join(OUT_DIR, fname))

    counts_by_cat = {"movies": 0, "series": 0, "anime": 0, "short_tv": 0}
    genre_counts = {}
    all_items = []
    for entry in done.values():
        entry["genres"] = normalize_genres(entry.get("genres"))
        cat = classify_category(entry)
        entry["category"] = cat
        counts_by_cat[cat] = counts_by_cat.get(cat, 0) + 1
        for g in (entry.get("genres") or []):
            genre_counts[g] = genre_counts.get(g, 0) + 1
        all_items.append(entry)

    sorted_genres = dict(sorted(genre_counts.items(), key=lambda kv: (-kv[1], kv[0])))

    part_num = 1
    current_chunk = []
    current_bytes = 2
    generated_files = []
    hasher = hashlib.sha256()

    for item in all_items:
        serialized = json.dumps(item, ensure_ascii=False, separators=(",", ":"))
        encoded = serialized.encode("utf-8")
        hasher.update(encoded)
        item_len = len(encoded) + 1

        if current_chunk and (current_bytes + item_len > MAX_CHUNK_BYTES):
            filename = f"catalog_part_{part_num}.json"
            tmp_path = os.path.join(OUT_DIR, filename + ".tmp")
            final_path = os.path.join(OUT_DIR, filename)
            with open(tmp_path, "w", encoding="utf-8") as fh:
                json.dump(current_chunk, fh, ensure_ascii=False, separators=(",", ":"))
            os.replace(tmp_path, final_path)
            generated_files.append(filename)

            part_num += 1
            current_chunk = []
            current_bytes = 2

        current_bytes += item_len
        current_chunk.append(item)

    if current_chunk:
        filename = f"catalog_part_{part_num}.json"
        tmp_path = os.path.join(OUT_DIR, filename + ".tmp")
        final_path = os.path.join(OUT_DIR, filename)
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(current_chunk, fh, ensure_ascii=False, separators=(",", ":"))
        os.replace(tmp_path, final_path)
        generated_files.append(filename)

    part_entries = []
    for filename in generated_files:
        fpath = os.path.join(OUT_DIR, filename)
        size_bytes = os.path.getsize(fpath)
        with open(fpath, "r", encoding="utf-8") as fh:
            part_len = len(json.load(fh))
        part_entries.append({
            "file": filename,
            "items": part_len,
            "size_bytes": size_bytes,
            "size_mb": round(size_bytes / (1024 * 1024), 2),
        })

    manifest = {
        "version_hash": hasher.hexdigest(),
        "updated_at_epoch": int(time.time()),
        "total_items": len(all_items),
        "categories": counts_by_cat,
        "genres": sorted_genres,
        "total_parts": len(generated_files),
        "files": generated_files,
        "parts": part_entries,
    }
    tmp_index = INDEX_FILE + ".tmp"
    with open(tmp_index, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)
    os.replace(tmp_index, INDEX_FILE)

    return {
        "movies": counts_by_cat["movies"],
        "series": counts_by_cat["series"],
        "anime": counts_by_cat["anime"],
        "short_tv": counts_by_cat["short_tv"],
        "genres": len(sorted_genres),
        "total": len(all_items),
        "parts": len(generated_files),
    }


def main():
    parser = argparse.ArgumentParser(description="Scrape MovieBox catalog into ~10MB JSON chunks.")
    parser.add_argument("--max-titles", type=int, default=3000,
                        help="Max titles to fetch in this run (default 3000; 0 = unlimited).")
    parser.add_argument("--shard-index", type=int, default=0,
                        help="Current worker shard index (0-based).")
    parser.add_argument("--total-shards", type=int, default=1,
                        help="Total parallel worker shards.")
    parser.add_argument("--merge-only", action="store_true",
                        help="Merge current run shard files into ~10MB JSON chunks and exit.")
    parser.add_argument("--seeds", default="",
                        help="Comma-separated search keywords instead of the default sweep.")
    args = parser.parse_args()

    os.makedirs(OUT_DIR, exist_ok=True)

    if args.merge_only:
        merged = load_all_merged_progress()
        counts = write_catalog(merged)
        log(f"MERGE DONE: movies={counts['movies']}, series={counts['series']}, "
            f"anime={counts['anime']}, short_tv={counts['short_tv']}, "
            f"genres={counts['genres']} -> {counts['parts']} chunks")
        return 0

    progress_file = get_progress_path(args.shard_index, args.total_shards)
    if os.path.exists(progress_file):
        os.remove(progress_file)

    log(f"shard {args.shard_index + 1}/{args.total_shards} | fresh run -> {progress_file}")
    done = {}

    seeds = SEARCH_SEEDS
    if args.seeds:
        seeds = [s.strip() for s in args.seeds.split(",") if s.strip()]

    mb = MovieBox()
    if not mb.login():
        log("could not log in - the API may be down or blocking this IP")
        return 1

    candidates = {}

    log("collecting homepage tabs")
    for tab in TAB_IDS:
        empty_pages = 0
        for page in range(1, MAX_HOMEPAGE_PAGES + 1):
            payload = mb.homepage(tab, page)
            time.sleep(REQUEST_DELAY)
            subjects = subjects_from_homepage(payload)
            if not subjects:
                break
            added = 0
            for s in subjects:
                sid = s.get("subjectId") or s.get("id")
                if sid is None:
                    continue
                sid = str(sid)
                if sid not in candidates:
                    candidates[sid] = s
                    added += 1
            if added == 0:
                empty_pages += 1
                if empty_pages >= 2:
                    break
            else:
                empty_pages = 0

    log("collecting search results")
    for seed in seeds:
        empty_pages = 0
        for page in range(1, MAX_SEARCH_PAGES + 1):
            payload = mb.search(seed, page)
            time.sleep(REQUEST_DELAY)
            subjects = subjects_from_search(payload)
            if not subjects:
                break
            added = 0
            for s in subjects:
                sid = s.get("subjectId") or s.get("id")
                if sid is None:
                    continue
                sid = str(sid)
                if sid not in candidates:
                    candidates[sid] = s
                    added += 1
            if added == 0 and page > 1:
                empty_pages += 1
                if empty_pages >= 2:
                    break
            else:
                empty_pages = 0

    if args.total_shards > 1:
        candidates = {
            sid: subj for sid, subj in candidates.items()
            if (int(hashlib.md5(sid.encode()).hexdigest(), 16) % args.total_shards) == args.shard_index
        }

    log(f"shard {args.shard_index + 1}/{args.total_shards} total assigned titles: {len(candidates)}")

    pending = list(candidates.keys())
    limit = args.max_titles if args.max_titles > 0 else None
    if limit:
        pending = pending[:limit]

    log(f"to fetch in this run: {len(pending)}")

    for i, sid in enumerate(pending, 1):
        subject = candidates[sid]
        try:
            entry = subject_to_entry(subject, mb, with_streams=True)
        except Exception as exc:
            log(f"[{i}/{len(pending)}] {sid} FAILED: {exc}")
            entry = None

        if entry:
            append_progress(progress_file, entry)
            done[sid] = entry
            sample_url = ""
            if i <= 10:
                first_st = next((s.get("url") for s in (entry.get("streams") or []) if s.get("url")), "")
                if not first_st:
                    for ep in (entry.get("episodes") or []):
                        first_st = next((s.get("url") for s in (ep.get("streams") or []) if s.get("url")), "")
                        if first_st:
                            break
                if first_st:
                    sample_url = f" sample_url={first_st}"
            log(f"[{i}/{len(pending)}] {entry['title'][:42]} "
                f"[{entry['category']}] streams={len(entry['streams'])} "
                f"eps={len(entry['episodes'])}{sample_url}")
        else:
            fallback = subject_to_entry(subject, mb, with_streams=False)
            if fallback:
                fallback["note"] = "details fetch failed"
                append_progress(progress_file, fallback)
                done[sid] = fallback
                log(f"[{i}/{len(pending)}] {sid} saved metadata only")

        if i % 50 == 0:
            compact_progress(progress_file, done)
            if args.total_shards == 1:
                counts = write_catalog(done)
                log(f"  checkpoint: {counts['total']} titles across {counts['parts']} chunks")

        time.sleep(REQUEST_DELAY)

    compact_progress(progress_file, done)
    if args.total_shards == 1:
        counts = write_catalog(done)
        log(f"done: {counts['total']} titles -> {counts['parts']} chunks in {OUT_DIR}")
    else:
        log(f"shard {args.shard_index + 1}/{args.total_shards} run complete ({len(done)} titles in shard)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
