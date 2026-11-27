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
    "https://api.inmoviebox.com",
]

RETRY_STATUS = {403, 406, 407, 429, 500, 502, 503, 504}
HTTP_TIMEOUT = 6

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
REQUEST_DELAY = 0.05
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
            resp = self.sess.request(method, url, headers=headers, data=data_bytes, timeout=HTTP_TIMEOUT)
            return resp.status_code, resp.headers, resp.text
        req = Request(url, data=data_bytes, headers=headers, method=method)
        try:
            with urlopen(req, timeout=HTTP_TIMEOUT) as r:
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
            self.token = None
            if self.login():
                return self._raw_request(method, path, body, self.token)
        return result

    def get(self, path):
        return self._request("GET", path)

    def post(self, path, payload):
        return self._request("POST", path, json.dumps(payload, separators=(",", ":")))

    def login(self):
        for attempt in range(2):
            val = self._raw_request(
                "POST", "/wefeed-mobile-bff/user-api/visitor-login", "{}", None
            )
            if isinstance(val, dict) and val.get("token"):
                self.token = val["token"]
                return True
            time.sleep(1.0)
        return False

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

        if
