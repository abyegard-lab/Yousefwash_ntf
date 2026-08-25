"""OpenSea-only X metadata lookup.

The bot accepts an X account only when OpenSea's collection endpoint supplies
a twitter_username. No X API/Bearer token is used. Temporary HTTP failures
(429/5xx/timeouts) are never cached as a negative result.
"""
import logging
import time
import requests

log = logging.getLogger("twitter-verifier")
_cache = {}
_last_status = {}
CACHE_DURATION = 3600
NEGATIVE_CACHE_DURATION = 300

def get_twitter_username_from_opensea(slug: str, opensea_api_key: str):
    now = time.time()
    item = _cache.get(slug)
    if item:
        value, timestamp, kind = item
        ttl = CACHE_DURATION if kind == "ok" else NEGATIVE_CACHE_DURATION
        if now - timestamp < ttl:
            return value
    try:
        r = requests.get(
            f"https://api.opensea.io/api/v2/collections/{slug}",
            headers={"x-api-key": opensea_api_key}, timeout=8)
        if r.status_code == 200:
            username = (r.json().get("twitter_username") or "").strip().lstrip("@").strip()
            if username:
                _cache[slug] = (username, now, "ok")
                _last_status[slug] = "ok"
                return username
            _cache[slug] = (None, now, "negative")
            _last_status[slug] = "negative"
            return None
        if r.status_code in (429, 500, 502, 503, 504):
            _last_status[slug] = "temporary"
            log.warning("OpenSea X lookup temporary HTTP %s for %s", r.status_code, slug)
            return None
        if r.status_code == 404:
            _cache[slug] = (None, now, "negative")
            _last_status[slug] = "negative"
            return None
        _last_status[slug] = "temporary"
        log.warning("OpenSea X lookup HTTP %s for %s", r.status_code, slug)
        return None
    except (requests.Timeout, requests.ConnectionError) as e:
        _last_status[slug] = "temporary"
        log.warning("OpenSea X lookup temporary failure for %s: %s", slug, e)
        return None
    except Exception as e:
        _last_status[slug] = "temporary"
        log.warning("OpenSea X lookup failed for %s: %s", slug, e)
        return None

def get_last_lookup_status(slug: str) -> str:
    return _last_status.get(slug, "unknown")

def is_valid_twitter_account(username: str) -> bool:
    # Validation source is OpenSea only. Presence of twitter_username is enough.
    return bool(username and str(username).strip())
