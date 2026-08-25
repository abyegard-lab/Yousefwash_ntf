"""OpenSea-only X metadata lookup with rate-limit backoff and caching."""
import logging
import time
import requests

log = logging.getLogger("twitter-verifier")

# slug -> (username_or_none, timestamp, kind)
_cache = {}
_last_status = {}
_retry_after = {}
_fail_count = {}

OK_CACHE_SECONDS = 3600
NEGATIVE_CACHE_SECONDS = 600
BASE_RETRY_SECONDS = 30
MAX_RETRY_SECONDS = 600
TIMEOUT = 8

TEMPORARY_CODES = {429, 500, 502, 503, 504}

def _backoff(slug: str) -> int:
    n = min(_fail_count.get(slug, 0) + 1, 5)
    delay = min(MAX_RETRY_SECONDS, BASE_RETRY_SECONDS * (2 ** (n - 1)))
    _fail_count[slug] = n
    _retry_after[slug] = time.time() + delay
    return delay

def get_twitter_username_from_opensea(slug: str, opensea_api_key: str):
    now = time.time()

    retry_at = _retry_after.get(slug, 0)
    if retry_at > now:
        _last_status[slug] = "temporary"
        return None

    item = _cache.get(slug)
    if item:
        value, timestamp, kind = item
        ttl = OK_CACHE_SECONDS if kind == "ok" else NEGATIVE_CACHE_SECONDS
        if now - timestamp < ttl:
            _last_status[slug] = kind
            return value

    try:
        r = requests.get(
            f"https://api.opensea.io/api/v2/collections/{slug}",
            headers={"x-api-key": opensea_api_key, "accept": "application/json"},
            timeout=TIMEOUT,
        )

        if r.status_code == 200:
            username = (r.json().get("twitter_username") or "").strip().lstrip("@").strip()
            _fail_count.pop(slug, None)
            _retry_after.pop(slug, None)
            if username:
                _cache[slug] = (username, now, "ok")
                _last_status[slug] = "ok"
                return username
            _cache[slug] = (None, now, "negative")
            _last_status[slug] = "negative"
            return None

        if r.status_code in TEMPORARY_CODES:
            delay = _backoff(slug)
            _last_status[slug] = "temporary"
            log.warning("OpenSea X lookup temporary HTTP %s for %s; retry in %ss", r.status_code, slug, delay)
            return None

        if r.status_code == 404:
            _fail_count.pop(slug, None)
            _retry_after.pop(slug, None)
            _cache[slug] = (None, now, "negative")
            _last_status[slug] = "negative"
            return None

        delay = _backoff(slug)
        _last_status[slug] = "temporary"
        log.warning("OpenSea X lookup HTTP %s for %s; retry in %ss", r.status_code, slug, delay)
        return None

    except (requests.Timeout, requests.ConnectionError) as e:
        delay = _backoff(slug)
        _last_status[slug] = "temporary"
        log.warning("OpenSea X lookup temporary failure for %s; retry in %ss: %s", slug, delay, e)
        return None
    except Exception as e:
        delay = _backoff(slug)
        _last_status[slug] = "temporary"
        log.warning("OpenSea X lookup failed for %s; retry in %ss: %s", slug, delay, e)
        return None

def get_last_lookup_status(slug: str) -> str:
    return _last_status.get(slug, "unknown")

def is_valid_twitter_account(username: str) -> bool:
    # OpenSea only: presence of twitter_username is sufficient.
    return bool(username and str(username).strip())
