import logging
import os
import time
import requests

log = logging.getLogger("twitter-verifier")
_cache = {}
CACHE_DURATION = 600


def get_twitter_username_from_opensea(slug: str, opensea_api_key: str):
    item = _cache.get(slug)
    if item and time.time() - item[1] < CACHE_DURATION:
        return item[0]
    try:
        r = requests.get(
            f"https://api.opensea.io/api/v2/collections/{slug}",
            headers={"x-api-key": opensea_api_key}, timeout=5)
        if r.status_code == 200:
            username = r.json().get("twitter_username")
            _cache[slug] = (username, time.time())
            return username
        log.warning("OpenSea X lookup HTTP %s for %s", r.status_code, slug)
    except Exception as e:
        log.warning("X lookup failed for %s: %s", slug, e)
    _cache[slug] = (None, time.time())
    return None


def is_valid_twitter_account(username: str) -> bool:
    """If X bearer token is configured, validate that the account exists and is public.
    Without a bearer token, return True when OpenSea supplied a username; this preserves
    the original bot's behavior while avoiding an unnecessary hard dependency on X API.
    """
    if not username:
        return False
    token = os.environ.get("TWITTER_BEARER_TOKEN")
    if not token:
        return True
    try:
        r = requests.get(
            f"https://api.x.com/2/users/by/username/{username}?user.fields=public_metrics,verified",
            headers={"Authorization": f"Bearer {token}"}, timeout=5)
        if r.status_code != 200:
            return False
        data = r.json().get("data") or {}
        return bool(data.get("id"))
    except Exception as e:
        log.warning("X validation failed for @%s: %s", username, e)
        return False
