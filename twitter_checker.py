import logging
import requests
import time

log = logging.getLogger("twitter-verifier")

_twitter_cache = {}

def get_twitter_username_from_opensea(slug: str, opensea_api_key: str):
    if slug in _twitter_cache:
        username, timestamp = _twitter_cache[slug]
        if time.time() - timestamp < 300:
            return username
    
    try:
        url = f"https://api.opensea.io/api/v2/collections/{slug}"
        headers = {"x-api-key": opensea_api_key}
        resp = requests.get(url, headers=headers, timeout=5)
        
        if resp.status_code == 200:
            username = resp.json().get("twitter_username")
            _twitter_cache[slug] = (username, time.time())
            return username
        else:
            log.warning(f"[Twitter] HTTP {resp.status_code} لـ {slug}")
    except Exception as e:
        log.warning(f"[Twitter] خطأ لـ {slug}: {e}")
    
    return None
