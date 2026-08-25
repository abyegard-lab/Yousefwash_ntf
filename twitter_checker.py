import os
import logging
import requests
import time

log = logging.getLogger("twitter-verifier")

_twitter_cache = {}
CACHE_DURATION = 300

def get_twitter_username_from_opensea(slug: str, opensea_api_key: str):
    if slug in _twitter_cache:
        username, timestamp = _twitter_cache[slug]
        if time.time() - timestamp < CACHE_DURATION:
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
            log.warning(f"[OpenSea] HTTP {resp.status_code} عند جلب '{slug}'")
    except Exception as e:
        log.warning(f"[Twitter] تعذر جلب معلومات {slug}: {e}")
    return None

def is_valid_twitter_account(username: str) -> bool:
    bearer_token = os.environ.get("TWITTER_BEARER_TOKEN")
    if not username or not bearer_token:
        return False

    try:
        url = f"https://api.x.com/2/users/by/username/{username}?user.fields=verified,public_metrics"
        headers = {"Authorization": f"Bearer {bearer_token}"}
        resp = requests.get(url, headers=headers, timeout=5)

        if resp.status_code == 200:
            user_data = resp.json().get("data", {})
            metrics = user_data.get("public_metrics", {})
            is_verified = user_data.get("verified", False)
            followers_count = metrics.get("followers_count", 0)

            if is_verified or followers_count >= 100:
                log.info(f"✅ حساب X موثوق: @{username}")
                return True
            else:
                log.info(f"⚠️ حساب X ضعيف: @{username}")
                return False
        else:
            log.error(f"[X API] HTTP {resp.status_code} عند فحص @{username}")
            return False

    except Exception as e:
        log.error(f"[X API] خطأ: {e}")
        return False
