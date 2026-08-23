import os
import logging
import requests

log = logging.getLogger("twitter-verifier")

def get_twitter_username_from_opensea(slug: str, opensea_api_key: str) -> str | None:
    try:
        url = f"https://api.opensea.io/api/v2/collections/{slug}"
        headers = {"x-api-key": opensea_api_key}
        resp = requests.get(url, headers=headers, timeout=5)
        if resp.status_code == 200:
            return resp.json().get("twitter_username")
        else:
            log.warning(f"[OpenSea Collections API] HTTP {resp.status_code} عند جلب '{slug}': {resp.text[:200]}")
    except Exception as e:
        log.warning(f"[Twitter Check] تعذر جلب معلومات المجموعة لـ {slug}: {e}")
    return None
