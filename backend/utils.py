
import os
import random
import python_socks
from urllib.parse import urlparse
from database import fetch_all, execute, now_iso

def get_device_fingerprint():
    """生成随机的设备指纹，用于伪装 Telegram 客户端"""
    devices = [
        {"device_model": "iPhone 13 Pro", "system_version": "15.4.1", "app_version": "9.6.3", "lang_code": "en", "system_lang_code": "en-US"},
        {"device_model": "iPhone 14", "system_version": "16.1", "app_version": "10.0.1", "lang_code": "en", "system_lang_code": "en-US"},
        {"device_model": "Samsung Galaxy S22", "system_version": "Android 12", "app_version": "9.5.0", "lang_code": "en", "system_lang_code": "en-US"},
        {"device_model": "Google Pixel 6", "system_version": "Android 13", "app_version": "9.7.1", "lang_code": "en", "system_lang_code": "en-US"},
        {"device_model": "OnePlus 10 Pro", "system_version": "Android 12", "app_version": "9.4.2", "lang_code": "en", "system_lang_code": "en-US"},
        {"device_model": "Xiaomi 12", "system_version": "Android 12", "app_version": "9.6.0", "lang_code": "en", "system_lang_code": "en-US"},
        {"device_model": "iPad Pro (11-inch)", "system_version": "15.5", "app_version": "9.5.4", "lang_code": "en", "system_lang_code": "en-US"},
        {"device_model": "Samsung Galaxy Tab S8", "system_version": "Android 12", "app_version": "9.6.1", "lang_code": "en", "system_lang_code": "en-US"}
    ]
    return random.choice(devices)

async def get_proxy_config():
    # 1. Try to get from DB
    try:
        rows = await fetch_all(
            "SELECT id, url FROM proxies WHERE status = 'active' ORDER BY COALESCE(last_used, '') ASC, id ASC LIMIT 1"
        )
        if rows:
            await execute("UPDATE proxies SET last_used = ? WHERE id = ?", (now_iso(), rows[0]["id"]))
            return parse_proxy_url(rows[0]["url"])
    except Exception:
        pass

    # 2. Try environment variables
    sys_proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or \
                os.environ.get("https_proxy") or os.environ.get("http_proxy")
    
    if sys_proxy:
        return parse_proxy_url(sys_proxy)
        
    return None

def parse_proxy_url(url):
    if not url:
        return None
        
    try:
        parsed = urlparse(url)
        scheme = parsed.scheme.lower()
        
        proxy_type = None
        if "socks5" in scheme:
            proxy_type = python_socks.ProxyType.SOCKS5
        elif "socks4" in scheme:
            proxy_type = python_socks.ProxyType.SOCKS4
        elif "http" in scheme:
            proxy_type = python_socks.ProxyType.HTTP
        else:
            return None
            
        return {
            "proxy_type": proxy_type,
            "addr": parsed.hostname,
            "port": parsed.port,
            "username": parsed.username,
            "password": parsed.password,
            "rdns": True
        }
    except Exception:
        return None
