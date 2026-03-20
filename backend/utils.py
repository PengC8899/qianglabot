
import os
import python_socks
from urllib.parse import urlparse
from database import fetch_all

async def get_proxy_config():
    # 1. Try to get from DB
    try:
        rows = await fetch_all("SELECT url FROM proxies WHERE status = 'active' ORDER BY RANDOM() LIMIT 1")
        if rows:
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
