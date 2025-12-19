import os
from typing import Dict, Any, Optional
from services.services_cache_service import cache


def get_proxy_config() -> Dict[str, Any]:
    cfg = {}
    try:
        cfg = cache.get('proxy_config') or {}
    except Exception:
        cfg = {}
    url = str(cfg.get('url') or '').strip()
    ignore_ssl = bool(cfg.get('ignore_ssl', False))
    if not url:
        https = os.environ.get('HTTPS_PROXY') or ''
        http = os.environ.get('HTTP_PROXY') or ''
        url = https or http
        if url:
            ignore_ssl = True
    return { 'url': url, 'ignore_ssl': ignore_ssl }


def set_proxy_config(url: str, ignore_ssl: bool) -> None:
    cache.set('proxy_config', { 'url': str(url or '').strip(), 'ignore_ssl': bool(ignore_ssl) }, expire=None)


def requests_kwargs(timeout: Optional[int] = 15, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    cfg = get_proxy_config()
    url = cfg.get('url') or ''
    verify = not bool(cfg.get('ignore_ssl', False))
    kw: Dict[str, Any] = { 'timeout': timeout or 15 }
    if headers:
        kw['headers'] = headers
    else:
        kw['headers'] = { 'User-Agent': 'Mozilla/5.0' }
    if url:
        kw['proxies'] = { 'http': url, 'https': url }
    kw['verify'] = verify
    return kw


def chrome_options(headless: bool = True):
    try:
        from selenium.webdriver.chrome.options import Options
        opts = Options()
        if headless:
            opts.add_argument('--headless=new')
        opts.add_argument('--disable-gpu')
        opts.add_argument('--window-size=1920,1080')
        opts.add_argument('--disable-dev-shm-usage')
        opts.add_argument('--no-sandbox')
        opts.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36')
        return opts
    except Exception:
        return None
