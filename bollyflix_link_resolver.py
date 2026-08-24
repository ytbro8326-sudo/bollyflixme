#!/usr/bin/env python3
"""
BollyFlix End-to-End Universal Link Flow Resolver
=================================================
Resolves all download & mirror links for ANY BollyFlix movie URL down to the LAST/FINAL direct links:
Features:
- Scrapes all release quality sections directly from any BollyFlix movie post
- FastDL -> GDFlix deep resolver:
    • Parses every single link inside <div class="text-center"> on https://new3.gdflix.io/file/<id>
    • INSTANT DL -> Direct instant.busycdn.xyz link  to  https://fastdl-one.pages.dev/?url=...
    • CLOUD DOWNLOAD [R2] -> Direct R2 storage URL
    • DIRECT SERVER [MGT] -> Direct indexserver stream link
    • FAST CLOUD / ZIPDISK -> Direct Cloudflare Workers Cloud Resume Download link
    • TELEGRAM -> Direct Telegram bot link (tgredirect / filesgram)
    • GOFILE / MIRRORS -> Direct Multiup / GoFlix link  to  https://gofile.io/d/...
      + Deeply extracts 1fichier.com and megaup.net mirror links from the mirror page
- LinksMod Mirror deep resolver:
    • Resolves VikingFile, MixDrop, Gofile, 1fichier, DailyUploads, MultiUp, MegaUp, etc.
"""

import sys
import os
import re
import json
import ssl
import time
from typing import Dict, List, Optional, Any, Tuple, Set
from urllib.parse import urlparse, parse_qs, unquote, urljoin, urlencode
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

try:
    from curl_cffi import requests as cffi_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False

# ─────────────────────────────────────────────────────────────────────────────
# TARGET URL CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
TARGET_YEAR_URL = "https://bollyflix.free/movies-by-year/2026/"
TXT_OUTPUT_FILE = "bollyflix.txt"
JSON_OUTPUT_FILE = "resolved_movie_links.json"
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "6fad3f86b8452ee232deb7977d7dcf58")

# Proxy & Debug Configuration

RESIDENTIAL_PROXIES = [
    "http://dxicdysy:yndikr9coeto@31.59.20.176:6754",
    "http://viqhajod:aisg6z1gsn25@31.59.20.176:6754",
    "http://viqhajod:aisg6z1gsn25@31.56.127.193:7684",
    "http://viqhajod:aisg6z1gsn25@45.38.107.97:6014",
    "http://viqhajod:aisg6z1gsn25@198.105.121.200:6462",
    "http://viqhajod:aisg6z1gsn25@64.137.96.74:6641",
    "http://viqhajod:aisg6z1gsn25@198.23.243.226:6361",
    "http://viqhajod:aisg6z1gsn25@38.154.185.97:6370",
    "http://viqhajod:aisg6z1gsn25@84.247.60.125:6095",
    "http://viqhajod:aisg6z1gsn25@142.111.67.146:5611",
    "http://viqhajod:aisg6z1gsn25@191.96.254.138:6185",
]


CUSTOM_ENV_PROXY = os.environ.get("RESIDENTIAL_PROXY", os.environ.get("PROXY", "")).strip()
if CUSTOM_ENV_PROXY and CUSTOM_ENV_PROXY not in RESIDENTIAL_PROXIES:
    RESIDENTIAL_PROXIES.insert(0, CUSTOM_ENV_PROXY)

ENABLE_DEBUG = os.environ.get("DEBUG", "1").strip().lower() in ["1", "true", "yes"]

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}



class LinkFlowSession:
    """
    HTTP Client with:
    1. curl_cffi Chrome-120 TLS fingerprint impersonation
    2. Optional / Fallback residential proxy support
    3. Multi-layer fallback to plain requests and urllib
    4. Verbose request/response debugger
    """
    def __init__(self, timeout: int = 25, proxy: Optional[str] = None):
        self.timeout = timeout
        self.proxy = proxy if proxy is not None else RESIDENTIAL_PROXY
        self.session = None
        self._init_session()

    def _init_session(self, use_proxy: bool = False):
        if HAS_CURL_CFFI:
            proxies = {"http": self.proxy, "https": self.proxy} if (use_proxy and self.proxy) else None
            try:
                self.session = cffi_requests.Session(impersonate="chrome120", proxies=proxies)
            except Exception as e:
                if ENABLE_DEBUG:
                    print(f"  [DEBUG] Failed to init curl_cffi session (proxy={use_proxy}): {e}")
                self.session = None
        else:
            self.session = None

    def log_debug(self, msg: str) -> None:
        if ENABLE_DEBUG:
            print(f"  [DEBUG] {msg}", flush=True)

    def get(self, url: str, headers: Optional[Dict[str, str]] = None, allow_redirects: bool = True, timeout: Optional[int] = None):
        t = timeout or self.timeout
        h = {**DEFAULT_HEADERS, **(headers or {})}
        
        # 1. Try curl_cffi direct
        if HAS_CURL_CFFI:
            try:
                s = cffi_requests.Session(impersonate="chrome120")
                resp = s.get(url, headers=h, allow_redirects=allow_redirects, timeout=t)
                text = resp.text if hasattr(resp, "text") else ""
                self.log_debug(f"GET (curl_cffi direct) {url[:70]} -> HTTP {resp.status_code} ({len(text)} bytes)")
                if resp.status_code == 200 and len(text) > 500 and "Just a moment" not in text:
                    return resp
            except Exception as exc:
                self.log_debug(f"curl_cffi direct failed for {url[:50]}: {exc}")

        # 2. Try curl_cffi rotating through Residential Proxy pool
        if HAS_CURL_CFFI and RESIDENTIAL_PROXIES:
            for p in RESIDENTIAL_PROXIES[:3]:
                try:
                    proxies = {"http": p, "https": p}
                    s = cffi_requests.Session(impersonate="chrome120", proxies=proxies)
                    resp = s.get(url, headers=h, allow_redirects=allow_redirects, timeout=t)
                    text = resp.text if hasattr(resp, "text") else ""
                    self.log_debug(f"GET (curl_cffi proxy {p.split('@')[-1]}) {url[:70]} -> HTTP {resp.status_code} ({len(text)} bytes)")
                    if resp.status_code == 200 and len(text) > 500 and "Just a moment" not in text:
                        return resp
                except Exception as exc:
                    self.log_debug(f"curl_cffi proxy ({p.split('@')[-1]}) failed: {exc}")

        # 3. Fallback to standard requests (direct)
        try:
            import requests as std_requests
            r = std_requests.get(url, headers=h, allow_redirects=allow_redirects, timeout=t)
            self.log_debug(f"GET (requests direct) {url[:70]} -> HTTP {r.status_code} ({len(r.text)} bytes)")
            if r.status_code == 200 and "Just a moment" not in r.text:
                return r
        except Exception as exc:
            self.log_debug(f"requests direct failed: {exc}")

        # 4. Fallback to standard requests with proxy pool rotation
        if RESIDENTIAL_PROXIES:
            for p in RESIDENTIAL_PROXIES[:3]:
                try:
                    import requests as std_requests
                    proxies = {"http": p, "https": p}
                    r = std_requests.get(url, headers=h, proxies=proxies, allow_redirects=allow_redirects, timeout=t)
                    self.log_debug(f"GET (requests proxy {p.split('@')[-1]}) {url[:70]} -> HTTP {r.status_code} ({len(r.text)} bytes)")
                    if r.status_code == 200 and "Just a moment" not in r.text:
                        return r
                except Exception as exc:
                    self.log_debug(f"requests proxy failed: {exc}")

        # 5. Final fallback to urllib
        try:
            req = urllib.request.Request(url, headers=h)
            resp = urllib.request.urlopen(req, timeout=t)
            self.log_debug(f"GET (urllib) {url[:70]} -> HTTP {resp.status}")
            return resp
        except Exception as exc:
            self.log_debug(f"urllib failed: {exc}")
            raise


    def post(self, url: str, data: Any = None, headers: Optional[Dict[str, str]] = None, timeout: Optional[int] = None):
        t = timeout or self.timeout
        h = {**DEFAULT_HEADERS, **(headers or {})}
        if HAS_CURL_CFFI:
            try:
                s = cffi_requests.Session(impersonate="chrome120")
                return s.post(url, data=data, headers=h, timeout=t)
            except Exception:
                pass
        
        try:
            import requests as std_requests
            return std_requests.post(url, data=data, headers=h, timeout=t)
        except Exception:
            pass

        encoded_data = urlencode(data).encode("utf-8") if isinstance(data, dict) else data
        req = urllib.request.Request(url, data=encoded_data, headers=h)
        return urllib.request.urlopen(req, timeout=t)



# ─────────────────────────────────────────────────────────────────────────────
# 1. FastDL -> GDFlix Deep Resolver (All <div class="text-center"> buttons)
# ─────────────────────────────────────────────────────────────────────────────
def resolve_fastdl_gdflix(url: str, session: Optional[LinkFlowSession] = None) -> Dict[str, Any]:
    session = session or LinkFlowSession(timeout=20)
    results = {
        "fastdl_url": url,
        "gdflix_url": "",
        "instant_dl": "",
        "instant_final": "",
        "r2_cloud": "",
        "direct_server_mgt": "",
        "fast_cloud_page": "",
        "fast_cloud_direct": "",
        "telegram_file": "",
        "gofile_multiup": "",
        "gofile_direct": "",
        "gofile_1fichier": "",
        "gofile_megaup": "",
        "all_buttons": []
    }
    
    gdflix_url = ""
    html = ""
    
    # Step 1: Resolve dl.fastdlserver.site with automatic redirection to GDFlix
    for attempt in range(3):
        try:
            resp = session.get(
                url,
                headers={"Referer": "https://bollyflix.free/"},
                allow_redirects=True,
                timeout=18
            )
            final_u = resp.url if hasattr(resp, "url") else ""
            if "gdflix" in final_u or "file/" in final_u:
                gdflix_url = final_u.replace("gdflix.dev", "new3.gdflix.io")
                html = resp.text if hasattr(resp, "text") else ""
                break
            elif hasattr(resp, "headers") and ("Location" in resp.headers or "location" in resp.headers):
                loc = resp.headers.get("Location") or resp.headers.get("location")
                gdflix_url = loc.replace("gdflix.dev", "new3.gdflix.io")
                break
        except Exception:
            time.sleep(1)

    results["gdflix_url"] = gdflix_url

    # Step 2: Fetch GDFlix page content if not already loaded
    if gdflix_url:
        if not html or "Instant DL" not in html:
            for attempt in range(3):
                try:
                    resp = session.get(gdflix_url, headers={"Referer": "https://gdflix.dev/"}, timeout=18)
                    t_html = resp.text if hasattr(resp, "text") else ""
                    if t_html:
                        html = t_html
                        break
                except Exception:
                    time.sleep(1)

        soup = BeautifulSoup(html, "html.parser")
        
        target_div = None
        for div in soup.find_all("div", class_="text-center"):
            if div.find("a", href=True):
                target_div = div
                break
                
        a_tags = target_div.find_all("a", href=True) if target_div else soup.find_all("a", href=True)
        
        for a in a_tags:
            href = a["href"].strip()
            txt = a.get_text(strip=True)
            if not href or href.startswith("#") or "/login" in href:
                continue
            
            results["all_buttons"].append({"text": txt, "href": href})
            
            # 1. Instant DL [10GBPS]
            if "instant.busycdn.xyz" in href or "Instant DL" in txt:
                results["instant_dl"] = href
                for i_att in range(3):
                    try:
                        ri = session.get(href, headers={"Referer": gdflix_url}, allow_redirects=True, timeout=15)
                        if ri.url and ri.url != href:
                            results["instant_final"] = ri.url
                            break
                    except Exception:
                        time.sleep(0.5)
                
            # 2. CLOUD DOWNLOAD [R2]
            elif ".r2.dev" in href or "R2" in txt:
                results["r2_cloud"] = href
                
            # 3. DIRECT SERVER [MGT]
            elif "indexserver.site" in href or "DIRECT SERVER" in txt:
                results["direct_server_mgt"] = href
                
            # 4. FAST CLOUD / ZIPDISK
            elif "/cloud/" in href or "/cflare/" in href or "FAST CLOUD" in txt or "ZIPDISK" in txt:
                cloud_page_url = urljoin("https://new3.gdflix.io", href)
                results["fast_cloud_page"] = cloud_page_url
                
            # 5. Telegram
            elif any(k in href for k in ["tgredirect", "filesgram", "t.me"]) or "Telegram" in txt:
                results["telegram_file"] = href
                
            # 6. GoFile / Multiup -> Deeply resolve mirror page to direct Gofile.io, 1fichier, Megaup
            elif any(k in href.lower() for k in ["goflix.sbs", "multiup2.workers.dev", "gofile", "multiup"]) or "GoFile" in txt or "Multiup" in txt:
                results["gofile_multiup"] = href
                for m_att in range(3):
                    try:
                        rm = session.get(href, headers={"Referer": gdflix_url}, timeout=15)
                        soup_m = BeautifulSoup(rm.text if hasattr(rm, "text") else "", "html.parser")
                        
                        for sec in soup_m.find_all("section"):
                            h4 = sec.find("h4")
                            host_txt = h4.get_text(strip=True).lower() if h4 else ""
                            footer_a = sec.find("footer").find("a") if sec.find("footer") else None
                            if footer_a:
                                fl = footer_a.get("link") or footer_a.get("href") or ""
                                if fl and not fl.startswith("/download-fast"):
                                    if "gofile" in host_txt or "gofile.io" in fl:
                                        results["gofile_direct"] = fl
                                    elif "1fichier" in host_txt or "1fichier.com" in fl:
                                        results["gofile_1fichier"] = fl
                                    elif "megaup" in host_txt or "megaup.net" in fl:
                                        results["gofile_megaup"] = fl

                        if not results["gofile_direct"]:
                            for ma in soup_m.find_all("a"):
                                mh = ma.get("href", "").strip()
                                m_link = ma.get("link", "").strip()
                                if "gofile.io" in mh or "gofile.io" in m_link:
                                    results["gofile_direct"] = m_link or mh
                                    break
                        if results["gofile_direct"]:
                            break
                    except Exception:
                        time.sleep(0.5)

        # Step 3: Deeply resolve the FAST CLOUD direct Cloud Resume Download link
        if results["fast_cloud_page"] and not results["fast_cloud_direct"]:
            for c_attempt in range(3):
                try:
                    resp_cloud = session.get(results["fast_cloud_page"], headers={"Referer": gdflix_url}, timeout=15)
                    cloud_html = resp_cloud.text if hasattr(resp_cloud, "text") else ""
                    soup_cloud = BeautifulSoup(cloud_html, "html.parser")
                    for ca in soup_cloud.find_all("a", href=True):
                        c_href = ca["href"].strip()
                        if any(k in c_href for k in ["workers.dev", "cloud-dl", "download", ".zip"]):
                            results["fast_cloud_direct"] = c_href
                            break
                    if results["fast_cloud_direct"]:
                        break
                except Exception:
                    time.sleep(0.5)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# 2. LinksMod Universal Resolver (with Automatic CSRF Unlock)
# ─────────────────────────────────────────────────────────────────────────────
def get_provider_name(url: str) -> str:
    domain = urlparse(url).netloc.lower()
    if "gofile" in domain:
        return "Gofile"
    elif "vikingfile" in domain or "vik1ngfile" in domain:
        return "VikingFile"
    elif "mixdrop" in domain or "miixdrop" in domain:
        return "MixDrop"
    elif "megaup" in domain:
        return "MegaUp"
    elif "dailyuploads" in domain:
        return "DailyUploads"
    elif "1fichier" in domain:
        return "1fichier"
    elif "multiup" in domain:
        return "MultiUp"
    elif "streamwish" in domain:
        return "StreamWish"
    elif "voe" in domain:
        return "VOE"
    return domain


def resolve_linksmod(url: str, session: Optional[LinkFlowSession] = None) -> Dict[str, Any]:
    session = session or LinkFlowSession(timeout=20)
    try:
        resp = session.get(url, headers={"Referer": "https://bollyflix.free/"}, timeout=15)
        html = resp.text if hasattr(resp, "text") else ""
    except Exception as e:
        return {"error": f"Failed to fetch LinksMod: {e}", "url": url, "mirrors": []}
    
    soup = BeautifulSoup(html, "html.parser")
    
    unlock_form = soup.find("form")
    if unlock_form:
        form_data = {}
        for inp in unlock_form.find_all("input"):
            name = inp.get("name")
            if name:
                form_data[name] = inp.get("value", "")
        
        if form_data:
            try:
                post_resp = session.post(
                    url,
                    data=form_data,
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded",
                        "Referer": url,
                    },
                    timeout=15
                )
                html = post_resp.text if hasattr(post_resp, "text") else ""
                soup = BeautifulSoup(html, "html.parser")
            except Exception:
                pass

    mirror_links = []
    seen = set()
    
    for a in soup.find_all("a", href=True):
        link = a["href"].strip()
        if any(ignore in link.lower() for ignore in [
            "linksmod.top", "google.com", "bollyflix", "cloudflare", "traffic",
            "facebook", "twitter", "uploadmaza.com", "producebreed.com", "javascript:", "#"
        ]):
            continue
        if link.startswith("http") and link not in seen:
            seen.add(link)
            provider = get_provider_name(link)
            resolved_info = {"provider": provider, "url": link}
            mirror_links.append(resolved_info)
            
    return {
        "linksmod_url": url,
        "mirrors_count": len(mirror_links),
        "mirrors": mirror_links,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 3. Quality Release Resolver Worker
# ─────────────────────────────────────────────────────────────────────────────
def resolve_single_quality(sec: Dict[str, Any]) -> Dict[str, Any]:
    session = LinkFlowSession(timeout=25)
    q_name = sec["quality_name"]
    q_result = {
        "quality": q_name,
        "fastdl_urls": [],
        "gdflix_flows": [],
        "linksmod_urls": [],
        "mirrors": []
    }
    
    for btn in sec["buttons"]:
        b_url = btn["url"]
        try:
            if "fastdl" in b_url or "gdflix" in b_url:
                q_result["fastdl_urls"].append(b_url)
                res = resolve_fastdl_gdflix(b_url, session)
                q_result["gdflix_flows"].append(res)
                q_result["gdflix_flow"] = res
                
                # Add 1fichier and MegaUp from the GDFlix mirror page directly to mirror hosts list
                if res.get("gofile_1fichier"):
                    q_result["mirrors"].append({"provider": "1fichier", "url": res["gofile_1fichier"]})
                if res.get("gofile_megaup"):
                    q_result["mirrors"].append({"provider": "MegaUp", "url": res["gofile_megaup"]})
                    
            elif "linksmod" in b_url:
                extracted_urls = re.findall(r'https?://linksmod\.top/view/[a-zA-Z0-9]+', b_url)
                if not extracted_urls:
                    extracted_urls = [b_url]
                for lu in extracted_urls:
                    q_result["linksmod_urls"].append(lu)
                    res = resolve_linksmod(lu, session)
                    if "mirrors" in res:
                        q_result["mirrors"].extend(res["mirrors"])
        except Exception:
            pass

    # Deduplicate mirrors list while preserving order
    seen_m = set()
    unique_mirrors = []
    for m in q_result["mirrors"]:
        u = m.get("url", "").strip()
        if u and u not in seen_m:
            seen_m.add(u)
            unique_mirrors.append(m)
    q_result["mirrors"] = unique_mirrors

    return q_result


# ─────────────────────────────────────────────────────────────────────────────
# 4. Master BollyFlix End-to-End Resolver
# ─────────────────────────────────────────────────────────────────────────────
def resolve_bollyflix_movie(url: str, session: Optional[LinkFlowSession] = None) -> Dict[str, Any]:
    session = session or LinkFlowSession(timeout=25)
    print(f"\n[+] Fetching BollyFlix Movie: {url}", flush=True)
    
    resp = session.get(url, timeout=20)
    html = resp.text if hasattr(resp, "text") else ""
    soup = BeautifulSoup(html, "html.parser")
    
    title_el = soup.find("title")
    movie_title = title_el.get_text(strip=True).split("|")[0].strip() if title_el else "BollyFlix Movie"
    print(f"[+] Movie Title: {movie_title}", flush=True)
    
    quality_sections = []
    
    for h in soup.find_all(["h3", "h4", "h5", "h6"]):
        txt = h.get_text(strip=True)
        if any(q in txt.lower() for q in ["480p", "720p", "1080p", "2160p", "4k", "hevc"]) \
           and len(txt) < 130 \
           and "available in" not in txt.lower() \
           and "related posts" not in txt.lower():
            
            sec_links = []
            curr = h.next_sibling
            while curr:
                if hasattr(curr, "name") and curr.name in ["h3", "h4", "h5", "h6"]:
                    break
                if hasattr(curr, "find_all"):
                    for a in curr.find_all("a", href=True):
                        href = a["href"].strip()
                        btn_txt = a.get_text(strip=True)
                        if any(k in href for k in ["fastdlserver.site", "linksmod.top", "gdflix", "goflix"]):
                            sec_links.append({"label": btn_txt, "url": href})
                curr = curr.next_sibling
            
            if sec_links:
                quality_sections.append({
                    "quality_name": txt,
                    "buttons": sec_links
                })

    print(f"[*] Found {len(quality_sections)} quality release section(s). Resolving flows...\n", flush=True)

    resolved_qualities = []
    for sec in quality_sections:
        q_name = sec["quality_name"]
        try:
            res = resolve_single_quality(sec)
            resolved_qualities.append(res)
            print(f"  [✓] Resolved: {q_name}", flush=True)
        except Exception as e:
            print(f"  [✗] Error resolving {q_name}: {e}", flush=True)

    return {
        "movie_url": url,
        "movie_title": movie_title,
        "total_qualities": len(resolved_qualities),
        "qualities": resolved_qualities
    }


# ─────────────────────────────────────────────────────────────────────────────
# 5. TMDB & IMDb ID Resolver
# ─────────────────────────────────────────────────────────────────────────────
def clean_movie_title_and_year(raw_title: str, url: str = "") -> Tuple[str, str]:
    year = ""
    m_year = re.search(r"\((\d{4})\)", raw_title) or re.search(r"\b(20\d\d|19\d\d)\b", raw_title)
    if m_year:
        year = m_year.group(1)
    elif url:
        m_url_year = re.search(r"-(20\d\d|19\d\d)-", url)
        if m_url_year:
            year = m_url_year.group(1)

    title = re.sub(r"^(Download\s+)+", "", raw_title, flags=re.IGNORECASE)
    if year:
        title = title.split(f"({year})")[0].split(year)[0]
    
    title = re.split(
        r"\{|\[|\b(480p|720p|1080p|2160p|4k|dual audio|multi audio|hindi|tamil|telugu|kannada|malayalam|marathi|english|movie|season|web-dl|hdtc|hevc|hdts|esub|clean)\b",
        title,
        flags=re.IGNORECASE
    )[0]
    title = re.sub(r"[:\-_|]+", " ", title).strip()

    if not title and url:
        slug = url.rstrip("/").split("/")[-1]
        if year:
            slug = slug.split(year)[0]
        title = slug.replace("-", " ").strip()
        title = re.sub(r"\b(movie|dual audio|hindi|tamil|telugu|kannada|malayalam|english)\b", "", title, flags=re.IGNORECASE).strip()

    return title.strip(), year


def resolve_tmdb_imdb_info(raw_title: str, url: str = "", api_key: str = TMDB_API_KEY) -> Dict[str, str]:
    clean_title, year = clean_movie_title_and_year(raw_title, url)
    empty_res = {"tmdb_id": "", "imdb_id": "", "tmdb_imdb": "", "tmdb_url": "", "imdb_url": ""}
    if not clean_title or not api_key:
        return empty_res
    
    q = urllib.parse.quote(clean_title)
    candidates = []

    search_queries = []
    if year:
        search_queries.append(f"https://api.themoviedb.org/3/search/movie?api_key={api_key}&query={q}&year={year}")
        search_queries.append(f"https://api.themoviedb.org/3/search/movie?api_key={api_key}&query={q}&primary_release_year={year}")
    search_queries.append(f"https://api.themoviedb.org/3/search/movie?api_key={api_key}&query={q}")

    for api_url in search_queries:
        try:
            req = urllib.request.Request(api_url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                results = data.get("results", [])
                if results:
                    candidates = results
                    break
        except Exception:
            continue

    if not candidates:
        return empty_res

    matched_movie = None
    if year:
        for c in candidates:
            rel_date = str(c.get("release_date") or "")
            if year in rel_date:
                matched_movie = c
                break
    if not matched_movie:
        matched_movie = candidates[0]

    tmdb_id = str(matched_movie.get("id") or "").strip()
    imdb_id = ""

    if tmdb_id:
        try:
            detail_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={api_key}"
            req_d = urllib.request.Request(detail_url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req_d, timeout=10) as resp_d:
                d_data = json.loads(resp_d.read().decode("utf-8"))
                imdb_id = str(d_data.get("imdb_id") or "").strip()
        except Exception:
            pass

    tmdb_imdb = f"{tmdb_id}/{imdb_id}" if (tmdb_id and imdb_id) else (tmdb_id or imdb_id)
    tmdb_url = f"https://www.themoviedb.org/movie/{tmdb_id}" if tmdb_id else ""
    imdb_url = f"https://www.imdb.com/title/{imdb_id}/" if imdb_id else ""

    return {
        "tmdb_id": tmdb_id,
        "imdb_id": imdb_id,
        "tmdb_imdb": tmdb_imdb,
        "tmdb_url": tmdb_url,
        "imdb_url": imdb_url,
    }


def enrich_movies_with_tmdb_imdb(movies: List[Dict[str, str]], api_key: str = TMDB_API_KEY) -> List[Dict[str, str]]:
    print(f"[*] Querying TMDB API for {len(movies)} movie(s)...", flush=True)
    
    def _lookup(idx: int, movie: Dict[str, str]) -> Tuple[int, Dict[str, str]]:
        info = resolve_tmdb_imdb_info(movie["title"], movie["url"], api_key)
        return idx, info

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(_lookup, i, m) for i, m in enumerate(movies)]
        for f in as_completed(futures):
            try:
                idx, info = f.result()
                movies[idx].update(info)
            except Exception:
                pass

    print("[✓] TMDB/IMDb IDs & URLs resolved successfully.\n", flush=True)
    return movies


# ─────────────────────────────────────────────────────────────────────────────
# 6. Year / Category Listing Scraper
# ─────────────────────────────────────────────────────────────────────────────
def fetch_movie_links_from_year_listing(
    listing_url: str = TARGET_YEAR_URL,
    session: Optional[LinkFlowSession] = None
) -> List[Dict[str, str]]:
    session = session or LinkFlowSession()
    print(f"[*] Crawling listing page: {listing_url}", flush=True)
    
    resp = session.get(listing_url, timeout=25)
    html = resp.text if hasattr(resp, "text") else ""
    if not html:
        print(f"[!] Warning: Empty response received for listing URL: {listing_url}")
        return []

    soup = BeautifulSoup(html, "html.parser")
    movies: List[Dict[str, str]] = []
    seen_urls = set()

    # Selector Strategy 1: Articles and Article Headers
    for art in soup.find_all("article"):
        h_tag = art.find(["h2", "h3", "h4", "h1"])
        a_tag = h_tag.find("a", href=True) if h_tag else art.find("a", href=True)
        if a_tag:
            href = a_tag.get("href", "").strip()
            title = a_tag.get_text(strip=True)
            if href and title and href.startswith("http"):
                clean_href = href.rstrip("/")
                if clean_href not in ["https://bollyflix.free", "http://bollyflix.free"] and not any(k in href for k in ["/movies-by-year/", "/category/", "/tag/", "/page/"]):
                    if href not in seen_urls:
                        seen_urls.add(href)
                        movies.append({"title": title, "url": href, "tmdb_imdb": "", "tmdb_url": "", "imdb_url": ""})

    # Selector Strategy 2: Header H2 links
    if not movies:
        for a in soup.select("header h2.title a, header h2.front-view-title a, header h2 a, h2.title a, h2.front-view-title a"):
            href = a.get("href", "").strip()
            title = a.get_text(strip=True)
            if href and title and href.startswith("http"):
                clean_href = href.rstrip("/")
                if clean_href not in ["https://bollyflix.free", "http://bollyflix.free"] and not any(k in href for k in ["/movies-by-year/", "/category/", "/tag/", "/page/"]):
                    if href not in seen_urls:
                        seen_urls.add(href)
                        movies.append({"title": title, "url": href, "tmdb_imdb": "", "tmdb_url": "", "imdb_url": ""})

    # Selector Strategy 3: Universal Regex Fallback over all links
    if not movies:
        for a in soup.find_all("a", href=True):
            href = a.get("href", "").strip()
            title = a.get_text(strip=True)
            if href and title and len(title) > 5 and "-movie" in href.lower():
                if href not in seen_urls and not any(k in href for k in ["/movies-by-year/", "/category/", "/tag/", "/page/"]):
                    seen_urls.add(href)
                    movies.append({"title": title, "url": href, "tmdb_imdb": "", "tmdb_url": "", "imdb_url": ""})

    print(f"[✓] Found {len(movies)} total movie post(s) on listing page.\n", flush=True)
    return movies



# ─────────────────────────────────────────────────────────────────────────────
# 7. Custom Pattern Formatter for Output File
# ─────────────────────────────────────────────────────────────────────────────
SEPARATOR_LONG = "=" * 228


def format_movie_to_custom_pattern(serial_no: int, movie: Dict[str, Any]) -> str:
    """
    Formats a resolved movie object into the exact output format including all deep resolved links.
    """
    lines = []
    lines.append(SEPARATOR_LONG)
    lines.append(f"serail no:{serial_no}")
    lines.append(f"tmdb/imdb:{movie.get('tmdb_imdb', '')}")
    
    movie_url = movie.get("movie_url") or movie.get("main_url", "")
    qualities = movie.get("qualities", [])
    
    for q_idx, q in enumerate(qualities, 1):
        q_name = q.get("quality", f"Quality {q_idx}")
        if q_idx == 1:
            lines.append(f"title:_1: {q_name}")
        else:
            lines.append(f"\n\ntitle_{q_idx}:{q_name}")
            
        lines.append("section_1: google_drive")
        lines.append(f"main_url:{movie_url}")
        
        gdflix_flows = q.get("gdflix_flows") or ([q["gdflix_flow"]] if q.get("gdflix_flow") else [])
        if gdflix_flows:
            gdf = gdflix_flows[0]
            fastdl_url = gdf.get("fastdl_url") or (q.get("fastdl_urls")[0] if q.get("fastdl_urls") else "")
            gdflix_url = gdf.get("gdflix_url") or ""
            instant_dl = gdf.get("instant_dl") or ""
            instant_final = gdf.get("instant_final") or ""
            r2_cloud = gdf.get("r2_cloud") or ""
            direct_server_mgt = gdf.get("direct_server_mgt") or ""
            fast_cloud_page = gdf.get("fast_cloud_page") or ""
            fast_cloud_direct = gdf.get("fast_cloud_direct") or ""
            telegram_file = gdf.get("telegram_file") or ""
            gofile_multiup = gdf.get("gofile_multiup") or ""
            gofile_direct = gdf.get("gofile_direct") or ""
            gofile_1fichier = gdf.get("gofile_1fichier") or ""
            gofile_megaup = gdf.get("gofile_megaup") or ""
            
            lines.append(f"google_drive:1st {fastdl_url}")
            lines.append(f"google_drive:1st to 2nd redirected url: {gdflix_url}")
            
            instant_line = f"{instant_dl}  to  {instant_final}" if (instant_dl and instant_final) else instant_dl
            lines.append(f"google_drive: 2nd_redirected_url_1st_link_instant_dl: {instant_line}")
            lines.append(f"google_drive: 2nd_redirected_url_2nd_cloud_dowload_r2: {r2_cloud}")
            lines.append(f"google_drive: 2nd_redirected_url_3rd_direct_server_mgt: {direct_server_mgt}")
            
            zipdisk_line = f"{fast_cloud_page}  to  {fast_cloud_direct}" if (fast_cloud_page and fast_cloud_direct) else (fast_cloud_page or fast_cloud_direct)
            lines.append(f"google_drive: 2nd_redirected_url_4th_first_cloud_zipdisk: {zipdisk_line}")
            
            if telegram_file:
                lines.append(f"google_drive: 2nd_redirected_url_5th_telegram: {telegram_file}")
                
            if gofile_multiup:
                gofile_line = f"{gofile_multiup}  to  {gofile_direct}" if gofile_direct else gofile_multiup
                lines.append(f"google_drive: 2nd_redirected_url_6th_gofile_mirrors: {gofile_line}")
                if gofile_1fichier:
                    lines.append(f"google_drive: 2nd_redirected_url_6th_gofile_mirrors_1fichier: {gofile_1fichier}")
                if gofile_megaup:
                    lines.append(f"google_drive: 2nd_redirected_url_6th_gofile_mirrors_megaup: {gofile_megaup}")
            
        lines.append("\n\nsection_2: Download_links")
        mirrors = q.get("mirrors", [])
        for m_idx, m in enumerate(mirrors, 1):
            m_url = m.get("url") or m.get("resolved_url", "")
            prefix = f"host-{m_idx}:" if m_idx % 2 == 1 else f"host_{m_idx}:"
            lines.append(f"{prefix} {m_url}")
        lines.append("\n")
        
    lines.append(SEPARATOR_LONG)
    return "\n".join(lines)


def save_resolved_movies_to_output_file(
    movies_list: List[Dict[str, Any]],
    filepath: str = JSON_OUTPUT_FILE
) -> None:
    output_blocks = []
    for idx, movie in enumerate(movies_list, 1):
        output_blocks.append(format_movie_to_custom_pattern(idx, movie))
    
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n\n".join(output_blocks) + "\n")


def extract_title_from_url_slug(url: str) -> str:
    """Extracts a clean readable title from URL slug."""
    slug = url.strip("/").split("/")[-1]
    slug = re.sub(r'-(movie|full-movie|download|dual-audio|hindi).*$', '', slug, flags=re.IGNORECASE)
    parts = slug.split("-")
    title_parts = [p.capitalize() for p in parts if p]
    return " ".join(title_parts) or slug


def load_bollyflix_txt(txt_path: str = TXT_OUTPUT_FILE) -> Tuple[Set[str], int, List[Dict[str, Any]]]:
    """
    Loads all existing entries from bollyflix.txt.
    Returns:
      (existing_urls_set, max_serial_no, list_of_parsed_entries)
    """
    existing_urls: Set[str] = set()
    max_serial = 0
    entries = []
    
    if not os.path.exists(txt_path):
        return existing_urls, max_serial, entries
        
    with open(txt_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()

    # Match block format
    blocks = re.findall(
        r'serail no:(\d+)\s*\n(?:extracted:[^\n]*\n)?tmdb/imdb:([^\n]*)\ntitle:([^\n]*)\nmain_url:([^\n]+)',
        content,
        flags=re.IGNORECASE
    )
    for s_no, tmdb_imdb, title, main_url in blocks:
        s_int = int(s_no)
        if s_int > max_serial:
            max_serial = s_int
        u_clean = main_url.strip()
        existing_urls.add(u_clean)
        entries.append({
            "serial": s_int,
            "tmdb_imdb": tmdb_imdb.strip(),
            "title": title.strip(),
            "url": u_clean
        })

    # Also parse any standalone raw URLs if present
    for line in content.splitlines():
        line = line.strip()
        if line.startswith("http") and not line.startswith("="):
            m_url = re.search(r'(https?://[^\s#]+)', line)
            if m_url:
                u = m_url.group(1).strip()
                if u not in existing_urls:
                    existing_urls.add(u)
                    max_serial += 1
                    entries.append({"serial": max_serial, "title": extract_title_from_url_slug(u), "url": u})

    return existing_urls, max_serial, entries


def format_bollyflix_txt_block(
    serial_no: int,
    title: str,
    tmdb_imdb: str,
    main_url: str,
    extracted_time: Optional[str] = None
) -> str:
    """Formats a single movie entry into the required bollyflix.txt block format with UTC+6 timestamp."""
    from datetime import datetime, timezone, timedelta
    if not extracted_time:
        utc_plus_6 = timezone(timedelta(hours=6))
        extracted_time = datetime.now(utc_plus_6).strftime("%Y-%m-%d %H:%M:%S UTC+6")
        
    block = [
        "============================================================================",
        f"serail no:{serial_no}",
        f"extracted: {extracted_time}",
        f"tmdb/imdb:{tmdb_imdb}",
        f"title:{title}",
        f"main_url:{main_url}",
        "============================================================================"
    ]
    return "\n".join(block)


def append_movies_to_bollyflix_txt(
    new_movies: List[Dict[str, Any]],
    start_serial: int,
    txt_path: str = TXT_OUTPUT_FILE
) -> None:
    """Appends new movie blocks to bollyflix.txt."""
    if not new_movies:
        return
        
    blocks = []
    current_serial = start_serial
    for m in new_movies:
        current_serial += 1
        b = format_bollyflix_txt_block(
            serial_no=current_serial,
            title=m.get("title", ""),
            tmdb_imdb=m.get("tmdb_imdb", ""),
            main_url=m.get("url", "")
        )
        blocks.append(b)

    with open(txt_path, "a", encoding="utf-8") as f:
        if os.path.exists(txt_path) and os.path.getsize(txt_path) > 0:
            f.write("\n\n" + "\n\n".join(blocks) + "\n")
        else:
            f.write("\n\n".join(blocks) + "\n")

    print(f"[✓] Added {len(blocks)} new movie(s) to: {txt_path}")


# ─────────────────────────────────────────────────────────────────────────────
# Main Entry Point
# ─────────────────────────────────────────────────────────────────────────────
def main():
    session = LinkFlowSession(timeout=25)
    
    # 1. Load existing URLs from bollyflix.txt to prevent duplicate processing
    existing_urls, max_serial, _ = load_bollyflix_txt(TXT_OUTPUT_FILE)
    print(f"[*] Found {len(existing_urls)} existing URL(s) in {TXT_OUTPUT_FILE} (Last Serial: #{max_serial})")
    
    discovered_movies: List[Dict[str, Any]] = []

    # 2. Check CLI argument
    if len(sys.argv) > 1:
        arg = sys.argv[1].strip()
        if arg.endswith(".txt") and os.path.isfile(arg):
            custom_urls, _, _ = load_bollyflix_txt(arg)
            print(f"[*] Loaded {len(custom_urls)} URL(s) from custom text file: {arg}")
            for u in custom_urls:
                if u not in existing_urls:
                    discovered_movies.append({"title": extract_title_from_url_slug(u), "url": u, "year": 2026})
        elif arg.startswith("http"):
            if "/movies-by-year/" in arg or "/category/" in arg or "/page/" in arg or arg.endswith("/movies/"):
                print(f"[*] Fetching listing from: {arg}")
                listing = fetch_movie_links_from_year_listing(arg, session)
                for m in listing:
                    if m.get("url") not in existing_urls:
                        discovered_movies.append(m)
            else:
                if arg in existing_urls:
                    print(f"[!] Movie URL '{arg}' is already in {TXT_OUTPUT_FILE}. Skipping.")
                    return
                print(f" Target Movie Source: {arg}\n")
                result = resolve_bollyflix_movie(arg, session)
                result.update(resolve_tmdb_imdb_info(result.get("movie_title", ""), arg))
                
                # Append to bollyflix.txt
                append_movies_to_bollyflix_txt([{
                    "title": result.get("movie_title", ""),
                    "tmdb_imdb": result.get("tmdb_imdb", ""),
                    "url": arg
                }], max_serial, TXT_OUTPUT_FILE)
                
                save_resolved_movies_to_output_file([result], JSON_OUTPUT_FILE)
                print(f"[✓] Formatted results saved to: {JSON_OUTPUT_FILE}")
                return
    else:
        # Default: Fetch listing from TARGET_YEAR_URL
        print(f" Target Listing URL: {TARGET_YEAR_URL}\n")
        listing = fetch_movie_links_from_year_listing(TARGET_YEAR_URL, session)
        
        # Filter out already extracted URLs
        skipped_count = 0
        for m in listing:
            m_url = m.get("url", "")
            if m_url in existing_urls:
                skipped_count += 1
            else:
                discovered_movies.append(m)
                
        if skipped_count > 0:
            print(f"[*] Skipped {skipped_count} movie(s) already extracted in {TXT_OUTPUT_FILE}.")

    if not discovered_movies:
        print(f"[✓] All movies are already up to date in {TXT_OUTPUT_FILE}. Nothing new to resolve.")
        return

    # Deduplicate discovered movies
    seen_urls = set()
    unique_movies = []
    for m in discovered_movies:
        if m.get("url") and m["url"] not in seen_urls:
            seen_urls.add(m["url"])
            unique_movies.append(m)
    discovered_movies = unique_movies

    print(f"[*] Found {len(discovered_movies)} new movie(s) to process.")
    discovered_movies = enrich_movies_with_tmdb_imdb(discovered_movies, TMDB_API_KEY)
    
    # Save newly discovered movies to bollyflix.txt in requested block format
    append_movies_to_bollyflix_txt(discovered_movies, max_serial, TXT_OUTPUT_FILE)
    
    print("=" * 86)
    print(f" RESOLVING FINAL STREAMS FOR {len(discovered_movies)} NEW MOVIE(S)".center(86))
    print("=" * 86 + "\n")
    
    all_resolved_results = []
    
    for idx, movie in enumerate(discovered_movies, 1):
        m_title = movie.get("title", "Unknown Movie")
        m_url = movie.get("url", "")
        m_tmdb_imdb = movie.get("tmdb_imdb", "")
        
        print(f"\n[{idx}/{len(discovered_movies)}] Processing: {m_title}")
        print(f"    TMDB/IMDb: {m_tmdb_imdb}")
        print(f"    Source: {m_url}")
        try:
            res = resolve_bollyflix_movie(m_url, session)
            res["tmdb_imdb"] = m_tmdb_imdb
            all_resolved_results.append(res)
            
            save_resolved_movies_to_output_file(all_resolved_results, JSON_OUTPUT_FILE)
            print(f"  [✓] Successfully resolved & saved movie #{idx} to {JSON_OUTPUT_FILE}")
        except Exception as e:
            print(f"  [✗] Failed to resolve movie '{m_title}': {e}", flush=True)
    
    print("\n" + "=" * 86)
    print(f" [✓] COMPLETED! Total resolved: {len(all_resolved_results)} movies.")
    print(f" [✓] Updated text database     : {TXT_OUTPUT_FILE}")
    print(f" [✓] Formatted streams output  : {JSON_OUTPUT_FILE}")
    print("=" * 86)


if __name__ == "__main__":
    main()


