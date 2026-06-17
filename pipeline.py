#!/usr/bin/env python3
"""
Search open-access article PDFs by title/query and download them next to this file.

Pipeline:
1. OpenAlex   -> metadata and DOI candidates
2. SciBban    -> direct PDF URL by DOI
3. Unpaywall  -> OA PDF by DOI
4. Europe PMC -> OA PDF/full-text copies
5. CORE       -> fallback OA search
6. Download PDF files

Examples:
    python3 download_articles_pipeline.py \
        "Active chitosan/PVA films with anthocyanins from Brassica oleraceae" \
        --out-dir ./my_pdfs \
        --page-size 25 \
        --email you@example.com \
        --openalex-key YOUR_OPENALEX_KEY \
        --core-key YOUR_CORE_KEY

    python3 pipeline.py \
        --query-url 'https://api.openalex.org/works?search.title_and_abstract=milk+fermentation&per_page=25' \
        --max-results 25 \
        --email you@example.com

PDFs and the JSON report are saved under <out-dir>/<query-name>/.

Unpaywall requires a real email. OpenAlex may require an API key. CORE requires
an API key, so the CORE step is skipped unless --core-key or CORE_API_KEY is set.
"""

from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import os
import re
import signal
import sys
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterator
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urlencode, urljoin, urlsplit, urlunsplit
from urllib.request import Request, urlopen


DEFAULT_QUERY = "milk fermentation"
DEFAULT_PAGE_SIZE = 100
OPENALEX_SEARCH_PARAM = "search.title_and_abstract"
DOI_INDEX_FILENAME = "doi_index.json"
DOI_NUMBERS_FILENAME = "doi_numbers.txt"
DOI_PATTERN = re.compile(r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+", re.I)
EUROPE_PMC_RENDER_TIMEOUT_MULTIPLIER = 3
EUROPE_PMC_RENDER_MIN_TIMEOUT = 120.0
OJS_DOWNLOAD_TIMEOUT_MULTIPLIER = 4
OJS_DOWNLOAD_MIN_TIMEOUT = 180.0
MAX_HTML_DOWNLOAD_FOLLOWS = 3
MDPI_CDN_URL = "https://mdpi-res.com/d_attachment/{slug}/{article_id}/article_deploy/{article_id}.pdf"
MDPI_DOI_CODE_TO_CDN_SLUG = {
    "app": "applsci",
    "nu": "nutrients",
    "rs": "remotesensing",
}
PUBLISHER_MIRROR_HOSTS = {
    "analyticalsciencejournals.onlinelibrary.wiley.com",
    "cell.com",
    "onlinelibrary.wiley.com",
    "sciencedirect.com",
    "www.cell.com",
    "www.sciencedirect.com",
}
REPOSITORY_MIRROR_HOST_PRIORITY = {
    "research.chalmers.se": 0,
    "orbit.dtu.dk": 1,
    "pmc.ncbi.nlm.nih.gov": 2,
    "www.ncbi.nlm.nih.gov": 2,
}
LOG_CONTEXT = threading.local()
LOG_WORKER_LOCK = threading.Lock()
LOG_WORKER_IDS: dict[int, int] = {}

OPENALEX_WORKS_URL = "https://api.openalex.org/works"
SCI_BBAN_PDF_URL = "https://sci.bban.top/pdf/{doi}.pdf"
UNPAYWALL_DOI_URL = "https://api.unpaywall.org/v2/{doi}"
EUROPE_PMC_SEARCH_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
CORE_V3_SEARCH_URL = "https://api.core.ac.uk/v3/search/works/"
CORE_V2_SEARCH_URL = "https://core.ac.uk/api-v2/articles/search/{query}"
CORE_V2_DOWNLOAD_URL = "https://core.ac.uk/api-v2/articles/get/{core_id}/download/pdf"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)


class ApiError(RuntimeError):
    pass


class DownloadError(RuntimeError):
    pass


@dataclass
class Article:
    title: str
    doi: str | None = None
    year: int | None = None
    openalex_id: str | None = None
    pmid: str | None = None
    pmcid: str | None = None
    source: str = "manual"
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PdfCandidate:
    url: str
    source: str
    article_title: str
    doi: str | None = None
    note: str | None = None


def normalize_text(value: str) -> str:
    """Normalize ligatures such as ﬁ -> fi and collapse whitespace."""
    value = unicodedata.normalize("NFKC", value)
    return re.sub(r"\s+", " ", value).strip()


def clean_doi(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip()
    value = re.sub(r"^https?://(?:dx\.)?doi\.org/", "", value, flags=re.I)
    value = re.sub(r"^doi:\s*", "", value, flags=re.I)
    value = value.strip().strip(".")
    return value or None


def extract_doi(value: str | None) -> str | None:
    if not value:
        return None
    match = DOI_PATTERN.search(value)
    if not match:
        return None
    return clean_doi(match.group(0).rstrip(".,;"))


def safe_filename(title: str, doi: str | None = None, suffix: str = ".pdf") -> str:
    base = title or doi or "article"
    base = unicodedata.normalize("NFKD", base).encode("ascii", "ignore").decode("ascii")
    base = re.sub(r"[^\w\s.-]+", "", base)
    base = re.sub(r"\s+", "_", base).strip("._-")
    if not base:
        base = "article"
    if doi:
        digest = hashlib.sha1(doi.encode("utf-8")).hexdigest()[:8]
    else:
        digest = hashlib.sha1(title.encode("utf-8")).hexdigest()[:8]
    return f"{base[:110]}_{digest}{suffix}"


def safe_dirname(value: str, max_length: int = 140) -> str:
    base = normalize_text(value)
    base = unicodedata.normalize("NFKD", base).encode("ascii", "ignore").decode("ascii")
    base = re.sub(r"[^A-Za-z0-9._-]+", "_", base).strip("._-")
    return base[:max_length].rstrip("._-") or "query"


def repo_relative_path(path_value: str | Path | None, repo_root: Path) -> str | None:
    if not path_value:
        return None
    path = Path(path_value)
    if not path.is_absolute():
        path = repo_root / path
    path = path.resolve()
    try:
        relative = path.relative_to(repo_root)
    except ValueError:
        relative = Path(os.path.relpath(path, repo_root))
    return relative.as_posix()


def resolve_stored_path(
    path_value: str | Path | None,
    *,
    repo_root: Path,
    report_path: Path | None = None,
) -> Path | None:
    if not path_value:
        return None
    path = Path(path_value)
    if path.is_absolute():
        return path.resolve()

    repo_candidate = (repo_root / path).resolve()
    if repo_candidate.exists() or not report_path:
        return repo_candidate

    report_candidate = (report_path.parent / path).resolve()
    if report_candidate.exists():
        return report_candidate
    return repo_candidate


def stored_path_exists(path_value: str | Path | None, repo_root: Path) -> bool:
    path = resolve_stored_path(path_value, repo_root=repo_root)
    return bool(path and path.exists())


def requote_url(url: str) -> str:
    parts = urlsplit(url.strip())
    path = quote(parts.path, safe="/:%")
    query = quote(parts.query, safe="=&;%:+,/?@")
    return urlunsplit((parts.scheme, parts.netloc, path, query, parts.fragment))


def redact_url(url: str) -> str:
    return re.sub(r"(?i)(apiKey|api_key|key|token)=([^&]+)", r"\1=<hidden>", url)


def http_headers(accept: str = "application/json", *, referer: str | None = None) -> dict[str, str]:
    headers = {
        "Accept": accept,
        "User-Agent": USER_AGENT,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "identity",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    if referer:
        headers["Referer"] = referer
    if "pdf" in accept.lower():
        headers.update(
            {
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "same-origin" if referer else "none",
            }
        )
    return headers


def get_json(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 30,
) -> dict[str, Any]:
    if params:
        url = f"{url}?{urlencode(params, doseq=True)}"
    request = Request(requote_url(url), headers=headers or http_headers())
    try:
        with urlopen(request, timeout=timeout) as response:
            raw = response.read()
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:700]
        raise ApiError(f"HTTP {exc.code} for {redact_url(url)}: {body}") from exc
    except URLError as exc:
        raise ApiError(f"Network error for {redact_url(url)}: {exc.reason}") from exc

    try:
        return json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:
        snippet = raw[:300].decode("utf-8", errors="replace")
        raise ApiError(f"Invalid JSON from {redact_url(url)}: {snippet}") from exc


def looks_like_pdf_url(url: str) -> bool:
    lower = urlsplit(url).path.lower()
    return lower.endswith(".pdf") or "/pdf/" in lower or "download/pdf" in lower


def normalized_mdpi_slug(value: str | None) -> str | None:
    if not value:
        return None
    slug = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "", slug.lower())
    return slug or None


def mdpi_pdf_parts(url: str) -> tuple[int, int] | None:
    parsed = urlsplit(url)
    host = parsed.netloc.lower()
    if host not in {"mdpi.com", "www.mdpi.com"}:
        return None

    path_parts = [part for part in parsed.path.strip("/").split("/") if part]
    if len(path_parts) < 5 or path_parts[-1].lower() != "pdf":
        return None

    try:
        volume = int(path_parts[-4])
        article_number = int(path_parts[-2])
    except ValueError:
        return None
    return volume, article_number


def mdpi_doi_slug(doi: str | None) -> str | None:
    cleaned = clean_doi(doi)
    if not cleaned:
        return None
    match = re.match(r"^10\.3390/([a-z]+)\d{6,}$", cleaned, flags=re.I)
    if not match:
        return None
    doi_code = match.group(1).lower()
    return MDPI_DOI_CODE_TO_CDN_SLUG.get(doi_code, doi_code)


def article_source_slugs(article: Article) -> list[str]:
    slugs: list[str] = []
    locations = []
    if isinstance(article.raw.get("primary_location"), dict):
        locations.append(article.raw["primary_location"])
    if isinstance(article.raw.get("best_oa_location"), dict):
        locations.append(article.raw["best_oa_location"])
    locations.extend(location for location in article.raw.get("locations") or [] if isinstance(location, dict))

    for location in locations:
        source = location.get("source") or {}
        if not isinstance(source, dict):
            continue
        for key in ("display_name", "display_name_alternatives"):
            value = source.get(key)
            values = value if isinstance(value, list) else [value]
            for item in values:
                slug = normalized_mdpi_slug(item if isinstance(item, str) else None)
                if slug:
                    slugs.append(slug)
    return slugs


def mdpi_cdn_pdf_urls(url: str, article: Article) -> list[str]:
    parts = mdpi_pdf_parts(url)
    if not parts:
        return []
    volume, article_number = parts

    slugs = [mdpi_doi_slug(article.doi), *article_source_slugs(article)]
    result: list[str] = []
    seen: set[str] = set()
    for slug in slugs:
        if not slug or slug in seen:
            continue
        seen.add(slug)
        article_id = f"{slug}-{volume}-{article_number:05d}"
        result.append(MDPI_CDN_URL.format(slug=slug, article_id=article_id))
    return result


def publisher_url_needs_repository_mirrors(url: str) -> bool:
    parsed = urlsplit(url)
    host = parsed.netloc.lower()
    if host.startswith("www."):
        host_without_www = host[4:]
    else:
        host_without_www = host
    return host in PUBLISHER_MIRROR_HOSTS or host_without_www in PUBLISHER_MIRROR_HOSTS


def pmcid_from_location(location: dict[str, Any]) -> str | None:
    for value in (location.get("id"), location.get("landing_page_url"), location.get("pdf_url")):
        if not isinstance(value, str):
            continue
        match = re.search(r"\bPMC?(\d{3,})\b", value, flags=re.I)
        if match:
            return f"PMC{match.group(1)}"
        match = re.search(r"pubmedcentral\.nih\.gov:(\d{3,})\b", value, flags=re.I)
        if match:
            return f"PMC{match.group(1)}"
    return None


def normalize_repository_mirror_url(url: str, location: dict[str, Any]) -> str | None:
    parsed = urlsplit(url)
    host = parsed.netloc.lower()
    if host in {"doi.org", "dx.doi.org", "pubmed.ncbi.nlm.nih.gov"}:
        return None
    if host in {"pmc.ncbi.nlm.nih.gov", "www.ncbi.nlm.nih.gov"} and "/pmc/articles/" in parsed.path:
        pmcid = pmcid_from_location(location)
        if not pmcid:
            return None
        return f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/"
    return url


def repository_mirror_priority(url: str) -> int:
    host = urlsplit(url).netloc.lower()
    return REPOSITORY_MIRROR_HOST_PRIORITY.get(host, 20)


def repository_mirror_urls(article: Article) -> list[tuple[str, str]]:
    locations = []
    if isinstance(article.raw.get("primary_location"), dict):
        locations.append(article.raw["primary_location"])
    if isinstance(article.raw.get("best_oa_location"), dict):
        locations.append(article.raw["best_oa_location"])
    locations.extend(location for location in article.raw.get("locations") or [] if isinstance(location, dict))

    mirrors: list[tuple[int, str, str]] = []
    seen: set[str] = set()
    for location in locations:
        source = location.get("source") or {}
        if not isinstance(source, dict) or source.get("type") != "repository":
            continue

        source_name = source.get("display_name") if isinstance(source.get("display_name"), str) else "repository"
        for key in ("pdf_url", "landing_page_url"):
            url = location.get(key)
            if not isinstance(url, str) or not url.startswith(("http://", "https://")):
                continue
            normalized_url = normalize_repository_mirror_url(url, location)
            if not normalized_url:
                continue
            key_url = requote_url(normalized_url)
            if key_url in seen:
                continue
            seen.add(key_url)
            mirrors.append((repository_mirror_priority(normalized_url), normalized_url, source_name))

    return [(url, source_name) for _, url, source_name in sorted(mirrors, key=lambda item: item[0])]


def add_pdf_candidate_with_mirrors(candidates: list[PdfCandidate], candidate: PdfCandidate, article: Article) -> None:
    for cdn_url in mdpi_cdn_pdf_urls(candidate.url, article):
        candidates.append(
            PdfCandidate(
                url=cdn_url,
                source=f"{candidate.source}/MDPI CDN",
                article_title=candidate.article_title,
                doi=candidate.doi,
                note="mdpi-res fallback",
            )
        )
    if publisher_url_needs_repository_mirrors(candidate.url):
        for mirror_url, source_name in repository_mirror_urls(article):
            candidates.append(
                PdfCandidate(
                    url=mirror_url,
                    source=f"{candidate.source}/Repository",
                    article_title=candidate.article_title,
                    doi=candidate.doi,
                    note=f"{source_name} mirror",
                )
            )
    candidates.append(candidate)


def deep_find_urls(value: Any) -> list[str]:
    found: list[str] = []
    if isinstance(value, dict):
        for nested in value.values():
            found.extend(deep_find_urls(nested))
    elif isinstance(value, list):
        for nested in value:
            found.extend(deep_find_urls(nested))
    elif isinstance(value, str) and value.startswith(("http://", "https://")):
        found.append(value)
    return found


def current_log_worker() -> str:
    worker = getattr(LOG_CONTEXT, "worker", None)
    if worker:
        return worker
    return "main"


def set_log_worker(worker: str) -> None:
    LOG_CONTEXT.worker = worker


def set_pool_log_worker() -> str:
    thread_id = threading.get_ident()
    with LOG_WORKER_LOCK:
        worker_number = LOG_WORKER_IDS.setdefault(thread_id, len(LOG_WORKER_IDS) + 1)
    worker = f"w{worker_number}"
    set_log_worker(worker)
    return worker


def log(message: str) -> None:
    prefix = f"[{current_log_worker()}]"
    if not message:
        print("", flush=True)
        return
    print(f"{prefix} {message}", flush=True)


def with_retry(func, *, retries: int = 2, delay: float = 1.0, backoff: float = 2.0):
    """Execute func with retries on connection errors. Skip after retries exhausted."""
    last_exc = None
    for attempt in range(retries + 1):
        try:
            return func()
        except (http.client.RemoteDisconnected, http.client.BadStatusLine,
                http.client.IncompleteRead, ConnectionError, URLError, TimeoutError) as exc:
            last_exc = exc
            if attempt < retries:
                wait_time = delay * (backoff ** attempt)
                log(f"   Connection error (attempt {attempt + 1}/{retries + 1}): {exc}. Retrying in {wait_time:g}s...")
                time.sleep(wait_time)
            else:
                log(f"   Connection closed after {retries + 1} attempts: {exc}. Skipping this source.")
                raise ApiError(f"Connection failed after retries: {exc}") from exc
        except ApiError:
            raise
        except Exception as exc:
            log(f"   Unexpected error: {exc}. Skipping this source.")
            raise ApiError(f"Unexpected error: {exc}") from exc
    raise ApiError(f"Connection failed after retries: {last_exc}") from last_exc


def format_count(value: int | None) -> str:
    return "unknown" if value is None else f"{value:,}"


def format_ratio(part: int, total: int | None) -> str:
    if not total:
        return "n/a"
    return f"{part / total:.2%}"


def build_openalex_articles(items: list[dict[str, Any]], fallback_title: str) -> list[Article]:
    articles: list[Article] = []
    for item in items:
        ids = item.get("ids") or {}
        title = normalize_text(item.get("title") or item.get("display_name") or fallback_title)
        articles.append(
            Article(
                title=title,
                doi=clean_doi(item.get("doi") or ids.get("doi")),
                year=item.get("publication_year"),
                openalex_id=item.get("id") or ids.get("openalex"),
                pmid=ids.get("pmid"),
                pmcid=ids.get("pmcid"),
                source="OpenAlex",
                raw=item,
            )
        )
    return articles


def article_metadata(article: Article) -> dict[str, Any]:
    data = asdict(article)
    data.pop("raw", None)
    return data


def empty_openalex_metadata_cache(query: str, page_size: int) -> dict[str, Any]:
    return {
        "version": 1,
        "query": query,
        "search_param": OPENALEX_SEARCH_PARAM,
        "page_size": page_size,
        "sort": "relevance_score:desc",
        "pages": [],
    }


def load_openalex_metadata_cache(path: Path, query: str, page_size: int) -> dict[str, Any]:
    if not path.exists():
        return empty_openalex_metadata_cache(query, page_size)
    try:
        cache = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return empty_openalex_metadata_cache(query, page_size)

    if (
        cache.get("version") != 1
        or cache.get("query") != query
        or cache.get("search_param") != OPENALEX_SEARCH_PARAM
        or cache.get("page_size") != page_size
        or cache.get("sort") != "relevance_score:desc"
        or not isinstance(cache.get("pages"), list)
    ):
        return empty_openalex_metadata_cache(query, page_size)

    return cache


def save_openalex_metadata_cache(path: Path, cache: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    save_json(path, cache)


def openalex_search_pages(
    query: str,
    *,
    email: str | None,
    api_key: str | None,
    max_results: int,
    page_size: int,
    timeout: float,
    cache_path: Path | None = None,
) -> Iterator[tuple[int, list[Article], dict[str, Any]]]:
    cache = (
        load_openalex_metadata_cache(cache_path, query, page_size)
        if cache_path
        else empty_openalex_metadata_cache(query, page_size)
    )
    remaining = max_results
    cached_pages = sorted(cache.get("pages") or [], key=lambda page: int(page.get("page_number") or 0))

    for page in cached_pages:
        if remaining <= 0:
            return
        results = page.get("results") or []
        if not results:
            continue
        page_number = int(page.get("page_number") or 0)
        meta = page.get("meta") or {}
        selected_results = results[:remaining]
        articles = build_openalex_articles(selected_results, query)
        yield page_number, articles, meta
        remaining -= len(articles)
        if len(selected_results) < len(results):
            return

    if remaining <= 0:
        return

    if cached_pages:
        last_page = cached_pages[-1]
        last_meta = last_page.get("meta") or {}
        cursor = last_meta.get("next_cursor")
        page_number = int(last_page.get("page_number") or len(cached_pages)) + 1
        last_result_count = len(last_page.get("results") or [])
        last_per_page = int(last_page.get("per_page") or page_size)
        if not cursor or last_result_count < last_per_page:
            return
    else:
        cursor = "*"
        page_number = 1

    while remaining > 0:
        current_page_size = min(page_size, remaining)
        params: dict[str, Any] = {
            OPENALEX_SEARCH_PARAM: query,
            "per_page": current_page_size,
            "cursor": cursor,
            "sort": "relevance_score:desc",
        }
        if email:
            params["mailto"] = email
        if api_key:
            params["api_key"] = api_key

        data = with_retry(
            lambda: get_json(
                OPENALEX_WORKS_URL,
                params=params,
                headers=http_headers(),
                timeout=timeout,
            ),
        )
        results = data.get("results") or []
        if not results:
            break

        meta = data.get("meta") or {}
        cache.setdefault("pages", []).append(
            {
                "page_number": page_number,
                "cursor": cursor,
                "per_page": current_page_size,
                "meta": meta,
                "results": results,
            },
        )
        if cache_path:
            save_openalex_metadata_cache(cache_path, cache)

        articles = build_openalex_articles(results, query)
        yield page_number, articles, meta
        remaining -= len(articles)

        next_cursor = meta.get("next_cursor")
        if not next_cursor or len(results) < current_page_size:
            break

        cursor = next_cursor
        page_number += 1


def openalex_pdf_candidates(article: Article) -> list[PdfCandidate]:
    candidates: list[PdfCandidate] = []
    locations = []
    if article.raw.get("best_oa_location"):
        locations.append(article.raw["best_oa_location"])
    if article.raw.get("primary_location"):
        locations.append(article.raw["primary_location"])
    locations.extend(article.raw.get("locations") or [])

    for location in locations:
        if not isinstance(location, dict):
            continue
        for key in ("pdf_url", "url_for_pdf"):
            url = location.get(key)
            if url:
                add_pdf_candidate_with_mirrors(
                    candidates,
                    PdfCandidate(
                        url=url,
                        source="OpenAlex",
                        article_title=article.title,
                        doi=article.doi,
                        note=key,
                    ),
                    article,
                )

    oa_url = (article.raw.get("open_access") or {}).get("oa_url")
    if oa_url and looks_like_pdf_url(oa_url):
        add_pdf_candidate_with_mirrors(
            candidates,
            PdfCandidate(
                url=oa_url,
                source="OpenAlex",
                article_title=article.title,
                doi=article.doi,
                note="open_access.oa_url",
            ),
            article,
        )
    return candidates


def sci_bban_candidates(article: Article) -> list[PdfCandidate]:
    if not article.doi:
        return []
    url = SCI_BBAN_PDF_URL.format(doi=quote(article.doi, safe="/"))
    try:
        request = Request(url, method="HEAD", headers=http_headers(accept="application/pdf"))
        with urlopen(request, timeout=10) as response:
            if response.getcode() == 404:
                return []
    except HTTPError as exc:
        if exc.code == 404:
            return []
    except (URLError, TimeoutError, ConnectionError, OSError):
        pass
    return [
        PdfCandidate(
            url=url,
            source="SciBban",
            article_title=article.title,
            doi=article.doi,
            note="direct DOI PDF endpoint",
        )
    ]


def strip_version_param(url: str) -> str:
    """Remove the 'version' query parameter from URL if present."""
    parts = urlsplit(url)
    if not parts.query:
        return url
    if "pdf" not in parts.path.lower():
        return url
    query_params = [
        part
        for part in parts.query.split("&")
        if part and part.split("=", 1)[0].lower() != "version"
    ]
    if not query_params:
        return urlunsplit((parts.scheme, parts.netloc, parts.path, "", parts.fragment))

    new_query = "&".join(query_params)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))

def unpaywall_candidates(
    article: Article,
    *,
    email: str | None,
    timeout: float,
) -> list[PdfCandidate]:
    if not article.doi:
        return []
    if not email:
        log("   Unpaywall пропущен: нужен --email или UNPAYWALL_EMAIL.")
        return []

    data = get_json(
        UNPAYWALL_DOI_URL.format(doi=quote(article.doi, safe="")),
        params={"email": email},
        headers=http_headers(),
        timeout=timeout,
    )

    candidates: list[PdfCandidate] = []
    locations = []
    if data.get("best_oa_location"):
        locations.append(data["best_oa_location"])
    locations.extend(data.get("oa_locations") or [])

    for location in locations:
        if not isinstance(location, dict):
            continue
        url_for_pdf = location.get("url_for_pdf")
        if url_for_pdf:
            add_pdf_candidate_with_mirrors(
                candidates,
                PdfCandidate(
                    url=strip_version_param(url_for_pdf),
                    source="Unpaywall",
                    article_title=article.title,
                    doi=article.doi,
                    note=location.get("host_type"),
                ),
                article,
            )
        url = location.get("url")
        if url and looks_like_pdf_url(url):
            add_pdf_candidate_with_mirrors(
                candidates,
                PdfCandidate(
                    url=strip_version_param(url),
                    source="Unpaywall",
                    article_title=article.title,
                    doi=article.doi,
                    note=location.get("host_type"),
                ),
                article,
            )
    return candidates


def europe_pmc_queries(article: Article, fallback_query: str) -> list[str]:
    queries: list[str] = []
    if article.doi:
        queries.append(f'DOI:"{article.doi}"')
    if article.pmid:
        queries.append(f'EXT_ID:{article.pmid}')
    if article.pmcid:
        queries.append(f'PMCID:{article.pmcid}')
    queries.append(f'TITLE:"{article.title}"')
    if not article.raw:
        queries.append(fallback_query)
    return list(dict.fromkeys(queries))


def europe_pmc_candidates(
    article: Article,
    *,
    fallback_query: str,
    email: str | None,
    timeout: float,
) -> list[PdfCandidate]:
    candidates: list[PdfCandidate] = []

    for query in europe_pmc_queries(article, fallback_query):
        data = get_json(
            EUROPE_PMC_SEARCH_URL,
            params={
                "query": query,
                "format": "json",
                "resultType": "core",
                "pageSize": 5,
            },
            headers=http_headers(),
            timeout=timeout,
        )
        results = (data.get("resultList") or {}).get("result") or []
        for result in results:
            title = normalize_text(result.get("title") or article.title)
            full_text_urls = (result.get("fullTextUrlList") or {}).get("fullTextUrl") or []
            for item in full_text_urls:
                if not isinstance(item, dict):
                    continue
                url = item.get("url")
                style = str(item.get("documentStyle") or "").lower()
                availability = str(item.get("availability") or "").lower()
                if url and ("pdf" in style or looks_like_pdf_url(url)):
                    candidates.append(
                        PdfCandidate(
                            url=url,
                            source="EuropePMC",
                            article_title=title,
                            doi=article.doi or clean_doi(result.get("doi")),
                            note=availability or None,
                        )
                    )

            pmcid = result.get("pmcid")
            if pmcid:
                candidates.append(
                    PdfCandidate(
                        url=f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/pdf/",
                        source="EuropePMC",
                        article_title=title,
                        doi=article.doi or clean_doi(result.get("doi")),
                        note="PMCID PDF endpoint",
                    )
                )
    return candidates


def core_candidates(
    article: Article,
    *,
    fallback_query: str,
    api_key: str | None,
    email: str | None,
    timeout: float,
) -> list[PdfCandidate]:
    if not api_key:
        log("   CORE пропущен: нужен --core-key или CORE_API_KEY.")
        return []

    terms = [article.doi, article.title, fallback_query]
    unique_terms = [term for term in dict.fromkeys(terms) if term]
    candidates: list[PdfCandidate] = []

    for term in unique_terms:
        try:
            data = get_json(
                CORE_V3_SEARCH_URL,
                params={"q": term, "limit": 5},
                headers={**http_headers(), "Authorization": f"Bearer {api_key}"},
                timeout=timeout,
            )
        except ApiError as exc:
            # log(f"   CORE v3 не ответил для {term!r}: {exc}")
            log(f"   CORE v3 не ответил для {term!r}")
            data = {}

        for item in data.get("results", []) or data.get("data", []) or []:
            title = normalize_text(item.get("title") or article.title)
            for url in deep_find_urls(item):
                if looks_like_pdf_url(url) or "download" in urlsplit(url).path.lower():
                    candidates.append(
                        PdfCandidate(
                            url=url,
                            source="CORE",
                            article_title=title,
                            doi=article.doi or clean_doi(item.get("doi")),
                            note="v3 search result",
                        )
                    )

        try:
            data_v2 = get_json(
                CORE_V2_SEARCH_URL.format(query=quote(term, safe="")),
                params={"apiKey": api_key, "page": 1, "pageSize": 5},
                headers=http_headers(),
                timeout=timeout,
            )
        except ApiError as exc:
            # log(f"   CORE v2 не ответил для {term!r}: {exc}")
            log(f"   CORE v2 не ответил для {term!r}")
            continue

        for item in data_v2.get("data", []) or []:
            core_id = item.get("id") or item.get("coreId")
            title = normalize_text(item.get("title") or article.title)
            doi = article.doi or clean_doi(item.get("doi"))
            for url in deep_find_urls(item):
                if looks_like_pdf_url(url):
                    candidates.append(
                        PdfCandidate(
                            url=url,
                            source="CORE",
                            article_title=title,
                            doi=doi,
                            note="v2 search result",
                        )
                    )
            if core_id:
                candidates.append(
                    PdfCandidate(
                        url=f"{CORE_V2_DOWNLOAD_URL.format(core_id=core_id)}?apiKey={api_key}",
                        source="CORE",
                        article_title=title,
                        doi=doi,
                        note="v2 download endpoint",
                    )
                )
    return candidates


def dedupe_candidates(candidates: list[PdfCandidate]) -> list[PdfCandidate]:
    result: list[PdfCandidate] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = requote_url(candidate.url)
        if key in seen:
            continue
        seen.add(key)
        result.append(candidate)
    return result


def looks_like_anti_bot_challenge(body: str, *urls: str | None) -> bool:
    lowered = " ".join([body, *(url or "" for url in urls)]).lower()
    strong_markers = (
        "checking your browser",
        "cf-browser-verification",
        "cf-challenge",
        "cf-mitigated",
        "enable cookies",
        "cookieabsent",
        "cookie absent",
        "pardon the interruption",
        "browser made us think you were a bot",
        "verify you are a human",
        "distil_r_captcha",
        "incapsula",
        "datadome",
        "making sure you're not a bot",
        "making sure you&#39;re not a bot",
        "anubis_challenge",
        "cloudpmc-viewer-pow",
        "pow_challenge",
        "proof-of-work",
        "/challenge",
    )
    if any(marker in lowered for marker in strong_markers):
        return True
    if "just a moment" in lowered and ("cloudflare" in lowered or "challenge" in lowered):
        return True
    if "cloudflare" in lowered and (
        "/cdn-cgi/challenge" in lowered
        or "challenge-platform" in lowered
        or "__cf_chl" in lowered
    ):
        return True
    if "preparing to download" in lowered and (
        "cloudpmc-viewer-pow" in lowered
        or "pow-" in lowered
        or "proof-of-work" in lowered
    ):
        return True
    return False


def looks_like_access_denied(body: str) -> bool:
    lowered = body.lower()
    return "access denied" in lowered and (
        "permission to access" in lowered
        or "akamai" in lowered
        or "reference" in lowered
    )


def is_ojs_download_url(url: str) -> bool:
    return "/article/download/" in urlsplit(url).path.lower()


def ojs_article_referer(url: str) -> str | None:
    parsed = urlsplit(url)
    path = parsed.path
    marker = "/article/download/"
    lower_path = path.lower()
    marker_index = lower_path.find(marker)
    if marker_index < 0:
        return None

    prefix = path[:marker_index]
    rest = path[marker_index + len(marker) :]
    article_id = rest.split("/", 1)[0]
    if not article_id:
        return None
    return urlunsplit((parsed.scheme, parsed.netloc, f"{prefix}/article/view/{article_id}", "", ""))


def download_timeout_for(candidate: PdfCandidate, base_timeout: float) -> float:
    source = candidate.source.lower()
    if is_ojs_download_url(candidate.url):
        return max(base_timeout * OJS_DOWNLOAD_TIMEOUT_MULTIPLIER, OJS_DOWNLOAD_MIN_TIMEOUT)
    if source in {"europepmc", "unpaywall"}:
        parsed = urlsplit(candidate.url)
        if "pdf=render" in parsed.query.lower() or "europepmc.org" in parsed.netloc.lower():
            return max(base_timeout * EUROPE_PMC_RENDER_TIMEOUT_MULTIPLIER, EUROPE_PMC_RENDER_MIN_TIMEOUT)
    return base_timeout


def download_referer_for(candidate: PdfCandidate) -> str | None:
    parsed = urlsplit(candidate.url)
    source = candidate.source.lower()
    if is_ojs_download_url(candidate.url):
        return ojs_article_referer(candidate.url)
    if mdpi_pdf_parts(candidate.url):
        return urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rsplit("/pdf", 1)[0], "", ""))
    if source == "europepmc" and "europepmc.org" in parsed.netloc.lower():
        return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
    if source == "unpaywall" and "unpaywall.org" in parsed.netloc.lower():
        return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
    return None


class DownloadLinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = {key.lower(): value or "" for key, value in attrs}
        if tag in {"a", "link"} and values.get("href"):
            self.links.append((values["href"], " ".join(values.values())))
        elif tag in {"iframe", "embed", "source"} and values.get("src"):
            self.links.append((values["src"], " ".join(values.values())))
        elif tag == "form" and values.get("action"):
            self.links.append((values["action"], " ".join(values.values())))


def download_link_score(url: str, context: str) -> int:
    text = f"{url} {context}".lower()
    score = 0
    if "download" in text:
        score += 60
    if "pdf" in text:
        score += 40
    if "/article/download/" in text:
        score += 30
    if "view" in text:
        score -= 10
    if url.startswith("#") or url.startswith("mailto:") or url.startswith("javascript:"):
        score -= 100
    return score


def html_download_links(html: str, base_url: str) -> list[str]:
    parser = DownloadLinkParser()
    parser.feed(html)
    scored: list[tuple[int, str]] = []
    for href, context in parser.links:
        absolute_url = urljoin(base_url, href.strip())
        score = download_link_score(absolute_url, context)
        if score <= 0:
            continue
        scored.append((score, absolute_url))

    result: list[str] = []
    seen: set[str] = set()
    for _, url in sorted(scored, key=lambda item: item[0], reverse=True):
        key = requote_url(url)
        if key in seen:
            continue
        seen.add(key)
        result.append(url)
    return result


def download_pdf(
    candidate: PdfCandidate,
    *,
    out_dir: Path,
    repo_root: Path,
    email: str | None,
    timeout: float,
    overwrite: bool,
) -> dict[str, Any]:
    filename = safe_filename(candidate.article_title, candidate.doi)
    destination = out_dir / filename
    if destination.exists() and not overwrite:
        return {
            "ok": True,
            "path": repo_relative_path(destination, repo_root),
            "bytes": destination.stat().st_size,
            "source": candidate.source,
            "skipped": "already_exists",
        }

    temp_destination = destination.with_suffix(destination.suffix + ".part")
    current_url = candidate.url
    current_referer = download_referer_for(candidate)
    visited_urls: set[str] = set()

    for follow_number in range(MAX_HTML_DOWNLOAD_FOLLOWS + 1):
        url_key = requote_url(current_url)
        if url_key in visited_urls:
            raise DownloadError(f"HTML download link loop at {redact_url(current_url)}")
        visited_urls.add(url_key)

        request = Request(
            url_key,
            headers=http_headers(
                accept="application/pdf,application/octet-stream,text/html,*/*;q=0.8",
                referer=current_referer,
            ),
        )

        try:
            with urlopen(request, timeout=timeout) as response:
                content_type = response.headers.get("Content-Type", "").lower()
                response_url = response.geturl() or current_url
                first_chunk = response.read(8192)
                first_nonspace = first_chunk.lstrip()[:32].lower()
                has_pdf_magic = b"%PDF-" in first_chunk[:2048]
                looks_like_html = "html" in content_type or first_nonspace.startswith((b"<!doctype", b"<html"))

                if not has_pdf_magic:
                    first_text = first_chunk.decode("utf-8", errors="replace")
                    if looks_like_anti_bot_challenge(first_text, response_url):
                        raise DownloadError(f"anti-bot challenge at {redact_url(response_url)}")

                if not has_pdf_magic and looks_like_html:
                    html = first_chunk + response.read(1024 * 1024)
                    html_text = html.decode("utf-8", errors="replace")
                    if looks_like_anti_bot_challenge(html_text, response_url):
                        raise DownloadError(f"anti-bot challenge at {redact_url(response_url)}")
                    links = html_download_links(html_text, response_url)
                    if not links:
                        raise DownloadError(f"response is HTML without a PDF/download link, Content-Type={content_type!r}")
                    if follow_number >= MAX_HTML_DOWNLOAD_FOLLOWS:
                        raise DownloadError("too many HTML download link follows")
                    current_referer = response_url
                    current_url = links[0]
                    continue

                if not has_pdf_magic and "pdf" not in content_type and not looks_like_pdf_url(current_url):
                    raise DownloadError(f"response is not a PDF, Content-Type={content_type!r}")

                sha256 = hashlib.sha256()
                total = 0
                with temp_destination.open("wb") as file:
                    file.write(first_chunk)
                    sha256.update(first_chunk)
                    total += len(first_chunk)
                    while True:
                        chunk = response.read(1024 * 128)
                        if not chunk:
                            break
                        file.write(chunk)
                        sha256.update(chunk)
                        total += len(chunk)
                break
        except HTTPError as exc:
            body = exc.read(16 * 1024).decode("utf-8", errors="replace")
            temp_destination.unlink(missing_ok=True)
            location = exc.headers.get("Location", "") if exc.headers else ""
            redirect_url = urljoin(current_url, location) if location else ""
            header_text = str(exc.headers) if exc.headers else ""
            if exc.code in {301, 302, 303, 307, 308, 403, 429, 503} and looks_like_anti_bot_challenge(
                body,
                header_text,
                exc.geturl(),
                redirect_url,
            ):
                raise DownloadError(f"HTTP {exc.code}: anti-bot challenge") from exc
            if exc.code == 403 and looks_like_access_denied(body):
                raise DownloadError("HTTP 403: access denied") from exc
            body = re.sub(r"\s+", " ", body).strip()[:300]
            raise DownloadError(f"HTTP {exc.code}: {body}") from exc
        except URLError as exc:
            temp_destination.unlink(missing_ok=True)
            raise DownloadError(f"network error: {exc.reason}") from exc
        except TimeoutError as exc:
            temp_destination.unlink(missing_ok=True)
            raise DownloadError(f"timeout after {timeout:g}s") from exc
        except (
            http.client.RemoteDisconnected,
            http.client.BadStatusLine,
            http.client.IncompleteRead,
            ConnectionError,
        ) as exc:
            temp_destination.unlink(missing_ok=True)
            raise DownloadError(f"connection closed: {exc}") from exc
        except Exception:
            temp_destination.unlink(missing_ok=True)
            raise
    else:
        raise DownloadError("too many HTML download link follows")

    temp_destination.replace(destination)
    return {
        "ok": True,
        "path": repo_relative_path(destination, repo_root),
        "bytes": total,
        "sha256": sha256.hexdigest(),
        "source": candidate.source,
    }


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def doi_key(doi: str | None) -> str | None:
    cleaned = clean_doi(doi)
    return cleaned.lower() if cleaned else None


def empty_doi_index() -> dict[str, Any]:
    return {
        "version": 1,
        "updated_at_utc": None,
        "items": {},
    }


def load_doi_index(path: Path) -> dict[str, Any]:
    if not path.exists():
        return empty_doi_index()

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        log(f"DOI index: не существует {path}: {exc}. Создан новый индекс.")
        return empty_doi_index()

    if isinstance(data, dict) and isinstance(data.get("items"), dict):
        return data

    if isinstance(data, list):
        index = empty_doi_index()
        for doi in data:
            key = doi_key(str(doi))
            if key:
                index["items"][key] = {"doi": clean_doi(str(doi))}
        return index

    log(f"DOI index: неизвестный формат {path}. Создан новый индекс.")
    return empty_doi_index()


def normalize_saved_path(
    path_value: str | None,
    *,
    repo_root: Path,
    report_path: Path | None = None,
) -> str | None:
    path = resolve_stored_path(path_value, repo_root=repo_root, report_path=report_path)
    if not path:
        return None
    return repo_relative_path(path, repo_root)


def normalize_index_paths(index: dict[str, Any], repo_root: Path) -> None:
    for entry in index.get("items", {}).values():
        entries = [entry, *(entry.get("duplicate_paths") or [])]
        for item in entries:
            for key in ("path", "report_path"):
                if item.get(key):
                    item[key] = normalize_saved_path(item[key], repo_root=repo_root)


def register_doi_entry(
    index: dict[str, Any],
    *,
    doi: str | None,
    path: str | None,
    title: str | None = None,
    query: str | None = None,
    source: str | None = None,
    bytes_count: int | None = None,
    report_path: str | None = None,
) -> bool:
    key = doi_key(doi)
    if not key:
        return False

    entry = {
        "doi": clean_doi(doi),
        "path": path,
        "title": title,
        "query": query,
        "source": source,
        "bytes": bytes_count,
        "report_path": report_path,
    }
    items = index.setdefault("items", {})
    existing = items.get(key)
    if not existing:
        items[key] = entry
        return True

    if path and existing.get("path") != path:
        duplicates = existing.setdefault("duplicate_paths", [])
        if not any(item.get("path") == path for item in duplicates):
            duplicates.append(entry)
            return True
    return False


def find_doi_duplicate(index: dict[str, Any], doi: str | None, repo_root: Path) -> dict[str, Any] | None:
    key = doi_key(doi)
    if not key:
        return None

    entry = index.get("items", {}).get(key)
    if not entry:
        return None

    entries = [entry, *(entry.get("duplicate_paths") or [])]
    for item in entries:
        path = item.get("path")
        if not path or stored_path_exists(path, repo_root):
            return item
    return None


def bootstrap_doi_index_from_reports(base_out_dir: Path, index: dict[str, Any], repo_root: Path) -> int:
    added = 0
    if not base_out_dir.exists():
        return added

    for report_path in sorted(base_out_dir.rglob("metadata_and_download_report.json")):
        try:
            report = json.loads(report_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue

        query = report.get("query")
        for article_report in report.get("articles", []):
            article = article_report.get("article") or {}
            download = article_report.get("download") or {}
            if not download:
                continue

            path = normalize_saved_path(download.get("path"), repo_root=repo_root, report_path=report_path)
            if register_doi_entry(
                index,
                doi=article.get("doi"),
                path=path,
                title=article.get("title"),
                query=query,
                source=download.get("source"),
                bytes_count=download.get("bytes"),
                report_path=repo_relative_path(report_path, repo_root),
            ):
                added += 1
    return added


def save_doi_files(base_out_dir: Path, index: dict[str, Any], repo_root: Path) -> tuple[Path, Path]:
    index["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
    normalize_index_paths(index, repo_root)
    index_path = base_out_dir / DOI_INDEX_FILENAME
    numbers_path = base_out_dir / DOI_NUMBERS_FILENAME
    save_json(index_path, index)

    dois = sorted(
        entry.get("doi") or key
        for key, entry in index.get("items", {}).items()
        if entry.get("doi") or key
    )
    numbers_path.write_text(("\n".join(dois) + "\n") if dois else "", encoding="utf-8")
    return index_path, numbers_path


def duplicate_log_message(doi: str | None, existing: dict[str, Any], skipped_path: Path) -> str:
    existing_path = existing.get("path") or "path unknown"
    return f"   Дубликат DOI {clean_doi(doi)}: уже есть файл {existing_path}"


def duplicate_report_entry(
    doi: str | None,
    existing: dict[str, Any],
    skipped_path: Path,
    repo_root: Path,
) -> dict[str, Any]:
    return {
        "doi": clean_doi(doi),
        "existing_path": normalize_saved_path(existing.get("path"), repo_root=repo_root),
        "existing_title": existing.get("title"),
        "existing_query": existing.get("query"),
        "existing_source": existing.get("source"),
        "skipped_path": repo_relative_path(skipped_path, repo_root),
    }


def positive_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def openalex_page_size(value: str) -> int:
    number = positive_int(value)
    if number > 200:
        raise argparse.ArgumentTypeError("OpenAlex page size must be between 1 and 200")
    return number


def query_from_openalex_url(url: str) -> tuple[str, int | None]:
    parts = urlsplit(url)
    params = parse_qs(parts.query)
    queries = params.get(OPENALEX_SEARCH_PARAM)
    if not queries or not queries[0].strip():
        raise argparse.ArgumentTypeError(f"--query-url must include `{OPENALEX_SEARCH_PARAM}`")

    page_size: int | None = None
    per_page_values = params.get("per_page")
    if per_page_values and per_page_values[0].strip():
        page_size = openalex_page_size(per_page_values[0])

    return queries[0], page_size


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find OA article PDFs through OpenAlex, SciBban, Unpaywall, Europe PMC and CORE.",
    )
    parser.add_argument(
        "query",
        nargs="?",
        default=None,
        help="Article title/search query.",
    )
    parser.add_argument(
        "--query-url",
        default=None,
        help=f"OpenAlex /works URL. The `{OPENALEX_SEARCH_PARAM}` parameter is used as the query.",
    )
    parser.add_argument(
        "--email",
        default=os.getenv("UNPAYWALL_EMAIL") or os.getenv("OPENALEX_MAILTO"),
        help="Email for Unpaywall/OpenAlex polite pool. Can also use UNPAYWALL_EMAIL.",
    )
    parser.add_argument(
        "--openalex-key",
        default=os.getenv("OPENALEX_API_KEY"),
        help="OpenAlex API key if your OpenAlex account requires one. Can also use OPENALEX_API_KEY.",
    )
    parser.add_argument(
        "--core-key",
        default=os.getenv("CORE_API_KEY"),
        help="Optional CORE API key. Can also use CORE_API_KEY.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Base download directory. PDFs go to <out-dir>/<query-name>. Defaults to ./downloaded_pdfs.",
    )
    parser.add_argument(
        "--max-results",
        type=positive_int,
        default=100,
        help="Total max OpenAlex works to process.",
    )
    parser.add_argument(
        "--page-size",
        type=openalex_page_size,
        default=DEFAULT_PAGE_SIZE,
        help=f"OpenAlex metadata page/chunk size, 1-200. Defaults to {DEFAULT_PAGE_SIZE}.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30,
        help="HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--workers",
        type=positive_int,
        default=1,
        help="Number of parallel workers for candidate lookup/download. Default: 1.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-download PDFs even if a file with the same name exists.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Search and write metadata, but do not download PDFs.",
    )
    args = parser.parse_args()

    if args.query_url:
        try:
            args.query, url_page_size = query_from_openalex_url(args.query_url)
        except argparse.ArgumentTypeError as exc:
            parser.error(str(exc))
        if url_page_size is not None and args.page_size == DEFAULT_PAGE_SIZE:
            args.page_size = url_page_size
    elif args.query is None:
        args.query = DEFAULT_QUERY

    return args


def main() -> int:
    args = parse_args()
    query = normalize_text(args.query)
    repo_root = Path(__file__).resolve().parent
    base_out_dir = args.out_dir.resolve() if args.out_dir else repo_root / "downloaded_pdfs"
    query_dir_name = safe_dirname(query)
    out_dir = base_out_dir / query_dir_name
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "metadata_and_download_report.json"
    openalex_cache_path = out_dir / "openalex_metadata_cache.json"
    doi_index_path = base_out_dir / DOI_INDEX_FILENAME
    doi_index = load_doi_index(doi_index_path)
    normalize_index_paths(doi_index, repo_root)
    bootstrapped_dois = bootstrap_doi_index_from_reports(base_out_dir, doi_index, repo_root)
    doi_index_path, doi_numbers_path = save_doi_files(base_out_dir, doi_index, repo_root)

    report: dict[str, Any] = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "query": args.query,
        "normalized_query": query,
        "openalex_total_works": None,
        "articles": [],
    }

    log(f"Папка для PDF: {out_dir}")
    log(f"OpenAlex metadata cache: {openalex_cache_path}")
    log(f"DOI index: {len(doi_index.get('items', {}))} DOI в {doi_index_path}")
    if bootstrapped_dois:
        log(f"DOI index: добавлено из старых отчетов: {bootstrapped_dois}")
    if not args.openalex_key:
        log("OpenAlex API key не задан: если OpenAlex вернет 401/403, передай --openalex-key или OPENALEX_API_KEY.")
    log(f"1. OpenAlex: получаю метаданные по {OPENALEX_SEARCH_PARAM} страницами до {args.page_size} работ...")
    articles: list[Article] = []
    openalex_total_works: int | None = None
    downloaded: list[dict[str, Any]] = []
    duplicate_articles: list[dict[str, Any]] = []
    try:
        pages = with_retry(lambda: openalex_search_pages(
            query,
            email=args.email,
            api_key=args.openalex_key,
            max_results=args.max_results,
            page_size=args.page_size,
            timeout=args.timeout,
            cache_path=openalex_cache_path,
        ))
        for page_number, page_articles, meta in pages:
            total_count = meta.get("count")
            if openalex_total_works is None and isinstance(total_count, int):
                openalex_total_works = total_count
                report["openalex_total_works"] = openalex_total_works
                log(f"   OpenAlex total works: {format_count(openalex_total_works)}")
            log(f"   Страница {page_number}: {len(page_articles)} работ")
            articles.extend(page_articles)
    except ApiError as exc:
        log(f"   OpenAlex не ответил: {exc}")

    if not articles:
        log("   OpenAlex не дал результатов, продолжу прямым поиском по запросу.")
        query_doi = extract_doi(query)
        if query_doi:
            log(f"   DOI из запроса: {query_doi}")
        articles = [Article(title=query, doi=query_doi)]
    else:
        log(f"   Найдено работ: {len(articles)}")

    doi_index_lock = threading.Lock()

    def process_article(index: int, article: Article) -> dict[str, Any]:
        log("")
        log(f"[{index}/{len(articles)}] {article.title}")
        if article.doi:
            log(f"   DOI: {article.doi}")

        article_report: dict[str, Any] = {
            "article": article_metadata(article),
            "source": None,
            "download": None,
            "duplicate": None,
            "errors": [],
        }

        article_key = doi_key(article.doi)
        with doi_index_lock:
            doi_entry = doi_index.get("items", {}).get(article_key) if article_key else None

        if doi_entry:
            candidates = [doi_entry, *(doi_entry.get("duplicate_paths") or [])]
            valid_entry = next(
                (item for item in candidates if item.get("path") and stored_path_exists(item["path"], repo_root)),
                None,
            )
            if valid_entry:
                skipped_path = out_dir / safe_filename(article.title, article.doi)
                article_report["duplicate"] = duplicate_report_entry(
                    article.doi, valid_entry, skipped_path, repo_root,
                )
                article_report["source"] = valid_entry.get("source")
                log(duplicate_log_message(article.doi, valid_entry, skipped_path))
                return article_report
            log(f"   DOI {clean_doi(article.doi)} найден в индексе, но файл отсутствует. Повторная загрузка...")

        source_steps = (
            ("2. SciBban: прямая ссылка по DOI...", "SciBban", lambda: sci_bban_candidates(article)),
            (
                "3. Unpaywall: ищу PDF...",
                "Unpaywall",
                lambda: unpaywall_candidates(article, email=args.email, timeout=args.timeout),
            ),
            (
                "4. Europe PMC: ищу OA копию...",
                "EuropePMC",
                lambda: europe_pmc_candidates(
                    article,
                    fallback_query=query,
                    email=args.email,
                    timeout=args.timeout,
                ),
            ),
            (
                "5. CORE: резервный поиск...",
                "CORE",
                lambda: core_candidates(
                    article,
                    fallback_query=query,
                    api_key=args.core_key,
                    email=args.email,
                    timeout=args.timeout,
                ),
            ),
            ("OpenAlex: проверяю PDF URL из метаданных...", "OpenAlex", lambda: openalex_pdf_candidates(article)),
        )

        download_step_logged = False
        for step_message, source_name, find_candidates in source_steps:
            log(step_message)
            try:
                source_candidates = dedupe_candidates(with_retry(find_candidates))
            except ApiError as exc:
                article_report["errors"].append({"source": source_name, "error": str(exc)})
                log(f"   {source_name} ошибка: {exc}")
                continue

            if not source_candidates:
                if source_name == "SciBban" and not article.doi:
                    log("   SciBban пропущен: у статьи нет DOI.")
                else:
                    log(f"   {source_name}: источник не найден.")
                continue

            log(f"   {source_name}: источников найдено: {len(source_candidates)}")

            if args.dry_run:
                candidate = source_candidates[0]
                article_report["source"] = candidate.source
                log(f"   Первый найденный источник: {candidate.source}")
                break

            if not download_step_logged:
                log("Скачать PDF...")
                download_step_logged = True

            source_finished = False
            for candidate in source_candidates:
                candidate_doi = candidate.doi or article.doi
                with doi_index_lock:
                    candidate_duplicate = find_doi_duplicate(doi_index, candidate_doi, repo_root)

                if candidate_duplicate:
                    skipped_path = out_dir / safe_filename(candidate.article_title, candidate_doi)
                    duplicate_info = duplicate_report_entry(candidate_doi, candidate_duplicate, skipped_path, repo_root)
                    article_report["source"] = candidate_duplicate.get("source") or candidate.source
                    article_report["duplicate"] = duplicate_info
                    log(duplicate_log_message(candidate_doi, candidate_duplicate, skipped_path))
                    source_finished = True
                    break

                candidate_timeout = download_timeout_for(candidate, args.timeout)
                log(f"   Пробую {candidate.source}: {redact_url(candidate.url)}")
                if candidate_timeout != args.timeout:
                    log(f"   Timeout для {candidate.source}: {candidate_timeout:g}s")
                try:
                    download_result = download_pdf(
                        candidate,
                        out_dir=out_dir,
                        repo_root=repo_root,
                        email=args.email,
                        timeout=candidate_timeout,
                        overwrite=args.overwrite,
                    )
                    article_report["source"] = download_result.get("source")
                    article_report["download"] = download_result
                    with doi_index_lock:
                        if register_doi_entry(
                            doi_index,
                            doi=candidate_doi,
                            path=download_result.get("path"),
                            title=article.title,
                            query=query,
                            source=download_result.get("source"),
                            bytes_count=download_result.get("bytes"),
                            report_path=None,
                        ):
                            save_doi_files(base_out_dir, doi_index, repo_root)
                    log(f"   Готово: {download_result['path']} ({download_result['bytes']} bytes)")
                    source_finished = True
                    break
                except DownloadError as exc:
                    error = str(exc)
                    article_report["errors"].append({"source": candidate.source, "error": error})
                    log(f"   Не получилось: {candidate.source}: {error}")
                    time.sleep(0.5)

            if source_finished:
                break

            log(f"   {source_name}: рабочий PDF не найден, перехожу к следующему источника.")

        if args.dry_run:
            log("Скачать PDF: dry-run, пропускаю загрузку.")
        elif not article_report["download"] and not article_report["duplicate"]:
            log("   PDF не скачан для этой работы.")

        return article_report

    interrupted = False

    def _on_sigint(signum, frame):
        nonlocal interrupted
        interrupted = True
        log("")
        log("Получен SIGINT (Ctrl+C), завершаю и сохраняю прогресс...")

    old_handler = signal.signal(signal.SIGINT, _on_sigint)

    try:
        set_log_worker("main")
        if args.workers == 1:
            for index, article in enumerate(articles, start=1):
                if interrupted:
                    break
                article_report = process_article(index, article)
                report["articles"].append(article_report)
                if article_report.get("duplicate"):
                    duplicate_articles.append(article_report["duplicate"])
        else:
            log(f"Worker pool: {args.workers} parallel downloads")
            indexed_articles = list(enumerate(articles, start=1))
            pending_reports: list[tuple[int, dict[str, Any]]] = []

            def process_article_in_worker(index: int, article: Article) -> dict[str, Any]:
                set_pool_log_worker()
                return process_article(index, article)

            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = {
                    executor.submit(process_article_in_worker, index, article): index
                    for index, article in indexed_articles
                }
                for future in as_completed(futures):
                    if interrupted:
                        executor.shutdown(wait=False, cancel_futures=True)
                        break
                    index = futures[future]
                    article_report = future.result()
                    pending_reports.append((index, article_report))
                    if article_report.get("duplicate"):
                        duplicate_articles.append(article_report["duplicate"])

            report["articles"].extend(
                article_report for _, article_report in sorted(pending_reports, key=lambda item: item[0])
            )
    finally:
        signal.signal(signal.SIGINT, old_handler)
        if interrupted:
            try_finalize(args, base_out_dir, doi_index, repo_root, report, report_path, downloaded, duplicate_articles, openalex_total_works)
            raise KeyboardInterrupt

    downloaded = [item for item in report["articles"] if item.get("download")]
    new_downloaded = [item for item in downloaded if not item["download"].get("skipped")]
    duplicate_count = len(duplicate_articles)
    saved_or_existing_pdf_count = len(downloaded)
    available_pdf_count = saved_or_existing_pdf_count + duplicate_count
    downloaded_count = len(new_downloaded)
    processed_count = len(report["articles"])
    downloaded_ratio = downloaded_count / openalex_total_works if openalex_total_works else None
    available_ratio = available_pdf_count / openalex_total_works if openalex_total_works else None
    processed_ratio = processed_count / openalex_total_works if openalex_total_works else None

    report["processed_article_count"] = processed_count
    report["processed_to_openalex_total_ratio"] = processed_ratio
    report["saved_or_existing_pdf_count"] = saved_or_existing_pdf_count
    report["available_pdf_count"] = available_pdf_count
    report["available_pdf_to_openalex_total_ratio"] = available_ratio
    report["downloaded_pdf_count"] = downloaded_count
    report["downloaded_pdf_to_openalex_total_ratio"] = downloaded_ratio
    report["duplicate_doi_count"] = duplicate_count

    log("")
    log("Результаты:")
    log(f"   OpenAlex total works: {format_count(openalex_total_works)}")
    log(f"   Обработано работ: {format_count(processed_count)} ({format_ratio(processed_count, openalex_total_works)})")
    log(f"   Новых PDF скачано: {format_count(downloaded_count)}")
    log(f"   PDF сохранены/найдены для текущих статей: {format_count(saved_or_existing_pdf_count)}")
    log(f"   DOI-дубликатов пропущено: {format_count(duplicate_count)}")
    log(f"   PDF доступны в базе после dedup: {format_count(available_pdf_count)}")
    log(
        "   Новые PDF / OpenAlex total: "
        f"{format_count(downloaded_count)} / {format_count(openalex_total_works)} "
        f"({format_ratio(downloaded_count, openalex_total_works)})"
    )
    log(
        "   Доступные PDF / OpenAlex total: "
        f"{format_count(available_pdf_count)} / {format_count(openalex_total_works)} "
        f"({format_ratio(available_pdf_count, openalex_total_works)})"
    )
    if openalex_total_works and processed_count < openalex_total_works:
        log(f"   Ограничение: обработаны только первые {format_count(processed_count)} из-за --max-results={args.max_results}.")

    if interrupted:
        try_finalize(args, base_out_dir, doi_index, repo_root, report, report_path, downloaded, duplicate_articles, openalex_total_works)
        signal.signal(signal.SIGINT, old_handler)
        return 130

    save_json(report_path, report)
    log("")
    log(f"Отчет сохранен: {report_path}")
    save_doi_files(base_out_dir, doi_index, repo_root)
    signal.signal(signal.SIGINT, old_handler)
    return 0 if downloaded or duplicate_articles or args.dry_run else 2


def try_finalize(args, base_out_dir, doi_index, repo_root, report, report_path, downloaded, duplicate_articles, openalex_total_works):
    try:
        downloaded_count = len([item for item in report["articles"] if item.get("download") and not item["download"].get("skipped")])
        duplicate_count = len(duplicate_articles)
        processed_count = len(report["articles"])
        report["processed_article_count"] = processed_count
        report["downloaded_pdf_count"] = downloaded_count
        report["duplicate_doi_count"] = duplicate_count
        report["saved_or_existing_pdf_count"] = len([item for item in report["articles"] if item.get("download")])
        report["available_pdf_count"] = report["saved_or_existing_pdf_count"] + duplicate_count
        report["processed_to_openalex_total_ratio"] = processed_count / openalex_total_works if openalex_total_works else None
        report["downloaded_pdf_to_openalex_total_ratio"] = downloaded_count / openalex_total_works if openalex_total_works else None
        report["available_pdf_to_openalex_total_ratio"] = report["available_pdf_count"] / openalex_total_works if openalex_total_works else None
        save_json(report_path, report)
        save_doi_files(base_out_dir, doi_index, repo_root)
        log("")
        log("Финализация после прерывания:")
        log(f"   Обработано работ: {format_count(processed_count)}")
        log(f"   Новых PDF скачано: {format_count(downloaded_count)}")
        log(f"   PDF сохранены: {format_count(report['saved_or_existing_pdf_count'])}")
        log(f"   DOI-дубликатов: {format_count(duplicate_count)}")
        log(f"   Отчет: {report_path}")
    except Exception as exc:
        log(f"Ошибка финализации: {exc}")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log("")
        log("Прервано пользователем.")
        sys.exit(130)
