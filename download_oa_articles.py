#!/usr/bin/env python3
"""
Search open-access article PDFs by title/query and download them next to this file.

Pipeline:
1. OpenAlex   -> metadata and DOI candidates
3. Unpaywall  -> OA PDF by DOI
4. Europe PMC -> OA PDF/full-text copies
5. CORE       -> fallback OA search
6. Download PDF files

Examples:
    python3 download_oa_articles.py \
        "Active chitosan/PVA films with anthocyanins from Brassica oleraceae" \
        --out-dir ./my_pdfs \
        --page-size 25 \
        --email you@example.com \
        --openalex-key YOUR_OPENALEX_KEY \
        --core-key YOUR_CORE_KEY

PDFs and the JSON report are saved under <out-dir>/<query-name>/.

Unpaywall requires a real email. OpenAlex may require an API key. CORE requires
an API key, so the CORE step is skipped unless --core-key or CORE_API_KEY is set.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
import unicodedata
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen


DEFAULT_QUERY = "milk fermentation"
DEFAULT_PAGE_SIZE = 100
OPENALEX_SEARCH_PARAM = "search.title_and_abstract"

OPENALEX_WORKS_URL = "https://api.openalex.org/works"
UNPAYWALL_DOI_URL = "https://api.unpaywall.org/v2/{doi}"
EUROPE_PMC_SEARCH_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
CORE_V3_SEARCH_URL = "https://api.core.ac.uk/v3/search/works/"
CORE_V2_SEARCH_URL = "https://core.ac.uk/api-v2/articles/search/{query}"
CORE_V2_DOWNLOAD_URL = "https://core.ac.uk/api-v2/articles/get/{core_id}/download/pdf"

USER_AGENT = "oa-pdf-downloader/1.0 (+https://openalex.org; mailto:{email})"


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


def requote_url(url: str) -> str:
    parts = urlsplit(url.strip())
    path = quote(parts.path, safe="/:%")
    query = quote(parts.query, safe="=&;%:+,/?@")
    return urlunsplit((parts.scheme, parts.netloc, path, query, parts.fragment))


def redact_url(url: str) -> str:
    return re.sub(r"(?i)(apiKey|api_key|key|token)=([^&]+)", r"\1=<hidden>", url)


def http_headers(email: str | None = None, accept: str = "application/json") -> dict[str, str]:
    shown_email = email or "unknown@example.com"
    return {
        "Accept": accept,
        "User-Agent": USER_AGENT.format(email=shown_email),
    }


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


def log(message: str) -> None:
    print(message, flush=True)


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


def openalex_search_pages(
    query: str,
    *,
    email: str | None,
    api_key: str | None,
    max_results: int,
    page_size: int,
    timeout: float,
) -> Iterator[tuple[int, list[Article], dict[str, Any]]]:
    cursor = "*"
    page_number = 1
    remaining = max_results

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

        data = get_json(
            OPENALEX_WORKS_URL,
            params=params,
            headers=http_headers(email),
            timeout=timeout,
        )
        results = data.get("results") or []
        if not results:
            break

        meta = data.get("meta") or {}
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
                candidates.append(
                    PdfCandidate(
                        url=url,
                        source="OpenAlex",
                        article_title=article.title,
                        doi=article.doi,
                        note=key,
                    )
                )

    oa_url = (article.raw.get("open_access") or {}).get("oa_url")
    if oa_url and looks_like_pdf_url(oa_url):
        candidates.append(
            PdfCandidate(
                url=oa_url,
                source="OpenAlex",
                article_title=article.title,
                doi=article.doi,
                note="open_access.oa_url",
            )
        )
    return candidates


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
        headers=http_headers(email),
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
            candidates.append(
                PdfCandidate(
                    url=url_for_pdf,
                    source="Unpaywall",
                    article_title=article.title,
                    doi=article.doi,
                    note=location.get("host_type"),
                )
            )
        url = location.get("url")
        if url and looks_like_pdf_url(url):
            candidates.append(
                PdfCandidate(
                    url=url,
                    source="Unpaywall",
                    article_title=article.title,
                    doi=article.doi,
                    note=location.get("host_type"),
                )
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
            headers=http_headers(email),
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
                headers={**http_headers(email), "Authorization": f"Bearer {api_key}"},
                timeout=timeout,
            )
        except ApiError as exc:
            log(f"   CORE v3 не ответил для {term!r}: {exc}")
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
                headers=http_headers(email),
                timeout=timeout,
            )
        except ApiError as exc:
            log(f"   CORE v2 не ответил для {term!r}: {exc}")
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


def download_pdf(
    candidate: PdfCandidate,
    *,
    out_dir: Path,
    email: str | None,
    timeout: float,
    overwrite: bool,
) -> dict[str, Any]:
    filename = safe_filename(candidate.article_title, candidate.doi)
    destination = out_dir / filename
    if destination.exists() and not overwrite:
        return {
            "ok": True,
            "path": str(destination),
            "bytes": destination.stat().st_size,
            "source": candidate.source,
            "url": redact_url(candidate.url),
            "skipped": "already_exists",
        }

    request = Request(requote_url(candidate.url), headers=http_headers(email, accept="application/pdf,*/*"))
    temp_destination = destination.with_suffix(destination.suffix + ".part")

    try:
        with urlopen(request, timeout=timeout) as response:
            content_type = response.headers.get("Content-Type", "").lower()
            first_chunk = response.read(8192)
            has_pdf_magic = b"%PDF-" in first_chunk[:2048]
            if not has_pdf_magic and "pdf" not in content_type and not looks_like_pdf_url(candidate.url):
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
    except HTTPError as exc:
        body = exc.read(300).decode("utf-8", errors="replace")
        temp_destination.unlink(missing_ok=True)
        raise DownloadError(f"HTTP {exc.code}: {body}") from exc
    except URLError as exc:
        temp_destination.unlink(missing_ok=True)
        raise DownloadError(f"network error: {exc.reason}") from exc
    except Exception:
        temp_destination.unlink(missing_ok=True)
        raise

    temp_destination.replace(destination)
    return {
        "ok": True,
        "path": str(destination),
        "bytes": total,
        "sha256": sha256.hexdigest(),
        "source": candidate.source,
        "url": redact_url(candidate.url),
    }


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find OA article PDFs through OpenAlex, Unpaywall, Europe PMC and CORE.",
    )
    parser.add_argument(
        "query",
        nargs="?",
        default=DEFAULT_QUERY,
        help="Article title/search query.",
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
        default=5,
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
        "--overwrite",
        action="store_true",
        help="Re-download PDFs even if a file with the same name exists.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Search and write metadata, but do not download PDFs.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    query = normalize_text(args.query)
    script_dir = Path(__file__).resolve().parent
    base_out_dir = args.out_dir.resolve() if args.out_dir else script_dir / "downloaded_pdfs"
    query_dir_name = safe_dirname(query)
    out_dir = base_out_dir / query_dir_name
    out_dir.mkdir(parents=True, exist_ok=True)

    report: dict[str, Any] = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "query": args.query,
        "normalized_query": query,
        "base_out_dir": str(base_out_dir),
        "query_dir_name": query_dir_name,
        "out_dir": str(out_dir),
        "openalex_search_param": OPENALEX_SEARCH_PARAM,
        "openalex_total_works": None,
        "openalex_max_results": args.max_results,
        "openalex_page_size": args.page_size,
        "articles": [],
    }

    log(f"Папка для PDF: {out_dir}")
    if not args.openalex_key:
        log("OpenAlex API key не задан: если OpenAlex вернет 401/403, передай --openalex-key или OPENALEX_API_KEY.")
    log(f"1. OpenAlex: получаю метаданные по {OPENALEX_SEARCH_PARAM} страницами до {args.page_size} работ...")
    articles: list[Article] = []
    openalex_total_works: int | None = None
    try:
        pages = openalex_search_pages(
            query,
            email=args.email,
            api_key=args.openalex_key,
            max_results=args.max_results,
            page_size=args.page_size,
            timeout=args.timeout,
        )
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
        articles = [Article(title=query)]
    else:
        log(f"   Найдено работ: {len(articles)}")

    for index, article in enumerate(articles, start=1):
        log("")
        log(f"[{index}/{len(articles)}] {article.title}")
        if article.doi:
            log(f"   DOI: {article.doi}")

        article_report: dict[str, Any] = {
            "article": asdict(article),
            "candidates": [],
            "download": None,
            "errors": [],
        }

        candidates: list[PdfCandidate] = []

        log("3. Unpaywall: ищу PDF...")
        try:
            candidates.extend(unpaywall_candidates(article, email=args.email, timeout=args.timeout))
        except ApiError as exc:
            article_report["errors"].append({"source": "Unpaywall", "error": str(exc)})
            log(f"   Unpaywall ошибка: {exc}")

        log("4. Europe PMC: ищу OA копию...")
        try:
            candidates.extend(
                europe_pmc_candidates(
                    article,
                    fallback_query=query,
                    email=args.email,
                    timeout=args.timeout,
                )
            )
        except ApiError as exc:
            article_report["errors"].append({"source": "EuropePMC", "error": str(exc)})
            log(f"   Europe PMC ошибка: {exc}")

        log("5. CORE: резервный поиск...")
        try:
            candidates.extend(
                core_candidates(
                    article,
                    fallback_query=query,
                    api_key=args.core_key,
                    email=args.email,
                    timeout=args.timeout,
                )
            )
        except ApiError as exc:
            article_report["errors"].append({"source": "CORE", "error": str(exc)})
            log(f"   CORE ошибка: {exc}")

        # OpenAlex often already contains a best_oa_location PDF URL; use it after
        # the requested sources so the planned order stays intact.
        candidates.extend(openalex_pdf_candidates(article))
        candidates = dedupe_candidates(candidates)

        article_report["candidates"] = [asdict(candidate) for candidate in candidates]
        log(f"   PDF-кандидатов: {len(candidates)}")

        if args.dry_run:
            log("6. Скачать PDF: dry-run, пропускаю загрузку.")
            report["articles"].append(article_report)
            continue

        log("6. Скачать PDF...")
        for candidate in candidates:
            try:
                log(f"   Пробую {candidate.source}: {redact_url(candidate.url)}")
                download_result = download_pdf(
                    candidate,
                    out_dir=out_dir,
                    email=args.email,
                    timeout=args.timeout,
                    overwrite=args.overwrite,
                )
                article_report["download"] = download_result
                log(f"   Готово: {download_result['path']} ({download_result['bytes']} bytes)")
                break
            except DownloadError as exc:
                message = f"{candidate.source}: {exc}"
                article_report["errors"].append({"source": candidate.source, "error": str(exc)})
                log(f"   Не получилось: {message}")
                time.sleep(0.5)

        if not article_report["download"]:
            log("   PDF не скачан для этой работы.")

        report["articles"].append(article_report)

    downloaded = [item for item in report["articles"] if item.get("download")]
    downloaded_count = len(downloaded)
    processed_count = len(report["articles"])
    downloaded_ratio = downloaded_count / openalex_total_works if openalex_total_works else None
    processed_ratio = processed_count / openalex_total_works if openalex_total_works else None

    report["processed_article_count"] = processed_count
    report["processed_to_openalex_total_ratio"] = processed_ratio
    report["downloaded_pdf_count"] = downloaded_count
    report["downloaded_pdf_to_openalex_total_ratio"] = downloaded_ratio

    log("")
    log("Итоговое сравнение:")
    log(f"   OpenAlex total works: {format_count(openalex_total_works)}")
    log(f"   Обработано работ: {format_count(processed_count)} ({format_ratio(processed_count, openalex_total_works)})")
    log(f"   Скачано PDF: {format_count(downloaded_count)}")
    log(
        "   PDF / OpenAlex total: "
        f"{format_count(downloaded_count)} / {format_count(openalex_total_works)} "
        f"({format_ratio(downloaded_count, openalex_total_works)})"
    )
    if openalex_total_works and processed_count < openalex_total_works:
        log(f"   Ограничение: обработаны только первые {format_count(processed_count)} из-за --max-results={args.max_results}.")

    report_path = out_dir / "metadata_and_download_report.json"
    save_json(report_path, report)
    log("")
    log(f"Отчет сохранен: {report_path}")
    return 0 if downloaded or args.dry_run else 2


if __name__ == "__main__":
    sys.exit(main())
