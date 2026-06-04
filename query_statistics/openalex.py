"""
Collect OpenAlex result counts for dairy fermentation search queries.

Examples:
    python3 -m query_statistics
    python3 -m query_statistics --email you@example.com --out openalex_stats.csv
"""

from __future__ import annotations

import argparse
import os
from time import sleep


OPENALEX_WORKS_URL = "https://api.openalex.org/works"

QUERIES = [
    "milk fermentation",
    "dairy fermentation",
    "fermented milk",
    "yogurt fermentation",
    "kefir fermentation",
    "starter cultures",
    "lactic acid bacteria",
    "probiotic dairy",
    "milk acidification",
    "fermentation kinetics",
    "fermentation optimization",
    "casein hydrolysis",
    "whey protein fermentation",
    "bioactive peptides",
    "proteolysis",
    "metabolomics dairy",
    "flavor compounds yogurt",
    "industrial dairy fermentation",
    "novel dairy processing",
    "future dairy technologies",
]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print and save OpenAlex result-count statistics for preset queries.",
    )
    parser.add_argument(
        "--email",
        default=os.getenv("OPENALEX_MAILTO"),
        help="Optional email for OpenAlex polite pool. Can also use OPENALEX_MAILTO.",
    )
    parser.add_argument(
        "--openalex-key",
        default=os.getenv("OPENALEX_API_KEY"),
        help="Optional OpenAlex API key. Can also use OPENALEX_API_KEY.",
    )
    parser.add_argument(
        "--out",
        default="openalex_query_statistics.csv",
        help="CSV output file path.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.2,
        help="Delay between OpenAlex requests in seconds.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30,
        help="HTTP timeout in seconds.",
    )
    return parser.parse_args(argv)


def fetch_count(query: str, args: argparse.Namespace) -> int:
    try:
        import requests
    except ImportError as exc:
        raise SystemExit("Missing dependency: requests. Run `pip install -r requirements.txt`.") from exc

    params = {
        "search": query,
        "per_page": 1,
    }
    if args.email:
        params["mailto"] = args.email
    if args.openalex_key:
        params["api_key"] = args.openalex_key

    response = requests.get(OPENALEX_WORKS_URL, params=params, timeout=args.timeout)
    response.raise_for_status()
    data = response.json()
    return int(data["meta"]["count"])


def print_table(df) -> None:
    try:
        print(df.to_markdown(index=False))
    except ImportError:
        print(df.to_string(index=False))


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    try:
        import pandas as pd
    except ImportError as exc:
        raise SystemExit("Missing dependency: pandas. Run `pip install -r requirements.txt`.") from exc

    rows = []

    for query in QUERIES:
        count = fetch_count(query, args)
        rows.append({"query": query, "count": count})
        sleep(args.delay)

    df = pd.DataFrame(rows).sort_values("count", ascending=False, ignore_index=True)
    print_table(df)
    df.to_csv(args.out, index=False)
    print(f"\nSaved CSV: {args.out}")
