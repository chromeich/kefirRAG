#!/usr/bin/env python3
"""
Extract starter-culture composition from Syromaniya OCR text files and deduplicate names.

Examples:
    python3 spec_downloader/extract_syromaniya_composition.py
    python3 spec_downloader/extract_syromaniya_composition.py --format txt
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_OCR_DIR = ROOT_DIR / "syromaniya" / "ocr"
DEFAULT_OUT_DIR = ROOT_DIR / "syromaniya"

GENERA = (
    "Bifidobacterium",
    "Brevibacterium",
    "Debaryomyces",
    "Geotrichum",
    "Kluyveromyces",
    "Lacticaseibacillus",
    "Lactiplantibacillus",
    "Lactobacillus",
    "Lactococcus",
    "Lactococcus",
    "Leuconostoc",
    "Limosilactobacillus",
    "Penicillium",
    "Pediococcus",
    "Propionibacterium",
    "Saccharomyces",
    "Streptococcus",
)

CONTAMINANT_GENERA = {
    "Enterobacteriaceae",
    "Enterobatteriacee",
    "Listeria",
    "Salmonella",
    "Staphylococcus",
}

GENERA_LOWER = {genus.lower() for genus in GENERA}
ALLOW_GENUS_ONLY = {"Leuconostoc"}
WORD_FIXES = {
    "diacetilactis": "diacetylactis",
    "diacetylactis": "diacetylactis",
    "cemoris": "cremoris",
    "salivarus": "salivarius",
}
ALIASES = {
    "Lactobacillus bulgaricus": "Lactobacillus delbrueckii subsp. bulgaricus",
    "Lactococcus lactis subsp. lactis biovar diacetilactis": (
        "Lactococcus lactis subsp. lactis biovar diacetylactis"
    ),
    "Propionibacterium shermanii": "Propionibacterium freudenreichii subsp. shermanii",
    "Streptococcus salivarius subsp. thermophilus": "Streptococcus thermophilus",
}

STOP_HEADINGS = re.compile(
    r"^(?:"
    r"применение|ротаци[ия]|хранение|условия хранения|срок годности|"
    r"клеточная концентрация|количество|температура|рекоменд|"
    r"код продукта|размер|тип упаковки|упаковка|цвет|формат|форма|"
    r"микробиологические показатели|микробиологические характеристики|"
    r"техническая информация|информация|описание|гмо|стандарты|"
    r"аллерген|способ применения|инструкция"
    r")\b",
    re.IGNORECASE,
)

COMPOSITION_HEADING = re.compile(
    r"^(?:микробиологический\s+состав|бактериальный\s+состав(?:\s+культур)?|состав)\s*:?\s*(.*)$",
    re.IGNORECASE,
)

COMPOSITION_SENTENCE = re.compile(
    r"(?:в\s+состав[^.\n]{0,160}?(?:входят|входит|включает)|"
    r"бактериальный\s+состав\s+культур)\s+(.{0,500}?)(?:\.|\n\s*\n)",
    re.IGNORECASE | re.DOTALL,
)

LATIN_NAME = re.compile(
    rf"\b(?P<genus>{'|'.join(sorted(set(GENERA), key=len, reverse=True))})"
    r"(?:\s+(?P<species>[a-zа-я][a-zа-я-]+))?"
    r"(?:\s+(?P<rank>subsp|ssp|spp)\.?\s+(?P<subspecies>[a-zа-я][a-zа-я-]+))?"
    r"(?:\s+biovar\.?\s+(?P<biovar>[a-zа-я][a-zа-я-]+))?",
    re.IGNORECASE,
)

OCR_LETTER_FIXES = str.maketrans(
    {
        "С": "C",
        "с": "c",
        "А": "A",
        "а": "a",
        "В": "B",
        "Е": "E",
        "е": "e",
        "Н": "H",
        "К": "K",
        "М": "M",
        "О": "O",
        "о": "o",
        "Р": "P",
        "р": "p",
        "Т": "T",
        "Х": "X",
        "х": "x",
        "у": "y",
    },
)


@dataclass
class Hit:
    raw: str
    canonical: str
    source: str
    context: str


@dataclass
class Cluster:
    canonical: str
    variants: set[str] = field(default_factory=set)
    sources: set[str] = field(default_factory=set)
    contexts: list[str] = field(default_factory=list)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract composition names from OCR .txt files and deduplicate them.",
    )
    parser.add_argument("--ocr-dir", type=Path, default=DEFAULT_OCR_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument(
        "--format",
        choices=("csv", "json", "txt", "all"),
        default="all",
        help="Output format. Default: all.",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.92,
        help="Fuzzy dedup threshold for canonical names. Default: 0.92.",
    )
    return parser.parse_args()


def clean_text(text: str) -> str:
    text = text.replace("\x0c", "\n")
    text = re.sub(r"[\u00a0\t]+", " ", text)
    text = re.sub(r" +", " ", text)
    return text


def source_pdf_name(text_path: Path, text: str) -> str:
    match = re.search(r"^Source PDF:\s*(.+)$", text, re.MULTILINE)
    if match:
        return Path(match.group(1)).name
    return text_path.name


def selected_page_text(text: str) -> str:
    """Prefer embedded text, but include Tesseract too for files with broken PDF text."""
    chunks: list[str] = []
    for marker in ("--- PyMuPDF text ---", "--- Tesseract OCR ---"):
        parts = text.split(marker)
        if len(parts) == 1:
            continue
        for part in parts[1:]:
            part = re.split(r"\n--- |\n===== PAGE ", part, maxsplit=1)[0]
            if readable_score(part) > 0.2:
                chunks.append(part)
    return "\n".join(chunks) if chunks else text


def readable_score(text: str) -> float:
    letters = re.findall(r"[A-Za-zА-Яа-я]", text)
    if not text.strip():
        return 0.0
    return len(letters) / max(len(text), 1)


def composition_blocks(text: str) -> list[str]:
    lines = [line.strip() for line in clean_text(text).splitlines()]
    blocks: list[str] = []

    for index, line in enumerate(lines):
        match = COMPOSITION_HEADING.match(line)
        if not match:
            continue

        block_lines: list[str] = []
        if match.group(1).strip():
            block_lines.append(match.group(1).strip())

        for next_line in lines[index + 1 : index + 14]:
            if not next_line:
                if block_lines:
                    break
                continue
            if STOP_HEADINGS.match(next_line):
                break
            block_lines.append(next_line)

        if block_lines:
            blocks.append("\n".join(block_lines))

    for match in COMPOSITION_SENTENCE.finditer(clean_text(text)):
        blocks.append(match.group(0))

    return blocks


def title_latin_word(word: str) -> str:
    word = word.translate(OCR_LETTER_FIXES).lower()
    return word[:1].upper() + word[1:] if word else word


def lower_latin_word(word: str) -> str:
    word = word.translate(OCR_LETTER_FIXES).lower()
    return WORD_FIXES.get(word, word)


def canonicalize_match(match: re.Match[str]) -> str | None:
    genus = title_latin_word(match.group("genus"))
    if genus in CONTAMINANT_GENERA:
        return None

    species = match.group("species")
    subspecies = match.group("subspecies")
    biovar = match.group("biovar")

    parts = [genus]
    if species:
        species_clean = lower_latin_word(species)
        if (
            species_clean in {"sp", "spp", "subsp", "ssp", "biovar"}
            or species_clean in GENERA_LOWER
            or len(species_clean) < 3
        ):
            species_clean = ""
        if species_clean:
            parts.append(species_clean)
    if subspecies:
        parts.extend(["subsp.", lower_latin_word(subspecies)])
    if biovar:
        parts.extend(["biovar", lower_latin_word(biovar)])

    if len(parts) == 1 and genus not in ALLOW_GENUS_ONLY:
        return None

    canonical = " ".join(parts)
    return ALIASES.get(canonical, canonical)


def extract_names(block: str, source: str) -> list[Hit]:
    normalized = block.replace(",", "\n")
    hits: list[Hit] = []
    for match in LATIN_NAME.finditer(normalized):
        canonical = canonicalize_match(match)
        if not canonical:
            continue
        raw = re.sub(r"\s+", " ", match.group(0)).strip()
        context = re.sub(r"\s+", " ", block).strip()
        hits.append(Hit(raw=raw, canonical=canonical, source=source, context=context[:300]))
    return hits


def canonical_key(name: str) -> str:
    return re.sub(r"[^a-z]+", "", name.lower())


def similarity(left: str, right: str) -> float:
    return SequenceMatcher(None, canonical_key(left), canonical_key(right)).ratio()


def deduplicate(hits: list[Hit], threshold: float) -> list[Cluster]:
    clusters: list[Cluster] = []
    for hit in sorted(hits, key=lambda item: (item.canonical.lower(), item.source)):
        best: Cluster | None = None
        best_score = 0.0
        for cluster in clusters:
            score = similarity(hit.canonical, cluster.canonical)
            if score > best_score:
                best = cluster
                best_score = score

        if best is None or best_score < threshold:
            best = Cluster(canonical=hit.canonical)
            clusters.append(best)
        elif len(hit.canonical) > len(best.canonical):
            best.canonical = hit.canonical

        best.variants.add(hit.raw)
        best.sources.add(hit.source)
        if hit.context not in best.contexts:
            best.contexts.append(hit.context)

    return sorted(clusters, key=lambda item: item.canonical.lower())


def rows_from_clusters(clusters: list[Cluster]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for cluster in clusters:
        rows.append(
            {
                "ingredient": cluster.canonical,
                "source_count": len(cluster.sources),
                "sources": "; ".join(sorted(cluster.sources)),
                "variants": "; ".join(sorted(cluster.variants)),
                "sample_context": cluster.contexts[0] if cluster.contexts else "",
            },
        )
    return rows


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=("ingredient", "source_count", "sources", "variants", "sample_context"),
        )
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_txt(path: Path, rows: list[dict[str, object]]) -> None:
    lines = [str(row["ingredient"]) for row in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    if not args.ocr_dir.exists():
        raise SystemExit(f"OCR directory does not exist: {args.ocr_dir}")
    if not 0 < args.threshold <= 1:
        raise SystemExit("--threshold must be between 0 and 1")

    hits: list[Hit] = []
    misses: defaultdict[str, int] = defaultdict(int)
    for text_path in sorted(args.ocr_dir.glob("*.txt")):
        raw_text = text_path.read_text(encoding="utf-8", errors="replace")
        source = source_pdf_name(text_path, raw_text)
        page_text = selected_page_text(raw_text)
        blocks = composition_blocks(page_text)
        file_hits = [hit for block in blocks for hit in extract_names(block, source)]
        if not file_hits:
            misses[source] += 1
        hits.extend(file_hits)

    clusters = deduplicate(hits, args.threshold)
    rows = rows_from_clusters(clusters)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    if args.format in {"csv", "all"}:
        write_csv(args.out_dir / "syromaniya_composition_dedup.csv", rows)
    if args.format in {"json", "all"}:
        write_json(args.out_dir / "syromaniya_composition_dedup.json", rows)
    if args.format in {"txt", "all"}:
        write_txt(args.out_dir / "syromaniya_composition_dedup.txt", rows)

    print(f"OCR files: {len(list(args.ocr_dir.glob('*.txt')))}")
    print(f"Extracted mentions: {len(hits)}")
    print(f"Deduplicated ingredients: {len(rows)}")
    print(f"Files without composition hits: {len(misses)}")
    print(f"Output directory: {args.out_dir}")


if __name__ == "__main__":
    main()
