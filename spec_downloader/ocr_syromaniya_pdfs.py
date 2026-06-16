#!/usr/bin/env python3
"""
Render Syromaniya PDF specifications with PyMuPDF and OCR them with local Tesseract.

Examples:
    python3 spec_downloader/ocr_syromaniya_pdfs.py
    python3 spec_downloader/ocr_syromaniya_pdfs.py --limit 5 --force
"""

from __future__ import annotations

import argparse
import hashlib
import subprocess
import sys
import tempfile
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_PDF_DIR = ROOT_DIR / "syromaniya" / "pdfs"
DEFAULT_OCR_DIR = ROOT_DIR / "syromaniya" / "ocr"
DEFAULT_DPI = 200
DEFAULT_LANG = "rus+eng"
DEFAULT_WORKERS = 4


@dataclass(frozen=True)
class OcrResult:
    status: str
    pdf_path: Path
    out_path: Path
    error: str | None = None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="OCR Syromaniya PDF files with PyMuPDF-rendered 200 DPI images and Tesseract.",
    )
    parser.add_argument(
        "--pdf-dir",
        type=Path,
        default=DEFAULT_PDF_DIR,
        help=f"Directory with source PDFs. Default: {DEFAULT_PDF_DIR}",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OCR_DIR,
        help=f"Directory for OCR text files. Default: {DEFAULT_OCR_DIR}",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=DEFAULT_DPI,
        help=f"Render DPI for OCR images. Default: {DEFAULT_DPI}",
    )
    parser.add_argument(
        "--lang",
        default=DEFAULT_LANG,
        help=f"Tesseract language set. Default: {DEFAULT_LANG}",
    )
    parser.add_argument(
        "--tesseract",
        default="tesseract",
        help="Path to local tesseract executable.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing .txt files.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only the first N PDF files.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Number of PDF files to OCR in parallel. Default: {DEFAULT_WORKERS}",
    )
    return parser.parse_args(argv)


def import_fitz():
    try:
        import fitz
    except ImportError as exc:
        raise SystemExit("Missing dependency: PyMuPDF. Install it with `pip install PyMuPDF`.") from exc
    return fitz


def is_pdf_candidate(path: Path) -> bool:
    return path.is_file() and ".pdf" in path.name.lower()


def safe_text_filename(pdf_path: Path) -> str:
    name = unicodedata.normalize("NFKC", pdf_path.name)
    digest = hashlib.sha1(str(pdf_path).encode("utf-8")).hexdigest()[:8]
    return f"{name}.{digest}.txt"


def relative_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def extract_native_text(document) -> list[str]:
    texts: list[str] = []
    for page in document:
        text = page.get_text("text").strip()
        texts.append(text)
    return texts


def run_tesseract(
    image_path: Path,
    *,
    tesseract: str,
    lang: str,
    dpi: int,
) -> str:
    cmd = [
        tesseract,
        str(image_path),
        "stdout",
        "-l",
        lang,
        "--dpi",
        str(dpi),
    ]
    result = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"tesseract exited with {result.returncode}")
    return result.stdout.strip()


def ocr_page(page, tmp_dir: Path, page_number: int, args: argparse.Namespace) -> str:
    image_path = tmp_dir / f"page_{page_number:04d}.png"
    pixmap = page.get_pixmap(dpi=args.dpi, alpha=False)
    pixmap.save(image_path)
    return run_tesseract(
        image_path,
        tesseract=args.tesseract,
        lang=args.lang,
        dpi=args.dpi,
    )


def process_pdf(pdf_path: Path, out_path: Path, args: argparse.Namespace) -> None:
    fitz = import_fitz()

    with fitz.open(pdf_path) as document:
        native_texts = extract_native_text(document)

        ocr_texts: list[str] = []
        with tempfile.TemporaryDirectory(prefix="syromaniya_ocr_") as tmp:
            tmp_dir = Path(tmp)
            for index, page in enumerate(document, start=1):
                print(f"    {pdf_path.name}: page {index}/{document.page_count}")
                ocr_texts.append(ocr_page(page, tmp_dir, index, args))

    lines: list[str] = [
        f"Source PDF: {relative_path(pdf_path)}",
        f"OCR: tesseract {args.lang}, {args.dpi} DPI",
        "",
    ]
    for index, (native_text, ocr_text) in enumerate(zip(native_texts, ocr_texts), start=1):
        lines.extend(
            [
                f"===== PAGE {index} =====",
                "",
                "--- PyMuPDF text ---",
                native_text or "[no embedded text]",
                "",
                "--- Tesseract OCR ---",
                ocr_text or "[no OCR text]",
                "",
            ],
        )

    out_path.write_text("\n".join(lines), encoding="utf-8")


def process_pdf_task(pdf_path: Path, args: argparse.Namespace) -> OcrResult:
    out_path = args.out_dir / safe_text_filename(pdf_path)
    if out_path.exists() and not args.force:
        return OcrResult(status="skipped", pdf_path=pdf_path, out_path=out_path)

    try:
        process_pdf(pdf_path, out_path, args)
    except Exception as exc:
        return OcrResult(status="failed", pdf_path=pdf_path, out_path=out_path, error=str(exc))

    return OcrResult(status="processed", pdf_path=pdf_path, out_path=out_path)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    if args.dpi <= 0:
        raise SystemExit("--dpi must be a positive integer")
    if args.workers <= 0:
        raise SystemExit("--workers must be a positive integer")
    if not args.pdf_dir.exists():
        raise SystemExit(f"PDF directory does not exist: {args.pdf_dir}")

    args.out_dir.mkdir(parents=True, exist_ok=True)

    pdf_paths = sorted(path for path in args.pdf_dir.iterdir() if is_pdf_candidate(path))
    if args.limit is not None:
        pdf_paths = pdf_paths[: args.limit]

    if not pdf_paths:
        raise SystemExit(f"No PDF files found in: {args.pdf_dir}")

    processed = 0
    skipped = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process_pdf_task, pdf_path, args): pdf_path for pdf_path in pdf_paths}

        for future in as_completed(futures):
            result = future.result()
            if result.status == "skipped":
                print(f"Skipping existing: {result.out_path}")
                skipped += 1
                continue
            if result.status == "failed":
                print(f"Failed: {result.pdf_path}: {result.error}", file=sys.stderr)
                failed += 1
                continue

            print(f"Saved: {result.out_path}")
            processed += 1

    print(f"\nDone. Processed: {processed}, skipped: {skipped}, failed: {failed}")


if __name__ == "__main__":
    main()
