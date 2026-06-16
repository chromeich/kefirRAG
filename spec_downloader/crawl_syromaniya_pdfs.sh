#!/usr/bin/env bash
set -euo pipefail

BASE_URL="https://syromaniya.ru"
CATALOG_URL="${BASE_URL}/katalog/zakvaski-dlya-syra/"
OUT_DIR="spec_downloader/syromaniya"
HTML_DIR="${OUT_DIR}/html"
PDF_DIR="${OUT_DIR}/pdfs"
URLS_FILE="${OUT_DIR}/pdf_urls.txt"

mkdir -p "${HTML_DIR}" "${PDF_DIR}"

wget \
  --recursive \
  --level=2 \
  --no-parent \
  --accept=html \
  --domains syromaniya.ru \
  --directory-prefix "${HTML_DIR}" \
  "${CATALOG_URL}"

{ grep -rhoE 'href="[^"]+\.pdf"' "${HTML_DIR}" || true; } \
  | sed -E 's/^href="//; s/"$//' \
  | awk -v base_url="${BASE_URL}" -v catalog_url="${CATALOG_URL}" '
      /^https?:\/\// { print; next }
      /^\/\// { print "https:" $0; next }
      /^\// { print base_url $0; next }
      NF { print catalog_url $0 }
    ' \
  | sort -u \
  > "${URLS_FILE}"

if [[ ! -s "${URLS_FILE}" ]]; then
  echo "No PDF links found in ${HTML_DIR}"
  exit 0
fi

wget \
  --directory-prefix "${PDF_DIR}" \
  --input-file "${URLS_FILE}"
