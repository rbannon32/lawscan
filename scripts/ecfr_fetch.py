#!/usr/bin/env python3
"""
eCFR API v1 explorer — pulls structure and near-full content for a title.

Examples:
  # Show all titles and their freshness dates
  python ecfr_fetch.py --list-titles

  # Download ALL parts for Title 40 (latest version date), saving XML and plain text
  python ecfr_fetch.py --title 40 --out ./ecfr_out

  # Download only Parts 50 and 600 of Title 40 (latest version date)
  python ecfr_fetch.py --title 40 --parts 50 600 --out ./ecfr_out

  # Pin to a specific historical date (YYYY-MM-DD)
  python ecfr_fetch.py --title 21 --date 2024-07-01 --out ./ecfr_out
"""

import argparse
import re
import sys
import time
from pathlib import Path
from typing import Dict, List

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE = "https://www.ecfr.gov"

# ---- Session with robust retries/backoff ----
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "ecfr-fetch/1.1 (research; contact: you@example.com)"
})
retry = Retry(
    total=5,                # up to 5 attempts
    connect=5,
    read=5,
    backoff_factor=1.0,     # 0s, 1s, 2s, 4s, 8s
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "HEAD", "OPTIONS"],
    raise_on_status=False,
)
adapter = HTTPAdapter(max_retries=retry)
SESSION.mount("https://", adapter)
SESSION.mount("http://", adapter)

THROTTLE_SECONDS = 0.5  # be nice to the public service


def get_json(url: str) -> dict:
    time.sleep(THROTTLE_SECONDS)
    r = SESSION.get(url, timeout=60)
    r.raise_for_status()
    return r.json()


def get_text(url: str) -> str:
    time.sleep(THROTTLE_SECONDS)
    r = SESSION.get(url, timeout=180)
    r.raise_for_status()
    return r.text


def list_titles() -> List[Dict]:
    """Return the /titles listing with freshness dates."""
    url = f"{BASE}/api/versioner/v1/titles.json"
    data = get_json(url)
    titles = data.get("titles", [])
    meta = data.get("meta", {})
    print(f"Titles meta: date={meta.get('date')} import_in_progress={meta.get('import_in_progress')}")
    for t in titles:
        num = t.get("number")
        name = t.get("name")
        la = t.get("latest_amended_on")
        li = t.get("latest_issue_date")
        utd = t.get("up_to_date_as_of")
        reserved = t.get("reserved")
        print(f"Title {num:>2}: {name} "
              f"| latest_amended_on={la} latest_issue_date={li} up_to_date_as_of={utd} reserved={reserved}")
    return titles


def latest_version_date_for_title(title: int) -> str:
    """Find the newest version date for a title, with robust fallbacks."""
    # 1) Try the canonical versions endpoint
    url = f"{BASE}/api/versioner/v1/versions/title-{title}.json"
    try:
        data = get_json(url)
        versions = (data.get("versions")
                    or data.get("dates")
                    or data.get("version_dates")
                    or [])
        if versions:
            return max(versions)  # ISO dates compare lexicographically
    except Exception:
        pass  # fall through to the titles fallback

    # 2) Fallback to titles.json -> use up_to_date_as_of (or latest_issue_date)
    titles = get_json(f"{BASE}/api/versioner/v1/titles.json").get("titles", [])
    for t in titles:
        try:
            if int(t.get("number")) == int(title):
                return (t.get("up_to_date_as_of")
                        or t.get("latest_issue_date")
                        or t.get("latest_amended_on"))
        except Exception:
            continue

    raise RuntimeError(f"Could not determine a version date for title {title}")


def get_structure(title: int, date: str) -> dict:
    """Get the hierarchical structure for a title on a given date."""
    url = f"{BASE}/api/versioner/v1/structure/{date}/title-{title}.json"
    return get_json(url)


def iter_parts_from_structure(struct: dict) -> List[str]:
    """
    Walk the structure and collect all Part numbers (as strings).
    The structure tree varies by title, but parts are usually labeled like 'part': '600' etc.
    """
    parts = set()

    def walk(node: dict):
        node_type = (node.get("type") or "").lower()
        label = node.get("label") or ""
        if node_type == "part":
            ident = node.get("identifier") or ""
            m = re.search(r"Part\s+([0-9A-Za-z\-]+)", label, re.IGNORECASE)
            if ident:
                parts.add(str(ident))
            elif m:
                parts.add(m.group(1))
        for child in node.get("children", []):
            walk(child)

    if "nodes" in struct and isinstance(struct["nodes"], list):
        for n in struct["nodes"]:
            walk(n)
    else:
        walk(struct)

    return sorted(parts, key=lambda x: (len(x), x))


def download_part_xml(title: int, date: str, part_id: str) -> str:
    """
    Download 'full' XML for a single part:
      /api/versioner/v1/full/{date}/title-{title}.xml?part={part}
    """
    url = f"{BASE}/api/versioner/v1/full/{date}/title-{title}.xml?part={part_id}"
    return get_text(url)


def xml_to_plain_text(xml_text: str) -> str:
    """
    Convert the eCFR XHTML-ish XML into a simple plaintext rendering.
    Keeps headings and paragraphs, strips tags and most artifacts.
    """
    # Use lxml-xml for the BS4 tree builder (ensure 'lxml' is installed)
    soup = BeautifulSoup(xml_text, "lxml-xml")

    blocks = []
    for tag in soup.find_all([
        "HD", "HEAD", "HED",
        "DIV1", "DIV2", "DIV3", "DIV4", "DIV5", "DIV6", "DIV7", "DIV8",
        "SECTNO", "SUBJECT", "P", "FP"
    ]):
        text = tag.get_text(separator=" ", strip=True)
        if text:
            blocks.append(text)

    if not blocks:
        blocks = [soup.get_text(separator="\n", strip=True)]

    return "\n".join(blocks)


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def is_real_part(p: str) -> bool:
    """Filter out ranges like '83-98' and non-numeric identifiers for batch runs."""
    return p.isdigit()


def main():
    ap = argparse.ArgumentParser(description="Explore the eCFR API and fetch near-full content for a title/parts.")
    ap.add_argument("--list-titles", action="store_true", help="List all titles and freshness dates, then exit.")
    ap.add_argument("--title", type=int, help="CFR Title number (e.g., 40).")
    ap.add_argument("--parts", nargs="*", help="Part numbers to fetch (e.g., 50 600). If omitted, fetches ALL parts.")
    ap.add_argument("--date", type=str, help="Version date in YYYY-MM-DD. If omitted, uses latest.")
    ap.add_argument("--out", type=str, default="./ecfr_out", help="Output directory for XML and TXT.")
    args = ap.parse_args()

    if args.list_titles:
        list_titles()
        return

    if not args.title:
        print("Please provide --title N (or use --list-titles).", file=sys.stderr)
        sys.exit(2)

    # Determine date
    date = args.date or latest_version_date_for_title(args.title)

    # Get structure and decide which parts to fetch
    struct = get_structure(args.title, date)
    all_parts = iter_parts_from_structure(struct)
    if not all_parts:
        print("Warning: no parts found in structure; you can still fetch a whole title by iterating chapters/subparts with other filters.")
    if args.parts:
        target_parts = [str(p) for p in args.parts]
        missing = [p for p in target_parts if p not in all_parts]
        if missing:
            print(f"Note: requested parts not found in structure listing (might still exist): {missing}")
    else:
        target_parts = all_parts or []  # if empty, we'll skip

    # Drop ranges / non-numeric id entries (e.g., '83-98')
    before = len(target_parts)
    target_parts = [p for p in target_parts if is_real_part(p)]
    if len(target_parts) != before:
        print(f"Filtered out {before - len(target_parts)} non-numeric/range part id(s).")

    outdir = Path(args.out)
    ensure_dir(outdir)
    title_dir = outdir / f"title-{args.title}" / date
    ensure_dir(title_dir)

    print(f"Title {args.title} — version date {date}")
    print(f"Saving to: {title_dir.resolve()}")
    print(f"Parts to download: {'(none found)' if not target_parts else ', '.join(target_parts)}")

    total_downloaded = 0
    for part in target_parts:
        xml_path = title_dir / f"part-{part}.xml"
        txt_path = title_dir / f"part-{part}.txt"

        # Skip if exists
        if xml_path.exists() and txt_path.exists():
            print(f"[skip] part {part}: already downloaded")
            continue

        print(f"[get ] part {part} XML...")
        try:
            xml_text = download_part_xml(args.title, date, part)
        except requests.HTTPError as e:
            code = getattr(e.response, "status_code", "unknown")
            print(f"[warn] part {part}: HTTP error {code}")
            continue
        except Exception as e:
            print(f"[warn] part {part}: request failed ({e})")
            continue

        # Save XML
        xml_path.write_text(xml_text, encoding="utf-8")

        # Make a basic plaintext rendering for quick inspection
        try:
            txt = xml_to_plain_text(xml_text)
        except Exception as ex:
            print(f"[warn] part {part}: failed to convert XML to text ({ex}); saving raw only.")
            txt = ""
        if txt:
            txt_path.write_text(txt, encoding="utf-8")

        total_downloaded += 1
        print(f"[done] part {part}")

    # Also emit a quick index file of the structure (parts list)
    idx_path = title_dir / "_parts_index.txt"
    idx_path.write_text("\n".join(all_parts), encoding="utf-8")

    print(f"\nCompleted. Downloaded {total_downloaded} part(s).")
    print(f"Structure index: {idx_path.resolve()}")
    print("Tip: If you need *everything* in one go, you can also hit the 'full' endpoint without a part filter, "
          "but the payload can be extremely large; pulling per-part is friendlier.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user.", file=sys.stderr)
        sys.exit(130)
