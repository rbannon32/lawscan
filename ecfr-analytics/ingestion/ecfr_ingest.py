#!/usr/bin/env python3
import argparse, json, os, re, sys, time, hashlib, datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
import requests
from dateutil import parser as dtp
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv
from lxml import etree

load_dotenv()

BASE = "https://www.ecfr.gov/api"

# ------------------------------ HTTP ------------------------------

def get_json(path: str, params: Optional[dict]=None, retries: int=3, backoff: float=1.5) -> Any:
    url = path if path.startswith("http") else f"{BASE}{path}"
    last = None
    for i in range(retries):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 200:
            try:
                return r.json()
            except Exception as e:
                raise RuntimeError(f"Invalid JSON from {url}: {e}")
        last = (r.status_code, r.text[:500])
        time.sleep(backoff ** (i+1))
    raise RuntimeError(f"GET failed {url} -> {last}")

# ------------------------------ Structure Discovery ------------------------------

def list_titles() -> List[Dict[str, Any]]:
    response = get_json("/versioner/v1/titles.json")
    return response.get("titles", [])

def get_title_structure(title: int, date: str) -> Dict[str, Any]:
    """Get title structure using the correct API format with date in path."""
    try:
        # Use the correct API format: /api/versioner/v1/structure/{date}/title-{title}.json
        return get_json(f"/versioner/v1/structure/{date}/title-{title}.json")
    except RuntimeError as e:
        print(f"  ! Structure API failed for Title {title} on {date}: {e}", file=sys.stderr)
        raise RuntimeError(f"Unable to fetch structure for Title {title} on {date}: {e}")

def _node_children(node: Any) -> Iterable[Any]:
    if isinstance(node, dict):
        for k in ("children", "content", "nodes", "subchapters", "subparts"):
            v = node.get(k)
            if isinstance(v, list):
                for c in v:
                    yield c
    elif isinstance(node, list):
        for c in node:
            yield c

def _node_type(node: Dict[str, Any]) -> str:
    for k in ("type", "node_type", "nodeType", "nodetype", "kind"):
        if isinstance(node, dict) and k in node and isinstance(node[k], str):
            return node[k].upper()
    # best-effort guess from identifier/label
    ident = str(node.get("identifier", "")).lower()
    label = str(node.get("label", "")).lower()
    if "section" in ident or label.startswith("§"):
        return "SECTION"
    if "part" in ident or label.startswith("part "):
        return "PART"
    if "chapter" in ident or label.startswith("chapter "):
        return "CHAPTER"
    if "subchapter" in ident or label.startswith("subchapter "):
        return "SUBCHAPTER"
    return ""

def _extract_num_from_identifier(identifier: str) -> Optional[str]:
    # common forms: "part-101", "section-101.9", "101.9"
    m = re.search(r'(\d+[A-Za-z0-9\.\-]*)', identifier or "")
    return m.group(1) if m else None

def enumerate_parts(struct: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Walk structure JSON and return a list of {part_num, part_label, chapter_label, subchapter_label, part_order}
    """
    parts = []
    order = 0

    def walk(node, chapter_label=None, subchapter_label=None):
        nonlocal order
        if not isinstance(node, dict):
            return
        ntype = _node_type(node)
        label = node.get("label") or node.get("label_text") or node.get("title") or ""
        identifier = node.get("identifier") or node.get("citation") or ""

        if ntype == "CHAPTER":
            chapter_label = label or chapter_label
        if ntype == "SUBCHAPTER":
            subchapter_label = label or subchapter_label

        if ntype == "PART":
            order += 1
            # Try to find a clean part number
            part_num = node.get("part", None)
            if not part_num:
                # from identifier or label (e.g., "part-101" or "Part 101 — Food Labeling")
                part_num = _extract_num_from_identifier(str(identifier)) or _extract_num_from_identifier(str(label))
            parts.append({
                "part_num": str(part_num) if part_num is not None else None,
                "part_label": label,
                "chapter_label": chapter_label,
                "subchapter_label": subchapter_label,
                "part_order": order
            })

        for c in _node_children(node):
            walk(c, chapter_label, subchapter_label)

    walk(struct)
    # Filter out any without a part number
    return [p for p in parts if p.get("part_num")]

# ------------------------------ Part Parsing ------------------------------

def get_part(title: int, part_num: str, date: str) -> Dict[str, Any]:
    """Get part content using the full XML endpoint and parsing it."""
    try:
        # Use the full XML endpoint: /api/versioner/v1/full/{date}/title-{title}.xml?part={part}
        xml_content = get_xml(f"/versioner/v1/full/{date}/title-{title}.xml", params={"part": part_num})
        return parse_part_xml(xml_content, part_num)
    except RuntimeError as e:
        print(f"  ! Full XML API failed for Title {title} Part {part_num}: {e}", file=sys.stderr)
        raise RuntimeError(f"Unable to fetch content for Title {title} Part {part_num} on {date}: {e}")

def get_xml(path: str, params: Optional[dict]=None, retries: int=3, backoff: float=1.5) -> str:
    """Get XML content from eCFR API."""
    url = path if path.startswith("http") else f"{BASE}{path}"
    last = None
    for i in range(retries):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 200:
            return r.text
        last = (r.status_code, r.text[:500])
        time.sleep(backoff ** (i+1))
    raise RuntimeError(f"GET failed {url} -> {last}")

def parse_part_xml(xml_content: str, part_num: str) -> Dict[str, Any]:
    """Parse XML content to extract sections and return as JSON structure."""
    try:
        # Parse the XML content
        root = etree.fromstring(xml_content.encode('utf-8'))
        
        # Create the part structure that mimics the old JSON format
        part_data = {
            "type": "part",
            "identifier": part_num,
            "label": f"Part {part_num}",
            "children": []
        }
        
        # Find all sections in the XML
        sections = []
        
        # Look for section elements - eCFR XML uses various tags
        # Common section patterns: <SECTION>, <DIV8> (sections), etc.
        section_elements = root.xpath(".//SECTION | .//DIV8[@N]")
        
        for section_elem in section_elements:
            section_data = parse_section_xml(section_elem, part_num)
            if section_data:
                sections.append(section_data)
        
        # If no sections found with standard tags, try alternative approaches
        if not sections:
            # Look for elements with section-like attributes or patterns
            potential_sections = root.xpath(".//*[@N and contains(@N, '.')]")
            for elem in potential_sections:
                section_data = parse_section_xml(elem, part_num)
                if section_data:
                    sections.append(section_data)
        
        part_data["children"] = sections
        return part_data
        
    except etree.XMLSyntaxError as e:
        print(f"  ! XML parsing failed for part {part_num}: {e}", file=sys.stderr)
        # Return minimal structure on parse failure
        return {"type": "part", "identifier": part_num, "children": []}
    except Exception as e:
        print(f"  ! Unexpected error parsing part {part_num}: {e}", file=sys.stderr)
        return {"type": "part", "identifier": part_num, "children": []}

def parse_section_xml(section_elem, part_num: str) -> Optional[Dict[str, Any]]:
    """Parse a single section element from XML."""
    try:
        # Extract section number from various possible attributes
        section_num = None
        for attr in ['N', 'SECTNO', 'number', 'id']:
            if section_elem.get(attr):
                section_num = section_elem.get(attr)
                break
        
        if not section_num:
            return None
            
        # Extract section heading/subject
        heading = ""
        subject_elem = section_elem.find(".//SUBJECT") or section_elem.find(".//HEAD")
        if subject_elem is not None and subject_elem.text:
            heading = subject_elem.text.strip()
        
        # Extract all text content from the section
        text_parts = []
        
        # Get all text content, excluding certain structural elements
        for elem in section_elem.iter():
            if elem.text and elem.tag not in ['SECTNO', 'SUBJECT', 'HEAD']:
                text = elem.text.strip()
                if text and text not in text_parts:
                    text_parts.append(text)
            if elem.tail:
                tail = elem.tail.strip()
                if tail and tail not in text_parts:
                    text_parts.append(tail)
        
        # Join all text content
        full_text = ' '.join(text_parts).strip()
        
        # Create section structure matching expected format
        section_data = {
            "type": "section",
            "identifier": f"section-{section_num}",
            "label": f"§ {section_num} {heading}".strip(),
            "subject": heading,
            "text": full_text,
            "section_num": section_num,
            "reserved": "reserved" in full_text.lower()
        }
        
        return section_data
        
    except Exception as e:
        print(f"  ! Error parsing section in part {part_num}: {e}", file=sys.stderr)
        return None
        
def check_api_availability() -> None:
    """Check if the eCFR API is accessible and print helpful diagnostics."""
    print("Checking eCFR API availability...", file=sys.stderr)
    try:
        titles = list_titles()
        print(f"✓ Successfully fetched {len(titles)} titles from API", file=sys.stderr)
    except Exception as e:
        print(f"✗ Failed to fetch titles list: {e}", file=sys.stderr)
        print("  This suggests the eCFR API may be down or blocked", file=sys.stderr)
        raise

def _collect_strings(obj: Any, acc: List[str]) -> None:
    """
    Depth-first text collector: pulls human-visible strings from common keys.
    """
    if obj is None:
        return
    if isinstance(obj, str):
        s = obj.strip()
        if s:
            acc.append(s)
        return
    if isinstance(obj, list):
        for x in obj:
            _collect_strings(x, acc); 
        return
    if isinstance(obj, dict):
        # prefer common content keys
        for k in ("text", "content_text", "P", "paragraph", "subject", "title"):
            if k in obj and isinstance(obj[k], str):
                _collect_strings(obj[k], acc)
        # generic scan of child containers
        for k in ("content", "children", "subsections", "paragraphs", "notes"):
            v = obj.get(k)
            if v is not None:
                _collect_strings(v, acc)
        return

def _is_section_node(node: Dict[str, Any]) -> bool:
    ntype = _node_type(node)
    if ntype == "SECTION":
        return True
    # Heuristics: label starting with "§", or identifier like "section-101.9"
    lbl = str(node.get("label", ""))
    ident = str(node.get("identifier", ""))
    return lbl.strip().startswith("§") or ident.startswith("section-")

def _section_citation_fields(node: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    # Try to extract section number and heading
    label = str(node.get("label") or "")
    title = str(node.get("title") or node.get("subject") or "")
    # e.g., "§ 101.9 Nutrition labeling of food."
    m = re.match(r'^\s*§\s*([0-9A-Za-z\.\-]+)\s*(.*)$', label)
    sec_num = m.group(1) if m else _extract_num_from_identifier(node.get("identifier", ""))
    heading = title or (m.group(2).strip() if m and m.group(2) else "")
    return sec_num, heading or None

def _normalize_text(s: str) -> str:
    return re.sub(r'\s+', ' ', s.lower()).strip()

def _word_count(s: str) -> int:
    if not s:
        return 0
    s2 = re.sub(r'[^a-z0-9]+', ' ', s.lower())
    toks = [t for t in s2.split(' ') if t]
    return len(toks)

def _regex_count(pattern: str, s: str) -> int:
    return len(re.findall(pattern, s, flags=re.IGNORECASE))

def rows_for_part(part_json: Dict[str, Any], meta: Dict[str, Any], title_num: int, title_name: str, version_date: str, snapshot_ts: str) -> Iterable[Dict[str, Any]]:
    order = 0
    rows = []

    def walk(node):
        nonlocal order
        if not isinstance(node, dict):
            return
        
        # Check if this is a section node (from XML parsing or old JSON)
        if node.get("type") == "section" or _is_section_node(node):
            order += 1
            
            # Handle XML-parsed sections
            if "section_num" in node:
                sec_num = node["section_num"]
                heading = node.get("subject", "")
                section_text = node.get("text", "")
                reserved = node.get("reserved", False)
            else:
                # Handle old JSON format sections
                sec_num, heading = _section_citation_fields(node)
                chunks: List[str] = []
                _collect_strings(node, chunks)
                section_text = "\n".join([c for c in chunks if not c.strip().startswith("§")])
                reserved = bool(re.search(r'\[?\s*reserved\s*\]?', section_text, flags=re.IGNORECASE))
            
            # Clean up the text and compute enhanced metrics
            if section_text:
                normalized = _normalize_text(section_text)
                section_hash = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
                wc = _word_count(section_text)
                
                # Regulatory burden metrics
                oblig = _regex_count(r'\b(shall|must|may not)\b', section_text)
                prohibitions = _regex_count(r'\b(prohibited|forbidden|banned|not permitted)\b', section_text)
                requirements = _regex_count(r'\b(required|mandatory|necessary|obligated)\b', section_text)
                exceptions = _regex_count(r'\b(except|unless|provided that|however)\b', section_text)
                
                # Legal reference density
                xref = _regex_count(r'§|\bCFR\b|\bU\.S\.C\.\b|\bUSC\b', section_text)
                density = (xref * 1000.0 / wc) if wc else 0.0
                
                # Complexity indicators
                sentences = len([s for s in re.split(r'[.!?]+', section_text) if s.strip()])
                avg_sentence_length = wc / sentences if sentences > 0 else 0
                
                # Dollar amounts mentioned (regulatory cost indicators)
                dollar_mentions = _regex_count(r'\$[\d,]+', section_text)
                
                # Time-sensitive language (deadlines, periods)
                temporal_refs = _regex_count(r'\b(\d+\s+(day|week|month|year)s?|within\s+\d+|before\s+\d+|after\s+\d+)\b', section_text)
                
                # Enforcement language
                enforcement = _regex_count(r'\b(penalty|fine|violation|enforcement|liable|subject to)\b', section_text)
                
                # Custom regulatory burden score (0-100)
                burden_score = min(100.0, (oblig + prohibitions + requirements) * 10.0 / max(1, wc / 100))
                
            else:
                normalized = ""
                section_hash = hashlib.sha256("".encode("utf-8")).hexdigest()
                wc = 0
                oblig = 0
                prohibitions = 0
                requirements = 0
                exceptions = 0
                xref = 0
                density = 0.0
                sentences = 0
                avg_sentence_length = 0.0
                dollar_mentions = 0
                temporal_refs = 0
                enforcement = 0
                burden_score = 0.0

            rows.append({
                "version_date": version_date,
                "snapshot_ts": snapshot_ts,
                "title_num": title_num,
                "title_name": title_name,
                "chapter_id": None,
                "chapter_label": meta.get("chapter_label"),
                "subchapter_id": None,
                "subchapter_label": meta.get("subchapter_label"),
                "part_num": meta.get("part_num"),
                "part_label": meta.get("part_label"),
                "subpart_id": None,
                "subpart_label": None,
                "section_num": sec_num,
                "section_citation": f"{title_num} CFR § {sec_num}" if sec_num else None,
                "section_heading": heading,
                "section_text": section_text,
                "reserved": reserved,
                "agency_name": _extract_agency(meta.get("chapter_label")),
                "references": [],
                "authority_uscode": [],
                "part_order": meta.get("part_order"),
                "section_order": order,
                "word_count": wc,
                "modal_obligation_terms_count": oblig,
                "crossref_density_per_1k": density,
                "section_hash": section_hash,
                "normalized_text": normalized,
                "raw_json": None,
                # Enhanced custom metrics for regulatory analysis
                "prohibition_count": prohibitions,
                "requirement_count": requirements,
                "exception_count": exceptions,
                "sentence_count": sentences,
                "avg_sentence_length": avg_sentence_length,
                "dollar_mentions": dollar_mentions,
                "temporal_references": temporal_refs,
                "enforcement_terms": enforcement,
                "regulatory_burden_score": burden_score,
            })
        
        # Recurse through children
        children = node.get("children", [])
        if isinstance(children, list):
            for child in children:
                walk(child)
        else:
            # Handle old format with various child keys
            for c in _node_children(node):
                walk(c)

    walk(part_json)
    return rows

def _extract_agency(chapter_label: Optional[str]) -> Optional[str]:
    if not chapter_label:
        return None
    # Examples: "CHAPTER I—FOOD AND DRUG ADMINISTRATION, DEPARTMENT OF HEALTH AND HUMAN SERVICES"
    # Keep the portion before the first comma for readability
    s = re.sub(r'CHAPTER\s+[IVXLC]+\s*—\s*', '', chapter_label or '', flags=re.IGNORECASE).strip()
    s = s.split(",")[0].strip() if "," in s else s
    return s or chapter_label

# ------------------------------ Historical Backfill ------------------------------

def generate_monthly_dates(start_date: str, end_date: str) -> List[str]:
    """Generate list of month-end dates for backfill."""
    start = dtp.parse(start_date).date()
    end = dtp.parse(end_date).date()
    
    dates = []
    current = start
    
    while current <= end:
        # Use last day of month for consistency
        next_month = current.replace(day=1) + relativedelta(months=1)
        month_end = next_month - datetime.timedelta(days=1)
        dates.append(month_end.strftime("%Y-%m-%d"))
        current = next_month
    
    return dates

def should_skip_title(title_num: int, target_date: str, titles_meta: List[Dict[str, Any]], smart_skip: bool = True) -> bool:
    """Determine if we should skip a title for a given date based on amendment history."""
    if not smart_skip:
        return False
        
    title_info = next((t for t in titles_meta if t.get("number") == title_num), None)
    if not title_info:
        return False
    
    latest_amended = title_info.get("latest_amended_on")
    if not latest_amended:
        return False
    
    try:
        amended_date = dtp.parse(latest_amended).date()
        target_date_obj = dtp.parse(target_date).date()
        
        # Skip if title was last amended before our target date by more than 1 month
        # This means no substantive changes happened in this month
        one_month_ago = target_date_obj - relativedelta(months=1)
        return amended_date < one_month_ago
        
    except Exception:
        # If we can't parse dates, don't skip
        return False

def run_backfill(args) -> int:
    """Run historical backfill for multiple dates."""
    start_date = args.start_date
    end_date = args.end_date or datetime.date.today().strftime("%Y-%m-%d")
    
    print(f"Starting monthly backfill from {start_date} to {end_date}", file=sys.stderr)
    
    # Generate monthly dates
    monthly_dates = generate_monthly_dates(start_date, end_date)
    print(f"Generated {len(monthly_dates)} monthly dates for backfill", file=sys.stderr)
    
    # Check API availability once
    check_api_availability()
    
    # Get titles metadata once (use current info for smart skipping)
    titles_meta = list_titles()
    title_lookup = {int(t["number"]): t.get("title", f"Title {t['number']}") for t in titles_meta if "number" in t}
    requested_titles = args.titles or [i for i in range(1, 51)]
    
    # Initialize BigQuery if needed
    if args.bigquery:
        client = setup_bigquery_client()
        dataset_id = args.dataset
        table_name = args.table
        table_id = f"{client.project}.{dataset_id}.{table_name}"
        
        ensure_dataset_exists(client, dataset_id)
        ensure_table_exists(client, table_id)
        print(f"BigQuery setup complete: {table_id}", file=sys.stderr)
    
    total_processed = 0
    
    for date in tqdm(monthly_dates, desc="Monthly backfill"):
        snapshot_ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
        batch_rows = [] if args.bigquery else None
        out_file = None
        
        # Setup output for this month
        if not args.bigquery:
            os.makedirs(args.out, exist_ok=True)
            out_path = os.path.join(args.out, f"ecfr_sections_{date}.ndjson")
            out_file = open(out_path, "w", encoding="utf-8")
        
        month_processed = 0
        
        try:
            for title_num in requested_titles:
                # Smart skipping logic
                if should_skip_title(title_num, date, titles_meta, args.smart_skip):
                    print(f"  Skipping Title {title_num} for {date} (no changes)", file=sys.stderr)
                    continue
                
                title_name = title_lookup.get(title_num, f"Title {title_num}")
                
                try:
                    # Get structure for this title at this date
                    struct = get_title_structure(title_num, date)
                    parts = enumerate_parts(struct)
                    
                    if not parts:
                        continue
                        
                    for meta in parts:
                        part_num = meta["part_num"]
                        try:
                            pj = get_part(title_num, part_num, date)
                            rows = rows_for_part(pj, {**meta, "part_num": part_num}, title_num, title_name, date, snapshot_ts)
                            
                            if args.bigquery:
                                batch_rows.extend(rows)
                                if len(batch_rows) >= args.batch_size:
                                    load_data_to_bigquery(client, table_id, batch_rows)
                                    total_processed += len(batch_rows)
                                    batch_rows = []
                            else:
                                for r in rows:
                                    out_file.write(json.dumps(r, ensure_ascii=False) + "\n")
                                month_processed += len(rows)
                                
                        except Exception as e:
                            print(f"    ! Error processing Title {title_num} Part {part_num} on {date}: {e}", file=sys.stderr)
                            continue
                            
                except Exception as e:
                    print(f"  ! Error processing Title {title_num} on {date}: {e}", file=sys.stderr)
                    continue
                    
            # Load remaining rows for this month
            if args.bigquery and batch_rows:
                load_data_to_bigquery(client, table_id, batch_rows)
                total_processed += len(batch_rows)
                
        finally:
            if out_file:
                out_file.close()
                print(f"Wrote {month_processed} rows to {out_path}", file=sys.stderr)
                
    # Create rollup tables if requested
    if args.bigquery and args.create_rollups:
        create_rollup_tables(client, dataset_id)
        
    print(f"Backfill complete! Processed {total_processed} total rows across {len(monthly_dates)} months", file=sys.stderr)
    return 0

# ------------------------------ BigQuery ------------------------------

def setup_bigquery_client() -> bigquery.Client:
    """Initialize BigQuery client with project from environment or metadata."""
    project_id = os.getenv("PROJECT_ID") or os.getenv("GCP_PROJECT")
    if project_id:
        return bigquery.Client(project=project_id)
    # Let client auto-detect from environment/metadata
    return bigquery.Client()

def ensure_dataset_exists(client: bigquery.Client, dataset_id: str) -> None:
    """Create dataset if it doesn't exist."""
    try:
        client.get_dataset(dataset_id)
        print(f"Dataset {dataset_id} already exists", file=sys.stderr)
    except NotFound:
        print(f"Creating dataset {dataset_id}", file=sys.stderr)
        dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")
        dataset.location = "US"  # Match the schema file location
        client.create_dataset(dataset)

def ensure_table_exists(client: bigquery.Client, table_id: str) -> None:
    """Create sections table if it doesn't exist."""
    try:
        client.get_table(table_id)
        print(f"Table {table_id} already exists", file=sys.stderr)
    except NotFound:
        print(f"Creating table {table_id}", file=sys.stderr)
        
        # Define the enhanced schema with custom regulatory metrics
        schema = [
            bigquery.SchemaField("version_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("snapshot_ts", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("title_num", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("title_name", "STRING"),
            bigquery.SchemaField("chapter_id", "STRING"),
            bigquery.SchemaField("chapter_label", "STRING"),
            bigquery.SchemaField("subchapter_id", "STRING"),
            bigquery.SchemaField("subchapter_label", "STRING"),
            bigquery.SchemaField("part_num", "STRING"),
            bigquery.SchemaField("part_label", "STRING"),
            bigquery.SchemaField("subpart_id", "STRING"),
            bigquery.SchemaField("subpart_label", "STRING"),
            bigquery.SchemaField("section_num", "STRING"),
            bigquery.SchemaField("section_citation", "STRING"),
            bigquery.SchemaField("section_heading", "STRING"),
            bigquery.SchemaField("section_text", "STRING"),
            bigquery.SchemaField("reserved", "BOOLEAN"),
            bigquery.SchemaField("agency_name", "STRING"),
            bigquery.SchemaField("references", "STRING", mode="REPEATED"),
            bigquery.SchemaField("authority_uscode", "STRING", mode="REPEATED"),
            bigquery.SchemaField("part_order", "INTEGER"),
            bigquery.SchemaField("section_order", "INTEGER"),
            bigquery.SchemaField("word_count", "INTEGER"),
            bigquery.SchemaField("modal_obligation_terms_count", "INTEGER"),
            bigquery.SchemaField("crossref_density_per_1k", "FLOAT"),
            bigquery.SchemaField("section_hash", "STRING"),
            bigquery.SchemaField("normalized_text", "STRING"),
            bigquery.SchemaField("raw_json", "JSON"),
            # Enhanced custom metrics for regulatory decision-making
            bigquery.SchemaField("prohibition_count", "INTEGER"),
            bigquery.SchemaField("requirement_count", "INTEGER"),
            bigquery.SchemaField("exception_count", "INTEGER"),
            bigquery.SchemaField("sentence_count", "INTEGER"),
            bigquery.SchemaField("avg_sentence_length", "FLOAT"),
            bigquery.SchemaField("dollar_mentions", "INTEGER"),
            bigquery.SchemaField("temporal_references", "INTEGER"),
            bigquery.SchemaField("enforcement_terms", "INTEGER"),
            bigquery.SchemaField("regulatory_burden_score", "FLOAT"),
        ]
        
        table = bigquery.Table(table_id, schema=schema)
        
        # Set partitioning and clustering to match schema
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="version_date"
        )
        table.clustering_fields = ["title_num", "agency_name", "part_num"]
        
        client.create_table(table)

def create_rollup_tables(client: bigquery.Client, dataset_id: str) -> None:
    """Create the rollup tables (parts_daily and agency_daily)."""
    # Read and execute the views.sql content
    views_sql = """
-- Per-part daily rollup (deterministic content hash by ordered child hashes)
CREATE OR REPLACE TABLE {dataset}.parts_daily AS
SELECT
  version_date,
  ANY_VALUE(snapshot_ts) AS snapshot_ts,
  title_num, ANY_VALUE(title_name) AS title_name,
  ANY_VALUE(chapter_label) AS chapter_label,
  ANY_VALUE(subchapter_label) AS subchapter_label,
  part_num, ANY_VALUE(part_label) AS part_label,
  ANY_VALUE(agency_name) AS agency_name,
  SUM(word_count) AS part_word_count,
  TO_HEX(SHA256(STRING_AGG(section_hash, '' ORDER BY section_citation))) AS part_hash
FROM {dataset}.sections
GROUP BY version_date, title_num, part_num;

-- Per-agency daily rollup
CREATE OR REPLACE TABLE {dataset}.agency_daily AS
SELECT
  version_date,
  agency_name,
  ANY_VALUE(snapshot_ts) AS snapshot_ts,
  SUM(part_word_count) AS agency_word_count,
  TO_HEX(SHA256(STRING_AGG(part_hash, '' ORDER BY title_num, part_num))) AS agency_hash
FROM {dataset}.parts_daily
GROUP BY version_date, agency_name;
    """.format(dataset=dataset_id)
    
    print(f"Creating rollup tables in {dataset_id}", file=sys.stderr)
    queries = [q.strip() for q in views_sql.split(';') if q.strip()]
    
    for query in queries:
        if query:
            job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
            client.query(query, job_config=job_config).result()

def load_data_to_bigquery(client: bigquery.Client, table_id: str, rows: List[Dict[str, Any]]) -> None:
    """Load data directly to BigQuery without creating intermediate files."""
    if not rows:
        return
        
    print(f"Loading {len(rows)} rows to {table_id}", file=sys.stderr)
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    
    # Convert rows to NDJSON string
    ndjson_data = '\n'.join(json.dumps(row, ensure_ascii=False) for row in rows)
    
    job = client.load_table_from_json(
        [json.loads(line) for line in ndjson_data.split('\n')],
        table_id,
        job_config=job_config
    )
    
    job.result()  # Wait for completion
    
    if job.errors:
        raise RuntimeError(f"BigQuery load failed: {job.errors}")

# ------------------------------ Main ------------------------------

def main():
    ap = argparse.ArgumentParser(description="eCFR → NDJSON/BigQuery (section grain)")
    ap.add_argument("--date", help="Point-in-time date (YYYY-MM-DD, required unless using --backfill)")
    ap.add_argument("--titles", nargs="+", type=int, help="Title numbers to ingest (default: all 1..50)")
    ap.add_argument("--out", default="./data", help="Output directory (for NDJSON mode)")
    ap.add_argument("--bigquery", action="store_true", help="Load directly to BigQuery instead of NDJSON files")
    ap.add_argument("--dataset", default="ecfr", help="BigQuery dataset name (default: ecfr)")
    ap.add_argument("--table", default="sections", help="BigQuery table name (default: sections)")
    ap.add_argument("--create-rollups", action="store_true", help="Create rollup tables after loading")
    ap.add_argument("--batch-size", type=int, default=1000, help="Batch size for BQ loading (default: 1000)")
    ap.add_argument("--backfill", action="store_true", help="Run monthly backfill from 2017 to present")
    ap.add_argument("--start-date", default="2017-01-31", help="Start date for backfill (YYYY-MM-DD, default: 2017-01-31)")
    ap.add_argument("--end-date", help="End date for backfill (YYYY-MM-DD, default: today)")
    ap.add_argument("--smart-skip", action="store_true", help="Skip titles that haven't changed (use with backfill)")
    args = ap.parse_args()

    # Handle backfill mode
    if args.backfill:
        return run_backfill(args)

    # Validate date for single-date mode
    if not args.date:
        print("Error: --date is required unless using --backfill", file=sys.stderr); sys.exit(2)
        
    try:
        dtp.parse(args.date)
    except Exception:
        print("Invalid --date; expected YYYY-MM-DD", file=sys.stderr); sys.exit(2)

    # Check API availability first
    check_api_availability()
    
    titles_meta = list_titles()
    title_lookup = {int(t["number"]): t.get("title", f"Title {t['number']}") for t in titles_meta if "number" in t}
    titles = args.titles or [i for i in range(1, 51)]
    snapshot_ts = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # Initialize BigQuery if needed
    if args.bigquery:
        client = setup_bigquery_client()
        dataset_id = args.dataset
        table_name = args.table
        table_id = f"{client.project}.{dataset_id}.{table_name}"
        
        # Ensure dataset and table exist
        ensure_dataset_exists(client, dataset_id)
        ensure_table_exists(client, table_id)
        
        print(f"Loading data to BigQuery table: {table_id}", file=sys.stderr)
        batch_rows = []
    else:
        # Traditional NDJSON file output
        os.makedirs(args.out, exist_ok=True)
        out_path = os.path.join(args.out, f"ecfr_sections_{args.date}.ndjson")
        print(f"Writing NDJSON to: {out_path}", file=sys.stderr)
        out_file = open(out_path, "w", encoding="utf-8")

    try:
        for t in titles:
            title_name = title_lookup.get(t, f"Title {t}")
            print(f"Discovering parts for Title {t} ({title_name}) @ {args.date} ...", file=sys.stderr)
            struct = get_title_structure(t, args.date)
            parts = enumerate_parts(struct)
            if not parts:
                print(f"  (no parts discovered for Title {t})", file=sys.stderr)
                continue

            for meta in tqdm(parts, desc=f"T{t} parts"):
                part_num = meta["part_num"]
                try:
                    pj = get_part(t, part_num, args.date)
                except Exception as e:
                    print(f"    ! skip part {t}/{part_num}: {e}", file=sys.stderr)
                    continue

                rows = rows_for_part(pj, {**meta, "part_num": part_num}, t, title_name, args.date, snapshot_ts)
                
                if args.bigquery:
                    # Batch rows for BigQuery loading
                    batch_rows.extend(rows)
                    if len(batch_rows) >= args.batch_size:
                        load_data_to_bigquery(client, table_id, batch_rows)
                        batch_rows = []
                else:
                    # Write to NDJSON file
                    for r in rows:
                        out_file.write(json.dumps(r, ensure_ascii=False) + "\n")

        # Load any remaining rows
        if args.bigquery and batch_rows:
            load_data_to_bigquery(client, table_id, batch_rows)
            
        # Create rollup tables if requested
        if args.bigquery and args.create_rollups:
            create_rollup_tables(client, dataset_id)
            
    finally:
        if not args.bigquery:
            out_file.close()
            print(f"Wrote {out_path}")
        else:
            print(f"Data loading complete to {table_id}")

    return 0

if __name__ == "__main__":
    sys.exit(main())
