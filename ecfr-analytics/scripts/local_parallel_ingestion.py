#!/usr/bin/env python3
"""
Local Parallel eCFR Ingestion
Runs parallel ingestion using multiprocessing instead of Cloud Functions
"""

import os
import json
import time
import argparse
import hashlib
import datetime
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Dict, Any, Optional
import requests
from google.cloud import bigquery
from dotenv import load_dotenv
from lxml import etree
import re

load_dotenv()

# Configuration
PROJECT_ID = os.getenv("PROJECT_ID", "lawscan")
DATASET = os.getenv("DATASET", "ecfr_enhanced") 
TABLE = os.getenv("TABLE", "sections_enhanced")
BASE_URL = "https://www.ecfr.gov/api"

def get_json(path: str, params: Optional[dict] = None, retries: int = 3, backoff: float = 1.5) -> Any:
    """HTTP request with retry logic"""
    import time
    url = path if path.startswith("http") else f"{BASE_URL}{path}"
    last = None
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=60)
            if r.status_code == 200:
                return r.json()
            last = (r.status_code, r.text[:500])
            time.sleep(backoff ** (i+1))
        except Exception as e:
            last = (0, str(e))
            time.sleep(backoff ** (i+1))
    
    raise RuntimeError(f"GET failed {url} -> {last}")

def get_part_structure(title: int, part: str, date: str) -> Dict[str, Any]:
    """Get structure for a specific part"""
    try:
        title_structure = get_json(f"/versioner/v1/structure/{date}/title-{title}.json")
        
        # Find the specific part in the title structure
        def find_part(node, target_part):
            if node.get("type") == "part" and node.get("identifier") == target_part:
                return node
            for child in node.get("children", []):
                result = find_part(child, target_part)
                if result:
                    return result
            return None
        
        part_node = find_part(title_structure, part)
        if not part_node:
            raise RuntimeError(f"Part {part} not found in Title {title}")
            
        return part_node
        
    except Exception as e:
        raise RuntimeError(f"Unable to fetch part structure for Title {title} Part {part}: {e}")

def get_section_content(title: int, part: str, section: str, date: str) -> Optional[str]:
    """Get full content for a specific section"""
    try:
        xml_data = requests.get(
            f"{BASE_URL}/versioner/v1/full/{date}/title-{title}.xml",
            params={"part": part, "section": section},
            timeout=120
        )
        if xml_data.status_code != 200:
            return None
            
        # Parse XML and extract text content
        root = etree.fromstring(xml_data.content)
        
        # Extract all text, removing XML tags
        text_content = etree.tostring(root, method="text", encoding="unicode")
        
        # Clean up whitespace
        cleaned_text = re.sub(r'\s+', ' ', text_content.strip())
        
        return cleaned_text
        
    except Exception as e:
        print(f"Warning: Could not fetch content for {title} CFR {part}.{section}: {e}")
        return None

def analyze_regulatory_content(text: str) -> Dict[str, Any]:
    """Analyze text for regulatory metrics"""
    if not text:
        return {
            "word_count": 0,
            "modal_obligation_terms_count": 0,
            "crossref_density_per_1k": 0.0,
            "prohibition_count": 0,
            "requirement_count": 0,
            "exception_count": 0,
            "sentence_count": 0,
            "avg_sentence_length": 0.0,
            "dollar_mentions": 0,
            "temporal_references": 0,
            "enforcement_terms": 0,
            "regulatory_burden_score": 0.0
        }
    
    words = text.split()
    word_count = len(words)
    
    # Modal obligation terms
    modal_terms = ["shall", "must", "will", "should", "may", "might", "could"]
    modal_count = sum(text.lower().count(term) for term in modal_terms)
    
    # Cross-references (CFR citations)
    cfr_refs = len(re.findall(r'\d+\s*CFR\s*\d+', text, re.IGNORECASE))
    crossref_density = (cfr_refs / max(word_count, 1)) * 1000
    
    # Regulatory language patterns
    prohibition_patterns = [r'\bprohibit\w*', r'\bforbid\w*', r'\bnot\s+permit\w*', r'\bshall\s+not\b']
    requirement_patterns = [r'\brequir\w*', r'\bmandator\w*', r'\bshall\b', r'\bmust\b']
    exception_patterns = [r'\bexcept\w*', r'\bunless\b', r'\bhowever\b', r'\bprovided\s+that\b']
    
    prohibition_count = sum(len(re.findall(pattern, text, re.IGNORECASE)) for pattern in prohibition_patterns)
    requirement_count = sum(len(re.findall(pattern, text, re.IGNORECASE)) for pattern in requirement_patterns)
    exception_count = sum(len(re.findall(pattern, text, re.IGNORECASE)) for pattern in exception_patterns)
    
    # Sentences
    sentences = re.split(r'[.!?]+', text)
    sentence_count = len([s for s in sentences if s.strip()])
    avg_sentence_length = word_count / max(sentence_count, 1)
    
    # Financial and temporal references
    dollar_mentions = len(re.findall(r'\$[\d,]+', text))
    temporal_patterns = [r'\bwithin\s+\d+\s+(days?|months?|years?)', r'\bafter\s+\d+\s+(days?|months?|years?)']
    temporal_references = sum(len(re.findall(pattern, text, re.IGNORECASE)) for pattern in temporal_patterns)
    
    # Enforcement terms
    enforcement_patterns = [r'\bpenalt\w*', r'\bfin\w*', r'\bviolat\w*', r'\benforc\w*', r'\bsanction\w*']
    enforcement_terms = sum(len(re.findall(pattern, text, re.IGNORECASE)) for pattern in enforcement_patterns)
    
    # Calculate regulatory burden score (0-100)
    burden_score = min(100.0, (
        (modal_count * 2) +
        (prohibition_count * 5) +
        (requirement_count * 3) +
        (enforcement_terms * 4) +
        (crossref_density * 0.5)
    ) / max(word_count / 100, 1))
    
    return {
        "word_count": word_count,
        "modal_obligation_terms_count": modal_count,
        "crossref_density_per_1k": crossref_density,
        "prohibition_count": prohibition_count,
        "requirement_count": requirement_count,
        "exception_count": exception_count,
        "sentence_count": sentence_count,
        "avg_sentence_length": avg_sentence_length,
        "dollar_mentions": dollar_mentions,
        "temporal_references": temporal_references,
        "enforcement_terms": enforcement_terms,
        "regulatory_burden_score": burden_score
    }

def create_ai_context_summary(section_citation: str, section_heading: str, section_text: str,
                             title_num: int, part_num: str, agency_name: str,
                             burden_score: float, obligations: int, prohibitions: int, requirements: int) -> str:
    """Create AI-optimized context summary"""
    risk_level = "High Risk" if burden_score > 50 else "Medium Risk" if burden_score > 25 else "Very Low Risk"
    
    summary = f"CFR Section: {section_citation} | Title: {title_num}, Part: {part_num} | Agency: {agency_name or 'N/A'} | "
    summary += f"Regulatory Risk: {risk_level} (Score: {burden_score:.1f}/100) | Heading: {section_heading} | "
    summary += f"Regulatory Complexity: {obligations} obligations, {prohibitions} prohibitions, {requirements} requirements"
    
    if section_text:
        # Add key enforcement terms if present
        enforcement_terms = []
        for term in ['penalty', 'fine', 'violat', 'enforce', 'sanction']:
            if term in section_text.lower():
                enforcement_terms.append(term)
        
        if enforcement_terms:
            summary += f" | Key Terms: {', '.join(enforcement_terms[:3])}"
        
        # Add truncated text
        summary += f" | Text: {section_text[:500]}{'...' if len(section_text) > 500 else ''}"
    
    return summary

def create_embedding_optimized_text(title_num: int, part_num: str, section_num: str, 
                                   section_heading: str, section_text: str) -> str:
    """Create embedding-optimized text"""
    optimized = f"Title {title_num} CFR Part {part_num} Section {section_num} "
    optimized += f"Subject: {section_heading} {section_text}"
    
    # Truncate for embedding limits (typical max ~8000 tokens)
    return optimized[:4000] if len(optimized) > 4000 else optimized

def process_section(section_node: Dict, title_data: Dict, part_data: Dict, 
                   title_num: int, date: str, snapshot_ts: str) -> Dict[str, Any]:
    """Process a single section into BigQuery format"""
    
    # Extract section information
    section_num = section_node.get("identifier", "")
    section_heading = section_node.get("label_description", "")
    section_citation = f"{title_num} CFR § {section_num}"
    
    # Get full section content
    section_text = get_section_content(title_num, part_data["identifier"], section_num, date) or ""
    
    # Analyze content
    metrics = analyze_regulatory_content(section_text)
    
    # Create hash for deduplication
    content_hash = hashlib.sha256(section_text.encode('utf-8')).hexdigest()
    normalized_text = section_text.lower().strip()
    
    # Create AI context
    ai_summary = create_ai_context_summary(
        section_citation, section_heading, section_text,
        title_num, part_data["identifier"], None,  # Agency name not available in structure
        metrics["regulatory_burden_score"], 
        metrics["modal_obligation_terms_count"],
        metrics["prohibition_count"], 
        metrics["requirement_count"]
    )
    
    embedding_text = create_embedding_optimized_text(
        title_num, part_data["identifier"], section_num, section_heading, section_text
    )
    
    return {
        "version_date": date,
        "snapshot_ts": snapshot_ts,
        "title_num": title_num,
        "title_name": title_data.get("label_description", f"Title {title_num}"),
        "chapter_id": None,
        "chapter_label": None,
        "subchapter_id": None,
        "subchapter_label": None,
        "part_num": part_data["identifier"],
        "part_label": part_data.get("label_description", ""),
        "subpart_id": None,
        "subpart_label": None,
        "section_num": section_num,
        "section_citation": section_citation,
        "section_heading": section_heading,
        "section_text": section_text,
        "reserved": section_node.get("reserved", False),
        "agency_name": None,
        "references": [],
        "authority_uscode": [],
        "part_order": 1,
        "section_order": 1,
        "word_count": metrics["word_count"],
        "modal_obligation_terms_count": metrics["modal_obligation_terms_count"],
        "crossref_density_per_1k": metrics["crossref_density_per_1k"],
        "section_hash": content_hash,
        "normalized_text": normalized_text,
        "raw_json": None,
        "prohibition_count": metrics["prohibition_count"],
        "requirement_count": metrics["requirement_count"],
        "exception_count": metrics["exception_count"],
        "sentence_count": metrics["sentence_count"],
        "avg_sentence_length": metrics["avg_sentence_length"],
        "dollar_mentions": metrics["dollar_mentions"],
        "temporal_references": metrics["temporal_references"],
        "enforcement_terms": metrics["enforcement_terms"],
        "regulatory_burden_score": metrics["regulatory_burden_score"],
        "ai_context_summary": ai_summary,
        "embedding_optimized_text": embedding_text
    }

def process_part_worker(args: tuple) -> Dict[str, Any]:
    """Worker function to process a single part (designed for multiprocessing)"""
    title_num, part_num, date = args
    
    try:
        print(f"🔄 Processing Title {title_num}, Part {part_num}")
        
        # Get part structure
        part_structure = get_part_structure(title_num, part_num, date)
        
        # Skip reserved parts
        if part_structure.get("reserved", False):
            print(f"⏭️ Skipping reserved part {part_num}")
            return {"title": title_num, "part": part_num, "status": "skipped", "sections_processed": 0}
        
        # Process all sections in this part
        sections = []
        snapshot_ts = datetime.datetime.utcnow().isoformat() + "+00:00"
        
        title_data = {"label_description": f"Title {title_num}"}
        
        for section_node in part_structure.get("children", []):
            if section_node.get("type") == "section":
                try:
                    section_data = process_section(
                        section_node, title_data, part_structure, title_num, date, snapshot_ts
                    )
                    sections.append(section_data)
                except Exception as e:
                    print(f"⚠️ Error processing section {section_node.get('identifier', 'unknown')}: {e}")
                    continue
        
        print(f"✅ Title {title_num}, Part {part_num}: {len(sections)} sections")
        
        return {
            "title": title_num,
            "part": part_num,
            "status": "success",
            "sections_processed": len(sections),
            "sections_data": sections
        }
        
    except Exception as e:
        error_msg = f"Error processing Title {title_num}, Part {part_num}: {str(e)}"
        print(f"❌ {error_msg}")
        return {"title": title_num, "part": part_num, "status": "error", "error": error_msg}

def get_parts_for_title(title_num: int, date: str = "2025-08-22") -> List[str]:
    """Get all parts for a given title from eCFR API"""
    print(f"🔍 Discovering parts for Title {title_num}...")
    
    try:
        api_url = f"https://www.ecfr.gov/api/versioner/v1/structure/{date}/title-{title_num}.json"
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        parts = []
        
        def extract_parts(node):
            if node.get("type") == "part" and not node.get("reserved", False):
                parts.append(node.get("identifier"))
            for child in node.get("children", []):
                extract_parts(child)
        
        extract_parts(data)
        
        print(f"✅ Found {len(parts)} parts for Title {title_num}")
        return sorted(parts, key=lambda x: int(x) if x.isdigit() else 999999)
        
    except Exception as e:
        print(f"❌ Error fetching parts for Title {title_num}: {e}")
        return []

def insert_to_bigquery_batch(all_sections: List[Dict[str, Any]], project_id: str, dataset: str, table: str, 
                             title_num: int = None, replace_title: bool = True) -> None:
    """Insert all processed sections into BigQuery in batches, with optional title replacement"""
    if not all_sections:
        return
        
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset).table(table)
    
    # If replace_title is True and we have a title_num, delete existing data for this title
    if replace_title and title_num:
        print(f"🗑️ Deleting existing data for Title {title_num} to avoid duplicates...")
        delete_query = f"""
        DELETE FROM `{project_id}.{dataset}.{table}` 
        WHERE title_num = {title_num}
        """
        delete_job = client.query(delete_query)
        delete_job.result()
        print(f"✅ Deleted existing data for Title {title_num}")
    
    print(f"📤 Inserting {len(all_sections)} sections to BigQuery...")
    
    # Configure load job
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    )
    
    # Insert in batches of 1000 rows
    batch_size = 1000
    for i in range(0, len(all_sections), batch_size):
        batch = all_sections[i:i + batch_size]
        job = client.load_table_from_json(batch, table_ref, job_config=job_config)
        job.result()  # Wait for completion
        print(f"✅ Inserted batch {i//batch_size + 1}: {len(batch)} sections")

def run_local_parallel_ingestion(title: int, date: str = "2025-08-22", max_workers: int = None, 
                                dry_run: bool = False) -> Dict[str, Any]:
    """Run parallel ingestion locally using multiprocessing"""
    
    if max_workers is None:
        max_workers = min(multiprocessing.cpu_count(), 20)  # Cap at 20 to be nice to the API
    
    print(f"🚀 Starting Local Parallel eCFR Ingestion")
    print(f"📋 Title: {title}, Date: {date}, Max Workers: {max_workers}")
    print(f"🔧 Dry Run: {dry_run}")
    print("=" * 60)
    
    start_time = time.time()
    
    # Get parts to process
    parts = get_parts_for_title(title, date)
    if not parts:
        print("❌ No parts found to process")
        return {"error": "No parts found"}
    
    print(f"📦 Will process {len(parts)} parts")
    
    # Create work items
    work_items = [(title, part, date) for part in parts]
    
    # Process parts in parallel
    results = []
    all_sections = []
    
    print(f"\n🔄 Processing {len(work_items)} parts with {max_workers} workers...")
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all work
        future_to_part = {executor.submit(process_part_worker, work_item): work_item[1] 
                         for work_item in work_items}
        
        # Process results as they complete
        for future in as_completed(future_to_part):
            part = future_to_part[future]
            try:
                result = future.result()
                results.append(result)
                
                # Collect sections for BigQuery insertion
                if result.get("status") == "success" and result.get("sections_data"):
                    all_sections.extend(result["sections_data"])
                    
            except Exception as e:
                print(f"❌ Exception processing part {part}: {e}")
                results.append({"title": title, "part": part, "status": "exception", "error": str(e)})
    
    end_time = time.time()
    
    # Insert to BigQuery unless dry run
    if not dry_run and all_sections:
        insert_to_bigquery_batch(all_sections, PROJECT_ID, DATASET, TABLE, title_num=title, replace_title=True)
    elif dry_run:
        print(f"🚫 Dry run - would have inserted {len(all_sections)} sections to BigQuery")
    
    # Analyze results
    successful = len([r for r in results if r.get("status") == "success"])
    failed = len([r for r in results if r.get("status") in ["error", "exception"]])
    skipped = len([r for r in results if r.get("status") == "skipped"])
    total_sections = sum(r.get("sections_processed", 0) for r in results if r.get("status") == "success")
    
    print(f"\n📊 LOCAL PARALLEL INGESTION SUMMARY")
    print(f"=" * 50)
    print(f"⏱️  Total Time: {end_time - start_time:.1f} seconds")
    print(f"🔄 Max Workers: {max_workers}")
    print(f"✅ Successful Parts: {successful}")
    print(f"❌ Failed Parts: {failed}")
    print(f"⏭️ Skipped Parts: {skipped}")
    print(f"📄 Total Sections: {total_sections:,}")
    print(f"⚡ Parts per Second: {len(parts) / (end_time - start_time):.1f}")
    print(f"📈 Sections per Second: {total_sections / (end_time - start_time):.1f}")
    
    if failed > 0:
        print(f"\n❌ Failed Parts:")
        for result in results:
            if result.get("status") in ["error", "exception"]:
                print(f"   - Part {result.get('part')}: {result.get('error', 'Unknown error')}")
    
    return {
        "title": title,
        "date": date,
        "total_time": end_time - start_time,
        "max_workers": max_workers,
        "parts_attempted": len(parts),
        "parts_successful": successful,
        "parts_failed": failed,
        "parts_skipped": skipped,
        "sections_ingested": total_sections,
        "dry_run": dry_run,
        "detailed_results": results
    }

def main():
    parser = argparse.ArgumentParser(description="Run parallel eCFR ingestion locally")
    parser.add_argument("--title", type=int, required=True, help="CFR Title number")
    parser.add_argument("--date", default="2025-08-22", help="Version date (YYYY-MM-DD)")
    parser.add_argument("--max-workers", type=int, help="Maximum worker processes (default: CPU count)")
    parser.add_argument("--dry-run", action="store_true", help="Don't insert to BigQuery, just test")
    parser.add_argument("--save-results", action="store_true", help="Save detailed results to JSON file")
    
    args = parser.parse_args()
    
    # Run the ingestion
    results = run_local_parallel_ingestion(
        title=args.title,
        date=args.date,
        max_workers=args.max_workers,
        dry_run=args.dry_run
    )
    
    # Save results if requested
    if args.save_results:
        filename = f"local_ingestion_results_title_{args.title}_{args.date}.json"
        with open(filename, "w") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"📁 Results saved to {filename}")
    
    return 0 if results.get("parts_failed", 1) == 0 else 1

if __name__ == "__main__":
    exit(main())