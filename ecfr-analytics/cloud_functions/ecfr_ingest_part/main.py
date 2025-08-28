#!/usr/bin/env python3
"""
GCP Cloud Function for eCFR Part Ingestion
Processes a single CFR title/part combination for parallel execution
"""

import os
import json
import hashlib
import datetime
from typing import Any, Dict, List, Optional
import requests
from dateutil import parser as dtp
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import functions_framework
from lxml import etree
import re

# Configuration from environment
PROJECT_ID = os.getenv("PROJECT_ID", "lawscan")
DATASET = os.getenv("DATASET", "ecfr_enhanced") 
TABLE = os.getenv("TABLE", "sections_enhanced")
BASE_URL = "https://www.ecfr.gov/api"

# Initialize BigQuery client
client = bigquery.Client(project=PROJECT_ID)

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

def get_agency_from_title(title_num: int) -> str:
    """Get agency name based on title number"""
    # CFR Title to Agency mapping
    title_to_agency = {
        1: "National Archives and Records Administration",
        2: "Office of Management and Budget",
        3: "Executive Office of the President",
        4: "Government Accountability Office",
        5: "Office of Personnel Management",
        6: "Federal Retirement Thrift Investment Board",
        7: "Department of Agriculture",
        8: "Aliens and Nationality (DHS/DOJ)",
        9: "Animals and Animal Products (USDA)",
        10: "Department of Energy",
        11: "Federal Elections Commission",
        12: "Department of the Treasury",
        13: "Business Credit and Assistance (SBA)",
        14: "Department of Transportation",
        15: "Environmental Protection Agency",
        16: "Department of Commerce",
        17: "Commodity Futures Trading Commission",
        18: "Conservation of Power and Water Resources (FERC)",
        19: "Customs Duties (CBP)",
        20: "Food and Drug Administration",
        21: "Food and Drug Administration",
    }
    return title_to_agency.get(title_num, f"Title {title_num} Agency")

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
        title_num, part_data["identifier"], get_agency_from_title(title_num),
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
        "agency_name": get_agency_from_title(title_num),
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

def insert_to_bigquery(rows: List[Dict[str, Any]]) -> None:
    """Insert processed sections into BigQuery"""
    table_ref = client.dataset(DATASET).table(TABLE)
    
    # Configure load job
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    )
    
    # Insert rows
    job = client.load_table_from_json(rows, table_ref, job_config=job_config)
    job.result()  # Wait for completion
    
    print(f"✅ Inserted {len(rows)} sections to BigQuery")

@functions_framework.http
def ingest_part(request):
    """Cloud Function entry point for ingesting a specific CFR part"""
    
    # Parse request
    request_json = request.get_json(silent=True)
    if not request_json:
        return {"error": "No JSON payload provided"}, 400
    
    title_num = request_json.get("title")
    part_num = request_json.get("part")
    date = request_json.get("date", "2025-08-22")
    
    if not title_num or not part_num:
        return {"error": "title and part parameters required"}, 400
    
    try:
        print(f"🔄 Processing Title {title_num}, Part {part_num} for {date}")
        
        # Get part structure
        part_structure = get_part_structure(title_num, part_num, date)
        
        # Skip reserved parts
        if part_structure.get("reserved", False):
            print(f"⏭️ Skipping reserved part {part_num}")
            return {"message": f"Skipped reserved part {part_num}", "sections_processed": 0}
        
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
        
        # Insert to BigQuery
        if sections:
            insert_to_bigquery(sections)
        
        result = {
            "message": f"Successfully processed Title {title_num}, Part {part_num}",
            "sections_processed": len(sections),
            "date": date
        }
        
        print(f"✅ Completed: {result}")
        return result
        
    except Exception as e:
        error_msg = f"Error processing Title {title_num}, Part {part_num}: {str(e)}"
        print(f"❌ {error_msg}")
        return {"error": error_msg}, 500

if __name__ == "__main__":
    # Local testing
    import json
    from unittest.mock import Mock
    
    request = Mock()
    request.get_json.return_value = {"title": 7, "part": "1", "date": "2025-08-22"}
    
    result = ingest_part(request)
    print(json.dumps(result, indent=2))