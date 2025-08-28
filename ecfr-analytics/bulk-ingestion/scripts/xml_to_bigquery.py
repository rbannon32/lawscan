#!/usr/bin/env python3
"""
Bulk XML to BigQuery Parser
Processes local GovInfo XML files and populates BigQuery sections_enhanced table
Bypasses eCFR API rate limits by using local XML files directly
"""

import os
import sys
import time
import hashlib
import datetime
from typing import Any, Dict, List, Optional
import logging
from lxml import etree
import json
import re
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Configure logging  
log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'xml_to_bigquery.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.getenv("PROJECT_ID", "lawscan")
DATASET = os.getenv("DATASET", "ecfr_enhanced") 
TABLE = os.getenv("TABLE", "sections_enhanced")
LOCAL_DATA_DIR = "../data"  # XML files location
DATE = datetime.datetime.now().strftime("%Y-%m-%d")

# Initialize BigQuery client
client = bigquery.Client(project=PROJECT_ID)

def get_agency_from_title(title_num: int) -> str:
    """Get agency name based on title number"""
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
        22: "Foreign Relations (State Department)",
        23: "Highways (DOT)",
        24: "Housing and Urban Development",
        25: "Indians (Bureau of Indian Affairs)",
        26: "Internal Revenue Service",
        27: "Alcohol, Tobacco, Firearms and Explosives",
        28: "Judicial Administration (DOJ)",
        29: "Labor Standards (DOL)",
        30: "Mineral Resources (Interior)",
        31: "Money and Finance (Treasury)",
        32: "National Defense (DOD)",
        33: "Navigation and Navigable Waters (Coast Guard)",
        34: "Education (Department of Education)",
        36: "Parks, Forests, and Public Property (Interior)",
        37: "Patents, Trademarks, and Copyrights (Commerce)",
        38: "Pensions, Bonuses, and Veterans' Relief (VA)",
        39: "Postal Service",
        40: "Environmental Protection Agency",
        41: "Public Contracts and Property Management (GSA)",
        42: "Public Health and Welfare (HHS)",
        43: "Public Lands (Interior)",
        44: "Emergency Management and Assistance (FEMA)",
        45: "Public Welfare (HHS)",
        46: "Shipping (DOT)",
        47: "Telecommunication (FCC)",
        48: "Federal Acquisition Regulation (GSA)",
        49: "Transportation (DOT)",
        50: "Wildlife and Fisheries (Interior)"
    }
    return title_to_agency.get(title_num, f"Title {title_num} Agency")

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
    risk_level = "High Risk" if burden_score > 50 else "Medium Risk" if burden_score > 25 else "Low Risk"
    
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

def extract_hierarchy_info(section_node) -> Dict[str, str]:
    """Extract hierarchical information from section node"""
    hierarchy = {
        "chapter_id": None,
        "chapter_label": None,
        "subchapter_id": None, 
        "subchapter_label": None,
        "subpart_id": None,
        "subpart_label": None
    }
    
    # Walk up parent nodes to find hierarchy
    parent = section_node.getparent()
    while parent is not None:
        tag = parent.tag.upper()
        
        if tag in ['DIV5', 'CHAPTER']:  # Chapter level
            hierarchy["chapter_id"] = parent.get('N') or parent.get('identifier')
            hierarchy["chapter_label"] = parent.get('label_description')
        elif tag in ['DIV4', 'SUBCHAPTER']:  # Subchapter level
            hierarchy["subchapter_id"] = parent.get('N') or parent.get('identifier')
            hierarchy["subchapter_label"] = parent.get('label_description')
        elif tag in ['DIV7', 'SUBPART']:  # Subpart level
            hierarchy["subpart_id"] = parent.get('N') or parent.get('identifier')
            hierarchy["subpart_label"] = parent.get('label_description')
        
        parent = parent.getparent()
    
    return hierarchy

def extract_section_from_xml(section_node, title_num: int, title_name: str, snapshot_ts: str, date: str) -> Optional[Dict[str, Any]]:
    """Extract a single section from XML node and format for BigQuery"""
    
    try:
        # Get section identifier (GovInfo format uses N attribute)
        section_id = section_node.get('N') or section_node.get('identifier') or "unknown"
        
        # Extract section number from N attribute (e.g., "§ 100.1" -> "100.1")
        section_num = section_id
        if section_id.startswith('§ '):
            section_num = section_id[2:]
        
        # Extract heading
        heading_elements = section_node.xpath('.//HEAD | .//head')
        heading = ""
        if heading_elements:
            heading = etree.tostring(heading_elements[0], method="text", encoding="unicode").strip()
        
        # Extract all text content
        all_text = etree.tostring(section_node, method="text", encoding="unicode")
        cleaned_text = re.sub(r'\s+', ' ', all_text.strip()) if all_text else ""
        
        if not cleaned_text or len(cleaned_text.strip()) < 10:
            return None
        
        # Extract part number from NODE attribute or parent
        part_num = "unknown"
        node_attr = section_node.get('NODE')
        if node_attr:
            # Parse NODE to extract part (e.g., "3:1.0.1.1.1.0.1.1")
            parts = node_attr.split(':')
            if len(parts) > 1:
                node_parts = parts[1].split('.')
                if len(node_parts) > 2:
                    part_num = node_parts[2]
        
        # Fallback: check parent elements
        if part_num == "unknown":
            parent = section_node.getparent()
            while parent is not None:
                if parent.tag in ['DIV6', 'PART']:
                    part_num = parent.get('N') or parent.get('identifier', 'unknown')
                    if part_num.startswith('Part '):
                        part_num = part_num[5:]  # Remove "Part " prefix
                    break
                parent = parent.getparent()
        
        # Get hierarchical information
        hierarchy = extract_hierarchy_info(section_node)
        
        # Analyze content
        metrics = analyze_regulatory_content(cleaned_text)
        
        # Create hash for deduplication
        content_hash = hashlib.sha256(cleaned_text.encode('utf-8')).hexdigest()
        normalized_text = cleaned_text.lower().strip()
        
        # Create section citation
        section_citation = f"{title_num} CFR § {section_num}"
        
        # Create AI context
        agency_name = get_agency_from_title(title_num)
        ai_summary = create_ai_context_summary(
            section_citation, heading, cleaned_text,
            title_num, part_num, agency_name,
            metrics["regulatory_burden_score"], 
            metrics["modal_obligation_terms_count"],
            metrics["prohibition_count"], 
            metrics["requirement_count"]
        )
        
        embedding_text = create_embedding_optimized_text(
            title_num, part_num, section_num, heading, cleaned_text
        )
        
        return {
            "version_date": date,
            "snapshot_ts": snapshot_ts,
            "title_num": title_num,
            "title_name": title_name,
            "chapter_id": hierarchy["chapter_id"],
            "chapter_label": hierarchy["chapter_label"],
            "subchapter_id": hierarchy["subchapter_id"],
            "subchapter_label": hierarchy["subchapter_label"],
            "part_num": part_num,
            "part_label": None,  # Could be extracted if needed
            "subpart_id": hierarchy["subpart_id"],
            "subpart_label": hierarchy["subpart_label"],
            "section_num": section_num,
            "section_citation": section_citation,
            "section_heading": heading,
            "section_text": cleaned_text,
            "reserved": section_node.get("reserved", False),
            "agency_name": agency_name,
            "references": [],  # Could be extracted from text if needed
            "authority_uscode": [],  # Could be extracted if needed
            "part_order": 1,  # Could be calculated if needed
            "section_order": 1,  # Could be calculated if needed
            "word_count": metrics["word_count"],
            "modal_obligation_terms_count": metrics["modal_obligation_terms_count"],
            "crossref_density_per_1k": metrics["crossref_density_per_1k"],
            "section_hash": content_hash,
            "normalized_text": normalized_text,
            "raw_json": None,  # Could store original XML if needed
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
        
    except Exception as e:
        logger.warning(f"⚠️ Error processing section {section_id}: {e}")
        return None

def process_xml_file(xml_file_path: str, date: str = DATE) -> List[Dict[str, Any]]:
    """Process a single XML file and extract all sections"""
    
    title_num = int(re.search(r'title(\d+)', os.path.basename(xml_file_path)).group(1))
    title_name = f"Title {title_num}"
    snapshot_ts = datetime.datetime.utcnow().isoformat() + "+00:00"
    
    logger.info(f"📖 Processing XML file for Title {title_num}")
    
    sections = []
    
    try:
        # Parse XML
        with open(xml_file_path, 'rb') as f:
            tree = etree.parse(f)
            root = tree.getroot()
        
        # Find all sections (GovInfo format uses DIV8 with TYPE="SECTION")
        section_nodes = root.xpath('.//DIV8[@TYPE="SECTION"] | .//section | .//SECTION')
        
        logger.info(f"Found {len(section_nodes)} sections in Title {title_num}")
        
        for section_node in section_nodes:
            section_data = extract_section_from_xml(section_node, title_num, title_name, snapshot_ts, date)
            if section_data:
                sections.append(section_data)
        
        logger.info(f"✅ Successfully processed {len(sections)} sections from Title {title_num}")
        
    except Exception as e:
        logger.error(f"❌ Error processing Title {title_num}: {e}")
    
    return sections

def insert_to_bigquery(rows: List[Dict[str, Any]], batch_size: int = 1000) -> None:
    """Insert sections into BigQuery in batches"""
    table_ref = client.dataset(DATASET).table(TABLE)
    
    # Configure load job
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    )
    
    total_inserted = 0
    
    # Process in batches
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        
        try:
            job = client.load_table_from_json(batch, table_ref, job_config=job_config)
            job.result()  # Wait for completion
            
            total_inserted += len(batch)
            logger.info(f"✅ Inserted batch {i//batch_size + 1}: {len(batch)} sections ({total_inserted}/{len(rows)} total)")
            
        except Exception as e:
            logger.error(f"❌ Failed to insert batch {i//batch_size + 1}: {e}")
            # Continue with next batch
            continue
    
    logger.info(f"🎉 Successfully inserted {total_inserted}/{len(rows)} sections to BigQuery")

def process_all_xml_files(titles: List[int] = None, batch_size: int = 1000) -> Dict[str, Any]:
    """Process all XML files and insert to BigQuery"""
    
    if titles is None:
        # Find all XML files
        xml_files = [f for f in os.listdir(LOCAL_DATA_DIR) if f.endswith('.xml')]
        titles = []
        for xml_file in xml_files:
            match = re.search(r'title(\d+)', xml_file)
            if match:
                titles.append(int(match.group(1)))
        titles.sort()
    
    logger.info(f"🚀 Starting BigQuery ingestion for {len(titles)} titles")
    
    results = {
        "started_at": datetime.datetime.now().isoformat(),
        "date": DATE,
        "titles_requested": len(titles),
        "titles_processed": 0,
        "total_sections": 0,
        "total_inserted": 0,
        "details": []
    }
    
    start_time = time.time()
    
    for title_num in titles:
        xml_file = os.path.join(LOCAL_DATA_DIR, f"ECFR-title{title_num}.xml")
        
        if not os.path.exists(xml_file):
            logger.warning(f"⚠️ XML file not found for Title {title_num}")
            continue
        
        # Process XML file
        sections = process_xml_file(xml_file, DATE)
        
        if sections:
            # Insert to BigQuery
            insert_to_bigquery(sections, batch_size)
            
            results["titles_processed"] += 1
            results["total_sections"] += len(sections)
            results["total_inserted"] += len(sections)
            
            results["details"].append({
                "title_num": title_num,
                "sections_found": len(sections),
                "sections_inserted": len(sections),
                "success": True
            })
        else:
            results["details"].append({
                "title_num": title_num,
                "sections_found": 0,
                "sections_inserted": 0,
                "success": False
            })
    
    results["total_time"] = round(time.time() - start_time, 2)
    results["completed_at"] = datetime.datetime.now().isoformat()
    
    # Print summary
    logger.info("=" * 60)
    logger.info("📊 BIGQUERY INGESTION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"✅ Processed: {results['titles_processed']}/{results['titles_requested']} titles")
    logger.info(f"📄 Total sections: {results['total_sections']:,}")
    logger.info(f"💾 Inserted to BigQuery: {results['total_inserted']:,}")
    logger.info(f"⏱️ Total time: {results['total_time']:.2f} seconds")
    
    return results

def main():
    """Main entry point"""
    import argparse
    
    global PROJECT_ID, DATASET, TABLE, DATE
    
    parser = argparse.ArgumentParser(description="Process XML files and insert to BigQuery")
    parser.add_argument("--titles", nargs="+", type=int, help="Specific titles to process")
    parser.add_argument("--range", nargs=2, type=int, metavar=("START", "END"), 
                       help="Range of titles to process")
    parser.add_argument("--batch-size", type=int, default=1000, help="BigQuery insert batch size")
    parser.add_argument("--date", default=DATE, help="Version date (YYYY-MM-DD)")
    parser.add_argument("--project", default=PROJECT_ID, help="GCP project ID")
    parser.add_argument("--dataset", default=DATASET, help="BigQuery dataset")
    parser.add_argument("--table", default=TABLE, help="BigQuery table")
    
    args = parser.parse_args()
    
    # Update configuration if specified
    if args.project:
        PROJECT_ID = args.project
    if args.dataset:
        DATASET = args.dataset
    if args.table:
        TABLE = args.table
    if args.date:
        DATE = args.date
    
    # Determine which titles to process
    if args.titles:
        titles = args.titles
    elif args.range:
        titles = list(range(args.range[0], args.range[1] + 1))
    else:
        # Process all available XML files
        titles = None
        logger.info("ℹ️ No titles specified, processing all available XML files")
    
    # Run processing
    results = process_all_xml_files(titles, args.batch_size)
    
    # Exit with appropriate code
    if results["titles_processed"] == results["titles_requested"]:
        sys.exit(0)
    else:
        logger.warning(f"⚠️ Only processed {results['titles_processed']}/{results['titles_requested']} titles")
        sys.exit(1)

if __name__ == "__main__":
    main()