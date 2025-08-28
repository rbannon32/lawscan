#!/usr/bin/env python3
"""
Convert downloaded CFR XML files to plaintext format
Extracts structured text from XML while preserving hierarchy
"""

import os
import sys
import time
import re
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging
from lxml import etree
import json
from google.cloud import storage

# Configure logging  
log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'xml_to_plaintext.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
LOCAL_DATA_DIR = "../data"  # XML files are in main data directory
OUTPUT_DIR = "../plaintext"
GCS_BUCKET = "ecfr-plaintext-2025"
DATE = datetime.now().strftime("%Y-%m-%d")

def extract_text_from_xml(xml_file_path: str) -> Dict[str, Any]:
    """Extract structured plaintext from CFR XML file"""
    
    title_num = int(re.search(r'title(\d+)', os.path.basename(xml_file_path)).group(1))
    
    result = {
        "title_num": title_num,
        "title_name": f"Title {title_num}",
        "processed_at": datetime.now().isoformat(),
        "sections": [],
        "total_text_length": 0,
        "total_sections": 0,
        "error": None
    }
    
    try:
        logger.info(f"📖 Processing XML for Title {title_num}")
        
        # Parse XML
        with open(xml_file_path, 'rb') as f:
            tree = etree.parse(f)
            root = tree.getroot()
        
        # Extract namespace if present
        nsmap = root.nsmap
        ns = {'ecfr': nsmap[None]} if None in nsmap else {}
        
        # Find all sections in the XML (GovInfo format uses DIV8 with TYPE="SECTION")
        section_nodes = root.xpath('.//section | .//SECTION | .//DIV8[@TYPE="SECTION"] | .//div8[@TYPE="section"]', namespaces=ns)
        
        for section_node in section_nodes:
            section_data = extract_section_text(section_node, title_num)
            if section_data and section_data["text_content"]:
                result["sections"].append(section_data)
                result["total_text_length"] += len(section_data["text_content"])
        
        result["total_sections"] = len(result["sections"])
        
        logger.info(f"✅ Title {title_num}: {result['total_sections']} sections, {result['total_text_length']:,} characters")
        
    except Exception as e:
        error_msg = f"Error processing Title {title_num}: {str(e)}"
        result["error"] = error_msg
        logger.error(f"❌ {error_msg}")
    
    return result

def extract_section_text(section_node, title_num: int) -> Optional[Dict[str, Any]]:
    """Extract text content from a section node"""
    
    try:
        # Get section identifier (GovInfo format uses N attribute for section number)
        section_id = section_node.get('N') or section_node.get('identifier') or section_node.get('id') or "unknown"
        
        # Extract section number from N attribute (e.g., "§ 100.1" -> "100.1")
        section_num = section_id
        if section_id.startswith('§ '):
            section_num = section_id[2:]  # Remove "§ " prefix
        
        # Extract heading/title (GovInfo format uses HEAD elements)
        heading_elements = section_node.xpath('.//HEAD | .//head')
        heading = ""
        if heading_elements:
            heading = clean_text(etree.tostring(heading_elements[0], method="text", encoding="unicode"))
        
        # Extract all text content
        all_text = etree.tostring(section_node, method="text", encoding="unicode")
        cleaned_text = clean_text(all_text)
        
        if not cleaned_text or len(cleaned_text.strip()) < 10:
            return None
        
        # Try to extract part number from parent elements or NODE attribute
        part_num = "unknown"
        node_attr = section_node.get('NODE')  # GovInfo format: "3:1.0.1.1.1.0.1.1"
        if node_attr:
            # Parse NODE to extract part (second number after first colon)
            parts = node_attr.split(':')
            if len(parts) > 1:
                node_parts = parts[1].split('.')
                if len(node_parts) > 2:
                    part_num = node_parts[2]  # Usually the part number
        
        # Fallback: check parent elements
        if part_num == "unknown":
            parent = section_node.getparent()
            while parent is not None:
                if parent.tag in ['part', 'PART', 'DIV6']:  # DIV6 often contains parts
                    part_num = parent.get('N') or parent.get('identifier') or parent.get('id') or parent.get('part', 'unknown')
                    break
                parent = parent.getparent()
        
        return {
            "title_num": title_num,
            "part_num": part_num,
            "section_num": section_num,
            "section_citation": f"{title_num} CFR § {section_num}",
            "heading": heading,
            "text_content": cleaned_text,
            "word_count": len(cleaned_text.split()),
            "char_count": len(cleaned_text)
        }
        
    except Exception as e:
        logger.warning(f"⚠️ Error extracting section {section_id}: {e}")
        return None

def clean_text(text: str) -> str:
    """Clean and normalize text content"""
    if not text:
        return ""
    
    # Remove excessive whitespace
    text = re.sub(r'\s+', ' ', text)
    
    # Remove control characters but preserve newlines
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', text)
    
    # Clean up common XML artifacts
    text = re.sub(r'&lt;', '<', text)
    text = re.sub(r'&gt;', '>', text)
    text = re.sub(r'&amp;', '&', text)
    text = re.sub(r'&quot;', '"', text)
    text = re.sub(r'&apos;', "'", text)
    
    return text.strip()

def upload_to_gcs(title_num: int, local_file_paths: List[str]) -> Dict[str, str]:
    """Upload plaintext files to Google Cloud Storage"""
    gcs_paths = {}
    
    try:
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)
        
        for local_path in local_file_paths:
            # Create blob name with date folder structure
            filename = os.path.basename(local_path)
            blob_name = f"{DATE}/title_{title_num:02d}/{filename}"
            blob = bucket.blob(blob_name)
            
            logger.info(f"☁️ Uploading {filename} to gs://{GCS_BUCKET}/{blob_name}")
            
            # Upload file
            blob.upload_from_filename(local_path)
            
            gcs_paths[filename] = f"gs://{GCS_BUCKET}/{blob_name}"
        
        logger.info(f"✅ Title {title_num} plaintext files uploaded to GCS")
        return gcs_paths
        
    except Exception as e:
        logger.error(f"❌ Failed to upload Title {title_num} to GCS: {e}")
        return {}

def save_plaintext_files(title_data: Dict[str, Any], upload_gcs: bool = False) -> Dict[str, str]:
    """Save extracted text to files and optionally upload to GCS"""
    
    title_num = title_data["title_num"]
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Save individual section files
    title_dir = os.path.join(OUTPUT_DIR, f"title_{title_num:02d}")
    os.makedirs(title_dir, exist_ok=True)
    
    # Save combined title file (existing format)
    combined_path = os.path.join(OUTPUT_DIR, f"title_{title_num:02d}_combined.txt")
    
    # Save full title document (new format - all text concatenated)
    full_title_path = os.path.join(OUTPUT_DIR, f"title_{title_num:02d}_full.txt")
    
    section_summaries = []
    file_paths = {"combined": combined_path, "full": full_title_path}
    
    # Write combined file (with headers and structure)
    with open(combined_path, 'w', encoding='utf-8') as combined_file:
        combined_file.write(f"TITLE {title_num} CFR - COMPLETE TEXT\n")
        combined_file.write("=" * 60 + "\n\n")
        
        for i, section in enumerate(title_data["sections"]):
            # Write to combined file
            combined_file.write(f"SECTION: {section['section_citation']}\n")
            if section["heading"]:
                combined_file.write(f"HEADING: {section['heading']}\n")
            combined_file.write(f"PART: {section['part_num']}\n")
            combined_file.write("-" * 40 + "\n")
            combined_file.write(section["text_content"] + "\n\n")
            
            # Save individual section file
            section_filename = f"section_{section['section_num']}.txt"
            section_path = os.path.join(title_dir, section_filename)
            with open(section_path, 'w', encoding='utf-8') as section_file:
                section_file.write(f"Citation: {section['section_citation']}\n")
                if section["heading"]:
                    section_file.write(f"Heading: {section['heading']}\n")
                section_file.write(f"Part: {section['part_num']}\n")
                section_file.write("-" * 40 + "\n")
                section_file.write(section["text_content"])
            
            # Add to summary
            section_summaries.append({
                "section": section["section_citation"],
                "heading": section["heading"],
                "word_count": section["word_count"],
                "file": section_filename
            })
    
    # Write full title document (plain text only, no formatting)
    with open(full_title_path, 'w', encoding='utf-8') as full_file:
        full_file.write(f"Title {title_num} Code of Federal Regulations\n\n")
        
        for section in title_data["sections"]:
            # Just write the raw text content
            full_file.write(section["text_content"] + "\n\n")
    
    # Save metadata/summary
    summary_path = os.path.join(title_dir, "summary.json")
    summary = {
        "title_num": title_num,
        "title_name": title_data["title_name"],
        "processed_at": title_data["processed_at"],
        "total_sections": title_data["total_sections"],
        "total_text_length": title_data["total_text_length"],
        "total_words": sum(s["word_count"] for s in title_data["sections"]),
        "sections": section_summaries,
        "combined_file": f"title_{title_num:02d}_combined.txt",
        "full_file": f"title_{title_num:02d}_full.txt"
    }
    
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2)
    
    # Upload to GCS if requested
    if upload_gcs:
        gcs_paths = upload_to_gcs(title_num, [combined_path, full_title_path, summary_path])
        file_paths.update(gcs_paths)
    
    logger.info(f"💾 Saved Title {title_num}: {len(section_summaries)} sections to {title_dir}")
    return file_paths

def process_all_xml_files(titles: List[int] = None, upload: bool = False) -> Dict[str, Any]:
    """Process all XML files to plaintext"""
    
    if titles is None:
        # Find all XML files
        xml_files = [f for f in os.listdir(LOCAL_DATA_DIR) if f.endswith('.xml')]
        titles = []
        for xml_file in xml_files:
            match = re.search(r'title(\d+)', xml_file)
            if match:
                titles.append(int(match.group(1)))
        titles.sort()
    
    logger.info(f"🚀 Starting plaintext extraction for {len(titles)} titles")
    
    results = {
        "started_at": datetime.now().isoformat(),
        "date": DATE,
        "titles_requested": len(titles),
        "titles_processed": 0,
        "total_sections": 0,
        "total_text_length": 0,
        "details": []
    }
    
    start_time = time.time()
    
    for title_num in titles:
        xml_file = os.path.join(LOCAL_DATA_DIR, f"ECFR-title{title_num}.xml")
        
        if not os.path.exists(xml_file):
            logger.warning(f"⚠️ XML file not found for Title {title_num}")
            continue
        
        # Extract text
        title_data = extract_text_from_xml(xml_file)
        results["details"].append(title_data)
        
        if not title_data.get("error"):
            results["titles_processed"] += 1
            results["total_sections"] += title_data["total_sections"]
            results["total_text_length"] += title_data["total_text_length"]
            
            # Save plaintext files
            save_plaintext_files(title_data, upload_gcs=upload)
    
    results["total_time"] = round(time.time() - start_time, 2)
    results["completed_at"] = datetime.now().isoformat()
    
    # Save processing summary
    summary_path = os.path.join(OUTPUT_DIR, f"extraction_summary_{DATE}.json")
    with open(summary_path, 'w') as f:
        json.dump(results, f, indent=2)
    
    # Print summary
    logger.info("=" * 60)
    logger.info("📊 PLAINTEXT EXTRACTION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"✅ Processed: {results['titles_processed']}/{results['titles_requested']} titles")
    logger.info(f"📄 Total sections: {results['total_sections']:,}")
    logger.info(f"💭 Total text: {results['total_text_length']:,} characters")
    logger.info(f"⏱️ Total time: {results['total_time']:.2f} seconds")
    logger.info(f"📁 Results saved to: {OUTPUT_DIR}")
    logger.info(f"📋 Summary: {summary_path}")
    
    return results

def main():
    """Main entry point"""
    import argparse
    
    global OUTPUT_DIR, GCS_BUCKET
    
    parser = argparse.ArgumentParser(description="Convert CFR XML files to plaintext")
    parser.add_argument("--titles", nargs="+", type=int, help="Specific titles to process")
    parser.add_argument("--range", nargs=2, type=int, metavar=("START", "END"), 
                       help="Range of titles to process")
    parser.add_argument("--output-dir", default=OUTPUT_DIR, help="Output directory for plaintext files")
    parser.add_argument("--upload", action="store_true", help="Upload plaintext files to GCS")
    parser.add_argument("--bucket", default=GCS_BUCKET, help="GCS bucket name for uploads")
    
    args = parser.parse_args()
    
    # Update output directory and bucket if specified
    if args.output_dir:
        OUTPUT_DIR = args.output_dir
    if args.bucket:
        GCS_BUCKET = args.bucket
    
    # Determine which titles to process
    if args.titles:
        titles = args.titles
    elif args.range:
        titles = list(range(args.range[0], args.range[1] + 1))
    else:
        # Process all available XML files
        titles = None
        logger.info("ℹ️ No titles specified, processing all available XML files")
    
    # Run extraction
    results = process_all_xml_files(titles, upload=args.upload)
    
    # Exit with appropriate code
    if results["titles_processed"] == results["titles_requested"]:
        sys.exit(0)
    else:
        logger.warning(f"⚠️ Only processed {results['titles_processed']}/{results['titles_requested']} titles")
        sys.exit(1)

if __name__ == "__main__":
    main()