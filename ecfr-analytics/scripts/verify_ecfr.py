#!/usr/bin/env python3
"""
eCFR Data Verification Script
Compares BigQuery ingested data against live eCFR API to verify completeness
"""

import os
import sys
import requests
import argparse
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

# Configuration
PROJECT_ID = os.getenv("PROJECT_ID", "lawscan")
DATASET = os.getenv("DATASET", "ecfr_enhanced") 
TABLE = os.getenv("TABLE", "sections_enhanced")

# eCFR API base
ECFR_API_BASE = "https://www.ecfr.gov/api"

def get_ecfr_api_counts(title_num):
    """Get part and section counts from eCFR API."""
    print(f"  🌐 Fetching Title {title_num} from eCFR API...")
    
    try:
        # Use current date for API
        url = f"{ECFR_API_BASE}/versioner/v1/structure/current/title-{title_num}.json"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Parse structure
        api_parts = set()
        api_sections = []
        reserved_count = 0
        
        # The response is the title structure directly
        def traverse_node(node, parent_part=None):
            nonlocal api_parts, api_sections, reserved_count
            
            # Check if this is a part
            if node.get("type") == "part":
                part_num = node.get("identifier", "")
                if part_num and not node.get("reserved", False):  # Skip reserved parts
                    # Ensure part number is stored as string for consistent comparison
                    api_parts.add(str(part_num))
                parent_part = str(part_num)
            
            # Check if this is a section
            elif node.get("type") == "section":
                section_num = node.get("identifier", "")
                section_label = node.get("label", "")
                
                # Check if reserved
                is_reserved = node.get("reserved", False) or "[Reserved]" in section_label or "reserved" in section_label.lower()
                if is_reserved:
                    reserved_count += 1
                    
                api_sections.append({
                    'part': parent_part,
                    'section': section_num,
                    'reserved': is_reserved
                })
            
            # Recursively process children
            for child in node.get("children", []):
                traverse_node(child, parent_part)
        
        # Start traversing from the title node
        traverse_node(data)
        
        return {
            'parts': len(api_parts),
            'sections': len(api_sections),
            'reserved': reserved_count,
            'part_list': sorted(api_parts, key=lambda x: (int(x) if x.isdigit() else 999999, x)),
            'section_details': api_sections
        }
        
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Error fetching from API: {e}")
        return None
    except Exception as e:
        print(f"  ❌ Error parsing API response: {e}")
        return None

def get_bigquery_counts(client, title_num):
    """Get part and section counts from BigQuery (all data, not date-specific)."""
    print(f"  📊 Querying BigQuery for Title {title_num}...")
    
    sql = f"""
    SELECT 
        COUNT(DISTINCT part_num) as unique_parts,
        COUNT(*) as total_sections,
        COUNT(CASE WHEN reserved = true THEN 1 END) as reserved_sections,
        ARRAY_AGG(DISTINCT part_num) as part_list
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE title_num = @title_num
    """
    
    job = client.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("title_num", "INT64", title_num)
        ]
    ))
    
    result = list(job.result())
    if result:
        row = result[0]
        return {
            'parts': row.unique_parts or 0,
            'sections': row.total_sections or 0,
            'reserved': row.reserved_sections or 0,
            'part_list': list(row.part_list) if row.part_list else []
        }
    return None

def compare_counts(title_num, api_counts, bq_counts):
    """Compare and display differences between API and BigQuery."""
    print(f"\n📋 TITLE {title_num} VERIFICATION RESULTS")
    print("=" * 60)
    
    if not api_counts:
        print("  ⚠️  Could not fetch API data for comparison")
        print(f"  📊 BigQuery: {bq_counts['parts']} parts, {bq_counts['sections']} sections")
        return False
    
    if not bq_counts:
        print("  ⚠️  No data in BigQuery")
        print(f"  🌐 API: {api_counts['parts']} parts, {api_counts['sections']} sections")
        return False
    
    # Compare counts
    parts_match = api_counts['parts'] == bq_counts['parts']
    sections_match = api_counts['sections'] == bq_counts['sections']
    
    print(f"  Parts:     API={api_counts['parts']:4d}  BQ={bq_counts['parts']:4d}  {'✅ MATCH' if parts_match else '❌ MISMATCH'}")
    print(f"  Sections:  API={api_counts['sections']:4d}  BQ={bq_counts['sections']:4d}  {'✅ MATCH' if sections_match else '❌ MISMATCH'}")
    print(f"  Reserved:  API={api_counts['reserved']:4d}  BQ={bq_counts['reserved']:4d}")
    
    # Show missing parts if any
    if not parts_match and api_counts.get('part_list') and bq_counts.get('part_list'):
        api_parts = set(api_counts['part_list'])
        bq_parts = set(bq_counts['part_list'])
        
        missing_in_bq = api_parts - bq_parts
        extra_in_bq = bq_parts - api_parts
        
        if missing_in_bq:
            print(f"\n  🔍 Parts in API but missing from BigQuery:")
            for part in sorted(missing_in_bq, key=lambda x: (int(x) if x.isdigit() else 999999, x)):
                print(f"     - Part {part}")
        
        if extra_in_bq:
            print(f"\n  🔍 Parts in BigQuery but not in API:")
            for part in sorted(extra_in_bq, key=lambda x: (int(x) if x.isdigit() else 999999, x)):
                print(f"     - Part {part}")
    
    return parts_match and sections_match

def verify_all_titles(client, titles):
    """Verify multiple titles."""
    print(f"\n🔍 ECFR DATA VERIFICATION")
    print("=" * 60)
    
    results = []
    for title_num in titles:
        print(f"\n🔄 Processing Title {title_num}...")
        
        api_counts = get_ecfr_api_counts(title_num)
        bq_counts = get_bigquery_counts(client, title_num)
        
        match = compare_counts(title_num, api_counts, bq_counts)
        results.append({
            'title': title_num,
            'match': match,
            'api': api_counts,
            'bq': bq_counts
        })
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 VERIFICATION SUMMARY")
    print("=" * 60)
    
    matches = sum(1 for r in results if r['match'])
    total = len(results)
    
    print(f"\n  ✅ Matching titles: {matches}/{total}")
    
    if matches < total:
        print(f"  ❌ Mismatched titles:")
        for r in results:
            if not r['match']:
                api = r['api'] or {'parts': 0, 'sections': 0}
                bq = r['bq'] or {'parts': 0, 'sections': 0}
                print(f"     - Title {r['title']}: API({api['parts']} parts, {api['sections']} sections) vs BQ({bq['parts']} parts, {bq['sections']} sections)")
    
    return matches == total

def main():
    parser = argparse.ArgumentParser(description="Verify eCFR data against API")
    parser.add_argument("--titles", nargs="+", type=int, help="Title numbers to verify", default=[3, 7])
    parser.add_argument("--all", action="store_true", help="Verify all titles in BigQuery")
    
    args = parser.parse_args()
    
    client = bigquery.Client(project=PROJECT_ID)
    
    if args.all:
        # Get all titles from BigQuery
        sql = f"""
        SELECT DISTINCT title_num
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        ORDER BY title_num
        """
        job = client.query(sql)
        titles = [row.title_num for row in job.result()]
        print(f"Found {len(titles)} titles in BigQuery to verify")
    else:
        titles = args.titles
    
    success = verify_all_titles(client, titles)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()