#!/usr/bin/env python3
"""
Full CFR Ingestion Script
Processes all 50 CFR titles using local parallel processing with verification
"""

import os
import json
import time
import argparse
import subprocess
import sys
from typing import Dict, List, Any, Optional
from datetime import datetime
import requests

# Add the scripts directory to Python path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from local_parallel_ingestion import run_local_parallel_ingestion
    # Import BigQuery client directly instead of using the orchestrator class
    from google.cloud import bigquery
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure you're running this from the project root directory")
    sys.exit(1)

# Configuration from environment
PROJECT_ID = os.getenv("PROJECT_ID", "lawscan")
DATASET = os.getenv("DATASET", "ecfr_enhanced") 
TABLE = os.getenv("TABLE", "sections_enhanced")

def get_all_cfr_titles() -> List[int]:
    """Get list of all CFR titles from the API"""
    try:
        response = requests.get("https://www.ecfr.gov/api/versioner/v1/titles.json", timeout=30)
        response.raise_for_status()
        data = response.json()
        
        titles = []
        for title_info in data.get("titles", []):
            title_num = title_info.get("number")
            reserved = title_info.get("reserved", False)
            
            if title_num and not reserved:
                titles.append(title_num)
        
        return sorted(titles)
    
    except Exception as e:
        print(f"⚠️ Could not fetch titles from API: {e}")
        # Return standard 1-50 range as fallback
        return list(range(1, 51))

def get_title_info(title_num: int) -> Dict[str, Any]:
    """Get basic info about a title from the API"""
    try:
        response = requests.get("https://www.ecfr.gov/api/versioner/v1/titles.json", timeout=30)
        data = response.json()
        
        for title_info in data.get("titles", []):
            if title_info.get("number") == title_num:
                return {
                    "number": title_num,
                    "name": title_info.get("name", f"Title {title_num}"),
                    "reserved": title_info.get("reserved", False),
                    "latest_issue_date": title_info.get("latest_issue_date"),
                    "up_to_date_as_of": title_info.get("up_to_date_as_of")
                }
        
        return {"number": title_num, "name": f"Title {title_num}", "reserved": False}
    
    except Exception as e:
        print(f"⚠️ Could not fetch info for Title {title_num}: {e}")
        return {"number": title_num, "name": f"Title {title_num}", "reserved": False}

def estimate_title_size(title_num: int, date: str = "2025-08-22") -> Dict[str, int]:
    """Quickly estimate the size of a title"""
    try:
        api_url = f"https://www.ecfr.gov/api/versioner/v1/structure/{date}/title-{title_num}.json"
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        parts = 0
        sections = 0
        
        def count_nodes(node):
            nonlocal parts, sections
            if node.get("type") == "part" and not node.get("reserved", False):
                parts += 1
            elif node.get("type") == "section":
                sections += 1
            
            for child in node.get("children", []):
                count_nodes(child)
        
        count_nodes(data)
        
        return {"parts": parts, "sections": sections}
    
    except Exception as e:
        print(f"⚠️ Could not estimate size for Title {title_num}: {e}")
        return {"parts": 0, "sections": 0}

def run_title_verification(title_num: int, date: str = "2025-08-22") -> Dict[str, Any]:
    """Run verification for a single title"""
    try:
        # Use the existing verification logic
        orchestrator = ParallelIngestOrchestrator("lawscan", "us-central1")
        
        # Get API counts
        api_url = f"https://www.ecfr.gov/api/versioner/v1/structure/{date}/title-{title_num}.json"
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        api_parts = set()
        api_sections = []
        reserved_count = 0
        
        def traverse_node(node, parent_part=None):
            nonlocal api_parts, api_sections, reserved_count
            
            if node.get("type") == "part":
                part_num = node.get("identifier", "")
                if part_num and not node.get("reserved", False):
                    api_parts.add(part_num)
                parent_part = part_num
            
            elif node.get("type") == "section":
                section_num = node.get("identifier", "")
                section_label = node.get("label", "")
                
                is_reserved = node.get("reserved", False) or "[Reserved]" in section_label
                if is_reserved:
                    reserved_count += 1
                    
                api_sections.append({
                    'part': parent_part,
                    'section': section_num,
                    'reserved': is_reserved
                })
            
            for child in node.get("children", []):
                traverse_node(child, parent_part)
        
        traverse_node(data)
        
        api_counts = {
            'parts': len(api_parts),
            'sections': len(api_sections),
            'reserved': reserved_count
        }
        
        # Get BigQuery counts
        try:
            client = bigquery.Client(project=PROJECT_ID)
            
            query = f"""
            SELECT 
                COUNT(DISTINCT part_num) as parts_ingested,
                COUNT(*) as sections_ingested,
                COUNT(CASE WHEN reserved = true THEN 1 END) as reserved_sections
            FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
            WHERE title_num = @title_num 
              AND version_date = DATE(@date)
            """
            
            job = client.query(query, job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("title_num", "INT64", title_num),
                    bigquery.ScalarQueryParameter("date", "STRING", date)
                ]
            ))
            
            result = list(job.result())
            if result:
                row = result[0]
                bq_stats = {
                    "parts_ingested": row.parts_ingested or 0,
                    "sections_ingested": row.sections_ingested or 0,
                    "reserved_sections": row.reserved_sections or 0
                }
            else:
                bq_stats = {"parts_ingested": 0, "sections_ingested": 0, "reserved_sections": 0}
        except Exception as e:
            bq_stats = {"parts_ingested": 0, "sections_ingested": 0, "reserved_sections": 0}
        
        # Compare
        parts_match = api_counts['parts'] == bq_stats['parts_ingested']
        sections_match = api_counts['sections'] == bq_stats['sections_ingested']
        
        return {
            "title": title_num,
            "api_counts": api_counts,
            "bq_counts": bq_stats,
            "parts_match": parts_match,
            "sections_match": sections_match,
            "overall_match": parts_match and sections_match
        }
    
    except Exception as e:
        return {
            "title": title_num,
            "error": str(e),
            "overall_match": False
        }

def run_full_cfr_ingestion(titles: List[int], date: str = "2025-08-22", 
                          max_workers: int = None, verify: bool = True,
                          resume_from: int = None, dry_run: bool = False) -> Dict[str, Any]:
    """Run full CFR ingestion for all specified titles"""
    
    print(f"🚀 FULL CFR INGESTION STARTING")
    print(f"=" * 60)
    print(f"📅 Date: {date}")
    print(f"📊 Titles: {len(titles)} titles")
    print(f"🔧 Max Workers: {max_workers or 'Auto'}")
    print(f"✅ Verification: {'Enabled' if verify else 'Disabled'}")
    print(f"🚫 Dry Run: {'Yes' if dry_run else 'No'}")
    if resume_from:
        print(f"▶️ Resuming from Title: {resume_from}")
    print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    overall_start = time.time()
    results = {
        "started_at": datetime.now().isoformat(),
        "titles_processed": 0,
        "titles_successful": 0,
        "titles_failed": 0,
        "total_parts_ingested": 0,
        "total_sections_ingested": 0,
        "verification_matches": 0,
        "verification_mismatches": 0,
        "detailed_results": []
    }
    
    # Filter titles if resuming
    if resume_from:
        titles = [t for t in titles if t >= resume_from]
        print(f"📋 Resuming with {len(titles)} remaining titles")
    
    for i, title_num in enumerate(titles, 1):
        print(f"\n{'='*60}")
        print(f"📖 PROCESSING TITLE {title_num} ({i}/{len(titles)})")
        print(f"{'='*60}")
        
        title_start = time.time()
        
        # Get title info
        title_info = get_title_info(title_num)
        print(f"📋 Title {title_num}: {title_info['name']}")
        
        if title_info.get('reserved', False):
            print(f"⏭️ Skipping reserved Title {title_num}")
            results["detailed_results"].append({
                "title": title_num,
                "status": "skipped_reserved",
                "title_info": title_info
            })
            continue
        
        # Estimate size for planning
        size_estimate = estimate_title_size(title_num, date)
        print(f"📊 Estimated: {size_estimate['parts']} parts, {size_estimate['sections']} sections")
        
        if size_estimate['parts'] == 0:
            print(f"⏭️ Skipping Title {title_num} (no parts found)")
            results["detailed_results"].append({
                "title": title_num,
                "status": "skipped_empty",
                "title_info": title_info,
                "size_estimate": size_estimate
            })
            continue
        
        # Adjust workers based on title size
        title_max_workers = max_workers
        if not title_max_workers:
            if size_estimate['parts'] < 10:
                title_max_workers = 4
            elif size_estimate['parts'] < 50:
                title_max_workers = 8
            elif size_estimate['parts'] < 200:
                title_max_workers = 12
            else:
                title_max_workers = 16
        
        print(f"🔧 Using {title_max_workers} workers for this title")
        
        # Run ingestion
        print(f"\n🔄 Starting ingestion...")
        try:
            ingestion_result = run_local_parallel_ingestion(
                title=title_num,
                date=date,
                max_workers=title_max_workers,
                dry_run=dry_run
            )
            
            if ingestion_result.get('parts_failed', 0) > 0:
                print(f"⚠️ Ingestion completed with {ingestion_result['parts_failed']} failed parts")
            else:
                print(f"✅ Ingestion successful: {ingestion_result.get('sections_ingested', 0):,} sections")
            
            results["titles_processed"] += 1
            results["total_parts_ingested"] += ingestion_result.get('parts_successful', 0)
            results["total_sections_ingested"] += ingestion_result.get('sections_ingested', 0)
            
            if ingestion_result.get('parts_failed', 0) == 0:
                results["titles_successful"] += 1
            else:
                results["titles_failed"] += 1
            
        except Exception as e:
            print(f"❌ Ingestion failed: {e}")
            ingestion_result = {"error": str(e), "parts_failed": 1}
            results["titles_failed"] += 1
        
        # Run verification if enabled and ingestion was successful
        verification_result = None
        if verify and not dry_run and ingestion_result.get('parts_failed', 0) == 0:
            print(f"\n🔍 Running verification...")
            try:
                verification_result = run_title_verification(title_num, date)
                
                if verification_result.get('overall_match', False):
                    print(f"✅ Verification passed")
                    results["verification_matches"] += 1
                else:
                    api_counts = verification_result.get('api_counts', {})
                    bq_counts = verification_result.get('bq_counts', {})
                    print(f"❌ Verification failed:")
                    print(f"   API: {api_counts.get('parts', 0)} parts, {api_counts.get('sections', 0)} sections")
                    print(f"   BigQuery: {bq_counts.get('parts_ingested', 0)} parts, {bq_counts.get('sections_ingested', 0)} sections")
                    results["verification_mismatches"] += 1
                    
            except Exception as e:
                print(f"⚠️ Verification failed: {e}")
                verification_result = {"error": str(e)}
                results["verification_mismatches"] += 1
        
        title_time = time.time() - title_start
        
        # Store detailed results
        results["detailed_results"].append({
            "title": title_num,
            "status": "completed",
            "title_info": title_info,
            "size_estimate": size_estimate,
            "ingestion_result": ingestion_result,
            "verification_result": verification_result,
            "processing_time": title_time
        })
        
        print(f"⏱️ Title {title_num} completed in {title_time:.1f}s")
        
        # Progress summary
        elapsed = time.time() - overall_start
        avg_time = elapsed / i
        remaining = len(titles) - i
        eta = remaining * avg_time
        
        print(f"\n📊 PROGRESS SUMMARY:")
        print(f"   Completed: {i}/{len(titles)} titles ({i/len(titles)*100:.1f}%)")
        print(f"   Successful: {results['titles_successful']}")
        print(f"   Failed: {results['titles_failed']}")
        print(f"   Elapsed: {elapsed/60:.1f} minutes")
        print(f"   ETA: {eta/60:.1f} minutes remaining")
        print(f"   Total sections: {results['total_sections_ingested']:,}")
        
        # Small delay to be nice to the API
        time.sleep(2)
    
    overall_time = time.time() - overall_start
    results["completed_at"] = datetime.now().isoformat()
    results["total_time"] = overall_time
    
    return results

def save_results(results: Dict[str, Any], filename: str = None):
    """Save results to JSON file"""
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"full_cfr_ingestion_{timestamp}.json"
    
    with open(filename, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"📁 Results saved to {filename}")
    return filename

def print_final_summary(results: Dict[str, Any]):
    """Print comprehensive final summary"""
    print(f"\n🎉 FULL CFR INGESTION COMPLETE")
    print(f"=" * 60)
    print(f"⏱️  Total Time: {results['total_time']/60:.1f} minutes")
    print(f"📖 Titles Processed: {results['titles_processed']}")
    print(f"✅ Successful: {results['titles_successful']}")
    print(f"❌ Failed: {results['titles_failed']}")
    print(f"📄 Total Sections: {results['total_sections_ingested']:,}")
    print(f"📦 Total Parts: {results['total_parts_ingested']:,}")
    
    if results.get('verification_matches', 0) > 0 or results.get('verification_mismatches', 0) > 0:
        total_verifications = results['verification_matches'] + results['verification_mismatches']
        print(f"🔍 Verifications: {results['verification_matches']}/{total_verifications} passed")
    
    # Show failed titles
    failed_titles = [r for r in results['detailed_results'] if r.get('ingestion_result', {}).get('parts_failed', 0) > 0]
    if failed_titles:
        print(f"\n❌ Failed Titles:")
        for result in failed_titles:
            title = result['title']
            error = result.get('ingestion_result', {}).get('error', 'Unknown error')
            print(f"   - Title {title}: {error}")
    
    # Show mismatched verifications
    mismatched = [r for r in results['detailed_results'] 
                 if r.get('verification_result', {}).get('overall_match') is False]
    if mismatched:
        print(f"\n🔍 Verification Mismatches:")
        for result in mismatched:
            title = result['title']
            vr = result.get('verification_result', {})
            api = vr.get('api_counts', {})
            bq = vr.get('bq_counts', {})
            print(f"   - Title {title}: API({api.get('parts', 0)},{api.get('sections', 0)}) vs BQ({bq.get('parts_ingested', 0)},{bq.get('sections_ingested', 0)})")

def main():
    parser = argparse.ArgumentParser(description="Run full CFR ingestion with local parallel processing")
    parser.add_argument("--titles", nargs="*", type=int, help="Specific titles to process (default: all)")
    parser.add_argument("--date", default="2025-08-22", help="Version date (YYYY-MM-DD)")
    parser.add_argument("--max-workers", type=int, help="Maximum worker processes per title")
    parser.add_argument("--no-verify", action="store_true", help="Skip verification step")
    parser.add_argument("--resume-from", type=int, help="Resume from specific title number")
    parser.add_argument("--dry-run", action="store_true", help="Don't insert to BigQuery, just test")
    parser.add_argument("--save-results", help="Save results to specific JSON file")
    parser.add_argument("--titles-range", nargs=2, type=int, metavar=("START", "END"), 
                       help="Process titles in range (inclusive)")
    
    args = parser.parse_args()
    
    # Determine which titles to process
    if args.titles:
        titles = args.titles
    elif args.titles_range:
        titles = list(range(args.titles_range[0], args.titles_range[1] + 1))
    else:
        titles = get_all_cfr_titles()
    
    print(f"🎯 Will process {len(titles)} titles: {titles[:10]}{'...' if len(titles) > 10 else ''}")
    
    # Confirm if processing many titles
    if len(titles) > 10 and not args.dry_run:
        response = input(f"\n⚠️  You're about to process {len(titles)} titles. This could take several hours. Continue? (y/N): ")
        if response.lower() not in ['y', 'yes']:
            print("Cancelled by user")
            return 1
    
    try:
        # Run the full ingestion
        results = run_full_cfr_ingestion(
            titles=titles,
            date=args.date,
            max_workers=args.max_workers,
            verify=not args.no_verify,
            resume_from=args.resume_from,
            dry_run=args.dry_run
        )
        
        # Save results
        filename = save_results(results, args.save_results)
        
        # Print final summary
        print_final_summary(results)
        
        # Return appropriate exit code
        return 0 if results['titles_failed'] == 0 else 1
        
    except KeyboardInterrupt:
        print(f"\n⚠️ Interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        return 1

if __name__ == "__main__":
    exit(main())