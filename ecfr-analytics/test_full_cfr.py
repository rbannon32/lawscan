#!/usr/bin/env python3
"""
Simple test of the full CFR ingestion concept
"""

import time
import requests
from typing import List, Dict, Any

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
        return list(range(1, 51))

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

def get_title_info(title_num: int) -> Dict[str, Any]:
    """Get basic info about a title"""
    try:
        response = requests.get("https://www.ecfr.gov/api/versioner/v1/titles.json", timeout=30)
        data = response.json()
        
        for title_info in data.get("titles", []):
            if title_info.get("number") == title_num:
                return {
                    "number": title_num,
                    "name": title_info.get("name", f"Title {title_num}"),
                    "reserved": title_info.get("reserved", False)
                }
        
        return {"number": title_num, "name": f"Title {title_num}", "reserved": False}
    
    except Exception:
        return {"number": title_num, "name": f"Title {title_num}", "reserved": False}

def analyze_full_cfr():
    """Analyze the complete CFR for ingestion planning"""
    print("🔍 ANALYZING COMPLETE CFR FOR INGESTION PLANNING")
    print("=" * 60)
    
    start_time = time.time()
    titles = get_all_cfr_titles()
    
    print(f"📋 Found {len(titles)} CFR titles")
    
    # Analyze each title
    title_data = []
    total_parts = 0
    total_sections = 0
    
    for i, title_num in enumerate(titles[:10], 1):  # First 10 for demo
        print(f"\n🔄 Analyzing Title {title_num} ({i}/10)...")
        
        title_info = get_title_info(title_num)
        size_estimate = estimate_title_size(title_num)
        
        if title_info.get('reserved') or size_estimate['parts'] == 0:
            print(f"⏭️ Skipping Title {title_num} (reserved or empty)")
            continue
        
        print(f"📊 Title {title_num}: {title_info['name']}")
        print(f"   📦 Parts: {size_estimate['parts']:,}")
        print(f"   📄 Sections: {size_estimate['sections']:,}")
        
        # Estimate processing time
        if size_estimate['parts'] < 10:
            est_time = "< 1 min"
            workers = 4
        elif size_estimate['parts'] < 50:
            est_time = "1-3 min"
            workers = 8
        elif size_estimate['parts'] < 200:
            est_time = "3-8 min" 
            workers = 12
        else:
            est_time = "8-15 min"
            workers = 16
        
        print(f"   ⏱️ Est. Time (parallel): {est_time} ({workers} workers)")
        print(f"   ⏱️ Est. Time (sequential): {size_estimate['parts'] * 3 / 60:.1f} min")
        
        title_data.append({
            "title": title_num,
            "info": title_info,
            "size": size_estimate,
            "workers": workers
        })
        
        total_parts += size_estimate['parts']
        total_sections += size_estimate['sections']
    
    analysis_time = time.time() - start_time
    
    print(f"\n📊 ANALYSIS SUMMARY (First 10 Titles)")
    print("=" * 50)
    print(f"⏱️  Analysis Time: {analysis_time:.1f}s")
    print(f"📖 Active Titles: {len(title_data)}")
    print(f"📦 Total Parts: {total_parts:,}")
    print(f"📄 Total Sections: {total_sections:,}")
    
    # Processing estimates
    parallel_time = sum(max(1, t['size']['parts'] / t['workers']) * 3 for t in title_data) / 60
    sequential_time = sum(t['size']['parts'] * 3 for t in title_data) / 60
    
    print(f"\n⚡ PROCESSING ESTIMATES (First 10 Titles)")
    print(f"🔄 Sequential: {sequential_time:.1f} minutes")
    print(f"⚡ Parallel: {parallel_time:.1f} minutes")  
    print(f"🚀 Speedup: {sequential_time/parallel_time:.1f}x")
    
    # Show title categories
    small_titles = [t for t in title_data if t['size']['parts'] < 50]
    medium_titles = [t for t in title_data if 50 <= t['size']['parts'] < 200]
    large_titles = [t for t in title_data if t['size']['parts'] >= 200]
    
    print(f"\n📊 TITLE CATEGORIES")
    print(f"🟢 Small (< 50 parts): {len(small_titles)} titles")
    print(f"🟡 Medium (50-199 parts): {len(medium_titles)} titles")  
    print(f"🔴 Large (200+ parts): {len(large_titles)} titles")
    
    if large_titles:
        print(f"\n🔴 LARGE TITLES (Priority for parallel processing):")
        for t in large_titles:
            print(f"   - Title {t['title']}: {t['info']['name']} ({t['size']['parts']} parts)")
    
    return title_data

def show_usage_examples():
    """Show practical usage examples"""
    print(f"\n🚀 USAGE EXAMPLES")
    print("=" * 50)
    
    examples = [
        {
            "name": "Test Run (Safe)",
            "command": "python scripts/full_cfr_ingestion.py --titles 1 2 3 --dry-run",
            "description": "Test with small titles, no BigQuery changes"
        },
        {
            "name": "Single Large Title",
            "command": "python scripts/full_cfr_ingestion.py --titles 7 --max-workers 12",
            "description": "Process Title 7 (Agriculture) with verification"
        },
        {
            "name": "First 10 Titles", 
            "command": "python scripts/full_cfr_ingestion.py --titles-range 1 10",
            "description": "Process titles 1-10 with auto-scaling workers"
        },
        {
            "name": "Resume from Interruption",
            "command": "python scripts/full_cfr_ingestion.py --resume-from 25",
            "description": "Continue processing from Title 25"
        },
        {
            "name": "Full CFR Production",
            "command": "python scripts/full_cfr_ingestion.py --save-results full_cfr.json",
            "description": "Complete CFR ingestion with results saved"
        }
    ]
    
    for example in examples:
        print(f"\n📋 {example['name']}:")
        print(f"   Command: {example['command']}")
        print(f"   Purpose: {example['description']}")

if __name__ == "__main__":
    analyze_full_cfr()
    show_usage_examples()
    
    print(f"\n✅ READY FOR FULL CFR INGESTION")
    print("The scripts are ready to process all 50 CFR titles efficiently!")
    print("\nRecommended next steps:")
    print("1. Test: python scripts/full_cfr_ingestion.py --titles 1 --dry-run")  
    print("2. Small batch: python scripts/full_cfr_ingestion.py --titles-range 1 5")
    print("3. Full run: python scripts/full_cfr_ingestion.py")