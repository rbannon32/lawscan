#!/usr/bin/env python3
"""
Simple test of local parallel ingestion without BigQuery
"""

import time
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Dict, Any
import requests

def get_parts_for_title(title_num: int, date: str = "2025-08-22") -> List[str]:
    """Get all parts for a given title from eCFR API"""
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
        return sorted(parts, key=lambda x: int(x) if x.isdigit() else 999999)
    except Exception as e:
        print(f"Error fetching parts: {e}")
        return []

def process_part_simple(args: tuple) -> Dict[str, Any]:
    """Simple worker that just counts sections in a part"""
    title_num, part_num, date = args
    
    try:
        # Get title structure
        api_url = f"https://www.ecfr.gov/api/versioner/v1/structure/{date}/title-{title_num}.json"
        response = requests.get(api_url, timeout=30)
        data = response.json()
        
        # Find the specific part
        def find_part(node, target_part):
            if node.get("type") == "part" and node.get("identifier") == target_part:
                return node
            for child in node.get("children", []):
                result = find_part(child, target_part)
                if result:
                    return result
            return None
        
        part_node = find_part(data, part_num)
        if not part_node:
            return {"title": title_num, "part": part_num, "status": "not_found", "sections": 0}
        
        if part_node.get("reserved", False):
            return {"title": title_num, "part": part_num, "status": "reserved", "sections": 0}
        
        # Count sections
        section_count = len([child for child in part_node.get("children", []) 
                           if child.get("type") == "section"])
        
        print(f"✅ Title {title_num}, Part {part_num}: {section_count} sections")
        
        # Simulate some processing time
        time.sleep(0.5)
        
        return {
            "title": title_num,
            "part": part_num, 
            "status": "success",
            "sections": section_count
        }
        
    except Exception as e:
        print(f"❌ Title {title_num}, Part {part_num}: {str(e)}")
        return {"title": title_num, "part": part_num, "status": "error", "error": str(e)}

def test_local_parallel(title: int, max_workers: int = 4):
    """Test local parallel processing"""
    print(f"🧪 Testing Local Parallel Processing")
    print(f"📋 Title: {title}, Max Workers: {max_workers}")
    print("=" * 50)
    
    start_time = time.time()
    
    # Get parts
    parts = get_parts_for_title(title)[:10]  # Limit to first 10 for testing
    print(f"📦 Testing with {len(parts)} parts: {parts}")
    
    # Create work items
    work_items = [(title, part, "2025-08-22") for part in parts]
    
    results = []
    
    # Test sequential first
    print(f"\n🔄 Sequential Processing:")
    seq_start = time.time()
    for work_item in work_items:
        result = process_part_simple(work_item)
        results.append(result)
    seq_time = time.time() - seq_start
    
    # Test parallel
    print(f"\n🔄 Parallel Processing ({max_workers} workers):")
    par_start = time.time()
    results_parallel = []
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_part = {executor.submit(process_part_simple, work_item): work_item[1] 
                         for work_item in work_items}
        
        for future in as_completed(future_to_part):
            try:
                result = future.result()
                results_parallel.append(result)
            except Exception as e:
                print(f"Exception: {e}")
    
    par_time = time.time() - par_start
    total_time = time.time() - start_time
    
    # Summary
    successful = len([r for r in results_parallel if r.get("status") == "success"])
    total_sections = sum(r.get("sections", 0) for r in results_parallel if r.get("status") == "success")
    
    print(f"\n📊 RESULTS:")
    print(f"⏱️  Sequential Time: {seq_time:.1f}s")
    print(f"⚡ Parallel Time: {par_time:.1f}s") 
    print(f"🚀 Speedup: {seq_time/par_time:.1f}x")
    print(f"✅ Successful Parts: {successful}/{len(parts)}")
    print(f"📄 Total Sections: {total_sections}")
    print(f"⏱️  Total Test Time: {total_time:.1f}s")

if __name__ == "__main__":
    # Test with different configurations
    print("Testing local parallel processing capabilities:\n")
    
    # Test Title 3 (small)
    test_local_parallel(title=3, max_workers=4)
    
    print("\n" + "="*60 + "\n")
    
    # Test Title 5 (larger) with first 20 parts
    print("🧪 Testing with Title 5 (first 20 parts):")
    parts = get_parts_for_title(5)[:20]
    work_items = [(5, part, "2025-08-22") for part in parts]
    
    start_time = time.time()
    
    with ProcessPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(process_part_simple, item) for item in work_items]
        results = []
        
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                print(f"Exception: {e}")
    
    end_time = time.time()
    
    successful = len([r for r in results if r.get("status") == "success"])
    total_sections = sum(r.get("sections", 0) for r in results if r.get("status") == "success")
    
    print(f"\n📊 Title 5 (20 parts) Results:")
    print(f"⏱️  Time: {end_time - start_time:.1f}s")
    print(f"✅ Successful: {successful}/20 parts")
    print(f"📄 Sections: {total_sections}")
    print(f"⚡ Rate: {successful/(end_time - start_time):.1f} parts/sec")