#!/usr/bin/env python3
"""
Mock test of parallel ingestion functionality without actual Cloud Function deployment
Simulates the parallel processing logic locally
"""

import asyncio
import aiohttp
import time
import requests
from typing import List, Dict, Any

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

async def mock_process_part(session: aiohttp.ClientSession, title: int, part: str, date: str) -> Dict[str, Any]:
    """Mock processing of a single part (simulate what the Cloud Function would do)"""
    
    # Simulate variable processing time (some parts take longer)
    processing_time = 0.5 + (int(part) % 5) * 0.3  # 0.5 to 2.0 seconds
    
    try:
        # Simulate API call to get part structure
        api_url = f"https://www.ecfr.gov/api/versioner/v1/structure/{date}/title-{title}.json"
        async with session.get(api_url, timeout=30) as response:
            # We don't need to parse the full response for this test
            await asyncio.sleep(processing_time)  # Simulate processing time
            
            # Mock section count (estimated based on part number)
            mock_sections = max(1, (int(part) % 20) + 1)  # 1-20 sections per part
            
            print(f"✅ Title {title}, Part {part}: {mock_sections} sections (simulated)")
            
            return {
                "title": title,
                "part": part,
                "status": "success",
                "sections_processed": mock_sections,
                "processing_time": processing_time
            }
    
    except Exception as e:
        print(f"❌ Title {title}, Part {part}: {str(e)}")
        return {
            "title": title,
            "part": part,
            "status": "error",
            "error": str(e),
            "processing_time": processing_time
        }

async def run_mock_parallel_ingestion(title: int, parts: List[str], batch_size: int = 10):
    """Simulate parallel ingestion with configurable concurrency"""
    print(f"🔄 Starting mock parallel ingestion for {len(parts)} parts (batch size: {batch_size})")
    
    results = []
    start_time = time.time()
    
    # Process in batches to simulate concurrency control
    for i in range(0, len(parts), batch_size):
        batch = parts[i:i + batch_size]
        print(f"\n📦 Processing batch {i//batch_size + 1}: Parts {batch[0]} to {batch[-1]}")
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            for part in batch:
                task = mock_process_part(session, title, part, "2025-08-22")
                tasks.append(task)
            
            batch_results = await asyncio.gather(*tasks)
            results.extend(batch_results)
        
        # Small delay between batches
        if i + batch_size < len(parts):
            await asyncio.sleep(0.1)  # Minimal delay for testing
    
    end_time = time.time()
    
    # Analyze results
    successful = len([r for r in results if r.get("status") == "success"])
    failed = len(results) - successful
    total_sections = sum(r.get("sections_processed", 0) for r in results if r.get("status") == "success")
    avg_processing_time = sum(r.get("processing_time", 0) for r in results) / len(results)
    
    print(f"\n📊 MOCK PARALLEL INGESTION SUMMARY")
    print(f"=" * 50)
    print(f"⏱️  Total Time: {end_time - start_time:.1f} seconds")
    print(f"⚡ Average Processing Time per Part: {avg_processing_time:.2f}s")
    print(f"✅ Successful Parts: {successful}/{len(parts)}")
    print(f"❌ Failed Parts: {failed}")
    print(f"📄 Total Sections (estimated): {total_sections:,}")
    print(f"🚀 Speedup vs Sequential: {(len(parts) * avg_processing_time) / (end_time - start_time):.1f}x")
    
    return results

async def compare_sequential_vs_parallel():
    """Compare sequential vs parallel processing times"""
    
    print("🧪 PARALLEL INGESTION PERFORMANCE TEST")
    print("=" * 60)
    
    # Test with Title 5 parts (first 50 parts for quick testing)
    title = 5
    all_parts = get_parts_for_title(title)
    test_parts = all_parts[:50]  # Test with first 50 parts
    
    print(f"\n🎯 Testing with Title {title}, first {len(test_parts)} parts")
    
    # Test different batch sizes
    batch_sizes = [1, 5, 10, 20]
    
    for batch_size in batch_sizes:
        print(f"\n" + "="*30)
        print(f"Testing batch size: {batch_size}")
        print(f"="*30)
        
        results = await run_mock_parallel_ingestion(title, test_parts, batch_size)
        
        # Wait a bit between tests
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(compare_sequential_vs_parallel())