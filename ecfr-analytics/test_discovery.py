#!/usr/bin/env python3
"""
Test the discovery functionality of the parallel ingestion system
"""

import requests
import json

def test_discovery(title_num: int, date: str = "2025-08-22"):
    """Test discovering parts for a title"""
    print(f"🔍 Testing discovery for Title {title_num}...")
    
    try:
        api_url = f"https://www.ecfr.gov/api/versioner/v1/structure/{date}/title-{title_num}.json"
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        parts = []
        
        def extract_parts(node):
            if node.get("type") == "part" and not node.get("reserved", False):
                parts.append({
                    "identifier": node.get("identifier"),
                    "label": node.get("label_description", ""),
                    "children_count": len(node.get("children", []))
                })
            for child in node.get("children", []):
                extract_parts(child)
        
        extract_parts(data)
        
        print(f"✅ Found {len(parts)} parts for Title {title_num}")
        
        # Show first few parts with details
        for i, part in enumerate(parts[:5]):
            print(f"  Part {part['identifier']}: {part['children_count']} sections - {part['label'][:50]}...")
        
        if len(parts) > 5:
            print(f"  ... and {len(parts) - 5} more parts")
        
        return parts
        
    except Exception as e:
        print(f"❌ Error discovering parts for Title {title_num}: {e}")
        return []

if __name__ == "__main__":
    # Test discovery for different titles
    for title in [3, 5, 7]:
        parts = test_discovery(title)
        total_sections = sum(part['children_count'] for part in parts)
        print(f"📊 Title {title}: {len(parts)} parts, ~{total_sections} sections\n")