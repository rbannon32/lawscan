#!/usr/bin/env python3
"""
Test the Cloud Function locally before deployment
"""

import sys
import os
sys.path.append('cloud_functions/ecfr_ingest_part')

from main import ingest_part
from unittest.mock import Mock

def test_cloud_function():
    """Test the Cloud Function with Title 5 Part 500"""
    print("🧪 Testing Cloud Function locally...")
    
    # Mock request
    request = Mock()
    request.get_json.return_value = {
        "title": 5, 
        "part": "500", 
        "date": "2025-08-22"
    }
    
    try:
        result = ingest_part(request)
        print(f"✅ Local test result: {result}")
        return True
    except Exception as e:
        print(f"❌ Local test failed: {e}")
        return False

if __name__ == "__main__":
    test_cloud_function()