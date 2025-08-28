#!/usr/bin/env python3
"""
Download all 50 CFR title XML files from eCFR bulk data
and upload them to Google Cloud Storage
"""

import os
import sys
import time
import requests
from datetime import datetime
from typing import List, Dict, Any
import logging
from google.cloud import storage
import json

# Configure logging  
log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'download_xml.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
GCS_BUCKET = "ecfr-xml-bulk-2025"
BASE_URL = "https://www.govinfo.gov/bulkdata/ECFR"
DATE = datetime.now().strftime("%Y-%m-%d")  # Current date for tracking
LOCAL_DATA_DIR = "../data"

def get_xml_url(title: int) -> str:
    """Generate the URL for a specific CFR title XML file from GovInfo"""
    # GovInfo bulk XML URL pattern
    return f"{BASE_URL}/title-{title}/ECFR-title{title}.xml"

def download_xml(title: int, date: str = DATE) -> Dict[str, Any]:
    """Download a single CFR title XML file"""
    url = get_xml_url(title)
    local_path = os.path.join(LOCAL_DATA_DIR, f"ECFR-title{title}.xml")
    
    result = {
        "title": title,
        "url": url,
        "local_path": local_path,
        "status": "pending",
        "size_mb": 0,
        "download_time": 0,
        "error": None
    }
    
    try:
        logger.info(f"📥 Downloading Title {title} from {url}")
        start_time = time.time()
        
        # Download with streaming to handle large files
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()
        
        # Write to local file
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        total_size = 0
        
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    total_size += len(chunk)
        
        download_time = time.time() - start_time
        size_mb = total_size / (1024 * 1024)
        
        result.update({
            "status": "downloaded",
            "size_mb": round(size_mb, 2),
            "download_time": round(download_time, 2)
        })
        
        logger.info(f"✅ Title {title}: {size_mb:.2f} MB in {download_time:.2f}s")
        return result
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            result.update({"status": "not_found", "error": "Title does not exist"})
            logger.warning(f"⚠️ Title {title}: Not found (404)")
        else:
            result.update({"status": "error", "error": str(e)})
            logger.error(f"❌ Title {title}: HTTP error - {e}")
    except Exception as e:
        result.update({"status": "error", "error": str(e)})
        logger.error(f"❌ Title {title}: Error - {e}")
    
    return result

def upload_to_gcs(local_path: str, title: int, date: str = DATE) -> bool:
    """Upload XML file to Google Cloud Storage"""
    try:
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)
        
        # Create blob name with date folder structure
        blob_name = f"{date}/ECFR-title{title}.xml"
        blob = bucket.blob(blob_name)
        
        logger.info(f"☁️ Uploading Title {title} to gs://{GCS_BUCKET}/{blob_name}")
        
        # Upload with progress tracking
        blob.upload_from_filename(local_path)
        
        logger.info(f"✅ Title {title} uploaded to GCS")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to upload Title {title}: {e}")
        return False

def download_all_titles(titles: List[int] = None, upload: bool = True) -> Dict[str, Any]:
    """Download all specified CFR titles"""
    if titles is None:
        titles = list(range(1, 51))  # All 50 titles
    
    logger.info(f"🚀 Starting bulk download of {len(titles)} CFR titles")
    logger.info(f"📅 Date: {DATE}")
    logger.info(f"☁️ GCS Bucket: {GCS_BUCKET}")
    logger.info(f"📁 Local dir: {LOCAL_DATA_DIR}")
    
    results = {
        "started_at": datetime.now().isoformat(),
        "date": DATE,
        "titles_requested": len(titles),
        "titles_downloaded": 0,
        "titles_uploaded": 0,
        "total_size_mb": 0,
        "total_time": 0,
        "details": []
    }
    
    start_time = time.time()
    
    for title in titles:
        # Add delay to avoid rate limiting
        if title > 1:
            time.sleep(2)  # 2 second delay between downloads
        
        # Download XML
        download_result = download_xml(title, DATE)
        results["details"].append(download_result)
        
        if download_result["status"] == "downloaded":
            results["titles_downloaded"] += 1
            results["total_size_mb"] += download_result["size_mb"]
            
            # Upload to GCS if requested
            if upload:
                if upload_to_gcs(download_result["local_path"], title, DATE):
                    results["titles_uploaded"] += 1
                    download_result["gcs_uploaded"] = True
                else:
                    download_result["gcs_uploaded"] = False
                
                # Optional: Delete local file after upload to save space
                # os.remove(download_result["local_path"])
    
    results["total_time"] = round(time.time() - start_time, 2)
    results["completed_at"] = datetime.now().isoformat()
    
    # Save results summary
    summary_path = os.path.join(LOCAL_DATA_DIR, f"download_summary_{DATE}.json")
    with open(summary_path, 'w') as f:
        json.dump(results, f, indent=2)
    
    # Print summary
    logger.info("=" * 60)
    logger.info("📊 DOWNLOAD SUMMARY")
    logger.info("=" * 60)
    logger.info(f"✅ Downloaded: {results['titles_downloaded']}/{results['titles_requested']} titles")
    logger.info(f"☁️ Uploaded to GCS: {results['titles_uploaded']} titles")
    logger.info(f"💾 Total size: {results['total_size_mb']:.2f} MB")
    logger.info(f"⏱️ Total time: {results['total_time']:.2f} seconds")
    logger.info(f"📁 Results saved to: {summary_path}")
    
    return results

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Download CFR XML files in bulk")
    parser.add_argument("--titles", nargs="+", type=int, help="Specific titles to download")
    parser.add_argument("--range", nargs=2, type=int, metavar=("START", "END"), 
                       help="Range of titles to download")
    parser.add_argument("--no-upload", action="store_true", help="Skip GCS upload")
    parser.add_argument("--date", default=DATE, help="Version date (YYYY-MM-DD)")
    
    args = parser.parse_args()
    
    # Determine which titles to download
    if args.titles:
        titles = args.titles
    elif args.range:
        titles = list(range(args.range[0], args.range[1] + 1))
    else:
        # Default: test with first 3 titles
        titles = [1, 2, 3]
        logger.info("ℹ️ No titles specified, defaulting to titles 1-3")
    
    # Update date if specified  
    if args.date:
        download_date = args.date
    else:
        download_date = DATE
    
    # Run download
    results = download_all_titles(titles, upload=not args.no_upload)
    
    # Exit with appropriate code
    if results["titles_downloaded"] == results["titles_requested"]:
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()