#!/usr/bin/env python3
"""
Orchestration script for parallel eCFR ingestion using GCP Cloud Functions
Deploys multiple Cloud Functions and coordinates their execution
"""

import os
import json
import time
import argparse
import asyncio
import subprocess
from typing import List, Dict, Any, Optional
import requests
from google.cloud import bigquery
from dotenv import load_dotenv

try:
    import aiohttp
except ImportError:
    print("Installing aiohttp...")
    subprocess.run(["pip", "install", "aiohttp"], check=True)
    import aiohttp

load_dotenv()

# Configuration
PROJECT_ID = os.getenv("PROJECT_ID", "lawscan")
REGION = os.getenv("REGION", "us-central1")
DATASET = os.getenv("DATASET", "ecfr_enhanced")
TABLE = os.getenv("TABLE", "sections_enhanced")

class ParallelIngestOrchestrator:
    def __init__(self, project_id: str, region: str):
        self.project_id = project_id
        self.region = region
        self.function_name = "ecfr-ingest-part"
        self.source_dir = "cloud_functions/ecfr_ingest_part"
        self.deployed_functions = []
        
    def get_parts_for_title(self, title_num: int, date: str = "2025-08-22") -> List[str]:
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
    
    def deploy_cloud_function(self) -> bool:
        """Deploy the Cloud Function"""
        print(f"🚀 Deploying Cloud Function: {self.function_name}")
        
        try:
            cmd = [
                "gcloud", "functions", "deploy", self.function_name,
                "--runtime", "python311",
                "--trigger-http",
                "--allow-unauthenticated",
                "--source", self.source_dir,
                "--entry-point", "ingest_part",
                "--memory", "2GB",
                "--timeout", "540s",  # 9 minutes
                "--region", self.region,
                "--project", self.project_id,
                "--env-vars-file", f"{self.source_dir}/.env.yaml"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            
            if result.returncode == 0:
                print(f"✅ Successfully deployed {self.function_name}")
                return True
            else:
                print(f"❌ Deployment failed: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            print(f"❌ Deployment timed out after 10 minutes")
            return False
        except Exception as e:
            print(f"❌ Deployment error: {e}")
            return False
    
    def get_function_url(self) -> Optional[str]:
        """Get the deployed Cloud Function URL"""
        try:
            cmd = [
                "gcloud", "functions", "describe", self.function_name,
                "--region", self.region,
                "--project", self.project_id,
                "--format", "value(httpsTrigger.url)"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                return result.stdout.strip()
            else:
                print(f"❌ Error getting function URL: {result.stderr}")
                return None
                
        except Exception as e:
            print(f"❌ Error getting function URL: {e}")
            return None
    
    async def invoke_function_async(self, session: aiohttp.ClientSession, 
                                  function_url: str, title: int, part: str, date: str) -> Dict[str, Any]:
        """Invoke Cloud Function asynchronously"""
        payload = {"title": title, "part": part, "date": date}
        
        try:
            async with session.post(
                function_url, 
                json=payload,
                timeout=aiohttp.ClientTimeout(total=600)  # 10 minutes
            ) as response:
                result = await response.json()
                
                if response.status == 200:
                    print(f"✅ Title {title}, Part {part}: {result.get('sections_processed', 0)} sections")
                    return {"title": title, "part": part, "status": "success", "result": result}
                else:
                    error_msg = result.get("error", "Unknown error")
                    print(f"❌ Title {title}, Part {part}: {error_msg}")
                    return {"title": title, "part": part, "status": "error", "error": error_msg}
                    
        except asyncio.TimeoutError:
            print(f"⏰ Title {title}, Part {part}: Timeout")
            return {"title": title, "part": part, "status": "timeout", "error": "Function timeout"}
        except Exception as e:
            print(f"❌ Title {title}, Part {part}: {str(e)}")
            return {"title": title, "part": part, "status": "error", "error": str(e)}
    
    async def invoke_all_parts_async(self, function_url: str, title: int, 
                                   parts: List[str], date: str, batch_size: int = 50) -> List[Dict[str, Any]]:
        """Invoke Cloud Functions for all parts with concurrency control"""
        print(f"🔄 Starting parallel ingestion for {len(parts)} parts (batch size: {batch_size})")
        
        results = []
        
        # Process in batches to avoid overwhelming the system
        for i in range(0, len(parts), batch_size):
            batch = parts[i:i + batch_size]
            print(f"\n📦 Processing batch {i//batch_size + 1}: Parts {batch[0]} to {batch[-1]}")
            
            async with aiohttp.ClientSession() as session:
                tasks = []
                for part in batch:
                    task = self.invoke_function_async(session, function_url, title, part, date)
                    tasks.append(task)
                
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Handle exceptions
                for j, result in enumerate(batch_results):
                    if isinstance(result, Exception):
                        print(f"❌ Exception for part {batch[j]}: {result}")
                        results.append({
                            "title": title, 
                            "part": batch[j], 
                            "status": "exception", 
                            "error": str(result)
                        })
                    else:
                        results.append(result)
            
            # Small delay between batches
            if i + batch_size < len(parts):
                print(f"⏳ Waiting 10 seconds before next batch...")
                await asyncio.sleep(10)
        
        return results
    
    def cleanup_function(self) -> bool:
        """Delete the deployed Cloud Function"""
        print(f"🧹 Cleaning up Cloud Function: {self.function_name}")
        
        try:
            cmd = [
                "gcloud", "functions", "delete", self.function_name,
                "--region", self.region,
                "--project", self.project_id,
                "--quiet"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"✅ Successfully deleted {self.function_name}")
                return True
            else:
                print(f"⚠️ Delete failed (function may not exist): {result.stderr}")
                return False
                
        except Exception as e:
            print(f"❌ Cleanup error: {e}")
            return False
    
    def verify_data_completeness(self, title: int, date: str) -> Dict[str, int]:
        """Verify ingestion completeness against BigQuery"""
        print(f"🔍 Verifying data completeness for Title {title}...")
        
        try:
            client = bigquery.Client(project=self.project_id)
            
            query = f"""
            SELECT 
                COUNT(DISTINCT part_num) as parts_ingested,
                COUNT(*) as sections_ingested,
                COUNT(CASE WHEN reserved = true THEN 1 END) as reserved_sections
            FROM `{self.project_id}.{DATASET}.{TABLE}`
            WHERE title_num = @title_num 
              AND version_date = DATE(@date)
            """
            
            job = client.query(query, job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("title_num", "INT64", title),
                    bigquery.ScalarQueryParameter("date", "STRING", date)
                ]
            ))
            
            result = list(job.result())[0]
            
            stats = {
                "parts_ingested": result.parts_ingested or 0,
                "sections_ingested": result.sections_ingested or 0,
                "reserved_sections": result.reserved_sections or 0
            }
            
            print(f"📊 BigQuery Stats: {stats['parts_ingested']} parts, {stats['sections_ingested']} sections")
            
            return stats
            
        except Exception as e:
            print(f"❌ Error verifying data: {e}")
            return {"parts_ingested": 0, "sections_ingested": 0, "reserved_sections": 0}

async def main():
    parser = argparse.ArgumentParser(description="Deploy and run parallel eCFR ingestion")
    parser.add_argument("--title", type=int, required=True, help="CFR Title number")
    parser.add_argument("--date", default="2025-08-22", help="Version date (YYYY-MM-DD)")
    parser.add_argument("--batch-size", type=int, default=50, help="Concurrent functions per batch")
    parser.add_argument("--cleanup", action="store_true", help="Clean up Cloud Function after completion")
    parser.add_argument("--verify-only", action="store_true", help="Only verify existing data")
    
    args = parser.parse_args()
    
    orchestrator = ParallelIngestOrchestrator(PROJECT_ID, REGION)
    
    if args.verify_only:
        orchestrator.verify_data_completeness(args.title, args.date)
        return
    
    try:
        # Get parts to process
        parts = orchestrator.get_parts_for_title(args.title, args.date)
        if not parts:
            print("❌ No parts found to process")
            return
        
        print(f"📋 Will process {len(parts)} parts: {parts[:10]}{'...' if len(parts) > 10 else ''}")
        
        # Deploy Cloud Function
        if not orchestrator.deploy_cloud_function():
            print("❌ Failed to deploy Cloud Function")
            return
        
        # Get function URL
        function_url = orchestrator.get_function_url()
        if not function_url:
            print("❌ Failed to get Cloud Function URL")
            return
        
        print(f"🌐 Function URL: {function_url}")
        
        # Execute parallel ingestion
        start_time = time.time()
        results = await orchestrator.invoke_all_parts_async(
            function_url, args.title, parts, args.date, args.batch_size
        )
        end_time = time.time()
        
        # Analyze results
        successful = len([r for r in results if r.get("status") == "success"])
        failed = len(results) - successful
        total_sections = sum(r.get("result", {}).get("sections_processed", 0) 
                           for r in results if r.get("status") == "success")
        
        print(f"\n📊 INGESTION SUMMARY")
        print(f"=" * 50)
        print(f"⏱️  Total Time: {end_time - start_time:.1f} seconds")
        print(f"✅ Successful Parts: {successful}/{len(parts)}")
        print(f"❌ Failed Parts: {failed}")
        print(f"📄 Total Sections: {total_sections:,}")
        
        if failed > 0:
            print(f"\n❌ Failed Parts:")
            for result in results:
                if result.get("status") != "success":
                    print(f"   - Part {result.get('part')}: {result.get('error', 'Unknown error')}")
        
        # Verify completeness
        stats = orchestrator.verify_data_completeness(args.title, args.date)
        
        # Save detailed results
        with open(f"ingestion_results_title_{args.title}_{args.date}.json", "w") as f:
            json.dump({
                "summary": {
                    "title": args.title,
                    "date": args.date,
                    "total_time": end_time - start_time,
                    "parts_attempted": len(parts),
                    "parts_successful": successful,
                    "parts_failed": failed,
                    "sections_ingested": total_sections
                },
                "bigquery_stats": stats,
                "detailed_results": results
            }, f, indent=2)
        
    finally:
        # Cleanup if requested
        if args.cleanup:
            orchestrator.cleanup_function()

if __name__ == "__main__":
    asyncio.run(main())