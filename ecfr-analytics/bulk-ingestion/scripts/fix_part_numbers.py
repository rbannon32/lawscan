#!/usr/bin/env python3
"""
Part Number Cleanup Script
Fixes incorrect part numbers in BigQuery where part number was misextracted from XML
Uses pattern matching on section numbers to determine correct part numbers
"""

import os
import logging
from google.cloud import bigquery
from typing import Dict, List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.getenv("PROJECT_ID", "lawscan")
DATASET = os.getenv("DATASET", "ecfr_enhanced")
TABLE = os.getenv("TABLE", "sections_enhanced")

def extract_part_from_section_number(section_num: str) -> str:
    """
    Extract part number from section number patterns like:
    - "1003.1" -> "1003" 
    - "158.1003" -> "158"
    - "3.1" -> "3"
    """
    if not section_num or section_num == "unknown":
        return "unknown"
    
    # Handle patterns like "1003.1", "158.1003", "3.1"
    parts = section_num.split('.')
    if len(parts) >= 1:
        # First part before the dot is typically the part number
        return parts[0]
    
    return section_num

def identify_incorrect_part_numbers(client: bigquery.Client) -> List[Dict]:
    """
    Find sections where the part_num doesn't match the section_num pattern
    """
    query = f"""
    SELECT 
        title_num,
        part_num,
        section_num,
        section_citation,
        COUNT(*) as count
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE 
        -- Find cases where section number suggests different part than part_num
        SPLIT(section_num, '.')[OFFSET(0)] != part_num
        AND section_num != 'unknown'
        AND part_num != 'unknown'
        AND REGEXP_CONTAINS(section_num, r'^[0-9]+\\.')
    GROUP BY title_num, part_num, section_num, section_citation
    ORDER BY title_num, CAST(part_num AS INT64), section_num
    LIMIT 100
    """
    
    logger.info("🔍 Identifying sections with incorrect part numbers...")
    results = client.query(query).to_dataframe()
    
    if not results.empty:
        logger.info(f"Found {len(results)} section patterns with potential part number mismatches")
        for _, row in results.head(10).iterrows():
            expected_part = extract_part_from_section_number(row['section_num'])
            logger.info(f"  {row['section_citation']}: part_num='{row['part_num']}' but section suggests part '{expected_part}'")
    else:
        logger.info("No obvious part number mismatches found")
    
    return results.to_dict('records')

def fix_part_numbers_batch(client: bigquery.Client, dry_run: bool = True) -> Dict:
    """
    Fix part numbers by updating them based on section number patterns
    """
    update_query = f"""
    UPDATE `{PROJECT_ID}.{DATASET}.{TABLE}`
    SET 
        part_num = SPLIT(section_num, '.')[OFFSET(0)],
        section_citation = CONCAT(CAST(title_num AS STRING), ' CFR § ', section_num)
    WHERE 
        -- Only update where section number suggests different part
        SPLIT(section_num, '.')[OFFSET(0)] != part_num
        AND section_num != 'unknown'
        AND part_num != 'unknown' 
        AND REGEXP_CONTAINS(section_num, r'^[0-9]+\\.')
        AND LENGTH(SPLIT(section_num, '.')[OFFSET(0)]) <= 4  -- Sanity check: part numbers shouldn't be too long
    """
    
    if dry_run:
        # Count how many would be affected
        count_query = f"""
        SELECT COUNT(*) as affected_rows
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE 
            SPLIT(section_num, '.')[OFFSET(0)] != part_num
            AND section_num != 'unknown'
            AND part_num != 'unknown' 
            AND REGEXP_CONTAINS(section_num, r'^[0-9]+\\.')
            AND LENGTH(SPLIT(section_num, '.')[OFFSET(0)]) <= 4
        """
        
        result = client.query(count_query).to_dataframe()
        affected_count = result.iloc[0]['affected_rows']
        logger.info(f"🔍 DRY RUN: Would update {affected_count} sections")
        return {"affected_rows": affected_count, "dry_run": True}
    
    else:
        logger.info("🔧 Executing part number corrections...")
        job = client.query(update_query)
        job.result()  # Wait for completion
        
        logger.info(f"✅ Updated {job.num_dml_affected_rows} sections")
        return {"affected_rows": job.num_dml_affected_rows, "dry_run": False}

def verify_corrections(client: bigquery.Client) -> Dict:
    """
    Verify the corrections by checking specific known cases
    """
    # Check Title 6 Part 1003 sections
    verification_query = f"""
    SELECT 
        title_num,
        part_num, 
        section_num,
        section_citation,
        section_heading
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE title_num = 6 AND section_num LIKE '1003%'
    ORDER BY section_num
    """
    
    results = client.query(verification_query).to_dataframe()
    logger.info(f"🔍 Verification - Title 6 Part 1003 sections:")
    
    for _, row in results.iterrows():
        logger.info(f"  {row['section_citation']}: part_num='{row['part_num']}' | {row['section_heading']}")
    
    return {"verification_results": len(results)}

def main():
    """Main execution function"""
    logger.info("🚀 Starting part number cleanup process...")
    
    try:
        client = bigquery.Client(project=PROJECT_ID)
        
        # Step 1: Identify problematic records
        logger.info("\n=== STEP 1: IDENTIFYING ISSUES ===")
        issues = identify_incorrect_part_numbers(client)
        
        if not issues:
            logger.info("✅ No part number issues found!")
            return
        
        # Step 2: Dry run
        logger.info("\n=== STEP 2: DRY RUN ===")
        dry_result = fix_part_numbers_batch(client, dry_run=True)
        
        if dry_result["affected_rows"] == 0:
            logger.info("✅ No corrections needed!")
            return
        
        # Step 3: Confirm and execute
        logger.info(f"\n=== STEP 3: EXECUTING CORRECTIONS ===")
        logger.info(f"About to update {dry_result['affected_rows']} sections")
        
        # Execute the corrections
        result = fix_part_numbers_batch(client, dry_run=False)
        
        # Step 4: Verification
        logger.info("\n=== STEP 4: VERIFICATION ===")
        verify_corrections(client)
        
        logger.info(f"\n✅ Part number cleanup completed successfully!")
        logger.info(f"Updated {result['affected_rows']} sections")
        
    except Exception as e:
        logger.error(f"❌ Error during cleanup: {str(e)}")
        raise

if __name__ == "__main__":
    main()