#!/usr/bin/env python3
"""
Comprehensive Part Number Fix Script
Applies advanced part extraction to ALL titles and removes duplicates
"""

import os
import logging
from google.cloud import bigquery
from typing import Dict

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

def remove_duplicates(client: bigquery.Client, dry_run: bool = True) -> Dict:
    """Remove duplicate sections (keeping the first occurrence)."""
    
    if dry_run:
        # Find duplicates
        count_query = f"""
        WITH duplicates AS (
            SELECT 
                title_num,
                section_citation,
                COUNT(*) as count
            FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
            GROUP BY title_num, section_citation
            HAVING COUNT(*) > 1
        )
        SELECT COUNT(*) as duplicate_groups, SUM(count - 1) as rows_to_delete
        FROM duplicates
        """
        result = client.query(count_query).to_dataframe()
        if not result.empty:
            groups = result.iloc[0]['duplicate_groups'] or 0
            rows = result.iloc[0]['rows_to_delete'] or 0
            logger.info(f"🔍 Found {groups} duplicate section groups ({rows} extra rows to remove)")
            return {"duplicate_groups": groups, "rows_to_delete": rows, "dry_run": True}
        return {"duplicate_groups": 0, "rows_to_delete": 0, "dry_run": True}
    else:
        # Delete duplicates keeping the first row
        delete_query = f"""
        DELETE FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE STRUCT(title_num, section_citation) IN (
            SELECT AS STRUCT title_num, section_citation
            FROM (
                SELECT title_num, section_citation,
                       ROW_NUMBER() OVER (PARTITION BY title_num, section_citation ORDER BY snapshot_ts) as rn
                FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
            )
            WHERE rn > 1
        )
        """
        job = client.query(delete_query)
        job.result()
        logger.info(f"✅ Removed {job.num_dml_affected_rows} duplicate rows")
        return {"rows_deleted": job.num_dml_affected_rows, "dry_run": False}

def fix_all_letter_parts(client: bigquery.Client, dry_run: bool = True) -> Dict:
    """Fix letter suffix parts across ALL titles."""
    
    update_query = f"""
    UPDATE `{PROJECT_ID}.{DATASET}.{TABLE}`
    SET 
        part_num = CASE 
            -- Hyphenated parts (101-1.5 -> 101-1)
            WHEN REGEXP_CONTAINS(section_num, r'^\\d+-\\d+\\.')
                THEN REGEXP_EXTRACT(section_num, r'^(\\d+-\\d+)\\.')
            -- Letter suffix parts (15a.1 -> 15a, 16A.1 -> 16A)
            WHEN REGEXP_CONTAINS(section_num, r'^\\d+[a-zA-Z]+\\.')
                THEN REGEXP_EXTRACT(section_num, r'^(\\d+[a-zA-Z]+)\\.')
            -- Standard pattern for reasonable lengths
            WHEN REGEXP_CONTAINS(section_num, r'^\\d{{1,4}}\\.')
                THEN REGEXP_EXTRACT(section_num, r'^(\\d{{1,4}})\\.')
            ELSE part_num
        END
    WHERE 
        section_num != 'unknown'
        AND (
            -- Update if pattern suggests different part
            (REGEXP_CONTAINS(section_num, r'^\\d+-\\d+\\.') 
                AND part_num != REGEXP_EXTRACT(section_num, r'^(\\d+-\\d+)\\.'))
            OR (REGEXP_CONTAINS(section_num, r'^\\d+[a-zA-Z]+\\.')
                AND part_num != REGEXP_EXTRACT(section_num, r'^(\\d+[a-zA-Z]+)\\.'))
            OR (REGEXP_CONTAINS(section_num, r'^\\d{{1,4}}\\.')
                AND LENGTH(REGEXP_EXTRACT(section_num, r'^(\\d{{1,4}})\\.')) <= 4
                AND part_num != REGEXP_EXTRACT(section_num, r'^(\\d{{1,4}})\\.'))
        )
    """
    
    if dry_run:
        count_query = f"""
        SELECT COUNT(*) as affected_rows
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE 
            section_num != 'unknown'
            AND (
                (REGEXP_CONTAINS(section_num, r'^\\d+-\\d+\\.') 
                    AND part_num != REGEXP_EXTRACT(section_num, r'^(\\d+-\\d+)\\.'))
                OR (REGEXP_CONTAINS(section_num, r'^\\d+[a-zA-Z]+\\.')
                    AND part_num != REGEXP_EXTRACT(section_num, r'^(\\d+[a-zA-Z]+)\\.'))
                OR (REGEXP_CONTAINS(section_num, r'^\\d{{1,4}}\\.')
                    AND LENGTH(REGEXP_EXTRACT(section_num, r'^(\\d{{1,4}})\\.')) <= 4
                    AND part_num != REGEXP_EXTRACT(section_num, r'^(\\d{{1,4}})\\.'))
            )
        """
        result = client.query(count_query).to_dataframe()
        affected = result.iloc[0]['affected_rows'] if not result.empty else 0
        logger.info(f"🔍 Would update {affected} sections across all titles")
        return {"affected_rows": affected, "dry_run": True}
    else:
        logger.info("🔧 Applying comprehensive part number fixes...")
        job = client.query(update_query)
        job.result()
        logger.info(f"✅ Updated {job.num_dml_affected_rows} sections")
        return {"affected_rows": job.num_dml_affected_rows, "dry_run": False}

def check_suspicious_low_parts(client: bigquery.Client) -> None:
    """Check for sections incorrectly assigned to parts 1-5."""
    
    query = f"""
    WITH suspicious AS (
        SELECT 
            title_num,
            part_num,
            COUNT(*) as section_count,
            ARRAY_AGG(DISTINCT section_num ORDER BY section_num LIMIT 5) as sample_sections
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE part_num IN ('1', '2', '3', '4', '5')
            AND NOT REGEXP_CONTAINS(section_num, r'^[1-5]\\.')
        GROUP BY title_num, part_num
        HAVING COUNT(*) > 0
    )
    SELECT * FROM suspicious
    ORDER BY title_num, CAST(part_num AS INT64)
    LIMIT 20
    """
    
    logger.info("\n🔍 Checking for suspicious low-number part assignments...")
    results = client.query(query).to_dataframe()
    
    if not results.empty:
        logger.info(f"Found {len(results)} suspicious part assignments:")
        for _, row in results.head(10).iterrows():
            sections = ', '.join(row['sample_sections'][:3])
            logger.info(f"  Title {row['title_num']} Part {row['part_num']}: {row['section_count']} sections (e.g., {sections})")

def verify_fixes(client: bigquery.Client) -> None:
    """Verify the fixes worked."""
    
    # Check some known problem cases
    test_queries = [
        ("Title 7 Part 15a", "SELECT COUNT(*) as c FROM `{}.{}.{}` WHERE title_num = 7 AND part_num = '15a'"),
        ("Title 8 Part 274a", "SELECT COUNT(*) as c FROM `{}.{}.{}` WHERE title_num = 8 AND part_num = '274a'"),
        ("Title 12 Part 261a", "SELECT COUNT(*) as c FROM `{}.{}.{}` WHERE title_num = 12 AND part_num = '261a'"),
        ("Title 7 duplicates", "SELECT COUNT(*) as c FROM (SELECT section_citation, COUNT(*) as cnt FROM `{}.{}.{}` WHERE title_num = 7 GROUP BY section_citation HAVING COUNT(*) > 1)")
    ]
    
    logger.info("\n🔍 Verifying fixes:")
    for label, query_template in test_queries:
        query = query_template.format(PROJECT_ID, DATASET, TABLE)
        result = client.query(query).to_dataframe()
        count = result.iloc[0]['c'] if not result.empty else 0
        status = "✅" if (count > 0 and "duplicate" not in label) or (count == 0 and "duplicate" in label) else "❌"
        logger.info(f"  {status} {label}: {count}")

def main():
    """Main execution."""
    logger.info("🚀 Starting comprehensive part number cleanup...")
    
    try:
        client = bigquery.Client(project=PROJECT_ID)
        
        # Step 1: Remove duplicates
        logger.info("\n=== STEP 1: REMOVING DUPLICATES ===")
        dup_dry = remove_duplicates(client, dry_run=True)
        if dup_dry.get("rows_to_delete", 0) > 0:
            remove_duplicates(client, dry_run=False)
        
        # Step 2: Check suspicious assignments
        logger.info("\n=== STEP 2: ANALYZING SUSPICIOUS ASSIGNMENTS ===")
        check_suspicious_low_parts(client)
        
        # Step 3: Fix ALL letter parts
        logger.info("\n=== STEP 3: FIXING ALL LETTER/HYPHEN PARTS ===")
        fix_dry = fix_all_letter_parts(client, dry_run=True)
        if fix_dry["affected_rows"] > 0:
            fix_all_letter_parts(client, dry_run=False)
        
        # Step 4: Verification
        logger.info("\n=== STEP 4: VERIFICATION ===")
        verify_fixes(client)
        
        logger.info("\n✅ Comprehensive cleanup completed!")
        
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise

if __name__ == "__main__":
    main()