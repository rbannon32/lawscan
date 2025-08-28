#!/usr/bin/env python3
"""
Advanced Part Number Cleanup Script
Fixes incorrect part numbers in BigQuery with sophisticated pattern handling:
- Handles letter suffixes (15a, 16A, 5c)
- Handles hyphenated parts (101-1, 1150-1159)
- Preserves special formats (S 50, ECFR prefixes)
- Better detection of actual part vs section numbering
"""

import os
import logging
import re
from google.cloud import bigquery
from typing import Dict, List, Optional, Tuple

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

def analyze_section_pattern(section_num: str) -> Tuple[Optional[str], str]:
    """
    Analyze section number to extract likely part number with sophisticated handling.
    Returns (extracted_part, reasoning)
    """
    if not section_num or section_num == "unknown":
        return None, "unknown section"
    
    # Clean up the section number
    section_num = section_num.strip()
    
    # Pattern 1: Hyphenated parts like "101-1.5" -> Part "101-1"
    # Matches patterns where hyphen is followed by a single digit before the dot
    hyphen_match = re.match(r'^(\d+)-(\d+)\.(\d+)', section_num)
    if hyphen_match:
        part = f"{hyphen_match.group(1)}-{hyphen_match.group(2)}"
        return part, f"hyphenated part pattern"
    
    # Pattern 2: Letter suffix parts like "15a.1", "16A.5" -> Part "15a", "16A"
    letter_match = re.match(r'^(\d+[a-zA-Z]+)\.(\d+)', section_num)
    if letter_match:
        part = letter_match.group(1)
        return part, f"letter suffix pattern"
    
    # Pattern 3: Special prefix parts like "S 50.1" -> Part "S 50"
    special_match = re.match(r'^([A-Z]+\s+\d+)\.(\d+)', section_num)
    if special_match:
        part = special_match.group(1)
        return part, f"special prefix pattern"
    
    # Pattern 4: ECFR special identifiers
    if section_num.startswith("ECFR"):
        # Don't try to extract part from ECFR identifiers
        return None, "ECFR special identifier"
    
    # Pattern 5: Range patterns like "1202-1219 [RESERVED] 1220-1239"
    # These are typically part identifiers themselves, not sections
    if re.search(r'\d+-\d+.*\[RESERVED\]', section_num) or re.search(r'\d+-\d+\s+\d+-\d+', section_num):
        return None, "range pattern - likely a part identifier"
    
    # Pattern 6: Standard numeric pattern like "1003.1" -> Part "1003"
    standard_match = re.match(r'^(\d+)\.(\d+)', section_num)
    if standard_match:
        part = standard_match.group(1)
        # Sanity check: part numbers typically aren't more than 4 digits
        if len(part) <= 4:
            return part, f"standard pattern"
        else:
            return None, f"part number too long ({len(part)} digits)"
    
    # Pattern 7: Just digits with no dot - could be a part reference itself
    if re.match(r'^\d+$', section_num):
        # If it's just a number with no dot, it might be the part itself
        # But we should be cautious about this
        if len(section_num) <= 4:
            return section_num, "numeric only - possible part"
        else:
            return None, "numeric only - too long for part"
    
    # Pattern 8: Check if it looks like a multi-level section (e.g., "101.1.1")
    if section_num.count('.') > 1:
        # For multi-level, take the first component
        parts = section_num.split('.')
        if parts[0] and re.match(r'^\d+[a-zA-Z]*$', parts[0]):
            if len(parts[0]) <= 5:  # Allow slightly longer for letter suffixes
                return parts[0], "multi-level section"
    
    return None, f"unrecognized pattern: {section_num}"

def identify_problematic_parts(client: bigquery.Client, limit: int = 1000) -> List[Dict]:
    """
    Find sections where the part_num might need updating based on section_num patterns.
    Focuses on titles with known issues from verification.
    """
    
    # Focus on titles with known discrepancies
    problematic_titles = [26, 29, 32, 38, 41, 42, 43, 44, 45, 46, 48, 49]
    
    query = f"""
    WITH analyzed AS (
        SELECT 
            title_num,
            part_num,
            section_num,
            section_citation,
            -- Extract potential part from section_num
            CASE 
                -- Hyphenated parts (101-1.5 -> 101-1)
                WHEN REGEXP_CONTAINS(section_num, r'^\\d+-\\d+\\.') 
                    THEN REGEXP_EXTRACT(section_num, r'^(\\d+-\\d+)\\.')
                -- Letter suffix parts (15a.1 -> 15a)
                WHEN REGEXP_CONTAINS(section_num, r'^\\d+[a-zA-Z]+\\.')
                    THEN REGEXP_EXTRACT(section_num, r'^(\\d+[a-zA-Z]+)\\.')
                -- Special prefix (S 50.1 -> S 50)
                WHEN REGEXP_CONTAINS(section_num, r'^[A-Z]+\\s+\\d+\\.')
                    THEN REGEXP_EXTRACT(section_num, r'^([A-Z]+\\s+\\d+)\\.')
                -- Standard pattern (1003.1 -> 1003)
                WHEN REGEXP_CONTAINS(section_num, r'^\\d{{1,4}}\\.')
                    THEN REGEXP_EXTRACT(section_num, r'^(\\d{{1,4}})\\.')
                ELSE NULL
            END as suggested_part
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE title_num IN ({','.join(map(str, problematic_titles))})
            AND section_num != 'unknown'
    )
    SELECT 
        title_num,
        part_num as current_part,
        suggested_part,
        section_num,
        section_citation,
        COUNT(*) as section_count
    FROM analyzed
    WHERE suggested_part IS NOT NULL 
        AND suggested_part != part_num
    GROUP BY title_num, part_num, suggested_part, section_num, section_citation
    ORDER BY title_num, suggested_part, section_num
    LIMIT {limit}
    """
    
    logger.info("🔍 Analyzing sections for advanced part number patterns...")
    results = client.query(query).to_dataframe()
    
    if not results.empty:
        logger.info(f"Found {len(results)} section groups needing updates")
        
        # Show sample of findings by title
        for title in results['title_num'].unique()[:5]:
            title_data = results[results['title_num'] == title]
            logger.info(f"\n  Title {title}: {len(title_data)} patterns found")
            for _, row in title_data.head(3).iterrows():
                logger.info(f"    {row['section_citation']}: '{row['current_part']}' → '{row['suggested_part']}'")
    
    return results.to_dict('records')

def update_part_numbers_advanced(client: bigquery.Client, dry_run: bool = True) -> Dict:
    """
    Update part numbers using advanced pattern recognition.
    """
    
    # Focus on titles with known issues
    problematic_titles = [26, 29, 32, 38, 41, 42, 43, 44, 45, 46, 48, 49]
    
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
            -- Special prefix (S 50.1 -> S 50)
            WHEN REGEXP_CONTAINS(section_num, r'^[A-Z]+\\s+\\d+\\.')
                THEN REGEXP_EXTRACT(section_num, r'^([A-Z]+\\s+\\d+)\\.')
            -- Standard pattern (1003.1 -> 1003) - only for reasonable lengths
            WHEN REGEXP_CONTAINS(section_num, r'^\\d{{1,4}}\\.')
                THEN REGEXP_EXTRACT(section_num, r'^(\\d{{1,4}})\\.')
            ELSE part_num  -- Keep existing if no pattern matches
        END,
        -- Update citation to match
        section_citation = CONCAT(CAST(title_num AS STRING), ' CFR § ', section_num)
    WHERE 
        title_num IN ({','.join(map(str, problematic_titles))})
        AND section_num != 'unknown'
        AND (
            -- Only update if we can extract a different part
            (REGEXP_CONTAINS(section_num, r'^\\d+-\\d+\\.') 
                AND part_num != REGEXP_EXTRACT(section_num, r'^(\\d+-\\d+)\\.'))
            OR (REGEXP_CONTAINS(section_num, r'^\\d+[a-zA-Z]+\\.')
                AND part_num != REGEXP_EXTRACT(section_num, r'^(\\d+[a-zA-Z]+)\\.'))
            OR (REGEXP_CONTAINS(section_num, r'^[A-Z]+\\s+\\d+\\.')
                AND part_num != REGEXP_EXTRACT(section_num, r'^([A-Z]+\\s+\\d+)\\.'))
            OR (REGEXP_CONTAINS(section_num, r'^\\d{{1,4}}\\.')
                AND part_num != REGEXP_EXTRACT(section_num, r'^(\\d{{1,4}})\\.'))
        )
    """
    
    if dry_run:
        # Count affected rows
        count_query = f"""
        SELECT COUNT(*) as affected_rows
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE 
            title_num IN ({','.join(map(str, problematic_titles))})
            AND section_num != 'unknown'
            AND (
                (REGEXP_CONTAINS(section_num, r'^\\d+-\\d+\\.') 
                    AND part_num != REGEXP_EXTRACT(section_num, r'^(\\d+-\\d+)\\.'))
                OR (REGEXP_CONTAINS(section_num, r'^\\d+[a-zA-Z]+\\.')
                    AND part_num != REGEXP_EXTRACT(section_num, r'^(\\d+[a-zA-Z]+)\\.'))
                OR (REGEXP_CONTAINS(section_num, r'^[A-Z]+\\s+\\d+\\.')
                    AND part_num != REGEXP_EXTRACT(section_num, r'^([A-Z]+\\s+\\d+)\\.'))
                OR (REGEXP_CONTAINS(section_num, r'^\\d{{1,4}}\\.')
                    AND part_num != REGEXP_EXTRACT(section_num, r'^(\\d{{1,4}})\\.'))
            )
        """
        
        result = client.query(count_query).to_dataframe()
        affected_count = result.iloc[0]['affected_rows'] if not result.empty else 0
        logger.info(f"🔍 DRY RUN: Would update {affected_count} sections in problematic titles")
        return {"affected_rows": affected_count, "dry_run": True}
    
    else:
        logger.info("🔧 Executing advanced part number corrections...")
        job = client.query(update_query)
        job.result()  # Wait for completion
        
        logger.info(f"✅ Updated {job.num_dml_affected_rows} sections")
        return {"affected_rows": job.num_dml_affected_rows, "dry_run": False}

def verify_specific_fixes(client: bigquery.Client):
    """
    Verify that specific known issues are fixed.
    """
    test_cases = [
        (26, "15a", "Title 26 Part 15a"),
        (29, "1912a", "Title 29 Part 1912a"), 
        (41, "101-1", "Title 41 Part 101-1"),
        (44, "S 50", "Title 44 Part S 50"),
        (42, "136a", "Title 42 Part 136a")
    ]
    
    logger.info("\n🔍 Verifying specific fixes:")
    
    for title_num, part_num, description in test_cases:
        query = f"""
        SELECT COUNT(*) as count
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE title_num = {title_num} AND part_num = '{part_num}'
        """
        result = client.query(query).to_dataframe()
        count = result.iloc[0]['count'] if not result.empty else 0
        
        if count > 0:
            logger.info(f"  ✅ {description}: {count} sections")
        else:
            logger.info(f"  ❌ {description}: NOT FOUND")

def revert_bad_fixes(client: bigquery.Client, dry_run: bool = True):
    """
    Revert fixes that created too many parts (like Title 43 with 499 parts).
    This handles cases where our initial fix was overly aggressive.
    """
    
    # Title 43 is the most problematic - it should have ~180 parts, not 499
    revert_query = f"""
    UPDATE `{PROJECT_ID}.{DATASET}.{TABLE}`
    SET 
        -- For Title 43, many "parts" are actually subparts of the form "3XXX"
        -- Real parts in Title 43 are typically 1-4 digits or special formats
        part_num = CASE
            -- If section is like "3501.1" and part is "3501", 
            -- but "3501" should really be under a parent part, revert to a more conservative extraction
            WHEN title_num = 43 
                AND LENGTH(part_num) = 4 
                AND part_num LIKE '3%'
                AND section_num LIKE CONCAT(part_num, '.%')
                THEN SUBSTRING(part_num, 1, 2)  -- Take first 2 digits for 3XXX parts
            -- Similar for 2XXX, 4XXX, 5XXX patterns
            WHEN title_num = 43
                AND LENGTH(part_num) = 4
                AND REGEXP_CONTAINS(part_num, r'^[2-9]\\d{{3}}$')
                AND section_num LIKE CONCAT(part_num, '.%')
                THEN SUBSTRING(part_num, 1, 2)
            ELSE part_num
        END
    WHERE title_num = 43
        AND LENGTH(part_num) = 4
        AND REGEXP_CONTAINS(part_num, r'^[2-9]\\d{{3}}$')
    """
    
    if dry_run:
        count_query = f"""
        SELECT COUNT(*) as affected_rows
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE title_num = 43
            AND LENGTH(part_num) = 4
            AND REGEXP_CONTAINS(part_num, r'^[2-9]\\d{{3}}$')
        """
        
        result = client.query(count_query).to_dataframe()
        affected_count = result.iloc[0]['affected_rows'] if not result.empty else 0
        logger.info(f"🔄 DRY RUN: Would revert {affected_count} over-extracted parts in Title 43")
        return {"reverted_rows": affected_count, "dry_run": True}
    
    else:
        logger.info("🔄 Reverting over-aggressive part extractions...")
        job = client.query(revert_query)
        job.result()
        
        logger.info(f"✅ Reverted {job.num_dml_affected_rows} rows")
        return {"reverted_rows": job.num_dml_affected_rows, "dry_run": False}

def main():
    """Main execution function"""
    logger.info("🚀 Starting advanced part number cleanup...")
    
    try:
        client = bigquery.Client(project=PROJECT_ID)
        
        # Step 1: Analyze problematic patterns
        logger.info("\n=== STEP 1: ANALYZING PATTERNS ===")
        issues = identify_problematic_parts(client, limit=100)
        
        if not issues:
            logger.info("✅ No advanced pattern issues found!")
            return
        
        # Step 2: Revert bad fixes first (like Title 43)
        logger.info("\n=== STEP 2: REVERTING OVER-AGGRESSIVE FIXES ===")
        revert_result = revert_bad_fixes(client, dry_run=True)
        if revert_result["reverted_rows"] > 0:
            logger.info(f"Reverting {revert_result['reverted_rows']} over-extracted parts...")
            revert_bad_fixes(client, dry_run=False)
        
        # Step 3: Apply advanced fixes
        logger.info("\n=== STEP 3: DRY RUN ADVANCED FIXES ===")
        dry_result = update_part_numbers_advanced(client, dry_run=True)
        
        if dry_result["affected_rows"] == 0:
            logger.info("✅ No advanced corrections needed!")
            return
        
        # Step 4: Execute the corrections
        logger.info(f"\n=== STEP 4: EXECUTING ADVANCED CORRECTIONS ===")
        logger.info(f"About to update {dry_result['affected_rows']} sections with advanced patterns")
        
        result = update_part_numbers_advanced(client, dry_run=False)
        
        # Step 5: Verification
        logger.info("\n=== STEP 5: VERIFICATION ===")
        verify_specific_fixes(client)
        
        # Final summary
        logger.info(f"\n✅ Advanced cleanup completed!")
        logger.info(f"   - Reverted {revert_result.get('reverted_rows', 0)} over-extracted parts")
        logger.info(f"   - Updated {result['affected_rows']} sections with advanced patterns")
        logger.info(f"   - Now handling letter suffixes, hyphens, and special formats")
        
    except Exception as e:
        logger.error(f"❌ Error during advanced cleanup: {str(e)}")
        raise

if __name__ == "__main__":
    main()