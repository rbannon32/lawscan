#!/usr/bin/env python3
"""
eCFR Hierarchy Inventory Script
Generates a tree view of all regulatory assets in the BigQuery table
"""

import os
import argparse
from collections import defaultdict
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

# Configuration
PROJECT_ID = os.getenv("PROJECT_ID", "lawscan")
DATASET = os.getenv("DATASET", "ecfr_enhanced") 
TABLE = os.getenv("TABLE", "sections_enhanced")

def get_hierarchy_data(client, date="2025-08-22"):
    """Fetch hierarchy data from BigQuery."""
    sql = f"""
    SELECT 
        title_num,
        title_name,
        chapter_id,
        chapter_label,
        part_num,
        part_label,
        subpart_id,
        subpart_label,
        section_citation,
        section_heading,
        word_count,
        regulatory_burden_score,
        prohibition_count,
        requirement_count,
        enforcement_terms,
        reserved
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE version_date = DATE(@date)
    ORDER BY title_num, part_num, section_order
    """
    
    job = client.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("date", "STRING", date)]
    ))
    
    return list(job.result())

def build_hierarchy_tree(rows):
    """Build a nested hierarchy tree from flat section data."""
    tree = defaultdict(lambda: {
        'info': {},
        'chapters': defaultdict(lambda: {
            'info': {},
            'parts': defaultdict(lambda: {
                'info': {},
                'subparts': defaultdict(lambda: {
                    'info': {},
                    'sections': []
                })
            })
        })
    })
    
    # Track counts for each level
    title_stats = defaultdict(lambda: {
        'sections': 0, 'words': 0, 'burden_sum': 0, 'prohibitions': 0, 
        'requirements': 0, 'enforcement': 0, 'reserved': 0
    })
    
    for row in rows:
        title_num = row.title_num
        title_name = row.title_name or f"Title {title_num}"
        chapter_id = row.chapter_id or "Main"
        chapter_label = row.chapter_label or f"Chapter {chapter_id}"
        part_num = row.part_num or "Unknown"
        part_label = row.part_label or f"Part {part_num}"
        subpart_id = row.subpart_id or "Main" 
        subpart_label = row.subpart_label or f"Subpart {subpart_id}"
        
        # Store hierarchy info
        tree[title_num]['info'] = {'name': title_name}
        tree[title_num]['chapters'][chapter_id]['info'] = {'label': chapter_label}
        tree[title_num]['chapters'][chapter_id]['parts'][part_num]['info'] = {'label': part_label}
        tree[title_num]['chapters'][chapter_id]['parts'][part_num]['subparts'][subpart_id]['info'] = {'label': subpart_label}
        
        # Add section
        section_info = {
            'citation': row.section_citation,
            'heading': row.section_heading or 'No heading',
            'word_count': row.word_count or 0,
            'burden_score': row.regulatory_burden_score or 0.0,
            'prohibitions': row.prohibition_count or 0,
            'requirements': row.requirement_count or 0,
            'enforcement': row.enforcement_terms or 0,
            'reserved': row.reserved or False
        }
        
        tree[title_num]['chapters'][chapter_id]['parts'][part_num]['subparts'][subpart_id]['sections'].append(section_info)
        
        # Update stats
        stats = title_stats[title_num]
        stats['sections'] += 1
        stats['words'] += section_info['word_count']
        stats['burden_sum'] += section_info['burden_score']
        stats['prohibitions'] += section_info['prohibitions']
        stats['requirements'] += section_info['requirements']
        stats['enforcement'] += section_info['enforcement']
        if section_info['reserved']:
            stats['reserved'] += 1
    
    return tree, title_stats

def print_tree(tree, title_stats, show_sections=False, max_titles=None):
    """Print the hierarchy tree in a readable format."""
    
    print("=" * 80)
    print("eCFR REGULATORY HIERARCHY INVENTORY")
    print("=" * 80)
    
    sorted_titles = sorted(tree.keys())
    if max_titles:
        sorted_titles = sorted_titles[:max_titles]
    
    total_sections = sum(stats['sections'] for stats in title_stats.values())
    total_words = sum(stats['words'] for stats in title_stats.values())
    
    print(f"\n📊 SUMMARY: {len(tree)} titles, {total_sections:,} sections, {total_words:,} total words")
    print("-" * 80)
    
    for title_num in sorted_titles:
        title_data = tree[title_num]
        stats = title_stats[title_num]
        
        avg_burden = stats['burden_sum'] / max(1, stats['sections'])
        
        print(f"\n📖 TITLE {title_num}: {title_data['info']['name']}")
        print(f"   📈 {stats['sections']:,} sections • {stats['words']:,} words • Avg burden: {avg_burden:.1f}")
        print(f"   ⚖️  {stats['requirements']} requirements • {stats['prohibitions']} prohibitions • {stats['enforcement']} enforcement terms")
        if stats['reserved'] > 0:
            print(f"   🚫 {stats['reserved']} reserved sections")
        
        chapters = title_data['chapters']
        for chapter_id in sorted(chapters.keys()):
            chapter_data = chapters[chapter_id]
            parts_count = len(chapter_data['parts'])
            
            if len(chapters) > 1:  # Only show chapter if there are multiple
                print(f"   └── 📂 {chapter_data['info']['label']} ({parts_count} parts)")
                indent = "       "
            else:
                indent = "   "
            
            parts = chapter_data['parts']
            for part_num in sorted(parts.keys(), key=lambda x: int(x) if x.isdigit() else 999999):
                part_data = parts[part_num]
                subparts = part_data['subparts']
                
                # Count total sections in this part
                part_sections = sum(len(sub['sections']) for sub in subparts.values())
                part_words = sum(sum(s['word_count'] for s in sub['sections']) for sub in subparts.values())
                
                print(f"{indent}└── 📁 Part {part_num}: {part_data['info']['label']}")
                print(f"{indent}    📊 {part_sections} sections • {part_words:,} words")
                
                if show_sections:
                    for subpart_id in sorted(subparts.keys()):
                        subpart_data = subparts[subpart_id]
                        sections = subpart_data['sections']
                        
                        if len(subparts) > 1 or subpart_id != "Main":
                            print(f"{indent}    └── 📄 {subpart_data['info']['label']} ({len(sections)} sections)")
                            section_indent = f"{indent}        "
                        else:
                            section_indent = f"{indent}    "
                        
                        for section in sections[:10]:  # Limit to first 10 sections
                            burden_indicator = "🔴" if section['burden_score'] > 50 else "🟡" if section['burden_score'] > 25 else "🟢"
                            reserved_indicator = "🚫" if section['reserved'] else ""
                            
                            print(f"{section_indent}• {section['citation']} - {section['heading'][:60]}{'...' if len(section['heading']) > 60 else ''}")
                            if section['word_count'] > 0:
                                print(f"{section_indent}  {burden_indicator} Burden: {section['burden_score']:.1f} • {section['word_count']} words • {section['requirements']} req • {section['prohibitions']} prohib {reserved_indicator}")
                        
                        if len(sections) > 10:
                            print(f"{section_indent}... and {len(sections) - 10} more sections")

def print_summary_stats(title_stats):
    """Print summary statistics."""
    print("\n" + "=" * 80)
    print("REGULATORY BURDEN ANALYSIS")
    print("=" * 80)
    
    for title_num in sorted(title_stats.keys()):
        stats = title_stats[title_num]
        if stats['sections'] == 0:
            continue
            
        avg_burden = stats['burden_sum'] / stats['sections']
        burden_level = "🔴 HIGH" if avg_burden > 50 else "🟡 MED" if avg_burden > 25 else "🟢 LOW"
        
        print(f"Title {title_num:2d}: {burden_level} burden ({avg_burden:5.1f}) | "
              f"{stats['sections']:4d} sections | {stats['requirements']:3d} req | "
              f"{stats['prohibitions']:3d} prohib | {stats['enforcement']:3d} enforce")

def main():
    parser = argparse.ArgumentParser(description="Generate eCFR hierarchy inventory")
    parser.add_argument("--date", default="2025-08-22", help="Version date (YYYY-MM-DD)")
    parser.add_argument("--sections", action="store_true", help="Show individual sections")
    parser.add_argument("--max-titles", type=int, help="Limit number of titles shown")
    parser.add_argument("--stats-only", action="store_true", help="Show only summary statistics")
    
    args = parser.parse_args()
    
    print(f"🔍 Fetching eCFR hierarchy data for {args.date}...")
    
    client = bigquery.Client(project=PROJECT_ID)
    rows = get_hierarchy_data(client, args.date)
    
    if not rows:
        print(f"❌ No data found for date {args.date}")
        return
    
    print(f"✅ Found {len(rows):,} sections")
    
    tree, title_stats = build_hierarchy_tree(rows)
    
    if not args.stats_only:
        print_tree(tree, title_stats, show_sections=args.sections, max_titles=args.max_titles)
    
    print_summary_stats(title_stats)
    
    print(f"\n🎯 Data source: {PROJECT_ID}.{DATASET}.{TABLE} (date: {args.date})")

if __name__ == "__main__":
    main()