#!/usr/bin/env python3
"""
Create Unified Topics V3 Dataset.

Merges:
- Topics V2 (Nave + Torrey with reference_groups and ai_enrichment)
- Dictionary (Easton + Smith with definitions)

Output: data/dataset/topics_v3/

Schema V3:
{
    "topic": "SIN",
    "slug": "sin",
    "type": "topic|dictionary|both",
    
    # Base data (Nave + Torrey)
    "reference_groups": [...],
    
    # Definitions (Easton + Smith)
    "definitions": [...],
    
    # AI enrichment (from pipeline)
    "ai_enrichment": {...},
    "ai_themes_normalized": [...],
    
    # Entity links
    "entity_links": [...],
    
    # Phase 1 discovery
    "phase1_discovery": {...},
    
    # Metadata
    "sources": ["NAV", "TOR", "EAS", "SMI"],
    "stats": {...}
}
"""

import json
import re
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict
from typing import Any

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
V2_DIR = PROJECT_ROOT / "data" / "dataset" / "topics_v2"
DICT_DIR = PROJECT_ROOT / "data" / "dataset" / "dictionary"
V3_DIR = PROJECT_ROOT / "data" / "dataset" / "topics_v3"


def slugify(text: str) -> str:
    """Convert text to slug."""
    slug = text.lower()
    slug = re.sub(r'[^\w\s-]', '', slug)
    slug = re.sub(r'[\s_]+', '-', slug)
    slug = slug.strip('-')
    return slug


def load_v2_topics() -> dict[str, dict]:
    """Load all V2 topics into a dict keyed by uppercase name."""
    topics = {}
    
    for letter_dir in V2_DIR.iterdir():
        if not letter_dir.is_dir() or not letter_dir.name.isupper():
            continue
        
        for topic_file in letter_dir.glob("*.json"):
            try:
                with open(topic_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                name = data.get('topic', '').upper()
                if name:
                    topics[name] = data
            except Exception as e:
                print(f"  ⚠️ Error loading {topic_file}: {e}")
    
    return topics


def load_dictionary() -> dict[str, dict]:
    """Load all dictionary entries into a dict keyed by uppercase name."""
    entries = {}
    
    for dict_file in DICT_DIR.glob("*.json"):
        if dict_file.name.startswith('_'):
            continue
        
        try:
            with open(dict_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            for name, entry in data.items():
                entries[name.upper()] = entry
        except Exception as e:
            print(f"  ⚠️ Error loading {dict_file}: {e}")
    
    return entries


def merge_entry(v2_topic: dict | None, dict_entry: dict | None) -> dict:
    """
    Merge V2 topic and dictionary entry into V3 format.
    
    Priority:
    - V2 has reference_groups, ai_enrichment
    - Dictionary has definitions
    """
    v3 = {}
    
    # Determine type and name
    if v2_topic and dict_entry:
        entry_type = "both"
        name = v2_topic.get('topic', dict_entry.get('name', ''))
    elif v2_topic:
        entry_type = "topic"
        name = v2_topic.get('topic', '')
    else:
        entry_type = "dictionary"
        name = dict_entry.get('name', '')
    
    v3['topic'] = name
    v3['slug'] = slugify(name) if name else ''
    v3['type'] = entry_type
    
    # Sources
    sources = []
    
    # --- V2 data ---
    if v2_topic:
        # Sources from V2
        v2_sources = v2_topic.get('sources', {}).get('available', [])
        sources.extend(v2_sources)
        
        # Reference groups (main feature of V2)
        v3['reference_groups'] = v2_topic.get('reference_groups', [])
        
        # Biblical references
        v3['biblical_references'] = v2_topic.get('biblical_references', [])
        
        # AI enrichment
        if 'ai_enrichment' in v2_topic:
            v3['ai_enrichment'] = v2_topic['ai_enrichment']
        
        # AI themes
        if 'ai_themes_normalized' in v2_topic:
            v3['ai_themes_normalized'] = v2_topic['ai_themes_normalized']
        
        # Phase 1 discovery
        if 'phase1_discovery' in v2_topic:
            v3['phase1_discovery'] = v2_topic['phase1_discovery']
        
        # Entity links
        if 'entity_links' in v2_topic:
            v3['entity_links'] = v2_topic['entity_links']
        
        # See also
        if 'see_also' in v2_topic:
            v3['see_also'] = v2_topic['see_also']
        
        # Aliases
        if 'aliases' in v2_topic:
            v3['aliases'] = v2_topic['aliases']
        
        # Stats from V2
        v3['stats'] = v2_topic.get('stats', {})
    else:
        v3['reference_groups'] = []
        v3['biblical_references'] = []
        v3['stats'] = {}
    
    # --- Dictionary data ---
    if dict_entry:
        # Add dictionary sources
        dict_sources = dict_entry.get('sources', [])
        sources.extend(dict_sources)
        
        # Definitions (main feature of dictionary)
        v3['definitions'] = dict_entry.get('definitions', [])
        
        # Scripture refs from dictionary (if no V2 refs)
        if not v3['biblical_references'] and 'scripture_refs' in dict_entry:
            # Convert dictionary refs to standard format
            dict_refs = dict_entry.get('scripture_refs', [])
            v3['dictionary_refs'] = dict_refs  # Keep separate
    else:
        v3['definitions'] = []
    
    # Deduplicate sources
    v3['sources'] = list(dict.fromkeys(sources))
    
    # Metadata
    v3['metadata'] = {
        'schema_version': 'v3.0',
        'created_at': datetime.now(timezone.utc).isoformat(),
        'merged_from': {
            'v2': bool(v2_topic),
            'dictionary': bool(dict_entry)
        }
    }
    
    v3['version'] = '3.0.0'
    
    return v3


def save_v3_topics(topics: list[dict]) -> dict[str, int]:
    """Save V3 topics organized by first letter."""
    V3_DIR.mkdir(parents=True, exist_ok=True)
    
    # Group by first letter
    by_letter = defaultdict(list)
    for topic in topics:
        name = topic.get('topic', '')
        if name:
            first_letter = name[0].upper()
            if first_letter.isalpha():
                by_letter[first_letter].append(topic)
            else:
                by_letter['_'].append(topic)
    
    stats = {
        'total': 0,
        'by_letter': {},
        'by_type': defaultdict(int)
    }
    
    # Save each letter directory
    for letter, letter_topics in sorted(by_letter.items()):
        letter_dir = V3_DIR / letter
        letter_dir.mkdir(exist_ok=True)
        
        for topic in letter_topics:
            slug = topic.get('slug', '')
            if not slug:
                continue
            
            output_file = letter_dir / f"{slug}.json"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(topic, f, indent=2, ensure_ascii=False)
            
            stats['total'] += 1
            stats['by_type'][topic.get('type', 'unknown')] += 1
        
        stats['by_letter'][letter] = len(letter_topics)
    
    # Save index
    index = {
        'total_entries': stats['total'],
        'by_type': dict(stats['by_type']),
        'by_letter': stats['by_letter'],
        'sources': ['NAV', 'TOR', 'EAS', 'SMI'],
        'source_names': {
            'NAV': "Nave's Topical Bible",
            'TOR': "Torrey's New Topical Textbook",
            'EAS': "Easton's Bible Dictionary",
            'SMI': "Smith's Bible Dictionary"
        },
        'created_at': datetime.now(timezone.utc).isoformat(),
        'schema_version': 'v3.0'
    }
    
    with open(V3_DIR / '_index.json', 'w', encoding='utf-8') as f:
        json.dump(index, f, indent=2, ensure_ascii=False)
    
    return stats


def main():
    print("=" * 70)
    print("CREATING UNIFIED TOPICS V3")
    print("=" * 70)
    print()
    
    # Load V2 topics
    print("📚 Loading V2 topics...")
    v2_topics = load_v2_topics()
    print(f"   Loaded {len(v2_topics)} V2 topics")
    
    # Load dictionary
    print("\n📖 Loading dictionary entries...")
    dict_entries = load_dictionary()
    print(f"   Loaded {len(dict_entries)} dictionary entries")
    
    # Find all unique names
    all_names = set(v2_topics.keys()) | set(dict_entries.keys())
    print(f"\n🔗 Total unique entries to process: {len(all_names)}")
    
    # Analyze overlap
    overlap = set(v2_topics.keys()) & set(dict_entries.keys())
    v2_only = set(v2_topics.keys()) - set(dict_entries.keys())
    dict_only = set(dict_entries.keys()) - set(v2_topics.keys())
    
    print(f"\n📊 Overlap analysis:")
    print(f"   Both (topic + dict): {len(overlap)}")
    print(f"   V2 only (topics):    {len(v2_only)}")
    print(f"   Dict only:           {len(dict_only)}")
    
    # Merge all entries
    print("\n🔧 Merging entries...")
    v3_topics = []
    
    for name in sorted(all_names):
        v2_topic = v2_topics.get(name)
        dict_entry = dict_entries.get(name)
        
        merged = merge_entry(v2_topic, dict_entry)
        v3_topics.append(merged)
    
    print(f"   Created {len(v3_topics)} V3 entries")
    
    # Save
    print(f"\n💾 Saving to {V3_DIR}...")
    stats = save_v3_topics(v3_topics)
    
    # Summary
    print("\n" + "=" * 70)
    print("✅ V3 DATASET CREATED")
    print("=" * 70)
    print(f"\n📊 Summary:")
    print(f"   Total entries:     {stats['total']}")
    print(f"   Type 'both':       {stats['by_type'].get('both', 0)}")
    print(f"   Type 'topic':      {stats['by_type'].get('topic', 0)}")
    print(f"   Type 'dictionary': {stats['by_type'].get('dictionary', 0)}")
    
    print(f"\n📁 Output: {V3_DIR}")
    
    # Show examples
    print("\n" + "=" * 70)
    print("📝 SAMPLE ENTRIES")
    print("=" * 70)
    
    examples = ["SIN", "FAITH", "PAUL", "AARON", "JERUSALEM"]
    for name in examples:
        for topic in v3_topics:
            if topic.get('topic') == name:
                print(f"\n{name}:")
                print(f"  Type: {topic.get('type')}")
                print(f"  Sources: {topic.get('sources')}")
                print(f"  Reference groups: {len(topic.get('reference_groups', []))}")
                print(f"  Definitions: {len(topic.get('definitions', []))}")
                print(f"  Has AI enrichment: {'ai_enrichment' in topic}")
                break


if __name__ == '__main__':
    main()
