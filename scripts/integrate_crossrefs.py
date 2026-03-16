#!/usr/bin/env python3
"""
Integrate Cross-References into V3 Topics.

For each topic's biblical references, finds cross-references and:
1. Adds cross_reference_network to each topic
2. Calculates connected_topics based on shared refs
3. Computes cross-ref statistics

Cross-ref files: data/dataset/cross_references/unified/verses/{LETTER}/{BOOK.CHAPTER.VERSE}.json
V3 refs format: {"book": "1 John", "chapter": 3, "verses": [4]}
"""

import json
from pathlib import Path
from collections import defaultdict, Counter
from typing import Any

# Paths
REPO_ROOT = Path(__file__).parent.parent
V3_DIR = REPO_ROOT / "data" / "01_unified"
# NOTE: CROSSREF_DIR is an external dependency from bible-crossrefs-dataset.
#       Adjust this path to point to the local clone of that repo.
CROSSREF_DIR = REPO_ROOT / ".." / "bible-crossrefs-dataset" / "data" / "01_unified" / "verses"

# Book name to cross-ref abbreviation mapping
BOOK_TO_ABBREV = {
    # Old Testament
    "Genesis": "GEN", "Gen": "GEN",
    "Exodus": "EXO", "Exod": "EXO", "Ex": "EXO",
    "Leviticus": "LEV", "Lev": "LEV",
    "Numbers": "NUM", "Num": "NUM",
    "Deuteronomy": "DEU", "Deut": "DEU", "Dt": "DEU",
    "Joshua": "JOS", "Josh": "JOS",
    "Judges": "JDG", "Judg": "JDG", "Jud": "JDG",
    "Ruth": "RUT",
    "1 Samuel": "1SA", "1 Sam": "1SA", "1Sam": "1SA",
    "2 Samuel": "2SA", "2 Sam": "2SA", "2Sam": "2SA",
    "1 Kings": "1KI", "1 Kgs": "1KI", "1Kgs": "1KI",
    "2 Kings": "2KI", "2 Kgs": "2KI", "2Kgs": "2KI",
    "1 Chronicles": "1CH", "1 Chr": "1CH", "1Chr": "1CH",
    "2 Chronicles": "2CH", "2 Chr": "2CH", "2Chr": "2CH",
    "Ezra": "EZR", "Ezr": "EZR",
    "Nehemiah": "NEH", "Neh": "NEH",
    "Esther": "EST", "Esth": "EST",
    "Job": "JOB",
    "Psalms": "PSA", "Ps": "PSA", "Psalm": "PSA",
    "Proverbs": "PRO", "Prov": "PRO", "Pr": "PRO",
    "Ecclesiastes": "ECC", "Eccl": "ECC", "Ecc": "ECC",
    "Song of Solomon": "SNG", "Song": "SNG", "So": "SNG", "Cant": "SNG",
    "Isaiah": "ISA", "Isa": "ISA", "Is": "ISA",
    "Jeremiah": "JER", "Jer": "JER",
    "Lamentations": "LAM", "Lam": "LAM",
    "Ezekiel": "EZK", "Ezek": "EZK", "Eze": "EZK",
    "Daniel": "DAN", "Dan": "DAN",
    "Hosea": "HOS", "Hos": "HOS",
    "Joel": "JOL", "Joel": "JOL",
    "Amos": "AMO", "Amos": "AMO",
    "Obadiah": "OBA", "Obad": "OBA", "Ob": "OBA",
    "Jonah": "JON", "Jon": "JON",
    "Micah": "MIC", "Mic": "MIC",
    "Nahum": "NAM", "Nah": "NAM",
    "Habakkuk": "HAB", "Hab": "HAB",
    "Zephaniah": "ZEP", "Zeph": "ZEP",
    "Haggai": "HAG", "Hag": "HAG",
    "Zechariah": "ZEC", "Zech": "ZEC", "Zec": "ZEC",
    "Malachi": "MAL", "Mal": "MAL",
    # New Testament
    "Matthew": "MAT", "Matt": "MAT", "Mt": "MAT",
    "Mark": "MRK", "Mk": "MRK", "Mr": "MRK",
    "Luke": "LUK", "Lk": "LUK", "Lu": "LUK",
    "John": "JHN", "Jn": "JHN", "Joh": "JHN",
    "Acts": "ACT", "Ac": "ACT",
    "Romans": "ROM", "Rom": "ROM", "Ro": "ROM",
    "1 Corinthians": "1CO", "1 Cor": "1CO", "1Cor": "1CO",
    "2 Corinthians": "2CO", "2 Cor": "2CO", "2Cor": "2CO",
    "Galatians": "GAL", "Gal": "GAL",
    "Ephesians": "EPH", "Eph": "EPH",
    "Philippians": "PHP", "Phil": "PHP", "Php": "PHP",
    "Colossians": "COL", "Col": "COL",
    "1 Thessalonians": "1TH", "1 Thess": "1TH", "1Thess": "1TH",
    "2 Thessalonians": "2TH", "2 Thess": "2TH", "2Thess": "2TH",
    "1 Timothy": "1TI", "1 Tim": "1TI", "1Tim": "1TI",
    "2 Timothy": "2TI", "2 Tim": "2TI", "2Tim": "2TI",
    "Titus": "TIT", "Tit": "TIT",
    "Philemon": "PHM", "Philem": "PHM", "Phm": "PHM",
    "Hebrews": "HEB", "Heb": "HEB",
    "James": "JAS", "Jas": "JAS", "Jam": "JAS",
    "1 Peter": "1PE", "1 Pet": "1PE", "1Pet": "1PE",
    "2 Peter": "2PE", "2 Pet": "2PE", "2Pet": "2PE",
    "1 John": "1JN", "1 Jn": "1JN", "1Jn": "1JN",
    "2 John": "2JN", "2 Jn": "2JN", "2Jn": "2JN",
    "3 John": "3JN", "3 Jn": "3JN", "3Jn": "3JN",
    "Jude": "JUD",
    "Revelation": "REV", "Rev": "REV",
}


def get_crossref_path(book: str, chapter: int, verse: int) -> Path | None:
    """Get the path to cross-ref file for a verse."""
    abbrev = BOOK_TO_ABBREV.get(book)
    if not abbrev:
        return None
    
    filename = f"{abbrev}.{chapter}.{verse}.json"
    first_char = abbrev[0].upper()
    
    # Handle numeric prefixes (1CH, 2CH, etc.)
    if abbrev[0].isdigit():
        first_char = abbrev[0]
    
    path = CROSSREF_DIR / first_char / filename
    return path if path.exists() else None


def load_crossrefs_for_verse(book: str, chapter: int, verse: int) -> list[dict]:
    """Load cross-references for a specific verse."""
    path = get_crossref_path(book, chapter, verse)
    if not path:
        return []
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data.get('cross_references', [])
    except Exception:
        return []


def collect_topic_refs(topic_data: dict) -> list[tuple[str, int, int]]:
    """
    Collect all biblical references from a topic.
    Returns list of (book, chapter, verse) tuples.
    """
    refs = []
    seen = set()
    
    # From biblical_references
    for ref in topic_data.get('biblical_references', []):
        book = ref.get('book', '')
        chapter = ref.get('chapter', 0)
        verses = ref.get('verses', [])
        
        for v in verses:
            key = (book, chapter, v)
            if key not in seen:
                seen.add(key)
                refs.append(key)
    
    # From reference_groups
    for rg in topic_data.get('reference_groups', []):
        for ref in rg.get('ot_references', []) + rg.get('nt_references', []):
            if isinstance(ref, dict):
                book = ref.get('book', '')
                chapter = ref.get('chapter', 0)
                verses = ref.get('verses', [])
                
                for v in verses:
                    key = (book, chapter, v)
                    if key not in seen:
                        seen.add(key)
                        refs.append(key)
    
    # From definition_refs
    for ref in topic_data.get('definition_refs', []):
        book = ref.get('book', '')
        chapter = ref.get('chapter', 0)
        verses = ref.get('verses', [])
        
        for v in verses:
            key = (book, chapter, v)
            if key not in seen:
                seen.add(key)
                refs.append(key)
    
    return refs


def process_topic(topic_data: dict, verse_to_topics: dict) -> dict:
    """
    Process a topic and add cross-reference data.
    
    Adds:
    - cross_reference_network: {verse -> [cross_refs]}
    - cross_ref_stats: statistics about cross-refs
    """
    topic_name = topic_data.get('topic', '')
    refs = collect_topic_refs(topic_data)
    
    if not refs:
        return topic_data
    
    # Collect cross-refs for all verses
    cross_ref_network = {}
    all_target_verses = []
    total_crossrefs = 0
    strong_refs = 0
    
    for book, chapter, verse in refs:
        crossrefs = load_crossrefs_for_verse(book, chapter, verse)
        if not crossrefs:
            continue
        
        verse_key = f"{book} {chapter}:{verse}"
        
        # Filter and format cross-refs
        formatted_refs = []
        for cr in crossrefs[:20]:  # Limit per verse
            to_verse = cr.get('to_verse', '')
            score = cr.get('score', 0)
            votes = cr.get('votes', 0)
            strength = cr.get('connection_strength', 'weak')
            
            if not to_verse:
                continue
            
            formatted_refs.append({
                'to_verse': to_verse,
                'score': score,
                'votes': votes,
                'strength': strength
            })
            
            all_target_verses.append(to_verse)
            total_crossrefs += 1
            
            if strength in ['strong', 'medium'] or score >= 5:
                strong_refs += 1
        
        if formatted_refs:
            cross_ref_network[verse_key] = formatted_refs
    
    # Add to topic
    if cross_ref_network:
        topic_data['cross_reference_network'] = cross_ref_network
        
        # Statistics
        unique_targets = len(set(all_target_verses))
        topic_data['cross_ref_stats'] = {
            'source_verses': len(refs),
            'verses_with_crossrefs': len(cross_ref_network),
            'total_crossrefs': total_crossrefs,
            'unique_target_verses': unique_targets,
            'strong_refs': strong_refs,
            'coverage': round(len(cross_ref_network) / len(refs) * 100, 1) if refs else 0
        }
        
        # Register this topic's target verses for connected_topics calculation
        for target in all_target_verses:
            verse_to_topics[target].append(topic_name)
    
    return topic_data


def calculate_connected_topics(v3_topics: dict[str, dict], verse_to_topics: dict) -> None:
    """
    Calculate connected topics based on shared cross-reference targets.
    
    Two topics are connected if their cross-refs point to the same verses.
    """
    for topic_name, topic_data in v3_topics.items():
        network = topic_data.get('cross_reference_network', {})
        if not network:
            continue
        
        # Get all target verses for this topic
        target_verses = set()
        for refs in network.values():
            for ref in refs:
                target_verses.add(ref['to_verse'])
        
        # Find other topics that share these targets
        connected = Counter()
        for target in target_verses:
            for other_topic in verse_to_topics[target]:
                if other_topic != topic_name:
                    connected[other_topic] += 1
        
        # Keep top 20 most connected
        if connected:
            top_connected = [
                {'topic': t, 'shared_refs': c, 'strength': 'strong' if c >= 10 else 'medium' if c >= 5 else 'weak'}
                for t, c in connected.most_common(20)
            ]
            topic_data['connected_topics'] = top_connected


def main():
    print("=" * 70)
    print("INTEGRATING CROSS-REFERENCES INTO V3 TOPICS")
    print("=" * 70)
    print()
    
    # Load all V3 topics into memory for connected_topics calculation
    print("📚 Loading V3 topics...")
    v3_topics = {}
    topic_paths = {}
    
    for letter_dir in V3_DIR.iterdir():
        if not letter_dir.is_dir() or letter_dir.name.startswith('_'):
            continue
        for path in letter_dir.glob('*.json'):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    topic = json.load(f)
                topic_name = topic.get('topic', '')
                if topic_name:
                    v3_topics[topic_name] = topic
                    topic_paths[topic_name] = path
            except Exception as e:
                print(f"  ⚠️ Error loading {path}: {e}")
    
    print(f"   Loaded {len(v3_topics)} topics")
    
    # Track which topics share cross-ref targets
    verse_to_topics = defaultdict(list)
    
    # Process each topic
    print("\n🔗 Processing cross-references...")
    processed = 0
    with_crossrefs = 0
    
    for topic_name, topic_data in v3_topics.items():
        process_topic(topic_data, verse_to_topics)
        processed += 1
        
        if topic_data.get('cross_reference_network'):
            with_crossrefs += 1
        
        if processed % 1000 == 0:
            print(f"   Processed {processed} topics...")
    
    print(f"   Done! {with_crossrefs}/{processed} topics have cross-refs")
    
    # Calculate connected topics
    print("\n🕸️ Calculating connected topics...")
    calculate_connected_topics(v3_topics, verse_to_topics)
    
    topics_with_connections = sum(1 for t in v3_topics.values() if t.get('connected_topics'))
    print(f"   {topics_with_connections} topics have connected_topics")
    
    # Save updated topics
    print("\n💾 Saving updated topics...")
    for topic_name, topic_data in v3_topics.items():
        path = topic_paths[topic_name]
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(topic_data, f, indent=2, ensure_ascii=False)
    
    # Summary
    print("\n" + "=" * 70)
    print("✅ INTEGRATION COMPLETE")
    print("=" * 70)
    
    # Calculate totals
    total_crossrefs = sum(
        t.get('cross_ref_stats', {}).get('total_crossrefs', 0) 
        for t in v3_topics.values()
    )
    total_connections = sum(
        len(t.get('connected_topics', [])) 
        for t in v3_topics.values()
    )
    
    print(f"\n📊 Summary:")
    print(f"   Topics processed:       {processed}")
    print(f"   Topics with cross-refs: {with_crossrefs}")
    print(f"   Total cross-refs added: {total_crossrefs}")
    print(f"   Topics with connections: {topics_with_connections}")
    print(f"   Total connections:      {total_connections}")


if __name__ == '__main__':
    main()
