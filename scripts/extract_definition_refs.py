#!/usr/bin/env python3
"""
Extract and structure biblical references from dictionary definitions.

Parses references like:
- (Luke 19:1-10)
- (Gen. 3:1-6)
- (1 John 3:4; Rom. 4:15)
- (Rom. 6:12-17; 7:5-24)

Updates V3 topics with structured definition_refs.
"""

import json
import re
from pathlib import Path
from typing import Any

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
V3_DIR = PROJECT_ROOT / "data" / "dataset" / "topics_v3"

# Book abbreviation mappings (common patterns in dictionaries)
BOOK_PATTERNS = {
    # Old Testament - include full names and abbreviations
    r"Gen(?:esis)?\.?": "Genesis",
    r"Ex(?:od(?:us)?)?\.?": "Exodus",
    r"Lev(?:iticus)?\.?": "Leviticus",
    r"Num(?:bers)?\.?": "Numbers",
    r"Deut(?:eronomy)?\.?|Dt\.?": "Deuteronomy",
    r"Josh(?:ua)?\.?|Jos\.?": "Joshua",
    r"Judg(?:es)?\.?|Jud\.?": "Judges",
    r"Ruth": "Ruth",
    r"1\s*Sam(?:uel)?\.?": "1 Samuel",
    r"2\s*Sam(?:uel)?\.?": "2 Samuel",
    r"1\s*Ki(?:ngs)?\.?|1\s*Kgs\.?": "1 Kings",
    r"2\s*Ki(?:ngs)?\.?|2\s*Kgs\.?": "2 Kings",
    r"1\s*Chr(?:on(?:icles)?)?\.?": "1 Chronicles",
    r"2\s*Chr(?:on(?:icles)?)?\.?": "2 Chronicles",
    r"Ezra": "Ezra",
    r"Neh(?:emiah)?\.?": "Nehemiah",
    r"Esth(?:er)?\.?|Est\.?": "Esther",
    r"Job": "Job",
    r"Ps(?:a(?:lm(?:s)?)?)?\.?": "Psalms",
    r"Prov(?:erbs)?\.?|Pr\.?": "Proverbs",
    r"Eccl(?:esiastes)?\.?|Ecc\.?": "Ecclesiastes",
    r"Song(?:\s*of\s*Solomon)?|Cant\.?|S\.?\s*of\s*S\.?": "Song of Solomon",
    r"Isa(?:iah)?\.?|Is\.?": "Isaiah",
    r"Jer(?:emiah)?\.?": "Jeremiah",
    r"Lam(?:entations)?\.?": "Lamentations",
    r"Ezek(?:iel)?\.?|Eze\.?": "Ezekiel",
    r"Dan(?:iel)?\.?": "Daniel",
    r"Hos(?:ea)?\.?": "Hosea",
    r"Joel": "Joel",
    r"Amos": "Amos",
    r"Obad(?:iah)?\.?|Ob\.?": "Obadiah",
    r"Jon(?:ah)?\.?": "Jonah",
    r"Mic(?:ah)?\.?": "Micah",
    r"Nah(?:um)?\.?": "Nahum",
    r"Hab(?:akkuk)?\.?": "Habakkuk",
    r"Zeph(?:aniah)?\.?": "Zephaniah",
    r"Hag(?:gai)?\.?": "Haggai",
    r"Zech(?:ariah)?\.?|Zec\.?": "Zechariah",
    r"Mal(?:achi)?\.?": "Malachi",
    # New Testament - include full names and abbreviations
    r"Matt(?:hew)?\.?|Mt\.?": "Matthew",
    r"Mark|Mk\.?": "Mark",
    r"Luke|Lk\.?": "Luke",
    r"John|Jn\.?": "John",
    r"Acts": "Acts",
    r"Rom(?:ans)?\.?": "Romans",
    r"1\s*Cor(?:inthians)?\.?": "1 Corinthians",
    r"2\s*Cor(?:inthians)?\.?": "2 Corinthians",
    r"Gal(?:atians)?\.?": "Galatians",
    r"Eph(?:esians)?\.?": "Ephesians",
    r"Phil(?:ippians)?\.?|Php\.?": "Philippians",
    r"Col(?:ossians)?\.?": "Colossians",
    r"1\s*Thess(?:alonians)?\.?|1\s*Th\.?": "1 Thessalonians",
    r"2\s*Thess(?:alonians)?\.?|2\s*Th\.?": "2 Thessalonians",
    r"1\s*Tim(?:othy)?\.?": "1 Timothy",
    r"2\s*Tim(?:othy)?\.?": "2 Timothy",
    r"Tit(?:us)?\.?": "Titus",
    r"Philem(?:on)?\.?|Phm\.?": "Philemon",
    r"Heb(?:rews)?\.?": "Hebrews",
    r"Jam(?:es)?\.?|Jas\.?": "James",
    r"1\s*Pet(?:er)?\.?|1\s*Pe\.?": "1 Peter",
    r"2\s*Pet(?:er)?\.?|2\s*Pe\.?": "2 Peter",
    r"1\s*John|1\s*Jn\.?": "1 John",
    r"2\s*John|2\s*Jn\.?": "2 John",
    r"3\s*John|3\s*Jn\.?": "3 John",
    r"Jude": "Jude",
    r"Rev(?:elation)?\.?": "Revelation",
}

# OT books for testament classification
OT_BOOKS = {
    "Genesis", "Exodus", "Leviticus", "Numbers", "Deuteronomy",
    "Joshua", "Judges", "Ruth", "1 Samuel", "2 Samuel",
    "1 Kings", "2 Kings", "1 Chronicles", "2 Chronicles",
    "Ezra", "Nehemiah", "Esther", "Job", "Psalms", "Proverbs",
    "Ecclesiastes", "Song of Solomon", "Isaiah", "Jeremiah",
    "Lamentations", "Ezekiel", "Daniel", "Hosea", "Joel", "Amos",
    "Obadiah", "Jonah", "Micah", "Nahum", "Habakkuk", "Zephaniah",
    "Haggai", "Zechariah", "Malachi",
}


def normalize_book(abbrev: str) -> str | None:
    """Convert abbreviation to full book name."""
    abbrev = abbrev.strip()
    for pattern, book in BOOK_PATTERNS.items():
        if re.match(f"^{pattern}$", abbrev, re.IGNORECASE):
            return book
    return None


def parse_verse_range(verse_str: str) -> list[int]:
    """Parse verse string like '1-10' or '3,5,7' into list of verses."""
    verses = []
    parts = verse_str.split(',')
    for part in parts:
        part = part.strip()
        if '-' in part:
            try:
                start, end = part.split('-')
                verses.extend(range(int(start), int(end) + 1))
            except:
                pass
        else:
            try:
                verses.append(int(part))
            except:
                pass
    return sorted(set(verses))


def extract_single_ref(ref_str: str, default_book: str | None = None) -> dict | None:
    """
    Extract a single reference like 'Luke 19:1-10' or 'Gen. 3:1-6'.
    
    If default_book is provided, uses it when book is not present in ref_str.
    Returns structured dict or None if can't parse.
    """
    # Pattern: Optional Book + Chapter:Verses
    # Examples: Luke 19:1-10, Gen. 3:1-6, 1 John 3:4, 44:6 (book omitted)
    pattern = r"((?:\d\s*)?[A-Za-z]+\.?)?\s*(\d+):(\d+(?:-\d+)?(?:,\s*\d+(?:-\d+)?)*)"
    
    match = re.search(pattern, ref_str)
    if not match:
        return None
    
    book_abbrev = (match.group(1) or '').strip()
    chapter = match.group(2)
    verses_str = match.group(3)
    
    # Normalize book name - use default if not present
    book = normalize_book(book_abbrev) if book_abbrev else default_book
    if not book:
        return None
    
    # Parse chapter
    try:
        chapter_num = int(chapter)
    except ValueError:
        return None
    
    # Parse verses
    verses = parse_verse_range(verses_str)
    if not verses:
        return None
    
    # Determine testament
    testament = "OT" if book in OT_BOOKS else "NT"
    
    return {
        "book": book,
        "chapter": chapter_num,
        "verses": verses,
        "verse_count": len(verses),
        "testament": testament,
        "raw": ref_str.strip()
    }


def extract_refs_from_text(text: str) -> list[dict]:
    """
    Extract all biblical references from a text.
    
    Handles patterns like:
    - (Luke 19:1-10)
    - (Gen. 3:1-6; Rom. 4:15)
    - (Isa. 41:4; 44:6) - same book, different chapters
    - (Rev. 1:8, 11; 21:6; 22:13) - multiple chapters same book
    - References outside parentheses too
    """
    refs = []
    seen_keys = set()  # Avoid duplicates
    
    def add_ref(ref: dict | None) -> None:
        """Add ref if valid and not duplicate."""
        if ref:
            key = f"{ref['book']}-{ref['chapter']}-{tuple(ref['verses'])}"
            if key not in seen_keys:
                seen_keys.add(key)
                refs.append(ref)
    
    def build_ref_dict(book: str, chapter: int, verses: list[int], raw: str) -> dict:
        """Build a reference dict directly without re-parsing."""
        testament = "OT" if book in OT_BOOKS else "NT"
        return {
            "book": book,
            "chapter": chapter,
            "verses": verses,
            "verse_count": len(verses),
            "testament": testament,
            "raw": raw
        }
    
    # Collect text chunks to search (prioritize parenthetical refs)
    search_texts = []
    
    # Find references in parentheses (most reliable)
    paren_pattern = r"\(([^)]*\d+:\d+[^)]*)\)"
    for match in re.findall(paren_pattern, text):
        search_texts.append(match)
    
    # Also search full text for refs outside parentheses
    search_texts.append(text)
    
    for chunk in search_texts:
        # Split by semicolons - each segment may share a book
        segments = re.split(r';\s*', chunk)
        
        last_book: str | None = None
        
        for seg in segments:
            seg = seg.strip()
            if not seg:
                continue
            
            # Check if segment starts with a book name
            book_match = re.match(r"^((?:\d\s*)?[A-Za-z]+\.?)\s*\d+:", seg)
            if book_match:
                new_book = normalize_book(book_match.group(1))
                if new_book:
                    last_book = new_book
            
            # Find all chapter:verse patterns in this segment
            # Pattern matches: "41:4" or "41:4-6" or "41:4,6,8"
            cv_pattern = r"(\d+):(\d+(?:-\d+)?(?:,\s*\d+(?:-\d+)?)*)"
            
            for cv_match in re.finditer(cv_pattern, seg):
                chapter_str = cv_match.group(1)
                verses_str = cv_match.group(2)
                
                # Check if there's a book right before this chapter:verse
                # Look at the text just before the match
                before_text = seg[:cv_match.start()].strip()
                
                # Try to find book abbreviation at end of before_text
                book_before = re.search(r"((?:\d\s*)?[A-Za-z]+\.?)\s*$", before_text)
                if book_before:
                    found_book = normalize_book(book_before.group(1))
                    if found_book:
                        last_book = found_book
                
                # Build ref directly if we have a book
                if last_book:
                    try:
                        chapter_num = int(chapter_str)
                        verses = parse_verse_range(verses_str)
                        if verses:
                            raw = f"{last_book} {chapter_str}:{verses_str}"
                            ref = build_ref_dict(last_book, chapter_num, verses, raw)
                            add_ref(ref)
                    except ValueError:
                        pass
    
    return refs


def extract_refs_from_definition(definition: dict) -> list[dict]:
    """Extract refs from a single definition entry."""
    text = definition.get("text", "")
    source = definition.get("source", "")
    
    refs = extract_refs_from_text(text)
    
    # Add source to each ref
    for ref in refs:
        ref["source"] = source
    
    return refs


def process_topic(topic_data: dict) -> dict:
    """
    Process a V3 topic and add structured definition_refs.
    
    Updates definitions with extracted_refs and adds topic-level definition_refs.
    """
    definitions = topic_data.get("definitions", [])
    if not definitions:
        return topic_data
    
    all_refs = []
    updated_defs = []
    
    for defn in definitions:
        refs = extract_refs_from_definition(defn)
        
        # Add extracted refs to definition
        updated_def = {**defn}
        if refs:
            updated_def["extracted_refs"] = refs
        updated_defs.append(updated_def)
        
        all_refs.extend(refs)
    
    # Update topic
    topic_data["definitions"] = updated_defs
    
    # Add aggregated definition_refs at topic level
    if all_refs:
        # Deduplicate by book+chapter+verse group
        seen = set()
        unique_refs = []
        for ref in all_refs:
            # Normalize verses by tuple
            verses_tuple = tuple(sorted(ref['verses']))
            key = f"{ref['book']}-{ref['chapter']}-{verses_tuple}"
            if key not in seen:
                seen.add(key)
                unique_refs.append(ref)
        
        topic_data["definition_refs"] = unique_refs
        
        # Stats
        # For stats, count per-verse (expand ranges)
        verse_set = set()
        ot_count = 0
        nt_count = 0
        books = set()
        for r in unique_refs:
            books.add(r['book'])
            for v in r['verses']:
                verse_key = f"{r['book']} {r['chapter']}:{v}"
                if verse_key not in verse_set:
                    verse_set.add(verse_key)
                    if r['testament'] == 'OT':
                        ot_count += 1
                    else:
                        nt_count += 1

        topic_data["definition_refs_stats"] = {
            "total": len(verse_set),
            "ot": ot_count,
            "nt": nt_count,
            "books": sorted(list(books))
        }

        # Rebuild cross_reference_network: align keys to extracted_refs perverse list
        cr = topic_data.get('cross_reference_network', {}) or {}
        # Build set of extracted verse keys
        extracted_keys = set()
        for r in unique_refs:
            for v in r['verses']:
                extracted_keys.add(f"{r['book']} {r['chapter']}:{v}")

        new_cr = {}
        for k in sorted(extracted_keys):
            # keep existing mapping if present; else create empty list
            new_cr[k] = cr.get(k, [])

        topic_data['cross_reference_network'] = new_cr
    
    return topic_data


def main():
    print("=" * 60)
    print("EXTRACTING REFS FROM DEFINITIONS")
    print("=" * 60)
    print()
    
    stats = {
        "total_topics": 0,
        "topics_with_defs": 0,
        "topics_with_refs": 0,
        "total_refs_extracted": 0
    }
    
    for letter_dir in sorted(V3_DIR.iterdir()):
        if not letter_dir.is_dir() or letter_dir.name.startswith('_'):
            continue
        
        for path in letter_dir.glob("*.json"):
            stats["total_topics"] += 1
            
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    topic = json.load(f)
                
                if topic.get("definitions"):
                    stats["topics_with_defs"] += 1
                    
                    # Process and extract refs
                    updated = process_topic(topic)
                    
                    if updated.get("definition_refs"):
                        stats["topics_with_refs"] += 1
                        stats["total_refs_extracted"] += len(updated["definition_refs"])
                    
                    # Save updated topic
                    with open(path, 'w', encoding='utf-8') as f:
                        json.dump(updated, f, indent=2, ensure_ascii=False)
                        
            except Exception as e:
                print(f"  ⚠️ Error processing {path}: {e}")
    
    print(f"📊 Results:")
    print(f"   Total topics:           {stats['total_topics']}")
    print(f"   Topics with definitions: {stats['topics_with_defs']}")
    print(f"   Topics with refs found:  {stats['topics_with_refs']}")
    print(f"   Total refs extracted:    {stats['total_refs_extracted']}")
    
    # Show examples
    print("\n" + "=" * 60)
    print("📝 EXAMPLES")
    print("=" * 60)
    
    for slug in ['zacchaeus', 'sin', 'faith', 'abraham']:
        letter = slug[0].upper()
        path = V3_DIR / letter / f"{slug}.json"
        if path.exists():
            data = json.load(open(path, encoding='utf-8'))
            refs = data.get("definition_refs", [])
            if refs:
                print(f"\n{slug.upper()}:")
                for ref in refs[:3]:
                    print(f"  - {ref['book']} {ref['chapter']}:{ref['verses']} ({ref['testament']})")


if __name__ == '__main__':
    main()
