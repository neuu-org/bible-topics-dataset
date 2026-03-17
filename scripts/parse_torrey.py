#!/usr/bin/env python3
"""
Parse Torrey's Topical Textbook from CCEL ThML XML.

Reads:  data/00_raw/xml/torrey_ttt.xml
Writes: data/02_sources/torrey/A-Z/*.json

Extracts aspects with references directly from XML structure (scripRef tags),
avoiding text-based parsing that loses data.

Usage:
    python scripts/parse_torrey.py --all
    python scripts/parse_torrey.py --all --verbose
    python scripts/parse_torrey.py --topic "ANGER OF GOD, THE"
"""

import argparse
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup, NavigableString

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).parent.parent
XML_FILE = REPO_ROOT / "data" / "00_raw" / "xml" / "torrey_ttt.xml"
OUTPUT_DIR = REPO_ROOT / "data" / "02_sources" / "torrey"

# ---------------------------------------------------------------------------
# Book abbreviation → canonical name (shared with clean_sources.py)
# ---------------------------------------------------------------------------
BOOK_PATTERNS: dict[str, str] = {
    # Old Testament
    r"Ge(?:n(?:esis)?)?\.?": "Genesis",
    r"Ex(?:od(?:us)?)?\.?": "Exodus",
    r"Le(?:v(?:iticus)?)?\.?": "Leviticus",
    r"Nu(?:m(?:bers)?)?\.?|Nb\.?": "Numbers",
    r"De(?:ut(?:eronomy)?)?\.?|Dt\.?": "Deuteronomy",
    r"Jos(?:h(?:ua)?)?\.?": "Joshua",
    r"Jud(?:g(?:es)?)?\.?|Jg\.?|Jdj\.?": "Judges",
    r"Ru(?:th)?\.?": "Ruth",
    r"1\s*Sa(?:m(?:uel)?)?\.?": "1 Samuel",
    r"2\s*Sa(?:m(?:uel)?)?\.?": "2 Samuel",
    r"1\s*Ki(?:ngs)?\.?|1\s*Kgs\.?": "1 Kings",
    r"2\s*Ki(?:ngs)?\.?|2\s*Kgs\.?": "2 Kings",
    r"1\s*Ch(?:r(?:on(?:icles)?)?)?\.?": "1 Chronicles",
    r"2\s*Ch(?:r(?:on(?:icles)?)?)?\.?": "2 Chronicles",
    r"Ezr(?:a)?\.?": "Ezra",
    r"Ne(?:h(?:emiah)?)?\.?": "Nehemiah",
    r"Es(?:th?(?:er)?)?\.?": "Esther",
    r"Jo(?:b)?\.?": "Job",
    r"Ps(?:a(?:lm(?:s)?)?)?\.?": "Psalms",
    r"Pr(?:ov?(?:erbs)?)?\.?": "Proverbs",
    r"Ec(?:cl?(?:esiastes)?)?\.?": "Ecclesiastes",
    r"So(?:ng)?(?:\s*of\s*Sol(?:omon)?)?|Ca(?:nt)?\.?|SS\.?": "Song of Solomon",
    r"Isa(?:iah)?\.?|Is\.?": "Isaiah",
    r"Jer(?:emiah)?\.?|Je\.?": "Jeremiah",
    r"La(?:m(?:entations)?)?\.?": "Lamentations",
    r"Eze(?:k(?:iel)?)?\.?": "Ezekiel",
    r"Da(?:n(?:iel)?)?\.?": "Daniel",
    r"Ho(?:s(?:ea)?)?\.?": "Hosea",
    r"Joe(?:l)?\.?": "Joel",
    r"Am(?:os)?\.?": "Amos",
    r"Ob(?:ad(?:iah)?)?\.?": "Obadiah",
    r"Jon(?:ah)?\.?": "Jonah",
    r"Mi(?:c(?:ah)?)?\.?": "Micah",
    r"Na(?:h(?:um)?)?\.?": "Nahum",
    r"Hab(?:akkuk)?\.?": "Habakkuk",
    r"Zep(?:h(?:aniah)?)?\.?": "Zephaniah",
    r"Hag(?:gai)?\.?": "Haggai",
    r"Zec(?:h(?:ariah)?)?\.?": "Zechariah",
    r"Mal(?:achi)?\.?": "Malachi",
    # New Testament
    r"Mt\.?|Mat(?:t(?:hew)?)?\.?": "Matthew",
    r"Mr\.?|Mk\.?|Mar(?:k)?\.?": "Mark",
    r"Lu(?:ke)?\.?|Lk\.?": "Luke",
    r"Joh(?:n)?\.?|Jn\.?": "John",
    r"Ac(?:ts)?\.?": "Acts",
    r"Ro(?:m(?:ans)?)?\.?": "Romans",
    r"1\s*Co(?:r(?:inthians)?)?\.?": "1 Corinthians",
    r"2\s*Co(?:r(?:inthians)?)?\.?": "2 Corinthians",
    r"Ga(?:l(?:atians)?)?\.?": "Galatians",
    r"Eph(?:esians)?\.?": "Ephesians",
    r"Ph(?:il(?:ippians)?|p)\.?": "Philippians",
    r"Col(?:ossians)?\.?": "Colossians",
    r"1\s*Th(?:ess?(?:alonians)?)?\.?": "1 Thessalonians",
    r"2\s*Th(?:ess?(?:alonians)?)?\.?": "2 Thessalonians",
    r"1\s*Ti(?:m(?:othy)?)?\.?": "1 Timothy",
    r"2\s*Ti(?:m(?:othy)?)?\.?": "2 Timothy",
    r"Tit(?:us)?\.?": "Titus",
    r"Ph(?:ile)?m(?:on)?\.?": "Philemon",
    r"Heb(?:rews)?\.?": "Hebrews",
    r"Ja(?:s|m(?:es)?)\.?": "James",
    r"1\s*Pe(?:t(?:er)?)?\.?": "1 Peter",
    r"2\s*Pe(?:t(?:er)?)?\.?": "2 Peter",
    r"1\s*Jo(?:hn?)?\.?|1\s*Jn\.?": "1 John",
    r"2\s*Jo(?:hn?)?\.?|2\s*Jn\.?": "2 John",
    r"3\s*Jo(?:hn?)?\.?|3\s*Jn\.?": "3 John",
    r"Jude": "Jude",
    r"Re(?:v(?:elation)?)?\.?": "Revelation",
}

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
    """Convert abbreviation to full canonical book name."""
    abbrev = abbrev.strip()
    for pattern, book in BOOK_PATTERNS.items():
        if re.match(f"^{pattern}$", abbrev, re.IGNORECASE):
            return book
    return None


def parse_verse_range(verse_str: str) -> list[int]:
    """Parse verse string like '1-10' or '3,5,7' into sorted list of verses."""
    verses: list[int] = []
    for part in verse_str.split(","):
        part = part.strip()
        if "-" in part or "\u2013" in part:
            parts = re.split(r"[-\u2013]", part)
            try:
                start, end = int(parts[0]), int(parts[1])
                verses.extend(range(start, end + 1))
            except (ValueError, IndexError):
                pass
        else:
            try:
                verses.append(int(part))
            except ValueError:
                pass
    return sorted(set(verses))


def normalize_passage(passage: str) -> tuple[str | None, str | None]:
    """Normalize a passage string like 'Ro 9:18' → ('Romans', '9:18').

    Returns (canonical_book, chapter_verse) or (None, None) if unparseable.
    """
    passage = passage.strip()
    # Match: optional number prefix + book name + space + chapter:verse
    m = re.match(r"^(\d?\s*[A-Za-z]+)\s+(\d+.*)$", passage)
    if not m:
        return None, None
    book_abbrev = m.group(1).strip()
    cv_part = m.group(2).strip()
    canonical = normalize_book(book_abbrev)
    if not canonical:
        return None, None
    return canonical, cv_part


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def clean_topic_name(topic_name: str) -> str:
    """Clean and normalize topic name."""
    topic_name = topic_name.strip()
    topic_name = re.sub(r"[.,;:]+$", "", topic_name)
    if topic_name.startswith("The "):
        topic_name = topic_name[4:]
    return topic_name.strip()


def create_topic_slug(topic_name: str) -> str:
    """Create URL-friendly slug from topic name."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", topic_name.lower().strip())


# ---------------------------------------------------------------------------
# XML extraction
# ---------------------------------------------------------------------------

def extract_all_topics(soup: BeautifulSoup) -> list[str]:
    """Extract all topic names from XML glossary elements."""
    topics: list[str] = []
    for glossary in soup.find_all("glossary"):
        for term in glossary.find_all("term"):
            topic_name = term.get_text(strip=True)
            if topic_name:
                topic_name = clean_topic_name(topic_name)
                if topic_name:
                    topics.append(topic_name.upper())
    return sorted(set(topics))


def extract_see_also_from_def(def_element) -> list[str]:
    """Extract see_also links from definition."""
    see_also: list[str] = []
    for link in def_element.find_all("a", href=True):
        href = link.get("href", "")
        link_text = link.get_text(strip=True)
        if "ttt." in href and "?term=" in href:
            term = href.split("?term=")[1]
            term = term.replace("+", " ").replace("%20", " ")
            term = clean_topic_name(term)
            if term:
                see_also.append(term.upper())
        elif link_text and len(link_text) > 2:
            cleaned_text = clean_topic_name(link_text)
            if cleaned_text and len(cleaned_text) > 2:
                see_also.append(cleaned_text.upper())
    return sorted(set(see_also))


def _extract_label_from_p(p_element) -> str:
    """Extract the aspect label from a <p> — text before the first scripRef."""
    parts: list[str] = []
    for child in p_element.children:
        if isinstance(child, NavigableString):
            parts.append(str(child))
        elif child.name and child.name.lower() == "scripref":
            break  # refs start here
        elif child.name == "a":
            # See-also link text — include it in label
            parts.append(child.get_text())
        else:
            parts.append(child.get_text())
    label = "".join(parts).strip()
    # Clean leading/trailing separators and dots
    label = re.sub(r"^[.\s]+", "", label)
    label = re.sub(r"[\s\u2014\u2013—\-;,.\s]+$", "", label)
    return label.strip()


def _extract_refs_from_p(p_element) -> list[str]:
    """Extract normalized reference strings from scripRef tags within a <p>.

    Groups consecutive same-book-same-chapter refs into a single string.
    Example: scripRef(Ro 9:18), scripRef(Ro 9:20), scripRef(Ro 9:22)
             → ["Romans 9:18,20,22"]
    """
    # Collect all parsed refs from scripRef tags in this <p>
    raw_refs: list[tuple[str, int, str]] = []  # (book, chapter, verse_part)
    for sr in p_element.find_all(lambda tag: tag.name and tag.name.lower() == "scripref"):
        passage = sr.get("passage", "")
        if not passage:
            # Try to reconstruct from parsed attr
            parsed_attr = sr.get("parsed", "")
            if parsed_attr:
                parts = parsed_attr.strip("|").split("|")
                if len(parts) >= 3:
                    book_osis = parts[0]
                    chapter = parts[1]
                    start_v = parts[2]
                    end_v = parts[4] if len(parts) >= 5 and parts[4] != "0" else ""
                    canonical = normalize_book(book_osis)
                    if canonical and chapter != "0":
                        if end_v and end_v != start_v:
                            raw_refs.append((canonical, int(chapter), f"{start_v}-{end_v}"))
                        elif start_v != "0":
                            raw_refs.append((canonical, int(chapter), start_v))
            continue

        canonical, cv = normalize_passage(passage)
        if not canonical or not cv:
            continue

        # Parse chapter:verse from cv
        cv_match = re.match(r"(\d+):(.+)$", cv)
        if cv_match:
            chapter = int(cv_match.group(1))
            verse_part = cv_match.group(2).strip()
            raw_refs.append((canonical, chapter, verse_part))

    if not raw_refs:
        return []

    # Group consecutive refs that share book+chapter
    grouped: list[str] = []
    current_book, current_chapter = raw_refs[0][0], raw_refs[0][1]
    current_verses: list[str] = [raw_refs[0][2]]

    for book, chapter, verse_part in raw_refs[1:]:
        if book == current_book and chapter == current_chapter:
            current_verses.append(verse_part)
        else:
            # Flush current group
            grouped.append(f"{current_book} {current_chapter}:{','.join(current_verses)}")
            current_book, current_chapter = book, chapter
            current_verses = [verse_part]

    # Flush last group
    grouped.append(f"{current_book} {current_chapter}:{','.join(current_verses)}")

    return grouped


def extract_aspects_from_def(def_element) -> list[dict]:
    """Extract structured aspects directly from XML <p> elements.

    Each <p class="index1|index2|..."> becomes an aspect with:
    - label: text content before scripRef tags
    - references: list of normalized reference strings
    - level: hierarchy depth from CSS class (1=top, 2=child, etc.)

    Headers (level 1, no refs) are merged with their children (level 2+):
      "Experienced by" (no refs) + "Believers" (refs) → "Experienced by: Believers"
    """
    # First pass: extract raw aspects with level info
    raw_aspects: list[dict] = []

    for p in def_element.find_all("p"):
        label = _extract_label_from_p(p)
        refs = _extract_refs_from_p(p)

        # Extract level from CSS class (index1 → 1, index2 → 2, etc.)
        level = 1
        css_classes = p.get("class", [])
        for cls in css_classes:
            m = re.match(r"index(\d+)", cls)
            if m:
                level = int(m.group(1))
                break

        if label or refs:
            raw_aspects.append({
                "label": label if label else "General references",
                "references": refs,
                "level": level,
            })

    # Second pass: merge headers with children
    aspects: list[dict] = []
    i = 0
    while i < len(raw_aspects):
        asp = raw_aspects[i]
        label = asp["label"]
        refs = asp["references"]
        level = asp["level"]

        if not refs and level == 1 and not label.startswith("See"):
            # Header without refs — merge with following level 2+ children
            header = label
            j = i + 1
            merged_any = False
            while j < len(raw_aspects) and raw_aspects[j]["level"] > level:
                child = raw_aspects[j]
                child_label = child["label"]
                child_refs = child["references"]
                new_label = f"{header}: {child_label}" if child_label else header
                aspects.append({
                    "label": new_label,
                    "references": child_refs,
                    "level": level,
                })
                merged_any = True
                j += 1
            if not merged_any:
                # Standalone header with no children — keep as-is
                aspects.append({"label": label, "references": refs, "level": level})
            i = j
        else:
            aspects.append({"label": label, "references": refs, "level": level})
            i += 1

    return aspects


def build_biblical_references(aspects: list[dict]) -> list[dict]:
    """Build the biblical_references array from aspect references.

    Deduplicates by (book, chapter, verses_tuple).
    """
    refs: list[dict] = []
    seen: set[tuple] = set()

    for asp in aspects:
        for ref_str in asp.get("references", []):
            m = re.match(r"^(.+?)\s+(\d+):(.+)$", ref_str)
            if not m:
                continue
            book = m.group(1)
            chapter = int(m.group(2))
            verse_str = m.group(3)
            verses = parse_verse_range(verse_str)
            if not verses:
                continue

            key = (book, chapter, tuple(verses))
            if key in seen:
                continue
            seen.add(key)

            testament = "OT" if book in OT_BOOKS else "NT"
            refs.append({
                "book": book,
                "chapter": chapter,
                "verses": verses,
                "verse_count": len(verses),
                "testament": testament,
                "raw": ref_str,
            })

    return refs


# ---------------------------------------------------------------------------
# Core parsing
# ---------------------------------------------------------------------------

def parse_topic_from_xml(
    soup: BeautifulSoup, topic_name: str, *, verbose: bool = False
) -> Optional[dict]:
    """Parse a single topic from XML using ThML structure."""
    # Find the term element matching our topic
    term_element = None

    for glossary in soup.find_all("glossary"):
        for term in glossary.find_all("term"):
            term_text = clean_topic_name(term.get_text(strip=True))
            if term_text.upper() == topic_name:
                term_element = term
                break
        if term_element:
            break

    if not term_element:
        # Try partial match
        for glossary in soup.find_all("glossary"):
            for term in glossary.find_all("term"):
                term_text = clean_topic_name(term.get_text(strip=True))
                if (
                    topic_name in term_text.upper()
                    or term_text.upper() in topic_name
                ):
                    term_element = term
                    if verbose:
                        print(f"   Partial match: '{term_text}' for '{topic_name}'")
                    break
            if term_element:
                break

    if not term_element:
        if verbose:
            print(f"   Topic not found: {topic_name}")
        return None

    # Find the corresponding definition
    def_element = term_element.find_next_sibling("def")
    if not def_element:
        parent = term_element.parent
        def_element = parent.find("def")

    if not def_element:
        if verbose:
            print(f"   Definition not found for: {topic_name}")
        return None

    # Extract data directly from XML structure
    original_topic_name = clean_topic_name(term_element.get_text(strip=True))
    slug = create_topic_slug(original_topic_name)
    see_also = extract_see_also_from_def(def_element)
    aspects = extract_aspects_from_def(def_element)
    biblical_refs = build_biblical_references(aspects)

    # Stats
    books_mentioned = sorted(set(r["book"] for r in biblical_refs))
    ot_refs = sum(1 for r in biblical_refs if r["testament"] == "OT")
    nt_refs = sum(1 for r in biblical_refs if r["testament"] == "NT")
    book_counts = Counter(r["book"] for r in biblical_refs)
    top_books = [b for b, _ in book_counts.most_common(10)]

    topic_data = {
        "topic": original_topic_name.upper(),
        "slug": slug,
        "canonical_id": f"TOR:{slug}",
        "source": "TOR",
        "see_also": see_also,
        "aspects": aspects,
        "biblical_references": biblical_refs,
        "books_mentioned": books_mentioned,
        "stats": {
            "total_aspects": len(aspects),
            "total_refs": len(biblical_refs),
            "ot_refs": ot_refs,
            "nt_refs": nt_refs,
            "books_count": len(books_mentioned),
            "top_books": top_books,
        },
    }

    if verbose:
        print(f"   {len(aspects)} aspects, {len(biblical_refs)} refs")

    return topic_data


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------

def save_topic_data(
    topic_name: str, topic_data: dict, output_dir: Path, *, verbose: bool = False
) -> None:
    """Save topic data to JSON file in A-Z/ directory structure."""
    first_letter = topic_name[0].upper()
    topic_dir = output_dir / first_letter
    topic_dir.mkdir(parents=True, exist_ok=True)

    file_path = topic_dir / f"{topic_data['topic']}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(topic_data, f, indent=2, ensure_ascii=False)

    if verbose:
        print(f"   Saved: {file_path}")


# ---------------------------------------------------------------------------
# Topic processing
# ---------------------------------------------------------------------------

def process_topics(
    soup: BeautifulSoup,
    topics: list[str],
    output_dir: Path,
    *,
    verbose: bool = False,
    dry_run: bool = False,
) -> None:
    """Process list of topics."""
    total = len(topics)
    successful = 0
    total_aspects = 0
    total_refs = 0

    for i, topic in enumerate(topics, 1):
        if verbose:
            print(f"[{i}/{total}] Processing: {topic}")

        try:
            if dry_run:
                print(f"   Would process: {topic}")
                continue

            topic_data = parse_topic_from_xml(soup, topic, verbose=verbose)
            if topic_data:
                save_topic_data(topic, topic_data, output_dir, verbose=verbose)
                successful += 1
                total_aspects += topic_data["stats"]["total_aspects"]
                total_refs += topic_data["stats"]["total_refs"]

        except Exception as e:
            print(f"   Error processing {topic}: {e}")
            if verbose:
                import traceback
                traceback.print_exc()

    print(f"\nCompleted: {successful}/{total} topics")
    print(f"Total aspects: {total_aspects}")
    print(f"Total refs: {total_refs}")
    print(f"Avg aspects/topic: {total_aspects / max(successful, 1):.1f}")
    print(f"Avg refs/topic: {total_refs / max(successful, 1):.1f}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Parse Torrey's Topical Textbook from CCEL ThML XML."
    )
    parser.add_argument("--topic", type=str, help="Extract specific topic")
    parser.add_argument(
        "--topics", type=str, help="Comma-separated list of topics"
    )
    parser.add_argument(
        "--all", action="store_true", help="Extract all topics from XML"
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be processed"
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    verbose = args.verbose
    dry_run = args.dry_run
    output_dir = OUTPUT_DIR

    if verbose:
        print("CCEL Torrey's XML Parser v2")
        print("=" * 50)

    if not XML_FILE.exists():
        print(f"XML file not found: {XML_FILE}")
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    if verbose:
        print(f"Output directory: {output_dir}")

    with open(XML_FILE, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")

    if args.all:
        topics = extract_all_topics(soup)
    elif args.topics:
        topics = [t.strip().upper() for t in args.topics.split(",")]
    elif args.topic:
        topics = [args.topic.strip().upper()]
    else:
        print("Please specify --topic, --topics, or --all")
        return

    process_topics(soup, topics, output_dir, verbose=verbose, dry_run=dry_run)


if __name__ == "__main__":
    main()
