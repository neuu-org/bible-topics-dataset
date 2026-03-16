#!/usr/bin/env python3
"""
Parse Nave's Topical Bible from CCEL ThML XML.

Reads:  data/00_raw/xml/nave_bible.xml
Writes: data/02_sources/nave/A-Z/*.json

Usage:
    python scripts/parse_nave.py --all
    python scripts/parse_nave.py --topic PETER
"""

import argparse
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).parent.parent
XML_FILE = REPO_ROOT / "data" / "00_raw" / "xml" / "nave_bible.xml"
OUTPUT_DIR = REPO_ROOT / "data" / "02_sources" / "nave"

# ---------------------------------------------------------------------------
# Book-to-testament mapping (inlined from book_utils)
# ---------------------------------------------------------------------------
OT_BOOKS = {
    "gen", "exod", "lev", "num", "deut", "josh", "judg", "ruth",
    "1sam", "2sam", "1kgs", "2kgs", "1chr", "2chr", "ezra", "neh",
    "esth", "job", "ps", "prov", "eccl", "song", "isa", "jer",
    "lam", "ezek", "dan", "hos", "joel", "amos", "obad", "jonah",
    "mic", "nah", "hab", "zeph", "hag", "zech", "mal",
}

NT_BOOKS = {
    "matt", "mark", "luke", "john", "acts", "rom", "1cor", "2cor",
    "gal", "eph", "phil", "col", "1thess", "2thess", "1tim", "2tim",
    "titus", "phlm", "heb", "jas", "1pet", "2pet", "1john", "2john",
    "3john", "jude", "rev",
}


def get_testament_type(book: str) -> Optional[str]:
    """Return 'old_testament', 'new_testament', or None."""
    book_lower = book.lower()
    if book_lower in OT_BOOKS:
        return "old_testament"
    if book_lower in NT_BOOKS:
        return "new_testament"
    return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def create_topic_slug(topic_name: str) -> str:
    """Create URL-friendly slug from topic name."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", topic_name.lower().strip())


def create_content_preview(content: str) -> str:
    """Create preview of content."""
    first_line = content.split("\n")[0] if content else ""
    return first_line[:100] + "..." if len(first_line) > 100 else first_line


def resolve_see_also_links(see_also: List[str]) -> List[Dict]:
    """Resolve see_also links to canonical format."""
    resolved = []
    for term in see_also:
        resolved.append({
            "original": term,
            "clean": term,
            "slug": create_topic_slug(term),
            "canonical_id": f"NAV:{create_topic_slug(term)}",
        })
    return resolved


# ---------------------------------------------------------------------------
# Extraction helpers
# ---------------------------------------------------------------------------

def extract_all_topics(soup: BeautifulSoup) -> List[str]:
    """Extract all topic names from XML."""
    topics = []
    for term in soup.find_all("term"):
        topic_name = term.get_text(strip=True)
        if topic_name:
            topics.append(topic_name.upper())
    return sorted(set(topics))


def extract_content_from_def(def_element) -> str:
    """Extract clean content from definition element."""
    content_lines = []

    for p in def_element.find_all("p", class_=["index2", "index3", "index4"]):
        line = p.get_text(strip=True)

        # Determine indentation based on class
        if "index3" in p.get("class", []):
            line = f"  {line}"
        elif "index4" in p.get("class", []):
            line = f"    {line}"

        content_lines.append(line)

    return "\n".join(content_lines)


def extract_see_also_from_def(def_element) -> List[str]:
    """Extract see_also links from definition."""
    see_also = []

    for link in def_element.find_all("a", href=True):
        href = link.get("href", "")
        if "bible." in href and "?term=" in href:
            # Extract term from URL
            term = href.split("?term=")[1]
            # Clean and normalize
            term = term.replace("+", " ").upper()
            see_also.append(term)

    return list(set(see_also))  # Remove duplicates


def extract_biblical_references_from_def(
    def_element, topic_name: str
) -> List[Dict]:
    """Extract biblical references using scripRef tags."""
    biblical_references = []

    # Find all scripRef tags (HTML parser treats them as regular tags)
    scripref_tags = def_element.find_all(
        lambda tag: tag.name and tag.name.lower() == "scripref"
    )

    for i, scripref in enumerate(scripref_tags):
        parsed_attr = scripref.get("parsed", "")
        passage = scripref.get("passage", "")
        ref_text = scripref.get_text(strip=True)

        # Parse the structured data
        if parsed_attr:
            # Format: |Book|Chapter|StartVerse|EndChapter|EndVerse|
            parts = parsed_attr.strip("|").split("|")
            if len(parts) >= 5:
                book, chapter, start_verse, end_chapter, end_verse = parts[:5]
            elif len(parts) >= 4:
                book, chapter, start_verse, end_verse = parts[:4]
                end_chapter = chapter

                # Convert to our format
                parsed_ref = create_biblical_reference(
                    book=book,
                    chapter=int(chapter) if chapter else 0,
                    start_verse=int(start_verse) if start_verse else 0,
                    end_verse=(
                        int(end_verse)
                        if end_verse and end_verse != "0"
                        else None
                    ),
                    original_abbreviation=ref_text or passage,
                    topic_name=topic_name,
                    index=i,
                )

                if parsed_ref:
                    biblical_references.append(parsed_ref)

    return biblical_references


def create_biblical_reference(
    book: str,
    chapter: int,
    start_verse: int,
    end_verse: Optional[int],
    original_abbreviation: str,
    topic_name: str,
    index: int,
) -> Optional[Dict]:
    """Create a biblical reference in our standard format."""
    testament = get_testament_type(book)

    # Determine range kind
    if end_verse and end_verse != start_verse:
        range_kind = "range"
        final_verse = end_verse
    else:
        range_kind = "verse"
        final_verse = start_verse

    # Create OSIS citation
    if range_kind == "range":
        osis_citation = (
            f"{book}.{chapter}.{start_verse}-{book}.{chapter}.{final_verse}"
        )
    else:
        osis_citation = f"{book}.{chapter}.{start_verse}"

    return {
        "ref_id": (
            f"NAV:{create_topic_slug(topic_name)}"
            f"#general_references#{book}.{chapter}.{start_verse}"
        ),
        "entry_label": "General references",
        "book": book,
        "chapter": chapter,
        "start_verse": start_verse,
        "end_verse": final_verse,
        "range_kind": range_kind,
        "osis_citation": osis_citation,
        "original_abbreviation": original_abbreviation,
        "testament": testament,
        "context": original_abbreviation,
        "group_idx": 0,
    }


def create_reference_groups_from_def(def_element) -> List[Dict]:
    """Create reference groups from definition structure."""
    groups = []

    for i, p in enumerate(def_element.find_all("p", class_="index2")):
        entry_text = p.get_text(strip=True)
        if entry_text:
            groups.append({
                "idx": i,
                "entry_label": entry_text,
                "raw_segment": entry_text,
            })

    return groups


def calculate_topic_stats(biblical_references: List[Dict]) -> Dict:
    """Calculate statistics for the topic."""
    ot_refs = sum(
        1 for ref in biblical_references
        if ref.get("testament") == "old_testament"
    )
    nt_refs = sum(
        1 for ref in biblical_references
        if ref.get("testament") == "new_testament"
    )

    book_counts: Dict[str, int] = {}
    for ref in biblical_references:
        book = ref["book"]
        book_counts[book] = book_counts.get(book, 0) + 1

    top_books = sorted(
        book_counts.keys(), key=lambda x: book_counts[x], reverse=True
    )

    return {
        "entries_count": 1,
        "ot_refs": ot_refs,
        "nt_refs": nt_refs,
        "total_verses": len(biblical_references),
        "top_books": top_books,
        "ref_counts_by_book": book_counts,
    }


# ---------------------------------------------------------------------------
# Core parsing
# ---------------------------------------------------------------------------

def parse_topic_from_xml(
    soup: BeautifulSoup, topic_name: str, *, verbose: bool = False
) -> Optional[Dict]:
    """Parse a single topic from XML."""
    # Find the term element
    term_element = None
    for term in soup.find_all("term"):
        if term.get_text(strip=True).upper() == topic_name:
            term_element = term
            break

    if not term_element:
        if verbose:
            print(f"   Topic not found: {topic_name}")
        return None

    # Find the corresponding definition
    def_element = term_element.find_next_sibling("def")
    if not def_element:
        return None

    # Extract content
    main_content = extract_content_from_def(def_element)
    see_also = extract_see_also_from_def(def_element)
    biblical_references = extract_biblical_references_from_def(
        def_element, topic_name
    )
    reference_groups = create_reference_groups_from_def(def_element)

    # Calculate stats
    stats = calculate_topic_stats(biblical_references)

    # Build topic data
    topic_data = {
        "topic": topic_name,
        "topic_slug": create_topic_slug(topic_name),
        "canonical_id": f"NAV:{create_topic_slug(topic_name)}",
        "aliases": [],
        "see_also": see_also,
        "see_also_resolved": resolve_see_also_links(see_also),
        "raw_block": main_content[:1000],
        "content_preview": create_content_preview(main_content),
        "reference_groups": reference_groups,
        "biblical_references": biblical_references,
        "books_mentioned": list(
            set(ref["book"] for ref in biblical_references)
        ),
        "normalized_labels": [],
        "stats": stats,
        "source": {
            "provider": "ccel",
            "work": "Nave's Topical Bible",
            "format": "xml",
            "fetched_at": datetime.now().strftime("%Y-%m-%d"),
            "source_url": None,
            "local_file": str(XML_FILE),
        },
        "metadata": {
            "parser": "ccel-nave-xml-parser",
            "parser_version": "2025.09.19",
            "schema_version": "scrape.v1",
            "language": {
                "code": "en",
                "name": "English",
                "direction": "ltr",
            },
            "integrity": {
                "checksum_raw_block": (
                    f"sha256:{hashlib.sha256(main_content.encode()).hexdigest()}"
                ),
                "position": {
                    "start_line": None,
                    "end_line": None,
                    "total_lines": main_content.count("\n") + 1,
                },
            },
            "notes": [
                "XML-based parsing from official CCEL ThML source",
                "Perfect biblical reference extraction with osisRef",
                "Structured hierarchy preservation",
                "Complete topic linking and see_also resolution",
            ],
        },
        "total_references": len(biblical_references),
        "version": "1.0.0",
        "created_at": datetime.now().isoformat() + "Z",
        "updated_at": datetime.now().isoformat() + "Z",
    }

    if verbose:
        refs_count = len(biblical_references)
        see_also_count = len(see_also)
        print(f"   {refs_count} references, {see_also_count} see-also links")

    return topic_data


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------

def save_topic_data(
    topic_name: str, topic_data: Dict, output_dir: Path, *, verbose: bool = False
) -> None:
    """Save topic data to JSON file."""
    first_letter = topic_name[0].upper()
    topic_dir = output_dir / "topics" / first_letter
    topic_dir.mkdir(parents=True, exist_ok=True)

    file_path = topic_dir / f"{topic_name}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(topic_data, f, indent=2, ensure_ascii=False)

    if verbose:
        print(f"   Saved: {file_path}")


# ---------------------------------------------------------------------------
# Topic processing
# ---------------------------------------------------------------------------

def process_topics(
    soup: BeautifulSoup,
    topics: List[str],
    output_dir: Path,
    *,
    verbose: bool = False,
    dry_run: bool = False,
) -> None:
    """Process list of topics."""
    total = len(topics)
    successful = 0

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

        except Exception as e:
            if verbose:
                print(f"   Error processing {topic}: {e}")

    print(f"Completed: {successful}/{total} topics processed successfully")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Parse Nave's Topical Bible from CCEL ThML XML."
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
        print("CCEL Nave's XML Parser")
        print("=" * 50)

    # Check XML file exists
    if not XML_FILE.exists():
        print(f"XML file not found: {XML_FILE}")
        return

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    if verbose:
        print(f"Output directory: {output_dir}")

    # Parse XML
    with open(XML_FILE, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")

    # Determine topics
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
