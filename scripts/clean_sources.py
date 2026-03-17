#!/usr/bin/env python3
"""
Clean and restructure 02_sources/ topics.

Parses inline biblical references from raw_block text using the same
ref parser as extract_definition_refs.py, ensuring consistent book naming
across the entire NEUU ecosystem.

Reads:  data/02_sources/{nave,torrey}/A-Z/*.json
Writes: data/02_sources/{nave,torrey}/A-Z/*.json (in-place)
        data/02_sources/{nave,torrey}/_metadata.json

Usage:
    python scripts/clean_sources.py --source all
    python scripts/clean_sources.py --source nave
    python scripts/clean_sources.py --source torrey --verbose
    python scripts/clean_sources.py --source all --dry-run
"""

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from collections import Counter

REPO_ROOT = Path(__file__).parent.parent

# ---------------------------------------------------------------------------
# Book name normalization — SHARED with extract_definition_refs.py
# This is the canonical mapping for the entire NEUU topics ecosystem.
# Output names match bible-text-dataset (full English) and bible-dictionary-dataset.
# ---------------------------------------------------------------------------
BOOK_PATTERNS = {
    # Old Testament
    r"Ge(?:n(?:esis)?)?\.?": "Genesis",
    r"Ex(?:od(?:us)?)?\.?": "Exodus",
    r"Le(?:v(?:iticus)?)?\.?": "Leviticus",
    r"Nu(?:m(?:bers)?)?\.?|Nb\.?": "Numbers",
    r"De(?:ut(?:eronomy)?)?\.?|Dt\.?": "Deuteronomy",
    r"Jos(?:h(?:ua)?)?\.?": "Joshua",
    r"Jud(?:g(?:es)?)?\.?|Jg\.?": "Judges",
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
    verses = []
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


def extract_refs_from_text(text: str) -> list[dict]:
    """
    Extract all biblical references from inline text.

    Uses the same logic as extract_definition_refs.py for consistency.
    Handles: "Ex 6:16-20;Jos 21:4,10;1Ch 6:2,3"
    Also handles glued text: "ShechemitesJud 9:4"
    """
    refs = []
    seen = set()

    def add_ref(book: str, chapter: int, verses: list[int], raw: str):
        key = (book, chapter, tuple(verses))
        if key not in seen:
            seen.add(key)
            refs.append({
                "book": book,
                "chapter": chapter,
                "verses": verses,
                "verse_count": len(verses),
                "testament": "OT" if book in OT_BOOKS else "NT",
                "raw": raw,
            })

    # Split by semicolons — each segment may share a book
    segments = re.split(r"[;\n]", text)
    last_book = None

    for seg in segments:
        seg = seg.strip()
        if not seg:
            continue

        # Find all chapter:verse patterns
        cv_pattern = r"(\d+):(\d+(?:[-\u2013,]\d+)*)"
        for cv_match in re.finditer(cv_pattern, seg):
            chapter_str = cv_match.group(1)
            verses_str = cv_match.group(2)

            # Look for book abbreviation right before this chapter:verse
            # Try each BOOK_PATTERN at the end of the text before the match
            # This handles glued text like "ShechemitesJud 9:4"
            before = seg[:cv_match.start()]
            for pattern, book in BOOK_PATTERNS.items():
                m = re.search(rf"({pattern})\s*$", before, re.IGNORECASE)
                if m:
                    last_book = book
                    break

            if last_book:
                try:
                    chapter = int(chapter_str)
                    verses = parse_verse_range(verses_str)
                    if verses:
                        raw = f"{last_book} {chapter}:{verses_str}"
                        add_ref(last_book, chapter, verses, raw)
                except ValueError:
                    pass

    return refs


def _parse_single_line(line: str, last_book: str | None):
    """Parse a single line into (label, ref_strings, last_book).

    Returns the label text, list of parsed ref strings, and updated last_book.
    """
    # Remove leading dash
    if line.startswith(("\u2013", "-", "\u2014")):
        line = line[1:].strip()
    # Remove trailing period if it's just punctuation
    # Only strip trailing period if followed by nothing (end of line)
    if line.endswith("."):
        line = line[:-1].rstrip()

    # Try to find where refs start by matching a book pattern before chapter:verse
    split_pos = None
    matched_book = None
    for pattern, book in BOOK_PATTERNS.items():
        # Require word boundary or start-of-string before book pattern
        # to avoid matching "Lu" inside "Illustrated" or "Ge" inside "George"
        m = re.search(rf"(?:^|(?<=\s)|(?<=;)|(?<=[—\-\.]))({pattern})\s*\d+:", line, re.IGNORECASE)
        if m:
            if split_pos is None or m.start() < split_pos:
                split_pos = m.start()
                matched_book = book

    if split_pos is not None:
        label = re.sub(r"\s*[—\-]+\s*$", "", line[:split_pos].strip())
        ref_text = line[split_pos:]
        refs = extract_refs_from_text(ref_text)
        ref_strings = [r["raw"] for r in refs]
        return label, ref_strings, matched_book or last_book

    # No book pattern — check for orphan chapter:verse (continuation refs)
    orphan_cv = re.findall(r"\d+:\d+", line)
    if orphan_cv and last_book:
        first_cv = re.search(r"\d+:\d+", line)
        if first_cv:
            label = line[:first_cv.start()].strip().rstrip(";,").strip()
            ref_text = f"{last_book} " + line[first_cv.start():]
            refs = extract_refs_from_text(ref_text)
            ref_strings = [r["raw"] for r in refs]
            return label, ref_strings, last_book

    # No refs found at all — strip trailing em-dash/dash
    clean_label = re.sub(r"\s*[—\-]+\s*$", "", line.strip())
    return clean_label, [], last_book


def parse_aspects(raw_block: str) -> list[dict]:
    """Parse raw_block into structured aspects with labels and references.

    Handles:
    - Book abbreviation glued to text: "ShechemitesJud 9:4"
    - Refs without book name: "Pr 2:16;5:3;6:24" (all Proverbs)
    - Orphan numeric refs: "1:5,22;2:38" inherit book from context
    - Indented sub-items: parent header merged with children
    - Standalone headers: "Exemplified" followed by "Moses. —Ex 24:2"
    - Name-dot-dash pattern: "Moses. —Ex 24:2" (common in Torrey)
    """
    raw_lines = raw_block.split("\n")
    # Pre-process: identify indentation structure
    parsed_lines = []
    for line in raw_lines:
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip())
        is_indented = indent >= 2
        parsed_lines.append((line.strip(), is_indented))

    aspects = []
    last_book = None
    pending_header = None  # Header without refs waiting for children

    i = 0
    while i < len(parsed_lines):
        text, is_indented = parsed_lines[i]

        label, ref_strings, last_book = _parse_single_line(text, last_book)

        if ref_strings:
            # This line has refs
            if pending_header:
                # Merge with pending header: "Exemplified: Moses"
                full_label = f"{pending_header}: {label}" if label else pending_header
                aspects.append({
                    "label": full_label,
                    "references": ref_strings,
                })
                # Check if next lines are also indented children of same header
                j = i + 1
                while j < len(parsed_lines) and parsed_lines[j][1]:  # still indented
                    child_text, _ = parsed_lines[j]
                    child_label, child_refs, last_book = _parse_single_line(child_text, last_book)
                    if child_refs:
                        child_full = f"{pending_header}: {child_label}" if child_label else pending_header
                        aspects.append({
                            "label": child_full,
                            "references": child_refs,
                        })
                    j += 1
                pending_header = None
                i = j
                continue
            else:
                # Normal aspect with label + refs
                aspects.append({
                    "label": label if label else "General references",
                    "references": ref_strings,
                })
        else:
            # No refs — could be a header for indented children
            if label and not label.startswith("See"):
                # Check if next line is indented (this is a header)
                if i + 1 < len(parsed_lines) and parsed_lines[i + 1][1]:
                    pending_header = label
                    i += 1
                    continue
                else:
                    # Standalone label without refs and no children
                    # Skip it — it's a header for content that follows on same indent
                    # or a "See TOPIC" reference
                    pending_header = label
                    i += 1
                    continue

        pending_header = None
        i += 1

    return aspects


def _has_preextracted_aspects(data: dict) -> bool:
    """Check if data already has properly extracted aspects (from v2 parser)."""
    aspects = data.get("aspects", [])
    if not aspects or not isinstance(aspects, list):
        return False
    # v2 parser outputs aspects with "references" as list of strings
    # v1 parser outputs raw_block text that needs parse_aspects()
    first = aspects[0]
    return isinstance(first, dict) and "references" in first and isinstance(first["references"], list)


def clean_topic(data: dict) -> dict:
    """Clean a single topic JSON to structured format."""
    topic = data.get("topic", "")
    slug = data.get("topic_slug", data.get("slug", ""))
    canonical_id = data.get("canonical_id", "")
    source_code = "NAV" if canonical_id.startswith("NAV:") else "TOR"

    # Use pre-extracted aspects if available (v2 parser), otherwise parse from raw_block
    if _has_preextracted_aspects(data):
        aspects = data["aspects"]
        biblical_refs = data.get("biblical_references", [])
        see_also = data.get("see_also", [])
    else:
        raw_block = data.get("raw_block", "")
        aspects = parse_aspects(raw_block)
        biblical_refs = extract_refs_from_text(raw_block)
        see_also = data.get("see_also", [])
        if not see_also:
            see_matches = re.findall(r"[Ss]ee\s+([A-Z][A-Z\s,'\-]+?)(?:\n|$)", raw_block)
            see_also = [s.strip().rstrip(",") for s in see_matches if s.strip()]

    # Stats
    books_mentioned = sorted(set(r["book"] for r in biblical_refs))
    ot_refs = sum(1 for r in biblical_refs if r["testament"] == "OT")
    nt_refs = sum(1 for r in biblical_refs if r["testament"] == "NT")
    book_counts = Counter(r["book"] for r in biblical_refs)
    top_books = [b for b, _ in book_counts.most_common(10)]

    return {
        "topic": topic,
        "slug": slug,
        "canonical_id": canonical_id,
        "source": source_code,
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


def process_source(source_dir: Path, verbose: bool = False, dry_run: bool = False) -> dict:
    """Process all topics in a source directory."""
    stats = {"total": 0, "cleaned": 0, "refs_extracted": 0, "aspects_total": 0}
    book_counter = Counter()

    for f in sorted(source_dir.rglob("*.json")):
        if f.name.startswith("_"):
            continue
        stats["total"] += 1

        with open(f, encoding="utf-8") as fh:
            data = json.load(fh)

        cleaned = clean_topic(data)
        stats["refs_extracted"] += len(cleaned["biblical_references"])
        stats["aspects_total"] += len(cleaned["aspects"])
        for r in cleaned["biblical_references"]:
            book_counter[r["book"]] += 1

        if verbose:
            n = len(cleaned["biblical_references"])
            a = len(cleaned["aspects"])
            print(f"  {cleaned['topic']:30s} {n:4d} refs, {a:3d} aspects")

        if not dry_run:
            with open(f, "w", encoding="utf-8") as fh:
                json.dump(cleaned, fh, indent=2, ensure_ascii=False)
            stats["cleaned"] += 1

    stats["books_distribution"] = dict(book_counter.most_common())
    return stats


def write_metadata(source_dir: Path, source_name: str, stats: dict):
    """Write _metadata.json for a source."""
    metadata = {
        "source": source_name.upper()[:3],
        "source_name": {
            "nave": "Nave's Topical Bible",
            "torrey": "Torrey's New Topical Textbook",
        }.get(source_name, source_name),
        "version": "1.1.0",
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "processor": "clean_sources.py",
        "stats": {
            "total_topics": stats["total"],
            "total_refs_extracted": stats["refs_extracted"],
            "total_aspects": stats["aspects_total"],
            "avg_refs_per_topic": round(stats["refs_extracted"] / max(stats["total"], 1), 1),
            "avg_aspects_per_topic": round(stats["aspects_total"] / max(stats["total"], 1), 1),
        },
        "books_distribution": stats.get("books_distribution", {}),
    }

    out = source_dir / "_metadata.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    print(f"  Metadata: {out}")


def main():
    parser = argparse.ArgumentParser(description="Clean and restructure 02_sources/ topics")
    parser.add_argument("--source", choices=["nave", "torrey", "all"], default="all")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sources_dir = REPO_ROOT / "data" / "02_sources"
    targets = ["nave", "torrey"] if args.source == "all" else [args.source]

    for source_name in targets:
        source_dir = sources_dir / source_name
        if not source_dir.exists():
            print(f"Not found: {source_dir}")
            continue

        print(f"\n{'='*50}")
        print(f"Cleaning {source_name}")
        print(f"{'='*50}")

        stats = process_source(source_dir, verbose=args.verbose, dry_run=args.dry_run)

        print(f"  Topics: {stats['total']}")
        print(f"  Cleaned: {stats['cleaned']}")
        print(f"  Refs extracted: {stats['refs_extracted']}")
        print(f"  Aspects: {stats['aspects_total']}")
        print(f"  Avg refs/topic: {stats['refs_extracted'] / max(stats['total'], 1):.1f}")

        if not args.dry_run:
            write_metadata(source_dir, source_name, stats)


if __name__ == "__main__":
    main()
