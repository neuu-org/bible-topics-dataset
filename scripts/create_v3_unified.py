#!/usr/bin/env python3
"""
Merge Nave + Torrey sources into unified 01_parsed dataset.

Reads:  data/02_sources/nave/A-Z/*.json
        data/02_sources/torrey/A-Z/*.json
Writes: data/01_parsed/A-Z/*.json
        data/01_parsed/_index.json

Topics that exist in both sources are merged:
- aspects are combined (Nave first, then Torrey)
- biblical_references are deduplicated
- see_also are merged
- stats are recalculated

Usage:
    python scripts/create_v3_unified.py
    python scripts/create_v3_unified.py --verbose
    python scripts/create_v3_unified.py --dry-run
"""

import argparse
import json
import re
import shutil
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
SOURCES_DIR = REPO_ROOT / "data" / "02_sources"
OUTPUT_DIR = REPO_ROOT / "data" / "01_parsed"

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


def slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "_", text.lower().strip()).strip("_")


def load_source(source_dir: Path) -> dict[str, dict]:
    """Load all topics from a source directory, keyed by uppercase topic name."""
    topics = {}
    for f in sorted(source_dir.rglob("*.json")):
        if f.name.startswith("_"):
            continue
        with open(f, encoding="utf-8") as fh:
            data = json.load(fh)
        key = data.get("topic", "").upper()
        if key:
            topics[key] = data
    return topics


def merge_refs(refs_a: list[dict], refs_b: list[dict]) -> list[dict]:
    """Merge two lists of biblical_references, deduplicating by book+chapter+verses."""
    seen = set()
    merged = []
    for ref in refs_a + refs_b:
        key = (ref["book"], ref["chapter"], tuple(ref["verses"]))
        if key not in seen:
            seen.add(key)
            merged.append(ref)
    return merged


def calc_stats(refs: list[dict], aspects: list[dict]) -> dict:
    """Calculate stats from merged references."""
    books = sorted(set(r["book"] for r in refs))
    ot = sum(1 for r in refs if r.get("testament") == "OT")
    nt = sum(1 for r in refs if r.get("testament") == "NT")
    book_counts = Counter(r["book"] for r in refs)
    top_books = [b for b, _ in book_counts.most_common(10)]
    return {
        "total_aspects": len(aspects),
        "total_refs": len(refs),
        "ot_refs": ot,
        "nt_refs": nt,
        "books_count": len(books),
        "top_books": top_books,
    }


def merge_topics(nave: dict | None, torrey: dict | None) -> dict:
    """Merge a topic from Nave and/or Torrey into unified format."""
    # Determine which sources contribute
    sources = []
    if nave:
        sources.append("NAV")
    if torrey:
        sources.append("TOR")

    # Use Nave as primary (more topics), Torrey as secondary
    primary = nave or torrey
    topic_name = primary["topic"]
    slug = primary["slug"]

    # Merge aspects (Nave first, then Torrey)
    aspects = []
    if nave:
        for a in nave.get("aspects", []):
            aspects.append({**a, "source": "NAV"})
    if torrey:
        for a in torrey.get("aspects", []):
            aspects.append({**a, "source": "TOR"})

    # Merge biblical references (dedup)
    refs_nav = nave.get("biblical_references", []) if nave else []
    refs_tor = torrey.get("biblical_references", []) if torrey else []
    all_refs = merge_refs(refs_nav, refs_tor)

    # Merge see_also
    see_also_set = set()
    if nave:
        see_also_set.update(nave.get("see_also", []))
    if torrey:
        see_also_set.update(torrey.get("see_also", []))

    # Books mentioned
    books_mentioned = sorted(set(r["book"] for r in all_refs))

    # Stats
    stats = calc_stats(all_refs, aspects)

    return {
        "topic": topic_name,
        "slug": slug,
        "sources": sources,
        "see_also": sorted(see_also_set),
        "aspects": aspects,
        "biblical_references": all_refs,
        "books_mentioned": books_mentioned,
        "stats": stats,
    }


def main():
    parser = argparse.ArgumentParser(description="Merge Nave + Torrey into 01_parsed")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    # Load sources
    print("Loading sources...")
    nave_dir = SOURCES_DIR / "nave"
    torrey_dir = SOURCES_DIR / "torrey"

    nave_topics = load_source(nave_dir) if nave_dir.exists() else {}
    torrey_topics = load_source(torrey_dir) if torrey_dir.exists() else {}

    print(f"  Nave: {len(nave_topics)} topics")
    print(f"  Torrey: {len(torrey_topics)} topics")

    # Find all unique topic names
    all_names = sorted(set(nave_topics.keys()) | set(torrey_topics.keys()))
    overlap = set(nave_topics.keys()) & set(torrey_topics.keys())
    only_nave = set(nave_topics.keys()) - set(torrey_topics.keys())
    only_torrey = set(torrey_topics.keys()) - set(nave_topics.keys())

    print(f"\n  Total unique: {len(all_names)}")
    print(f"  Overlap (both): {len(overlap)}")
    print(f"  Only Nave: {len(only_nave)}")
    print(f"  Only Torrey: {len(only_torrey)}")

    if args.dry_run:
        print("\n[DRY RUN] Would generate files but not writing.")
        return

    # Clear output directory (except _index.json)
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Merge and write
    print(f"\nMerging {len(all_names)} topics...")
    total_refs = 0
    total_aspects = 0
    by_letter = Counter()

    for topic_name in all_names:
        nave = nave_topics.get(topic_name)
        torrey = torrey_topics.get(topic_name)

        merged = merge_topics(nave, torrey)
        total_refs += merged["stats"]["total_refs"]
        total_aspects += merged["stats"]["total_aspects"]

        # Write to letter directory
        letter = merged["slug"][0].upper() if merged["slug"] else "Z"
        letter_dir = OUTPUT_DIR / letter
        letter_dir.mkdir(exist_ok=True)

        out_file = letter_dir / f"{merged['slug']}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(merged, f, indent=2, ensure_ascii=False)

        by_letter[letter] += 1

        if args.verbose:
            src = "+".join(merged["sources"])
            print(f"  {merged['topic']:30s} [{src:7s}] {merged['stats']['total_refs']:4d} refs")

    # Write _index.json
    index = {
        "total_entries": len(all_names),
        "sources": {
            "nave": len(nave_topics),
            "torrey": len(torrey_topics),
            "overlap": len(overlap),
        },
        "total_refs": total_refs,
        "total_aspects": total_aspects,
        "by_letter": dict(sorted(by_letter.items())),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generator": "create_v3_unified.py",
    }

    with open(OUTPUT_DIR / "_index.json", "w", encoding="utf-8") as f:
        json.dump(index, f, indent=2, ensure_ascii=False)

    # Summary
    print(f"\n{'='*50}")
    print(f"  Topics: {len(all_names)}")
    print(f"  Refs: {total_refs}")
    print(f"  Aspects: {total_aspects}")
    print(f"  Overlap merged: {len(overlap)}")
    print(f"  Output: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
