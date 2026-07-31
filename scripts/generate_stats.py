#!/usr/bin/env python3
"""
Measure the repository's data layers and write data/stats.json.

Every number here comes from files on disk — nothing is copied from
`_index.json` (whose `total_entries` is known to disagree with the actual
file count on disk, see tests/test_data_integrity.py) and nothing is
hand-typed. Run this whenever the data changes and the README's Overview
table needs to be refreshed.

Reads:  data/01_parsed/*/*.json
        data/02_sources/nave/*/*.json
        data/02_sources/torrey/*/*.json
        data/03_pilot/*/*.json
Writes: data/stats.json

Usage:
    python scripts/generate_stats.py
"""

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
DATA_DIR = REPO_ROOT / "data"
OUTPUT_PATH = DATA_DIR / "stats.json"


def load_topics(pattern: str) -> list[dict]:
    """Load every topic JSON matching `pattern` (relative to DATA_DIR)."""
    topics = []
    for f in sorted(DATA_DIR.glob(pattern)):
        if f.name.startswith("_"):
            continue
        with open(f, encoding="utf-8") as fh:
            topics.append(json.load(fh))
    return topics


def analyze_01_parsed() -> dict:
    """Measure the merged V3 layer: counts, definitions/enrichment coverage,
    total biblical references, and topic counts by originating source."""
    topics = load_topics("01_parsed/*/*.json")

    total_refs = 0
    with_definitions = 0
    with_ai_enrichment = 0
    by_source = {"NAV": 0, "TOR": 0}

    for t in topics:
        total_refs += len(t.get("biblical_references", []))
        if t.get("definitions"):
            with_definitions += 1
        if t.get("ai_enrichment"):
            with_ai_enrichment += 1
        for src in t.get("sources", []):
            if src in by_source:
                by_source[src] += 1

    return {
        "topic_count": len(topics),
        "with_definitions": with_definitions,
        "with_ai_enrichment": with_ai_enrichment,
        "total_biblical_references": total_refs,
        "by_source": by_source,
    }


def analyze_source_layer(subdir: str) -> dict:
    """Measure a data/02_sources/<subdir> layer: topic count only — these
    files carry no `definitions` or `ai_enrichment` keys."""
    topics = load_topics(f"02_sources/{subdir}/*/*.json")
    return {"topic_count": len(topics)}


def analyze_03_pilot() -> dict:
    """Measure the AI-enriched pilot layer: counts and definitions/enrichment
    coverage, since this is the only layer where either currently exists."""
    topics = load_topics("03_pilot/*/*.json")

    with_definitions = sum(1 for t in topics if t.get("definitions"))
    with_ai_enrichment = sum(1 for t in topics if t.get("ai_enrichment"))

    return {
        "topic_count": len(topics),
        "with_definitions": with_definitions,
        "with_ai_enrichment": with_ai_enrichment,
    }


def get_git_short_sha() -> str:
    """Short SHA of the commit this measurement was taken against, or
    "unknown" if git is unavailable."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def build_stats() -> dict:
    """Assemble the full stats document."""
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "commit": get_git_short_sha(),
        "layers": {
            "01_parsed": analyze_01_parsed(),
            "02_sources_nave": analyze_source_layer("nave"),
            "02_sources_torrey": analyze_source_layer("torrey"),
            "03_pilot": analyze_03_pilot(),
        },
    }


def render_markdown_table(stats: dict) -> str:
    """Render a Markdown table summarizing `stats`, for pasting into README
    or diffing against what's already there."""
    layers = stats["layers"]
    parsed = layers["01_parsed"]
    nave = layers["02_sources_nave"]
    torrey = layers["02_sources_torrey"]
    pilot = layers["03_pilot"]

    def pct(n: int, total: int) -> str:
        return f"{n} ({n / total * 100:.1f}%)" if total else f"{n} (0.0%)"

    lines = [
        "| Metric | Value |",
        "|--------|-------|",
        f"| Total topics (`data/01_parsed`) | {parsed['topic_count']:,} |",
        f"| Nave source topics (`data/02_sources/nave`) | {nave['topic_count']:,} |",
        f"| Torrey source topics (`data/02_sources/torrey`) | {torrey['topic_count']:,} |",
        f"| `01_parsed` topics with `definitions` | {pct(parsed['with_definitions'], parsed['topic_count'])} |",
        f"| `01_parsed` topics with `ai_enrichment` | {pct(parsed['with_ai_enrichment'], parsed['topic_count'])} |",
        f"| `03_pilot` topics with `definitions` | {pct(pilot['with_definitions'], pilot['topic_count'])} |",
        f"| `03_pilot` topics with `ai_enrichment` | {pct(pilot['with_ai_enrichment'], pilot['topic_count'])} |",
        f"| Biblical references in `01_parsed` | {parsed['total_biblical_references']:,} |",
        f"| `01_parsed` by source: NAV | {parsed['by_source']['NAV']:,} |",
        f"| `01_parsed` by source: TOR | {parsed['by_source']['TOR']:,} |",
    ]
    return "\n".join(lines)


def main():
    stats = build_stats()

    OUTPUT_PATH.write_text(
        json.dumps(stats, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )

    print(f"Measured at commit {stats['commit']} ({stats['generated_at']})")
    print(f"Wrote {OUTPUT_PATH.relative_to(REPO_ROOT)}\n")
    print(render_markdown_table(stats))


if __name__ == "__main__":
    main()
