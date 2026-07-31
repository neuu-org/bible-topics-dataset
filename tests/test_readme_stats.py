"""Guard the numbers published in README.md and data/stats.json against the
actual data on disk, so they cannot silently drift the way the 7,873 claim
in README.md did before plan 005.
"""

import json
import re

from tests.conftest import DATA, REPO_ROOT

README = REPO_ROOT / "README.md"


def _measured_01_parsed_count() -> int:
    files = [
        f for f in (DATA / "01_parsed").glob("*/*.json") if f.name != "_index.json"
    ]
    assert files, "expected topic files under data/01_parsed/*/*.json"
    return len(files)


def test_readme_topic_count_matches_data():
    """The Overview table's "Total topics" row must match the number of
    files actually on disk under data/01_parsed."""
    text = README.read_text(encoding="utf-8")

    match = re.search(r"Total topics \(`data/01_parsed`\) \| ([\d,]+) \|", text)
    assert match, "expected a 'Total topics (`data/01_parsed`)' row in README.md"

    readme_count = int(match.group(1).replace(",", ""))
    assert readme_count == _measured_01_parsed_count()


def test_stats_json_matches_filesystem():
    """data/stats.json's 01_parsed topic_count must match a fresh glob count,
    so a stale stats.json cannot silently drift from the data it describes."""
    stats_path = DATA / "stats.json"
    stats = json.loads(stats_path.read_text(encoding="utf-8"))

    stats_count = stats["layers"]["01_parsed"]["topic_count"]
    assert stats_count == _measured_01_parsed_count()
