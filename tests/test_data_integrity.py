"""Pin the current data-integrity invariants of the pipeline output.

Two tests here are expected to FAIL against the current data on disk — that
is intentional. They are marked `xfail(strict=True)` so the suite is green
today, and will turn red (an unexpected pass) the day the underlying bug is
fixed, forcing whoever lands that fix to remove the marker deliberately.
"""

import json
import re

import pytest

from tests.conftest import DATA


def _slugify(name: str) -> str:
    """Same slug rule the pipeline uses to derive file names from topic names."""
    return re.sub(r"[^a-z0-9]", "_", name.lower().strip()).strip("_")


@pytest.mark.xfail(strict=True, reason="fixed by plan 004")
def test_index_total_matches_file_count():
    """`_index.json`'s total_entries is computed before the write loop, so it
    counts intended writes rather than files actually written to disk."""
    index_path = DATA / "01_parsed" / "_index.json"
    index = json.loads(index_path.read_text(encoding="utf-8"))

    actual_file_count = len(list((DATA / "01_parsed").glob("*/*.json")))

    assert index["total_entries"] == actual_file_count


@pytest.mark.xfail(strict=True, reason="fixed by plan 004")
def test_no_slug_collisions():
    """Two distinct topic names (e.g. "MERCY-SEAT" vs "MERCY SEAT") currently
    collapse to the same slug, silently colliding on disk."""
    files = list((DATA / "02_sources").glob("*/*/*.json"))
    assert files, "expected topic files under data/02_sources/*/*/*.json"

    names = set()
    for f in files:
        data = json.loads(f.read_text(encoding="utf-8"))
        topic = data.get("topic")
        if topic:
            names.add(topic)

    slug_map: dict[str, set[str]] = {}
    for name in names:
        slug = _slugify(name)
        slug_map.setdefault(slug, set()).add(name)

    collisions = {slug: names for slug, names in slug_map.items() if len(names) > 1}
    assert not collisions, f"slug collisions found: {collisions}"


def test_every_topic_has_required_keys():
    """Schema guard for data/01_parsed: every topic file must carry the keys
    that layer actually contains.

    Note: `canonical_id`, `definitions` and `ai_enrichment` are deliberately
    NOT asserted here. `canonical_id` belongs to the data/02_sources schema,
    not data/01_parsed (confirmed by inspecting the full 01_parsed layer);
    `definitions`/`ai_enrichment` only exist in data/03_pilot. Asserting any
    of them here would encode a schema that doesn't exist in this layer.
    """
    required_keys = {"topic", "slug", "aspects", "biblical_references", "stats"}

    files = [
        f
        for f in (DATA / "01_parsed").glob("*/*.json")
        if f.name != "_index.json"
    ]
    assert files, "expected topic files under data/01_parsed/*/*.json"

    for f in files:
        data = json.loads(f.read_text(encoding="utf-8"))
        missing = required_keys - set(data.keys())
        assert not missing, f"{f} is missing required keys: {missing}"


def test_all_json_is_parseable():
    """Guards against a truncated or corrupted write anywhere under data/."""
    files = list(DATA.rglob("*.json"))
    assert files, "expected JSON files under data/"

    for f in files:
        try:
            json.loads(f.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            pytest.fail(f"{f} is not valid JSON: {e}")
