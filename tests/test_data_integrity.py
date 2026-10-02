"""Pin the current data-integrity invariants of the pipeline output.

Topics must survive merging, with unique output slugs and an accurate index.
"""

import json

import pytest

from tests.conftest import DATA, load_script


def test_index_total_matches_file_count():
    """The index count must match files actually written to disk."""
    index_path = DATA / "01_parsed" / "_index.json"
    index = json.loads(index_path.read_text(encoding="utf-8"))

    actual_file_count = len(list((DATA / "01_parsed").glob("*/*.json")))

    assert index["total_entries"] == actual_file_count


def test_no_slug_collisions():
    """Distinct topic names must survive merging even when their source slugs collide."""
    files = list((DATA / "02_sources").glob("*/*/*.json"))
    assert files, "expected topic files under data/02_sources/*/*/*.json"

    names = set()
    for f in files:
        data = json.loads(f.read_text(encoding="utf-8"))
        topic = data.get("topic")
        if topic:
            names.add(topic)

    output = [json.loads(f.read_text(encoding="utf-8"))
              for f in (DATA / "01_parsed").glob("*/*.json")]
    assert {item["topic"].upper() for item in output} == {name.upper() for name in names}
    assert len({item["slug"] for item in output}) == len(output)


def test_disambiguation_preserves_winner_and_reserves_existing_suffixes():
    module = load_script("create_v3_unified")
    topics = [
        {"topic": "MERCY SEAT", "slug": "mercy_seat"},
        {"topic": "MERCY-SEAT", "slug": "mercy_seat"},
        {"topic": "MERCY_SEAT_2", "slug": "mercy_seat_2"},
    ]
    collisions = module.disambiguate_slugs(topics)
    assert [item["slug"] for item in topics] == ["mercy_seat_3", "mercy_seat", "mercy_seat_2"]
    assert collisions == [{"original_slug": "mercy_seat", "topics": [
        {"topic": "MERCY SEAT", "slug": "mercy_seat_3"},
        {"topic": "MERCY-SEAT", "slug": "mercy_seat"},
    ]}]
    assert module.disambiguate_slugs(topics) == []


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
