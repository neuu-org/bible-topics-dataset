"""Pin what the Nave parser *should* produce against the real source XML.

These tests describe correct behaviour for `scripts/parse_nave.py` reference
extraction. Two of them currently fail — that is intentional, and they are
the proof that plan 003's fix works when they flip to passing.
"""

import re

import pytest
from bs4 import BeautifulSoup

from tests.conftest import DATA, load_script


def test_scripref_parsed_attribute_always_has_five_parts():
    """Every `parsed="..."` attribute in the Nave source XML splits into
    exactly 5 parts on `|` after stripping leading/trailing `|`.

    This is the fact that makes the `elif len(parts) >= 4` branch in
    parse_nave.py's extract_biblical_references_from_def unreachable in
    practice: the `if len(parts) >= 5` branch is always taken, but it never
    builds or appends a reference.
    """
    xml_path = DATA / "00_raw" / "xml" / "nave_bible.xml"
    xml_text = xml_path.read_text(encoding="utf-8")

    parsed_values = re.findall(r'parsed="([^"]*)"', xml_text)
    assert parsed_values, "expected parsed=\"...\" attributes in the source XML"

    non_five_part = [
        v for v in parsed_values if len(v.strip("|").split("|")) != 5
    ]

    assert not non_five_part, (
        f"{len(non_five_part)} of {len(parsed_values)} parsed attributes "
        f"do not have exactly 5 parts"
    )


@pytest.mark.xfail(strict=True, reason="fixed by plan 003")
def test_nave_parser_extracts_references_for_a_known_topic():
    """extract_biblical_references_from_def should return one reference per
    scripRef tag with a 5-part parsed attribute. It currently returns none,
    because the reference-building code is nested only under the dead
    `elif len(parts) >= 4` branch."""
    parse_nave = load_script("parse_nave")

    fixture = """
    <def>
    <p class="index2">General references<scripRef passage="Gen 1:1" parsed="|Gen|1|1|1|1|">Gen 1:1</scripRef></p>
    <p class="index2">More<scripRef passage="Gen 2:1" parsed="|Gen|2|1|2|1|">Gen 2:1</scripRef></p>
    <p class="index2">More<scripRef passage="Gen 3:1" parsed="|Gen|3|1|3|1|">Gen 3:1</scripRef></p>
    </def>
    """
    soup = BeautifulSoup(fixture, "html.parser")
    def_element = soup.find("def")

    references = parse_nave.extract_biblical_references_from_def(
        def_element, "TESTTOPIC"
    )

    assert len(references) == 3


@pytest.mark.xfail(strict=True, reason="fixed by plan 003")
def test_large_topic_reference_count_is_plausible():
    """"JESUS, THE CHRIST" has roughly 4,100 <scripRef> tags in its region of
    the source XML, but only 21 make it into biblical_references today — a
    >99% loss caused by the same dead-branch bug."""
    import json

    topic_path = DATA / "02_sources" / "nave" / "J" / "JESUS, THE CHRIST.json"
    data = json.loads(topic_path.read_text(encoding="utf-8"))

    assert len(data["biblical_references"]) > 100
