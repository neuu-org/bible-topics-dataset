# Changelog

## [1.1.0] - 2026-03-16

### Added
- `parse_nave.py` — standalone parser: XML → 02_sources/nave/ (no Django dependency)
- `parse_torrey.py` — standalone parser: XML → 02_sources/torrey/ (no Django dependency)
- `00_raw/xml/nave_bible.xml` — original CCEL ThML (10.8 MB, 5,322 terms)
- `00_raw/xml/torrey_ttt.xml` — original CCEL ThML (8.7 MB, 623 terms)
- `00_raw/nave/bible_topics_naves.txt` — raw text export (75K lines)
- `02_sources/nave/` — 5,320 per-topic JSONs (canonical_id: NAV:*)
- `02_sources/torrey/` — 622 per-topic JSONs (canonical_id: TOR:*)

### Changed
- Renamed `01_unified/` → `01_parsed/` (aligned with bible-dictionary-dataset convention)
- Renamed `02_pilot/` → `03_pilot/` (sequential layer numbering)
- Removed `cross_reference_network`, `connected_topics`, `cross_ref_stats` from 6,577 files in 01_parsed (methodological independence from crossrefs)
- Fixed all script paths to match actual repo structure
- README clarifies Nave+Torrey are topical sources; Easton+Smith are external definition deps

### Fixed
- `_index.json` total_entries corrected from 7,901 to 7,873

## [1.0.0] - 2026-03-14

### Added
- Initial release with 7,873 unified topics from Nave + Torrey
- Definitions merged from Easton + Smith (external: bible-dictionary-dataset)
- V3 unified with definitions (75.6%), AI enrichment (48.7%)
- Enriched pilot subset (336 topics, 100% AI enrichment)
- Build scripts: create_v3_unified.py, extract_definition_refs.py, integrate_crossrefs.py
