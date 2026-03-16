# Bible Topics Dataset

7,873 biblical topics unified from two classic topical reference works (Nave + Torrey), enriched with dictionary definitions from [bible-dictionary-dataset](https://github.com/neuu-org/bible-dictionary-dataset).

Part of the [NEUU](https://github.com/neuu-org) biblical scholarship ecosystem.

## Overview

| Metric | Value |
|--------|-------|
| Total topics (parsed) | 7,873 |
| Nave source topics | 5,320 |
| Torrey source topics | 622 |
| Topics with definitions | 5,955 (75.6%) |
| Topics with AI enrichment | 3,837 (48.7%) |
| Definition references extracted | 17,715 |

## Sources

### Topical sources (this repo)

| Source | Code | Topics | Period |
|--------|:---:|--------|--------|
| Nave's Topical Bible | NAV | 5,320 | 1896 |
| Torrey's New Topical Textbook | TOR | 622 | 1897 |

### Definition sources (external dependency)

Definitions from Easton (EAS) and Smith (SMI) are merged during the `create_v3_unified.py` step from [bible-dictionary-dataset](https://github.com/neuu-org/bible-dictionary-dataset). They are **not** stored in this repo.

All sources are **public domain**.

## Repository Structure

```
bible-topics-dataset/
├── data/
│   ├── 00_raw/
│   │   ├── xml/                     # Original CCEL ThML XML files
│   │   │   ├── nave_bible.xml       # Nave's Topical Bible (10.8 MB)
│   │   │   └── torrey_ttt.xml       # Torrey's Textbook (8.7 MB)
│   │   └── nave/
│   │       └── bible_topics_naves.txt  # Raw text export (75K lines)
│   │
│   ├── 01_parsed/                   # V3 merged (Nave + Torrey + Easton + Smith)
│   │   ├── A/ ... Z/               # 7,873 topics
│   │   └── _index.json
│   │
│   ├── 02_sources/                  # Separated by original source
│   │   ├── nave/                    # 5,320 per-topic JSONs
│   │   │   └── A/ ... Z/
│   │   └── torrey/                  # 622 per-topic JSONs
│   │       └── A/ ... Z/
│   │
│   └── 03_pilot/                    # AI-enriched subset (336 topics)
│       └── A/ (334), C/ (1), D/ (1)
│
├── scripts/
│   ├── parse_nave.py                # 00_raw/xml → 02_sources/nave (standalone, bs4)
│   ├── parse_torrey.py              # 00_raw/xml → 02_sources/torrey (standalone, bs4)
│   ├── create_v3_unified.py         # 02_sources + dictionary → 01_parsed
│   ├── extract_definition_refs.py   # 01_parsed → 01_parsed (+ definition_refs)
│   └── integrate_crossrefs.py       # 01_parsed + crossrefs → 01_parsed (external dep)
│
├── CHANGELOG.md
└── LICENSE
```

## Data Layers

### 00_raw — Original XML + Text

Original CCEL ThML XML files for Nave and Torrey. These are the authoritative sources before any parsing.

### 01_parsed — Merged V3

7,873 topics unified from all four sources. Each topic may contain:

| Field | Description | Coverage |
|-------|-------------|----------|
| `topic`, `slug`, `type` | Identity | 100% |
| `sources` | Origin (NAV, TOR, EAS, SMI) | 100% |
| `reference_groups` | Biblical references | 100% |
| `definitions` | Dictionary definitions | 75.6% |
| `definition_refs` | Refs extracted from definitions | 68.6% |
| `ai_enrichment` | Summary, outline, key concepts | 48.7% |

### 02_sources — Separated by Source

Per-file topics from each source independently. Useful for provenance tracking and avoiding circularity in research.

- **nave/** — 5,320 topics with `canonical_id: NAV:*`, reference_groups, see_also
- **torrey/** — 622 topics with `canonical_id: TOR:*`, reference_groups, hierarchical structure

### 03_pilot — Fully Enriched Subset

336 topics (primarily letter A) with 100% AI enrichment. Used as the official pilot for hybrid search research.

## Pipeline

```
00_raw/xml/nave_bible.xml   → parse_nave.py     → 02_sources/nave/    (5,320 topics)
00_raw/xml/torrey_ttt.xml   → parse_torrey.py   → 02_sources/torrey/  (622 topics)
                                    ↓
02_sources/ + bible-dictionary-dataset → create_v3_unified.py → 01_parsed/ (7,873 merged)
                                    ↓
01_parsed/ → extract_definition_refs.py → 01_parsed/ (+ definition_refs, 68.6%)
                                    ↓
01_parsed/ → run_phase0.py (AI enrichment, external) → 03_pilot/ (336 fully enriched)
```

### Requirements

```bash
pip install beautifulsoup4  # For parse_nave.py and parse_torrey.py
```

### Reproducing from scratch

```bash
# Step 1: Parse XMLs into per-source topics
python scripts/parse_nave.py --all --verbose
python scripts/parse_torrey.py --all --verbose

# Step 2: Merge into unified V3 (requires bible-dictionary-dataset as sibling dir)
python scripts/create_v3_unified.py

# Step 3: Extract definition references
python scripts/extract_definition_refs.py

# Step 4 (optional): Integrate cross-references (requires bible-crossrefs-dataset)
python scripts/integrate_crossrefs.py
```

## License

All source works are **public domain** (1863-1897). Unified dataset and AI enrichments: **CC BY 4.0**.

## Related Datasets (NEUU Ecosystem)

- [bible-commentaries-dataset](https://github.com/neuu-org/bible-commentaries-dataset) — 31,218 patristic commentaries
- [bible-crossrefs-dataset](https://github.com/neuu-org/bible-crossrefs-dataset) — 1.1M+ cross-references
- [bible-dictionary-dataset](https://github.com/neuu-org/bible-dictionary-dataset) — 20,900 dictionary entries
- [bible-text-dataset](https://github.com/neuu-org/bible-text-dataset) — 17 Bible translations
- [bible-gazetteers-dataset](https://github.com/neuu-org/bible-gazetteers-dataset) — 2,474 entities + 347 symbols
- [bible-hybrid-search](https://github.com/neuu-org/bible-hybrid-search) — Hybrid retrieval research
