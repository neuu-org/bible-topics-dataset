# Bible Topics Dataset

Unified dataset of 7,873 biblical topics built from four classic reference works, enriched with cross-references, dictionary definitions, and AI-generated analysis.

Part of the [NEUU](https://github.com/neuu-org) biblical scholarship ecosystem.

## Overview

| Metric | Value |
|--------|-------|
| Total topics | 7,873 |
| Topics with definitions | 5,955 (75.6%) |
| Topics with cross-references | 6,577 (83.5%) |
| Topics with AI enrichment | 3,837 (48.7%) |
| Topic connections | 112,835 |
| Definition references extracted | 17,715 |

## Sources

| Source | Type | Topics | Period |
|--------|------|--------|--------|
| Nave's Topical Bible (NAV) | Topical index | 5,234 | 1896 |
| Torrey's New Topical Textbook (TOR) | Hierarchical topics | 618 | 1897 |
| Easton's Bible Dictionary (EAS) | Definitions | 3,956 | 1897 |
| Smith's Bible Dictionary (SMI) | Encyclopedia | 4,546 | 1863 |

All sources are **public domain**.

## Repository Structure

```
bible-topics-dataset/
├── data/
│   ├── 00_raw/
│   │   ├── nave/                    # Nave's Topical Bible (raw text + parsed)
│   │   └── torrey/                  # Torrey's parsed topics (622 JSON files)
│   │
│   ├── 01_unified/                  # V3 production (7,873 topics)
│   │   ├── A/ ... Z/               # 26 letter directories
│   │   └── _index.json             # General index
│   │
│   └── 02_pilot/                    # AI-enriched subset (336 topics, 100% enriched)
│       └── A/ (334), C/ (1), D/ (1)
│
├── scripts/
│   ├── create_v3_unified.py         # Merge sources into V3
│   ├── extract_definition_refs.py   # Extract refs from definitions
│   └── integrate_crossrefs.py       # Add cross-reference networks
│
├── CHANGELOG.md
└── LICENSE
```

## Data Layers

### 00_raw — Original Sources

Raw data from Nave's Topical Bible (text) and Torrey's New Topical Textbook (parsed JSON). These are the original inputs to the V3 creation process.

### 01_unified — V3 Production

7,873 topics unified from all four sources. Each topic may contain:

| Field | Description | Coverage |
|-------|-------------|----------|
| `topic`, `slug`, `type` | Identity | 100% |
| `sources` | Origin (NAV, TOR, EAS, SMI) | 100% |
| `reference_groups` | Biblical references | 100% |
| `definitions` | Dictionary definitions | 75.6% |
| `definition_refs` | Refs extracted from definitions | 68.6% |
| `cross_reference_network` | Cross-ref connections | 83.5% |
| `connected_topics` | Related topics (up to 20) | 83.5% |
| `ai_enrichment` | Summary, outline, key concepts | 48.7% |
| `ai_themes_normalized` | Extracted themes | 25.8% |
| `phase1_discovery` | Theme resolution results | 6.3% |

### 02_pilot — Fully Enriched Subset

336 topics (primarily letter A) with 100% AI enrichment. Used as the official pilot for the hybrid search research. Contains additional fields not present in all V3 topics:

- `ai_discovered_aspects` — new aspects not in Nave/Torrey
- `ai_entities` — biblical entity extraction
- `ai_relationships` — entity relationship graphs

## Build Chain

```
Nave + Torrey
    ↓ create_v3_unified.py (+ Easton + Smith definitions)
V3 base (7,873 topics)
    ↓ extract_definition_refs.py
+ definition_refs (68.6%)
    ↓ integrate_crossrefs.py
+ cross_reference_network (83.5%)
    ↓ run_phase0.py (AI enrichment, external)
+ ai_enrichment (48.7%)
```

## Usage

```bash
# Rebuild V3 from scratch (requires dictionary dataset)
python scripts/create_v3_unified.py

# Extract references from definitions
python scripts/extract_definition_refs.py

# Integrate cross-references (requires crossrefs dataset)
python scripts/integrate_crossrefs.py
```

## License

All source works are **public domain** (1863-1897). Unified dataset and AI enrichments: **CC BY 4.0**.

## Citation

```bibtex
@misc{neuu_bible_topics_2026,
  title={Bible Topics Dataset: Unified Biblical Topics from Classic Reference Works},
  author={NEUU},
  year={2026},
  publisher={GitHub},
  url={https://github.com/neuu-org/bible-topics-dataset}
}
```

## Related Datasets (NEUU Ecosystem)

- [bible-commentaries-dataset](https://github.com/neuu-org/bible-commentaries-dataset) — 31,218 patristic commentaries
- [bible-crossrefs-dataset](https://github.com/neuu-org/bible-crossrefs-dataset) — 1.1M+ cross-references
- [bible-hybrid-search](https://github.com/neuu-org/bible-hybrid-search) — Hybrid retrieval research
