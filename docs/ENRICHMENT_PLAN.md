# Enrichment Plan — AI-Enhanced Topics

## Status: PLANNED (not started)

The `03_pilot/` layer contains 336 topics with AI enrichment from the topical pipeline Phase 0.
This document describes the planned enrichment for the full dataset.

## Fields to add per aspect

| Field | Description | Model |
|-------|-------------|-------|
| `summary_pt` | Portuguese translation + explanation of the aspect | GPT-4o-mini |
| `theological_context` | Why this aspect matters within the topic | GPT-4o-mini |
| `doctrine_tags` | Theological tags (e.g., Propitiation, Redemption) | GPT-4o-mini |
| `difficulty_level` | basic / intermediate / advanced | GPT-4o-mini |

## Example

**Before (current):**
```json
{
  "label": "Is averted by Christ",
  "references": ["Romans 5:9", "Colossians 1:20", "1 Thessalonians 1:10"]
}
```

**After (enriched):**
```json
{
  "label": "Is averted by Christ",
  "references": ["Romans 5:9", "Colossians 1:20", "1 Thessalonians 1:10"],
  "summary_pt": "A ira de Deus e removida atraves do sacrificio de Cristo na cruz",
  "theological_context": "Connects the doctrine of propitiation with Christ's redemptive work",
  "doctrine_tags": ["Propitiation", "Redemption", "Soteriology"],
  "difficulty_level": "intermediate"
}
```

## Pipeline

```
02_sources/ (raw Torrey/Nave) → 01_parsed/ (merged) → 04_enriched/ (AI-enhanced)
```

## Priority

Low — the raw Torrey data is already gold-quality curated data.
Enrichment adds accessibility (PT translation, explanations) but is not required for the TCC benchmark.
