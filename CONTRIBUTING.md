# Contributing

Guide for correcting data, proposing a new source, and running the pipeline
and test suite locally. This is a dataset repository, not an application —
most contributions are either a fix to a single record or a new source added
through the pipeline.

## Correcting a Data Error

This is the most likely first contribution. Open an issue (use the
**Data correction** template) or a pull request with:

- **Which file** — the exact path under `data/`, e.g.
  `data/01_parsed/A/abraham.json`.
- **What is wrong** — the incorrect field and its current value.
- **What it should be** — the corrected value.
- **How it was found** — cross-checked against the source XML in
  `data/00_raw/xml/`, a downstream consumer, manual review, etc.

**Files under `data/` are generated, not hand-authored.** They are produced by
the scripts in `scripts/` from the raw XML in `data/00_raw/`. A pull request
that edits a generated JSON file directly, without also fixing the script
that produces it, will be silently overwritten the next time the pipeline
runs. If the error is in the source XML itself, say so — that is usually not
fixable here at all (see "What Gets Rejected" below). If the error is
introduced during parsing or merging, the fix belongs in the relevant script
under `scripts/` (`parse_nave.py`, `parse_torrey.py`, `clean_sources.py`, or
`create_v3_unified.py`), followed by regenerating the affected output.

## Proposing a New Source

The bar for a new topical source:

- **Public domain, or compatibly licensed** — state which, and cite the
  publication year and author. This project only carries works whose licence
  is at least as permissive as [CC BY 4.0](LICENSE).
- **Provenance documented** — where the digitised text came from (e.g. CCEL),
  and a link to it.
- **A parser** — a script under `scripts/` that turns the raw source into the
  per-topic JSON shape used by `data/02_sources/`, following the pattern of
  `parse_nave.py` / `parse_torrey.py`.

Open an issue with the **New source** template first, before writing a
parser, so the licence and scope can be confirmed.

## Running the Pipeline Locally

See the README's [Reproducing from scratch](README.md#reproducing-from-scratch)
section for the exact commands and the [Pipeline](README.md#pipeline) diagram
for how the layers relate. Dependencies are pinned in
[requirements.txt](requirements.txt):

```bash
pip install -r requirements.txt
```

## Running the Tests

```bash
pip install -r requirements-dev.txt
python -m pytest tests/ -v
```

Expect **9 tests: 5 pass, 4 xfail**. The 4 `xfail(strict=True)` results are
**deliberate tripwires for known, unfixed bugs** (slug collisions and the Nave
reference-extraction indentation bug) — they are not a broken suite. If your
change happens to fix one of them, the marked test will flip from an expected
failure to an **unexpected pass**, which turns the suite red. That is the
signal to remove that `xfail` marker as part of your fix, not to work around
it.

## The Schema Contract

Consumers depend on the shape of the files under `data/01_parsed/` and
`data/02_sources/` — the field names, the file layout (`{LETTER}/{slug}.json`),
and the meaning of `canonical_id` (`NAV:*` / `TOR:*`). Breaking changes
include: renaming or removing an existing field, changing a field's type,
changing the slug rule, or changing the directory layout. Additive changes
(a new optional field) are not breaking. If your PR requires a breaking
change, say so explicitly and explain why a non-breaking alternative isn't
possible — it will need a version bump and a CHANGELOG entry.

As above: **everything under `data/` is generated**. Regenerate it with the
script that owns that layer rather than hand-editing the JSON.

## Licence of Contributions

By contributing, you agree your contribution is offered under the same terms
as the rest of the dataset. See [LICENSE](LICENSE) for the full CC BY 4.0
text and [NOTICE](NOTICE) for attribution requirements and source provenance.
Do not propose a source whose licence is incompatible with CC BY 4.0.

## What Gets Rejected

- Non-public-domain or incompatibly licensed sources.
- Hand-edited files under `data/` that don't come with the script change that
  produces them.
- Changes to `data/00_raw/` — these are the original digitised source files
  and are not edited, only re-parsed.
- Renaming or reshaping existing fields without a migration plan (see "The
  Schema Contract").
- Style-only rewrites of scripts with no behavioural change.
- Dictionary-definition or AI-enrichment work aimed at the full
  `data/01_parsed` layer — that is tracked as a roadmap item in
  [docs/ENRICHMENT_PLAN.md](docs/ENRICHMENT_PLAN.md) and is not yet wired
  into this pipeline; open an issue to discuss before building it.

## Pull Requests

Use conventional commits (`fix:`, `feat:`, `docs:`, ...) and fill in the pull
request template — it asks what changed, which issue it closes, whether
generated data was regenerated and with which script, and how you verified
it.
