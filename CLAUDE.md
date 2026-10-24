# CLAUDE.md — RDAIngest.jl

Guidance for working in this package. Keep changes **simple, elegant, efficient**. Do not
over-engineer. Do not change the relational schema without explicit confirmation.

## What this package is

Julia package to (1) build a **Reference Data Archive (RDA)** relational database from scratch
and (2) ingest research data, metadata, and supporting documents into it. Designed to be
standardized enough for federated multi-site studies, yet flexible enough for many data types
(structured survey/surveillance/registry data, and — as a goal — unstructured text/image/audio
and metadata files: pdf, docx, xlsx, csv).

## Core data model (do not redesign without sign-off)

An **EAV (entity-attribute-value)** long store, so any dataset shape can be ingested generically:

- `variables` (attributes) belong to a `domain`; each has a `value_type_id` (see `constants.jl`:
  Integer/Float/String/Date/Datetime/Time/Categorical). Categorical values use a `vocabulary`.
- `datasets` → `datarows` (one `row_id` per source row) → `data(row_id, variable_id, value TEXT)`.
  Every value is stored as TEXT and reconstructed on read.
- `dataset_variables` links datasets to their variables.
- Provenance: `data_ingestions`, `transformations` (+ `transformation_inputs`/`_outputs`,
  `ingest_datasets`).
- Source/context: `sources`, `sites` (unique on `source_id`+`site_name`), `protocols`, `ethics`,
  `instruments` (documents stored as BLOBs).
- Mortality/VA specialization: `deaths` + `death_rows` link a "death" entity to data rows;
  `ingest_deaths`, `link_deathrows`, `death_in_ingest`, `get_last_deathingest`.
- Harmonization: `variablemaps` / `variablemappings` (PyCrossVA-style cross-instrument mapping).

## File map

- `src/RDAIngest.jl` (~1900 lines) — module, exports, config structs, reading, ingest workflow,
  EAV read/write, lookups, export.
- `src/rdadatabase.jl` (~1450 lines) — schema DDL (`create*`) + low-level DB helpers
  (`savedataframe`, `insertwithidentity`, `selectdataframe`, connection open/create).
- `src/constants.jl` — value types, transformation types/statuses, unit-of-analysis, study types.
- `src/countrycode_iso3166.csv` — ISO3 country mapping, preloaded as `iso3_mapping`.
- `test/main.jl` — end-to-end usage script (CHAMPS, COMSA); `test/runtests*.jl`.

## Key conventions

- **Dual backend by dispatch:** most DB functions have two methods, `db::SQLite.DB` and
  `db::ODBC.Connection` (SQL Server). Schema DDL is written twice, once per dialect — keep the two
  in sync if you touch a table.
- **Config via structs:** `rawSource`, `sourceIngest`, `userIngest` are `Base.@kwdef mutable`
  structs holding file/folder names, CSV parse options, and metadata. Users configure by
  constructing these.
- **Document reading is extensible:** `read_data` dispatches on `DocCSV`/`DocXLSX`/`DocPDF`. Add a
  new `DataDocument` subtype + `read_data` method to support a new format.
- **Env-driven paths:** `RDA_DATABASE_PATH`, `RDA_DBNAME`, `DATA_INGEST_PATH`,
  `DATA_DICTIONARY_PATH`, `ISO3_PATH` (via `ConfigEnv.dotenv()`).
- **Idempotency is uneven:** `add_source` is get-or-insert; some others (`add_sites`) plain-insert
  and will hit UNIQUE constraints on re-ingest. Prefer get-or-insert for new/edited add_* code.

## Typical workflow

```julia
createdatabase(dbpath, dbname; replace=true, sqlite=true)   # once
ingest_source(source, dbpath, dbname, datapath)             # sites, protocols, ethics, instruments
ingest_dictionary(ingest, dbpath, dbname, dictpath)         # variables + vocabularies
id = ingest_deaths(ingest, dbpath, dbname, datapath)        # (mortality/VA) primary entities
ingest_data(ingest, dbpath, dbname, datapath; ingestion_id=id)  # datasets → EAV
```

Each top-level call currently opens and closes its own DB connection.

Single data value: `update_datavalue(db, dataset, id_col, id, variable, new_value; old_value)` (SQLite +
SQL Server; checks `old_value`, returns `:updated`/`:unchanged`). Metadata cell: `updatevalue` (SQLite only).

## Gotchas

- XLSX/CSV sources sometimes have a trailing blank-header column that reads in as a column named
  `missing`; it must be dropped before ingest or `save_dataset` throws `KeyError("missing")`.
- Ingestion runs inside a transaction; a mid-step failure rolls back the whole step.
- `test/main.jl` references `CHAMPSSource()`/`COMSAMZSource()` defined outside the package.

## Working agreements

- Report findings/recommendations before editing when asked; **do not change existing code
  until asked.** Follow the maintainer's instructions; ask when uncertain.
- Keep the RDA relational structure stable unless a change is explicitly approved.
- End-to-end runs may touch external services (e.g. NADA/Datacite in downstream R scripts) — never
  execute those without confirmation.
