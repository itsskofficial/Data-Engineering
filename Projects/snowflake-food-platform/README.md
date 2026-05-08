# Food platform — Snowflake warehouse project

This repository is a **Snowflake-native** implementation inspired by the end-to-end food-aggregator walkthrough summarized in `temp.md` (Medium: Swiggy-style ELT, layers, streams, SCD2, and KPI views). It is **not a verbatim copy**: naming, layering, some transformations, the calendar build, and the Streamlit layout were reworked so you can use it as your own portfolio baseline.

## What you get

| Layer | Schema | Role |
|--------|--------|------|
| Raw ingest | `LANDING` | Wide tables, all text-friendly columns, file metadata, append-only streams |
| Conformed hub | `REFINED` | Typed entities and facts merged from streams |
| Star mart | `ANALYTICS` | SCD2 dimensions, `FACT_ORDER_LINE`, KPI views |
| Policy objects | `GOVERNANCE` | Tag + masking policies for PII-like columns |

Warehouse: `FOOD_ETL_WH`. Database: `FOOD_PLATFORM_DW`.

## Prerequisites

1. Snowflake account with permission to create databases, warehouses, and masking policies.
2. Sample CSV bundle aligned with the tutorial (same broad folder layout under an internal stage). The original article links a Google Drive archive; unzip and upload paths such as:
   - `@FOOD_PLATFORM_DW.LANDING.RAW_FILES_STG/initial/location/`
   - `@FOOD_PLATFORM_DW.LANDING.RAW_FILES_STG/initial/restaurant/`
   - `@FOOD_PLATFORM_DW.LANDING.RAW_FILES_STG/initial/customer/`
   - …and matching `delta/` paths if you replay incremental loads.

Use **Snowsight → Data → Stages** (or `PUT`) so directory names match the `COPY` statements in `sql/*.sql`.

## How to run the SQL

Execute scripts **in numeric order** with a role such as `SYSADMIN`:

1. `sql/00_setup.sql` — warehouse, database, schemas, file format, stage, tags, masking policies  
2. `sql/01_location.sql` … `sql/09_order_line.sql` — landing → refined → analytics pipelines (each script ends with merges that depend on prior objects)  
3. `sql/10_date_dim.sql` — rebuilds `ANALYTICS.DIM_DATE_SPINE` from the earliest `FACT_ORDER_HEADER.ordered_at` through today  
4. `sql/11_fact_and_views.sql` — fact population from `FACT_ORDER_LINE` stream plus KPI views  

**Note:** If `10_date_dim.sql` runs before orders exist, the spine still loads using a five-year lookback from today (see `COALESCE` in that file). Re-run `10` after the first successful order loads if you need the spine to start at the true business minimum.

If a `COPY` fails, confirm files exist at the stage paths and that CSV column order matches the `$1…$n` mapping (adjust mappings locally if your extract differs).

## Streamlit

- File: `streamlit/app.py` — written for **Streamlit in Snowflake** (`get_active_session()`).
- Point the app object at `streamlit/app.py` and ensure the app role can read `FOOD_PLATFORM_DW.ANALYTICS` views.
- For local prototyping outside Snowflake, replace session access with `snowflake.connector` or Snowpark session bootstrap and keep the same SQL against your warehouse.

Optional local deps: `pip install -r requirements.txt`.

## Reference

Conceptual source: [SWIGGY — End To End Data Engineering Project](https://data-engineering-simplified.medium.com/swiggy-end-to-end-data-engineering-project-3f1af55005bf) (Data Engineering Simplified, 2024). This project reimplements the ideas with the variations above.

## License

Use and modify freely for learning and portfolio work; respect the upstream dataset’s terms if you reuse their CSVs.
