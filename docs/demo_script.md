# 🎬 Demo Script — Healthcare Supply Chain Analytics Platform
# Estimated duration: 7–9 minutes
# Tone: Confident, conversational, technically precise
# Format: On-camera or screen recording with project files open

---

## ── OPENING  [0:00 – 0:45] ──────────────────────────────────────────────────

"Hi — in this walkthrough I'm going to take you through an end-to-end data
engineering project I built around healthcare supply chain analytics.

The core problem this platform solves is real: hospitals and health networks
struggle to track whether critical medical supplies — ARV drugs, HIV test kits,
surgical stock — are actually arriving on time, staying in stock, and being
procured efficiently. When that visibility breaks down, patient care breaks down
with it.

So I built a system that takes raw supply chain data from three public sources,
moves it through a fully automated AWS pipeline, lands it in a Redshift data
warehouse, and surfaces it as live operational dashboards. Let me show you how
it's put together."

---

## ── DATASETS & SOURCES  [0:45 – 2:00] ─────────────────────────────────────
[Open: datasets/data_sources.md]

"Let's start with the data, because good engineering starts with understanding
your sources.

I'm using three real public datasets — and I want to be clear, these are
production-quality sources, not toy CSVs.

The primary shipment data comes from the USAID Supply Chain Management System —
the SCMS dataset. It's mirrored on Kaggle (originally from USAID, now offline) and covers actual ARV
and HIV commodity shipments to sub-Saharan Africa and Southeast Asia going back
to 2006. The key columns I care about are things like: the vendor name, the
shipment mode — air, sea, truck — the scheduled versus actual delivery dates,
and the freight cost.

The inventory data comes from a hospital supply chain dataset on Kaggle. It
gives me item-level stock positions: quantity on hand, reorder levels, safety
stock thresholds — the things you need to know whether a facility is about to
run out of something critical.

And the procurement data comes from DataCo's Smart Supply Chain dataset — also
on Kaggle — which gives me order-level records: quantities, pricing, delivery
risk flags, and shipping modes.

Now, none of these datasets drop cleanly into a warehouse out of the box. They
have messy column names, mixed date formats, and none of them share a common
key. That's where the pipeline does the heavy lifting."

---

## ── ARCHITECTURE OVERVIEW  [2:00 – 3:00] ──────────────────────────────────
[Open: README.md — Architecture section]

"Here's the architecture at a high level.

Raw CSV files land in S3. AWS Glue Crawlers catalogue them automatically. Four
PySpark Glue jobs then pick up each entity — inventory, shipments, procurement,
and KPIs — and write clean Parquet output back to a curated S3 layer,
partitioned by date and category for query efficiency.

From there, Redshift loads the curated data into a star schema. dbt runs on top
of that — handling staging views, incremental fact table loads, and a KPI
summary table that Power BI reads directly.

The entire pipeline is orchestrated by Airflow, running daily at 3am UTC. And
everything that goes wrong — bad records, freshness failures, schema drift —
gets caught and logged before it reaches the warehouse."

---

## ── GLUE JOB: SHIPMENT PROCESSING  [3:00 – 4:30] ──────────────────────────
[Open: glue_jobs/shipment_processing.py]

"Let me open the shipment processing job, because it shows the pattern I use
across all four jobs.

You can see the source schema defined at the top — every column is named exactly
as it appears in the USAID CSV, with an inline comment mapping it to the project
column. So 'vendor' becomes 'supplier_name'. 'po_sent_to_vendor_date' becomes
'ship_date'. 'line_item_value' becomes 'shipment_cost'. That mapping is
intentional and documented — anyone picking this up knows exactly where each
field came from.

The transformation logic does a few key things.

First, date parsing. The USAID dataset uses at least two date formats — a
day-month-year format like '2-Jun-06', and a standard M/d/yyyy. The parse
function tries both using coalesce, so nothing silently drops a null date.

Second, I derive priority from the product group. ARVs become CRITICAL, HRDT
test kits become HIGH, and so on. That priority then drives the SLA threshold —
a CRITICAL shipment has a two-day window, STANDARD gets five.

And third, I compute 'is_sla_met' and 'delivery_delay_days' right here in the
Glue job — so by the time data reaches Redshift, the KPI logic is already baked
in. dbt doesn't have to re-derive it."

---

## ── DATA QUALITY FRAMEWORK  [4:30 – 5:15] ─────────────────────────────────
[Open: data_quality/quality_framework.py]

"Before anything writes to the curated layer, it goes through a data quality
engine I built as a reusable class.

Each entity has a pre-built rule set — null checks, duplicate checks, range
validation, accepted values, and freshness. Rules carry a severity: ERROR rules
fail the job and quarantine the bad records. WARN rules log the issue but let
the pipeline continue.

For shipments, for example: if a shipment ID is null, that's an ERROR — the
record goes to quarantine. If the carrier field is null, that's a WARN — it
gets flagged but still loads. Every result is written to an audit Parquet
partition, so we have a full quality history by date and entity."

---

## ── AIRFLOW DAG  [5:15 – 6:00] ────────────────────────────────────────────
[Open: airflow/dags/supply_chain_pipeline_dag.py]

"The Airflow DAG wires everything together.

It opens with a source file validation step — it checks that all four S3
prefixes actually have data before firing any Glue jobs. No point spinning up
a cluster if the files didn't land.

The three entity jobs — inventory, shipments, procurement — run in parallel.
Once all three complete, the KPI aggregation job runs. Then a freshness check
confirms the curated partitions exist for the run date. Then dbt runs its models
and tests. And finally a notification step fires on success.

I'm using 'EmptyOperator' for the start and end bookends — that's the Airflow
2.9 equivalent of the old DummyOperator, which is deprecated. Small thing, but
it matters in a production environment."

---

## ── DBT MODELS & STAR SCHEMA  [6:00 – 7:15] ──────────────────────────────
[Open: dbt/models/staging/stg_shipments.sql → then dbt/models/marts/fact_shipments.sql]

"On the transformation side, dbt handles two things: staging and mart models.

The staging layer is simple — it's a view that casts types, trims whitespace,
and filters out known bad rows. The comment header in each staging model
explicitly documents which source dataset and column it maps from. So anyone
reading 'stg_shipments' can see: 'this comes from the USAID dataset, and
shipment_mode maps to carrier.'

The mart layer is where the star schema lives. 'fact_shipments' is an
incremental model — it only processes records newer than the current
high-watermark, so it stays fast even as the table grows. It's distributed on
'supplier_key' in Redshift, which optimises the supplier performance queries
that hit this table most.

The schema tests in 'tests/schema.yml' cover the key guarantees: shipment_id
is unique and not null, priority only takes the four accepted values, and the
'kpi_operational_summary' table has a recency test that fails if the data is
more than one day stale."

---

## ── KPIs & DASHBOARDS  [7:15 – 8:15] ──────────────────────────────────────
[Open: docs/kpi_definitions.md → then dashboards/dashboard_design_guide.md]

"The platform tracks ten operational KPIs. Let me highlight the three that
matter most in a healthcare context.

Inventory Fill Rate — the percentage of SKUs that are not in stockout. Target
is 95%. Below 90% triggers an alert. A stock shortage of a critical ARV isn't
a supply chain inconvenience — it's a patient care failure.

Shipment SLA Percent — the share of completed deliveries that arrived on or
before the scheduled date, segmented by priority tier and carrier. Target is
90%.

And Operational Health Score — a composite: 40% inventory fill rate, 60%
shipment SLA. That single number tells an executive whether the network is
healthy at a glance. Target is 88.

The Power BI design guide specifies four dashboards: an executive overview,
inventory analytics with a per-hospital SKU matrix, shipment performance broken
down by carrier and priority, and supplier reliability with tier distribution."

---

## ── WRAP-UP  [8:15 – 9:00] ────────────────────────────────────────────────

"To wrap up — what I've built here is a production-pattern data pipeline: raw
data ingested and validated, a curated layer in Parquet, a Redshift star schema
optimised for analytical queries, dbt models with automated tests, and Airflow
orchestrating the daily run.

The design choices are deliberate. Everything is documented at the source level
— column mappings in the Glue jobs, dataset URLs in the schema SQL, transformation
logic in the dbt staging views. A new engineer can onboard to this without
needing to track me down.

The three things I'd highlight as production-ready patterns here are: the
quarantine-first quality framework, the incremental dbt mart strategy, and the
SLA-aware priority derivation that pushes business logic as early in the
pipeline as possible — in the Glue job — rather than scattering it across
multiple layers.

Thanks for watching."

---

## ── SCREEN FLOW GUIDE ───────────────────────────────────────────────────────

Segment             File to show                         Duration
──────────────────────────────────────────────────────────────────
Opening             Nothing / face cam                    0:45
Datasets            datasets/data_sources.md              1:15
Architecture        README.md (Architecture section)      1:00
Glue job            glue_jobs/shipment_processing.py      1:30
Data quality        data_quality/quality_framework.py     0:45
Airflow DAG         airflow/dags/supply_chain_pipeline_dag.py  0:45
dbt models          stg_shipments.sql → fact_shipments.sql  1:15
KPIs                docs/kpi_definitions.md → dashboard_design_guide.md  1:00
Wrap-up             README.md (KPI table) or face cam     0:45
──────────────────────────────────────────────────────────────────
Total                                                     ~9:00

## ── RECORDING TIPS ─────────────────────────────────────────────────────────

- Use a dark theme in your editor (VS Code / Cursor) — code is easier to read
- Increase font size to 16–18pt before recording
- Zoom into the specific function or section you're discussing — don't show
  the whole file at once; it looks cluttered
- Pause 1 second before switching files — gives viewers time to process
- For the Airflow DAG section, scroll slowly down the dependency graph at the
  bottom so viewers can follow the task chain
- If recording audio separately, use the script verbatim for the first pass,
  then shorten naturally on re-reads
