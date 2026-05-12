-- ============================================================
-- Redshift Star Schema DDL
-- Healthcare Supply Chain Analytics Platform
--
-- Dataset ranges covered:
--   Shipments  : USAID SCMS data 2006-01-01 onwards
--   Inventory  : Hospital Supply Chain (Kaggle) — date range per dataset
--   Procurement: DataCo Smart Supply Chain 2015-01-01 to 2019-12-31
-- ============================================================

CREATE SCHEMA IF NOT EXISTS hsc_dw;
CREATE SCHEMA IF NOT EXISTS hsc_staging;
CREATE SCHEMA IF NOT EXISTS hsc_audit;

SET search_path TO hsc_dw;

-- ── dim_date ──────────────────────────────────────────────
-- Range: 2006-01-01 (earliest USAID record) to 2030-12-31
CREATE TABLE IF NOT EXISTS hsc_dw.dim_date (
    date_key      INTEGER     NOT NULL ENCODE AZ64,
    full_date     DATE        NOT NULL,
    year          SMALLINT    NOT NULL,
    quarter       SMALLINT    NOT NULL,
    month         SMALLINT    NOT NULL,
    month_name    VARCHAR(10) NOT NULL,
    week_of_year  SMALLINT    NOT NULL,
    day_of_month  SMALLINT    NOT NULL,
    day_of_week   SMALLINT    NOT NULL,
    day_name      VARCHAR(10) NOT NULL,
    is_weekend    BOOLEAN     NOT NULL DEFAULT FALSE
)
DISTSTYLE ALL
SORTKEY (full_date);

-- ── dim_supplier ──────────────────────────────────────────
-- SCD Type 2: tracks changes to reliability_tier and reliability_score
-- Sourced from synthetic supplier master; supplier_id derived from USAID vendor hash
CREATE TABLE IF NOT EXISTS hsc_dw.dim_supplier (
    supplier_key       INTEGER IDENTITY(1,1) NOT NULL,
    supplier_id        VARCHAR(50)  NOT NULL,   -- SUP-{6-digit hash of vendor name}
    supplier_name      VARCHAR(200) NOT NULL,   -- USAID source: vendor column
    supplier_type      VARCHAR(50),             -- DISTRIBUTOR, MANUFACTURER, GPO, WHOLESALER
    country            VARCHAR(100),
    state              VARCHAR(100),
    city               VARCHAR(100),
    contact_email      VARCHAR(200),
    contract_start_dt  DATE,
    contract_end_dt    DATE,
    payment_terms      VARCHAR(50),
    preferred_flag     BOOLEAN DEFAULT FALSE,
    reliability_tier   VARCHAR(20),             -- PLATINUM, GOLD, SILVER, WATCH_LIST
    reliability_score  NUMERIC(5,2),
    is_active          BOOLEAN DEFAULT TRUE,
    -- SCD Type 2
    effective_start_dt DATE    NOT NULL DEFAULT CURRENT_DATE,
    effective_end_dt   DATE    NOT NULL DEFAULT '9999-12-31',
    is_current         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
DISTSTYLE ALL
SORTKEY (supplier_id, is_current);

-- ── dim_hospital ──────────────────────────────────────────
-- hospital_id derived from USAID "country" field (HOSP-{4-char country code})
-- Extended with facility metadata from hospital supply chain dataset
CREATE TABLE IF NOT EXISTS hsc_dw.dim_hospital (
    hospital_key    INTEGER IDENTITY(1,1) NOT NULL,
    hospital_id     VARCHAR(50)  NOT NULL,   -- e.g. HOSP-TANZ, HOSP-NIGE
    hospital_name   VARCHAR(200) NOT NULL,
    hospital_type   VARCHAR(50),             -- GENERAL, SPECIALTY, CLINIC
    region          VARCHAR(50),
    country         VARCHAR(100),            -- from USAID: country
    state           VARCHAR(100),
    city            VARCHAR(100),
    bed_count       INTEGER,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
DISTSTYLE ALL
SORTKEY (hospital_id);

-- ── dim_product ───────────────────────────────────────────
-- Product names from USAID item_description; categories from product_group
CREATE TABLE IF NOT EXISTS hsc_dw.dim_product (
    product_key     INTEGER IDENTITY(1,1) NOT NULL,
    product_id      VARCHAR(50)  NOT NULL,
    product_name    VARCHAR(500) NOT NULL,   -- USAID: item_description (can be long)
    category        VARCHAR(100) NOT NULL,   -- USAID: product_group (ARV, HRDT, ANTIM...)
    subcategory     VARCHAR(100),            -- USAID: sub_classification
    unit_of_measure VARCHAR(50),             -- USAID: unit_of_measure_per_pack
    is_critical     BOOLEAN DEFAULT FALSE,   -- USAID: first_line_designation = 'true'
    dosage_form     VARCHAR(100),            -- USAID: dosage_form
    manufacturer    VARCHAR(200),            -- USAID: manufacturing_site
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
DISTSTYLE ALL
SORTKEY (product_id, category);

-- ── fact_inventory ────────────────────────────────────────
-- Source : Hospital Supply Chain (Kaggle/vanpatangan)
-- Grain  : one row per inventory_id (item_id + hospital_id + record_date)
CREATE TABLE IF NOT EXISTS hsc_dw.fact_inventory (
    inventory_fact_key    BIGINT IDENTITY(1,1) NOT NULL,
    date_key              INTEGER      NOT NULL REFERENCES hsc_dw.dim_date(date_key),
    hospital_key          INTEGER      NOT NULL REFERENCES hsc_dw.dim_hospital(hospital_key),
    product_key           INTEGER      NOT NULL REFERENCES hsc_dw.dim_product(product_key),
    supplier_key          INTEGER               REFERENCES hsc_dw.dim_supplier(supplier_key),
    -- Natural key: item_id + hospital_id + record_date (constructed in Glue job)
    inventory_id          VARCHAR(100) NOT NULL,
    -- Measures from source: quantity_on_hand, reorder_level, safety_stock, unit_cost
    quantity_on_hand      INTEGER      NOT NULL DEFAULT 0,
    reorder_level         INTEGER,
    safety_stock          INTEGER,
    unit_cost             NUMERIC(10,4),
    inventory_value       NUMERIC(14,2),        -- derived: quantity * unit_cost
    days_since_restock    INTEGER,
    restock_urgency_score SMALLINT,             -- 0-100 derived score
    -- Status (derived in Glue from business rules)
    stock_status          VARCHAR(20),          -- STOCKOUT,CRITICAL_LOW,LOW,ADEQUATE,OVERSTOCK
    is_critical_shortage  BOOLEAN DEFAULT FALSE,
    record_date           DATE,
    etl_run_date          DATE,
    etl_load_ts           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
DISTKEY (hospital_key)
SORTKEY (date_key, product_key);

-- ── fact_shipments ────────────────────────────────────────
-- Source : USAID Supply Chain Shipment & Pricing Data (SCMS)
-- URL    : https://data.usaid.gov/api/views/a3rc-nmf6/rows.csv
-- Grain  : one row per shipment (USAID "id" column)
CREATE TABLE IF NOT EXISTS hsc_dw.fact_shipments (
    shipment_fact_key     BIGINT IDENTITY(1,1) NOT NULL,
    ship_date_key         INTEGER     NOT NULL REFERENCES hsc_dw.dim_date(date_key),
    delivery_date_key     INTEGER               REFERENCES hsc_dw.dim_date(date_key),
    supplier_key          INTEGER     NOT NULL  REFERENCES hsc_dw.dim_supplier(supplier_key),
    hospital_key          INTEGER               REFERENCES hsc_dw.dim_hospital(hospital_key),
    product_key           INTEGER               REFERENCES hsc_dw.dim_product(product_key),
    -- USAID "id" column → shipment_id
    shipment_id           VARCHAR(50) NOT NULL,
    quantity_shipped      INTEGER,              -- USAID: line_item_quantity
    shipment_cost         NUMERIC(12,2),        -- USAID: line_item_value (USD)
    cost_per_unit         NUMERIC(10,4),        -- USAID: unit_price
    freight_cost          NUMERIC(12,2),        -- USAID: freight_cost_usd
    planned_transit_days  SMALLINT,
    actual_transit_days   SMALLINT,
    delivery_delay_days   SMALLINT,             -- negative = early
    sla_threshold_days    SMALLINT,             -- derived from priority
    -- Derived columns
    priority              VARCHAR(20),          -- from product_group: ARV→CRITICAL, HRDT→HIGH
    carrier               VARCHAR(100),         -- USAID: shipment_mode (Air/Sea/Truck)
    delivery_performance  VARCHAR(30),          -- EARLY,ON_TIME,SLIGHTLY_LATE,SIGNIFICANTLY_LATE
    is_sla_met            BOOLEAN,
    is_critical           BOOLEAN,              -- USAID: first_line_designation
    ship_date             DATE,
    expected_delivery_date DATE,
    actual_delivery_date  DATE,
    etl_run_date          DATE,
    etl_load_ts           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
DISTKEY (supplier_key)
SORTKEY (ship_date_key, hospital_key);

-- ── fact_procurement ──────────────────────────────────────
-- Source : DataCo Smart Supply Chain for Big Data Analysis
-- URL    : https://www.kaggle.com/datasets/shashwatwork/dataco-smart-supply-chain-for-big-data-analysis
-- Grain  : one row per purchase_order_id (DataCo "Order Id" prefixed "PO-")
CREATE TABLE IF NOT EXISTS hsc_dw.fact_procurement (
    procurement_fact_key  BIGINT IDENTITY(1,1) NOT NULL,
    order_date_key        INTEGER      NOT NULL REFERENCES hsc_dw.dim_date(date_key),
    supplier_key          INTEGER      NOT NULL REFERENCES hsc_dw.dim_supplier(supplier_key),
    hospital_key          INTEGER               REFERENCES hsc_dw.dim_hospital(hospital_key),
    product_key           INTEGER               REFERENCES hsc_dw.dim_product(product_key),
    purchase_order_id     VARCHAR(50)  NOT NULL, -- DataCo: "PO-" + Order_Id
    quantity_ordered      INTEGER,               -- DataCo: Order_Item_Quantity
    unit_price            NUMERIC(10,4),         -- DataCo: Product_Price
    total_order_value     NUMERIC(14,2),         -- DataCo: Order_Item_Total
    budget_amount         NUMERIC(14,2),         -- derived: total × 1.05
    cost_variance_pct     NUMERIC(6,2),          -- (actual - budget) / budget × 100
    lead_time_days        SMALLINT,              -- DataCo: Days_for_shipment_scheduled
    po_status             VARCHAR(30),           -- mapped from DataCo: Order_Status
    approval_level        VARCHAR(20),           -- mapped from DataCo: Shipping_Mode
    is_sla_met            BOOLEAN,               -- inverted DataCo: Late_delivery_risk
    order_date            DATE,
    etl_run_date          DATE,
    etl_load_ts           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
DISTKEY (supplier_key)
SORTKEY (order_date_key, hospital_key);

-- ── Audit table ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS hsc_audit.pipeline_audit (
    audit_id          BIGINT IDENTITY(1,1),
    job_name          VARCHAR(200) NOT NULL,
    run_date          DATE         NOT NULL,
    entity            VARCHAR(50)  NOT NULL,
    raw_count         INTEGER,
    curated_count     INTEGER,
    quarantine_count  INTEGER,
    status            VARCHAR(20),
    error_message     VARCHAR(2000),
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
DISTSTYLE EVEN
SORTKEY (run_date, entity);

GRANT SELECT ON ALL TABLES IN SCHEMA hsc_dw    TO GROUP analytics_users;
GRANT ALL    ON ALL TABLES IN SCHEMA hsc_dw    TO GROUP data_engineers;
GRANT SELECT ON ALL TABLES IN SCHEMA hsc_audit TO GROUP data_engineers;

COMMENT ON TABLE hsc_dw.fact_shipments   IS 'USAID SCMS shipment records with SLA performance';
COMMENT ON TABLE hsc_dw.fact_inventory   IS 'Hospital Supply Chain daily inventory snapshots';
COMMENT ON TABLE hsc_dw.fact_procurement IS 'DataCo orders adapted as procurement POs';
COMMENT ON TABLE hsc_dw.dim_supplier     IS 'Supplier master with SCD Type 2 history';
