"""
AWS Glue ETL Job: Shipment Processing
Healthcare Supply Chain Analytics Platform

Source Dataset : USAID Supply Chain Shipment & Pricing Data
Source URL     : https://www.kaggle.com/datasets/apoorvwatsky/supply-chain-shipment-pricing-data
Kaggle Mirror  : https://www.kaggle.com/datasets/apoorvwatsky/supply-chain-shipment-pricing-data

Source Columns (exact):
    id, project_code, pq_#, po_so_#, asn_dn_#, country, managed_by,
    fulfill_via, vendor_inco_term, shipment_mode,
    pq_first_sent_to_client_date, po_sent_to_vendor_date,
    scheduled_delivery_date, delivered_to_client_date, delivery_recorded_date,
    product_group, sub_classification, vendor, item_description,
    molecule_test_type, brand, dosage, dosage_form, unit_of_measure_per_pack,
    line_item_quantity, line_item_value, pack_price, unit_price,
    manufacturing_site, first_line_designation,
    weight_kilograms, freight_cost_usd, line_item_insurance_usd

Job: hsc-shipment-processing
Schedule: Daily @ 02:30 UTC
Input:  s3://hsc-analytics-raw/shipments/          (CSV)
Output: s3://hsc-analytics-curated/shipments/      (Parquet, partitioned by ship_date)
"""

import sys
import logging
from datetime import datetime, date
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType
)
from pyspark.sql.window import Window

# ──────────────────────────────────────────────
# INIT
# ──────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'env', 'run_date'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RUN_DATE        = args.get('run_date', str(date.today()))
RAW_PATH        = "s3://hsc-analytics-raw/shipments/"
CURATED_PATH    = "s3://hsc-analytics-curated/shipments/"
QUARANTINE_PATH = f"s3://hsc-analytics-curated/quarantine/shipments/run_date={RUN_DATE}/"

# SLA thresholds (days) by mapped priority tier
SLA_DAYS = {"CRITICAL": 2, "HIGH": 3, "STANDARD": 5, "LOW": 7}

# Product group → priority mapping (USAID source uses product_group, not priority)
PRODUCT_GROUP_TO_PRIORITY = {
    "ARV":   "CRITICAL",
    "HRDT":  "HIGH",
    "ANTIM": "HIGH",
    "MRDT":  "STANDARD",
    "ACT":   "STANDARD",
    "OTHER": "LOW",
}


# ──────────────────────────────────────────────
# SOURCE SCHEMA  (raw USAID CSV columns)
# Column names are lowercased and spaces replaced with underscores on ingest
# ──────────────────────────────────────────────
RAW_SCHEMA = StructType([
    StructField("id",                              StringType(), True),
    StructField("project_code",                    StringType(), True),
    StructField("pq_number",                       StringType(), True),  # source: pq #
    StructField("po_so_number",                    StringType(), True),  # source: po / so #
    StructField("asn_dn_number",                   StringType(), True),  # source: asn/dn #
    StructField("country",                         StringType(), True),
    StructField("managed_by",                      StringType(), True),
    StructField("fulfill_via",                     StringType(), True),
    StructField("vendor_inco_term",                StringType(), True),
    StructField("shipment_mode",                   StringType(), True),
    StructField("pq_first_sent_to_client_date",    StringType(), True),
    StructField("po_sent_to_vendor_date",          StringType(), True),  # → ship_date
    StructField("scheduled_delivery_date",         StringType(), True),  # → expected_delivery_date
    StructField("delivered_to_client_date",        StringType(), True),  # → actual_delivery_date
    StructField("delivery_recorded_date",          StringType(), True),
    StructField("product_group",                   StringType(), True),  # → priority
    StructField("sub_classification",              StringType(), True),
    StructField("vendor",                          StringType(), True),  # → supplier_name
    StructField("item_description",                StringType(), True),  # → product_name
    StructField("molecule_test_type",              StringType(), True),
    StructField("brand",                           StringType(), True),
    StructField("dosage",                          StringType(), True),
    StructField("dosage_form",                     StringType(), True),
    StructField("unit_of_measure_per_pack",        IntegerType(), True),
    StructField("line_item_quantity",              IntegerType(), True), # → quantity_shipped
    StructField("line_item_value",                 DoubleType(),  True), # → shipment_cost
    StructField("pack_price",                      DoubleType(),  True),
    StructField("unit_price",                      DoubleType(),  True), # → cost_per_unit
    StructField("manufacturing_site",              StringType(), True),
    StructField("first_line_designation",          StringType(), True),  # → is_critical
    StructField("weight_kilograms",                DoubleType(),  True),
    StructField("freight_cost_usd",                DoubleType(),  True), # → freight_cost
    StructField("line_item_insurance_usd",         DoubleType(),  True),
])


# ──────────────────────────────────────────────
# EXTRACT
# ──────────────────────────────────────────────
def extract():
    logger.info("[EXTRACT] Reading raw USAID shipment data from S3...")
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "false")
          .schema(RAW_SCHEMA)
          .csv(RAW_PATH))
    logger.info(f"[EXTRACT] Loaded {df.count()} records")
    return df


# ──────────────────────────────────────────────
# VALIDATE & QUARANTINE
# ──────────────────────────────────────────────
def validate_and_quarantine(df):
    df = (df
          .withColumn("_qc_null_id",
              F.when(F.col("id").isNull(), True).otherwise(False))
          .withColumn("_qc_null_vendor",
              F.when(F.col("vendor").isNull(), True).otherwise(False))
          .withColumn("_qc_bad_ship_date",
              F.when(
                  F.to_date(F.col("po_sent_to_vendor_date"), "M/d/yyyy").isNull() &
                  F.to_date(F.col("po_sent_to_vendor_date"), "yyyy-MM-dd").isNull(),
                  True).otherwise(False))
          .withColumn("_qc_bad_sched_date",
              F.when(
                  F.to_date(F.col("scheduled_delivery_date"), "M/d/yyyy").isNull() &
                  F.to_date(F.col("scheduled_delivery_date"), "yyyy-MM-dd").isNull(),
                  True).otherwise(False))
    )

    bad  = df.filter(
        F.col("_qc_null_id") | F.col("_qc_null_vendor") |
        F.col("_qc_bad_ship_date") | F.col("_qc_bad_sched_date")
    )
    good = df.filter(
        ~F.col("_qc_null_id") & ~F.col("_qc_null_vendor") &
        ~F.col("_qc_bad_ship_date") & ~F.col("_qc_bad_sched_date")
    )

    qc_cols = ["_qc_null_id", "_qc_null_vendor", "_qc_bad_ship_date", "_qc_bad_sched_date"]
    if bad.count() > 0:
        bad.withColumn("quarantine_reason",
              F.when(F.col("_qc_null_id"),      "MISSING_SHIPMENT_ID")
               .when(F.col("_qc_null_vendor"),   "MISSING_VENDOR")
               .when(F.col("_qc_bad_ship_date"), "INVALID_SHIP_DATE")
               .otherwise("INVALID_SCHED_DATE")
           ).drop(*qc_cols).write.mode("overwrite").parquet(QUARANTINE_PATH)
        logger.warning(f"[QUARANTINE] {bad.count()} records quarantined → {QUARANTINE_PATH}")

    return good.drop(*qc_cols)


# ──────────────────────────────────────────────
# TRANSFORM — rename source cols → project schema + derive KPI fields
# ──────────────────────────────────────────────
def transform(df):
    logger.info("[TRANSFORM] Renaming source columns and applying business logic...")

    # ── Date parsing (USAID uses mixed formats like "2-Jun-06" and "M/d/yyyy") ──
    def parse_usaid_date(col_name):
        """Try multiple date formats common in USAID dataset."""
        return F.coalesce(
            F.to_date(F.col(col_name), "d-MMM-yy"),
            F.to_date(F.col(col_name), "M/d/yyyy"),
            F.to_date(F.col(col_name), "yyyy-MM-dd"),
        )

    # ── Priority map from product_group ──
    priority_map = F.create_map(
        F.lit("ARV"),   F.lit("CRITICAL"),
        F.lit("HRDT"),  F.lit("HIGH"),
        F.lit("ANTIM"), F.lit("HIGH"),
        F.lit("MRDT"),  F.lit("STANDARD"),
        F.lit("ACT"),   F.lit("STANDARD"),
    )

    # ── SLA threshold map by priority ──
    sla_map = F.create_map(
        F.lit("CRITICAL"), F.lit(2),
        F.lit("HIGH"),     F.lit(3),
        F.lit("STANDARD"), F.lit(5),
        F.lit("LOW"),      F.lit(7),
    )

    df = (df
        # ── Rename: natural key ──
        .withColumnRenamed("id", "shipment_id")
        .withColumnRenamed("vendor", "supplier_name")
        .withColumnRenamed("country", "destination_country")   # hospital proxy
        .withColumnRenamed("shipment_mode", "carrier")
        .withColumnRenamed("item_description", "product_name")
        .withColumnRenamed("product_group", "product_group_raw")
        .withColumnRenamed("line_item_quantity", "quantity_shipped")
        .withColumnRenamed("line_item_value", "shipment_cost")
        .withColumnRenamed("unit_price", "cost_per_unit")
        .withColumnRenamed("freight_cost_usd", "freight_cost")
        .withColumnRenamed("first_line_designation", "is_first_line")  # "true"/"false" string

        # ── Parse dates ──
        .withColumn("ship_date",             parse_usaid_date("po_sent_to_vendor_date"))
        .withColumn("expected_delivery_date",parse_usaid_date("scheduled_delivery_date"))
        .withColumn("actual_delivery_date",  parse_usaid_date("delivered_to_client_date"))

        # ── Derive supplier_id from vendor name (hash-based) ──
        .withColumn("supplier_id",
            F.concat(F.lit("SUP-"),
                     F.abs(F.hash(F.col("supplier_name"))).cast("string").substr(1, 6)))

        # ── Derive hospital_id from destination country ──
        .withColumn("hospital_id",
            F.concat(F.lit("HOSP-"),
                     F.upper(F.regexp_replace(F.col("destination_country"), "[^A-Za-z]", ""))
                      .substr(1, 4)))

        # ── Standardize strings ──
        .withColumn("supplier_name",    F.upper(F.trim(F.col("supplier_name"))))
        .withColumn("carrier",          F.upper(F.trim(F.col("carrier"))))
        .withColumn("product_group_raw",F.upper(F.trim(F.col("product_group_raw"))))
        .withColumn("product_name",     F.upper(F.trim(F.col("product_name"))))

        # ── Derive priority from product_group ──
        .withColumn("priority",
            F.coalesce(priority_map[F.col("product_group_raw")], F.lit("LOW")))

        # ── SLA threshold ──
        .withColumn("sla_threshold_days",
            F.coalesce(sla_map[F.col("priority")], F.lit(5)))

        # ── Transit & delay metrics ──
        .withColumn("planned_transit_days",
            F.datediff(F.col("expected_delivery_date"), F.col("ship_date")))
        .withColumn("actual_transit_days",
            F.when(F.col("actual_delivery_date").isNotNull(),
                   F.datediff(F.col("actual_delivery_date"), F.col("ship_date"))))
        .withColumn("delivery_delay_days",
            F.when(F.col("actual_delivery_date").isNotNull(),
                   F.datediff(F.col("actual_delivery_date"), F.col("expected_delivery_date"))))

        # ── SLA met flag ──
        .withColumn("is_sla_met",
            F.when(F.col("actual_delivery_date").isNull(), F.lit(None).cast("boolean"))
             .when(F.col("delivery_delay_days") <= 0, F.lit(True))
             .otherwise(F.lit(False)))

        # ── Delivery performance tier ──
        .withColumn("delivery_performance",
            F.when(F.col("actual_delivery_date").isNull(), "IN_TRANSIT")
             .when(F.col("delivery_delay_days") < 0,  "EARLY")
             .when(F.col("delivery_delay_days") == 0, "ON_TIME")
             .when(F.col("delivery_delay_days").between(1, 2), "SLIGHTLY_LATE")
             .otherwise("SIGNIFICANTLY_LATE"))

        # ── is_critical from first_line_designation ──
        .withColumn("is_critical",
            F.when(F.lower(F.col("is_first_line")) == "true", True).otherwise(False))

        # ── Null-safe numerics ──
        .withColumn("quantity_shipped",
            F.coalesce(F.col("quantity_shipped"), F.lit(0)))
        .withColumn("shipment_cost",
            F.coalesce(F.col("shipment_cost"), F.lit(0.0)))
        .withColumn("cost_per_unit",
            F.coalesce(F.col("cost_per_unit"), F.lit(0.0)))
        .withColumn("freight_cost",
            F.coalesce(F.col("freight_cost"), F.lit(0.0)))
    )

    # ── Drop raw source columns no longer needed ──
    drop_cols = [
        "po_sent_to_vendor_date", "scheduled_delivery_date", "delivered_to_client_date",
        "delivery_recorded_date", "pq_first_sent_to_client_date", "is_first_line",
        "pq_number", "po_so_number", "asn_dn_number", "project_code",
    ]
    df = df.drop(*[c for c in drop_cols if c in df.columns])

    # ── Deduplication ──
    w = Window.partitionBy("shipment_id").orderBy(F.col("actual_delivery_date").desc_nulls_last())
    df = (df.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn"))

    # ── Audit columns ──
    df = (df
          .withColumn("etl_run_date", F.lit(RUN_DATE))
          .withColumn("etl_job_name", F.lit(args['JOB_NAME']))
          .withColumn("etl_load_ts",  F.current_timestamp()))

    logger.info(f"[TRANSFORM] Complete. Output records: {df.count()}")
    return df


# ──────────────────────────────────────────────
# LOAD
# ──────────────────────────────────────────────
def load(df):
    df.write.mode("overwrite") \
            .partitionBy("ship_date", "priority") \
            .parquet(CURATED_PATH)
    logger.info(f"[LOAD] Written to {CURATED_PATH}")


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main():
    try:
        raw_df       = extract()
        validated_df = validate_and_quarantine(raw_df)
        curated_df   = transform(validated_df)
        load(curated_df)
        job.commit()
        logger.info("[DONE] Shipment processing complete.")
    except Exception as e:
        logger.error(f"[FAILED] {str(e)}")
        raise


if __name__ == "__main__":
    main()
