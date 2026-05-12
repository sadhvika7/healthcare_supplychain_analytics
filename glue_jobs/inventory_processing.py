"""
AWS Glue ETL Job: Inventory Processing
Healthcare Supply Chain Analytics Platform

Source Dataset : Hospital Supply Chain (Kaggle)
Source URL     : https://www.kaggle.com/datasets/vanpatangan/hospital-supply-chain
License        : CC0 Public Domain

Source Columns:
    item_id, item_name, category, hospital_id, supplier_id,
    quantity_on_hand, reorder_level, safety_stock, unit_cost,
    last_restocked_date, record_date

Job: hsc-inventory-processing
Schedule: Daily @ 02:00 UTC
Input:  s3://hsc-analytics-raw/inventory/         (CSV)
Output: s3://hsc-analytics-curated/inventory/     (Parquet, partitioned by record_date, category)
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
    StructType, StructField, StringType, IntegerType, DoubleType
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
RAW_PATH        = "s3://hsc-analytics-raw/inventory/"
CURATED_PATH    = "s3://hsc-analytics-curated/inventory/"
QUARANTINE_PATH = f"s3://hsc-analytics-curated/quarantine/inventory/run_date={RUN_DATE}/"


# ──────────────────────────────────────────────
# SOURCE SCHEMA — matches hospital-supply-chain Kaggle dataset
# ──────────────────────────────────────────────
RAW_SCHEMA = StructType([
    StructField("item_id",            StringType(),  True),  # → inventory_id (derived: item_id + hospital_id + record_date)
    StructField("item_name",          StringType(),  True),  # → product_name
    StructField("category",           StringType(),  True),
    StructField("hospital_id",        StringType(),  True),
    StructField("supplier_id",        StringType(),  True),
    StructField("quantity_on_hand",   IntegerType(), True),
    StructField("reorder_level",      IntegerType(), True),
    StructField("safety_stock",       IntegerType(), True),
    StructField("unit_cost",          DoubleType(),  True),
    StructField("last_restocked_date",StringType(),  True),  # → last_restocked_dt
    StructField("record_date",        StringType(),  True),
])


# ──────────────────────────────────────────────
# EXTRACT
# ──────────────────────────────────────────────
def extract():
    logger.info("[EXTRACT] Reading raw inventory data from S3...")
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
          .withColumn("_qc_null_item",
              F.when(F.col("item_id").isNull(), True).otherwise(False))
          .withColumn("_qc_null_hospital",
              F.when(F.col("hospital_id").isNull(), True).otherwise(False))
          .withColumn("_qc_negative_qty",
              F.when(F.col("quantity_on_hand") < 0, True).otherwise(False))
          .withColumn("_qc_invalid_cost",
              F.when(F.col("unit_cost").isNotNull() & (F.col("unit_cost") < 0), True)
               .otherwise(False))
    )
    qc_cols = ["_qc_null_item", "_qc_null_hospital", "_qc_negative_qty", "_qc_invalid_cost"]

    bad  = df.filter(F.col("_qc_null_item") | F.col("_qc_null_hospital") |
                     F.col("_qc_negative_qty") | F.col("_qc_invalid_cost"))
    good = df.filter(~F.col("_qc_null_item") & ~F.col("_qc_null_hospital") &
                     ~F.col("_qc_negative_qty") & ~F.col("_qc_invalid_cost"))

    if bad.count() > 0:
        bad.withColumn("quarantine_reason",
              F.when(F.col("_qc_null_item"),      "MISSING_ITEM_ID")
               .when(F.col("_qc_null_hospital"),   "MISSING_HOSPITAL_ID")
               .when(F.col("_qc_negative_qty"),    "NEGATIVE_QUANTITY")
               .otherwise("INVALID_UNIT_COST")
           ).drop(*qc_cols).write.mode("overwrite").parquet(QUARANTINE_PATH)
        logger.warning(f"[QUARANTINE] {bad.count()} records quarantined")

    return good.drop(*qc_cols)


# ──────────────────────────────────────────────
# TRANSFORM
# ──────────────────────────────────────────────
def transform(df):
    logger.info("[TRANSFORM] Renaming and applying business rules...")

    df = (df
        # ── Rename source → project columns ──
        .withColumnRenamed("item_name",           "product_name")
        .withColumnRenamed("last_restocked_date", "last_restocked_dt")

        # ── Derive inventory_id as composite natural key ──
        .withColumn("inventory_id",
            F.concat_ws("-",
                        F.upper(F.trim(F.col("item_id"))),
                        F.upper(F.trim(F.col("hospital_id"))),
                        F.regexp_replace(F.col("record_date"), "-", "")))

        # ── Derive product_id (same as item_id, standardised) ──
        .withColumn("product_id", F.upper(F.trim(F.col("item_id"))))

        # ── Standardize strings ──
        .withColumn("product_name", F.upper(F.trim(F.col("product_name"))))
        .withColumn("category",     F.upper(F.trim(F.col("category"))))
        .withColumn("hospital_id",  F.upper(F.trim(F.col("hospital_id"))))
        .withColumn("supplier_id",  F.upper(F.trim(F.col("supplier_id"))))

        # ── Parse dates ──
        .withColumn("last_restocked_dt",
            F.coalesce(
                F.to_date(F.col("last_restocked_dt"), "yyyy-MM-dd"),
                F.to_date(F.col("last_restocked_dt"), "MM/dd/yyyy"),
            ))
        .withColumn("record_date",
            F.coalesce(
                F.to_date(F.col("record_date"), "yyyy-MM-dd"),
                F.to_date(F.col("record_date"), "MM/dd/yyyy"),
            ))

        # ── Null-safe numerics ──
        .withColumn("quantity_on_hand", F.coalesce(F.col("quantity_on_hand"), F.lit(0)))
        .withColumn("reorder_level",    F.coalesce(F.col("reorder_level"),    F.lit(10)))
        .withColumn("safety_stock",     F.coalesce(F.col("safety_stock"),     F.lit(5)))
        .withColumn("unit_cost",        F.coalesce(F.col("unit_cost"),        F.lit(0.0)))
    )

    # ── Business rules ──
    df = (df
        .withColumn("stock_status",
            F.when(F.col("quantity_on_hand") == 0,                              "STOCKOUT")
             .when(F.col("quantity_on_hand") < F.col("safety_stock"),           "CRITICAL_LOW")
             .when(F.col("quantity_on_hand") < F.col("reorder_level"),          "LOW")
             .when(F.col("quantity_on_hand") > F.col("reorder_level") * 3,      "OVERSTOCK")
             .otherwise("ADEQUATE"))

        .withColumn("days_since_restock",
            F.datediff(F.col("record_date"), F.col("last_restocked_dt")))

        .withColumn("inventory_value",
            F.round(F.col("quantity_on_hand") * F.col("unit_cost"), 2))

        .withColumn("is_critical_shortage",
            F.col("stock_status").isin("STOCKOUT", "CRITICAL_LOW"))

        .withColumn("restock_urgency_score",
            F.when(F.col("stock_status") == "STOCKOUT",      F.lit(100))
             .when(F.col("stock_status") == "CRITICAL_LOW",  F.lit(80))
             .when(F.col("stock_status") == "LOW",           F.lit(50))
             .otherwise(F.lit(10)))
    )

    # ── Deduplication on composite key ──
    w = Window.partitionBy("inventory_id").orderBy(F.col("days_since_restock").asc_nulls_last())
    df = (df.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn", "item_id"))  # drop raw source key after derivation

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
            .partitionBy("record_date", "category") \
            .parquet(CURATED_PATH)
    logger.info(f"[LOAD] Written to {CURATED_PATH}")


def write_audit(raw_count, curated_count):
    from pyspark.sql.types import TimestampType
    audit_data = [(
        args['JOB_NAME'], RUN_DATE, "inventory",
        raw_count, curated_count, raw_count - curated_count,
        "SUCCESS", datetime.utcnow().isoformat()
    )]
    schema = StructType([
        StructField("job_name",         StringType(),  True),
        StructField("run_date",         StringType(),  True),
        StructField("entity",           StringType(),  True),
        StructField("raw_count",        IntegerType(), True),
        StructField("curated_count",    IntegerType(), True),
        StructField("quarantine_count", IntegerType(), True),
        StructField("status",           StringType(),  True),
        StructField("created_at",       StringType(),  True),
    ])
    (spark.createDataFrame(audit_data, schema)
          .write.mode("append")
          .parquet(f"s3://hsc-analytics-curated/audit/pipeline_audit/run_date={RUN_DATE}/"))
    logger.info("[AUDIT] Audit log written.")


def main():
    try:
        raw_df       = extract()
        raw_count    = raw_df.count()
        validated_df = validate_and_quarantine(raw_df)
        curated_df   = transform(validated_df)
        load(curated_df)
        write_audit(raw_count, curated_df.count())
        job.commit()
        logger.info("[DONE] Inventory processing complete.")
    except Exception as e:
        logger.error(f"[FAILED] {str(e)}")
        raise


if __name__ == "__main__":
    main()
