"""
AWS Glue ETL Job: Procurement Processing
Healthcare Supply Chain Analytics Platform

Source Dataset : DataCo Smart Supply Chain for Big Data Analysis
Source URL     : https://www.kaggle.com/datasets/shashwatwork/dataco-smart-supply-chain-for-big-data-analysis
Mendeley DOI   : https://data.mendeley.com/datasets/8gx2fvg2k6/5
License        : CC BY 4.0

Relevant Source Columns Used:
    Order Id, Order Date (DateOrders), Shipping Date (DateOrders),
    Order Status, Order Item Quantity, Order Item Total, Sales,
    Order Profit Per Order, Shipping Mode,
    Days for shipping (real), Days for shipment (scheduled),
    Delivery Status, Late_delivery_risk,
    Category Name, Department Name, Product Name, Product Price,
    Customer Id, Customer Country, Customer State, Customer City

Note: DataCo is a retail supply chain dataset; we apply healthcare context by mapping
    - Category Name → product category (PPE, MEDICATIONS, etc.)
    - Vendor / Department → supplier proxy
    - Customer location → hospital location
    Columns unrelated to procurement (Customer Email, Password, Image) are dropped.

Job: hsc-procurement-processing
Input:  s3://hsc-analytics-raw/procurement/
Output: s3://hsc-analytics-curated/procurement/
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

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'env', 'run_date'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RUN_DATE        = args.get('run_date', str(date.today()))
RAW_PATH        = "s3://hsc-analytics-raw/procurement/"
CURATED_PATH    = "s3://hsc-analytics-curated/procurement/"
QUARANTINE_PATH = f"s3://hsc-analytics-curated/quarantine/procurement/run_date={RUN_DATE}/"

# DataCo order status → project PO status mapping
ORDER_STATUS_MAP = F.create_map(
    F.lit("COMPLETE"),   F.lit("RECEIVED"),
    F.lit("PENDING"),    F.lit("APPROVED"),
    F.lit("PROCESSING"), F.lit("APPROVED"),
    F.lit("CLOSED"),     F.lit("RECEIVED"),
    F.lit("CANCELED"),   F.lit("CANCELLED"),
    F.lit("SUSPECTED_FRAUD"), F.lit("CANCELLED"),
)

# Shipping Mode → approval_level proxy
SHIP_MODE_MAP = F.create_map(
    F.lit("Same Day"),      F.lit("EXECUTIVE"),
    F.lit("First Class"),   F.lit("DIRECTOR"),
    F.lit("Second Class"),  F.lit("MANAGER"),
    F.lit("Standard Class"),F.lit("STANDARD"),
)


def extract():
    logger.info("[EXTRACT] Reading raw DataCo procurement data...")
    # DataCo uses mixed casing and spaces in column names; read with header
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(RAW_PATH))
    # Normalize column names: lowercase, spaces → underscores, remove parens/special chars
    for col in df.columns:
        safe = col.lower().replace(" ", "_").replace("(", "").replace(")", "").replace("/", "_")
        df = df.withColumnRenamed(col, safe)
    logger.info(f"[EXTRACT] Loaded {df.count()} records. Columns: {df.columns[:10]}...")
    return df


def validate_and_quarantine(df):
    df = (df
          .withColumn("_qc_null_order",
              F.when(F.col("order_id").isNull(), True).otherwise(False))
          .withColumn("_qc_null_product",
              F.when(F.col("product_name").isNull(), True).otherwise(False))
          .withColumn("_qc_invalid_qty",
              F.when(F.col("order_item_quantity") <= 0, True).otherwise(False))
    )
    qc_cols = ["_qc_null_order", "_qc_null_product", "_qc_invalid_qty"]
    bad  = df.filter(F.col("_qc_null_order") | F.col("_qc_null_product") | F.col("_qc_invalid_qty"))
    good = df.filter(~F.col("_qc_null_order") & ~F.col("_qc_null_product") & ~F.col("_qc_invalid_qty"))

    if bad.count() > 0:
        bad.withColumn("quarantine_reason",
              F.when(F.col("_qc_null_order"),   "MISSING_ORDER_ID")
               .when(F.col("_qc_null_product"), "MISSING_PRODUCT")
               .otherwise("INVALID_QUANTITY")
           ).drop(*qc_cols).write.mode("overwrite").parquet(QUARANTINE_PATH)
        logger.warning(f"[QUARANTINE] {bad.count()} records quarantined")
    return good.drop(*qc_cols)


def transform(df):
    logger.info("[TRANSFORM] Mapping DataCo columns to project procurement schema...")

    df = (df
        # ── Natural keys ──
        .withColumn("purchase_order_id",
            F.concat(F.lit("PO-"), F.col("order_id").cast("string")))
        .withColumn("hospital_id",
            F.concat(F.lit("HOSP-"),
                     F.upper(F.regexp_replace(
                         F.coalesce(F.col("customer_state"), F.lit("UNK")),
                         "[^A-Za-z]", "")).substr(1, 4)))
        .withColumn("supplier_id",
            F.concat(F.lit("SUP-"),
                     F.abs(F.hash(
                         F.coalesce(F.col("department_name"), F.lit("UNKNOWN"))
                     )).cast("string").substr(1, 6)))

        # ── Dates ──
        .withColumn("order_date",
            F.coalesce(
                F.to_date(F.col("order_date_dateorders"), "M/d/yyyy H:mm"),
                F.to_date(F.col("order_date_dateorders"), "yyyy-MM-dd"),
            ))
        .withColumn("ship_date",
            F.coalesce(
                F.to_date(F.col("shipping_date_dateorders"), "M/d/yyyy H:mm"),
                F.to_date(F.col("shipping_date_dateorders"), "yyyy-MM-dd"),
            ))

        # ── Core measures ──
        .withColumn("quantity_ordered",   F.col("order_item_quantity").cast(IntegerType()))
        .withColumn("unit_price",         F.col("product_price").cast(DoubleType()))
        .withColumn("total_order_value",  F.col("order_item_total").cast(DoubleType()))
        .withColumn("lead_time_days",     F.col("days_for_shipment_scheduled").cast(IntegerType()))

        # ── Budget proxy: derive from order total × 1.05 (5% budgeted margin) ──
        .withColumn("budget_amount",
            F.round(F.col("total_order_value") * 1.05, 2))
        .withColumn("cost_variance_pct",
            F.round((F.col("total_order_value") - F.col("budget_amount"))
                    / F.col("budget_amount") * 100, 2))

        # ── Status mappings ──
        .withColumn("po_status",
            F.coalesce(ORDER_STATUS_MAP[F.upper(F.trim(F.col("order_status")))],
                       F.lit("DRAFT")))
        .withColumn("approval_level",
            F.coalesce(SHIP_MODE_MAP[F.col("shipping_mode")], F.lit("STANDARD")))

        # ── Product fields ──
        .withColumn("product_name", F.upper(F.trim(F.col("product_name"))))
        .withColumn("category",     F.upper(F.trim(
            F.coalesce(F.col("category_name"), F.lit("GENERAL")))))

        # ── SLA met flag (inverted from Late_delivery_risk) ──
        .withColumn("is_sla_met",
            F.when(F.col("late_delivery_risk").cast(IntegerType()) == 0, True)
             .otherwise(False))
    )

    # Drop DataCo columns not needed in output
    keep_cols = [
        "purchase_order_id", "hospital_id", "supplier_id",
        "order_date", "ship_date",
        "product_name", "category",
        "quantity_ordered", "unit_price", "total_order_value",
        "budget_amount", "cost_variance_pct",
        "lead_time_days", "po_status", "approval_level", "is_sla_met",
        "etl_run_date", "etl_job_name", "etl_load_ts",
    ]

    df = df.withColumn("etl_run_date", F.lit(RUN_DATE)) \
           .withColumn("etl_job_name", F.lit(args['JOB_NAME'])) \
           .withColumn("etl_load_ts",  F.current_timestamp())

    # Deduplication
    w = Window.partitionBy("purchase_order_id").orderBy(F.col("order_date").desc_nulls_last())
    df = (df.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn"))

    df = df.select([c for c in keep_cols if c in df.columns])
    logger.info(f"[TRANSFORM] Complete. Records: {df.count()}")
    return df


def load(df):
    df.write.mode("overwrite") \
            .partitionBy("order_date", "po_status") \
            .parquet(CURATED_PATH)
    logger.info(f"[LOAD] Written to {CURATED_PATH}")


def main():
    try:
        raw_df       = extract()
        validated_df = validate_and_quarantine(raw_df)
        curated_df   = transform(validated_df)
        load(curated_df)
        job.commit()
        logger.info("[DONE] Procurement processing complete.")
    except Exception as e:
        logger.error(f"[FAILED] {str(e)}")
        raise


if __name__ == "__main__":
    main()
