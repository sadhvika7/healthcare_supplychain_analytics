"""
AWS Glue ETL Job: KPI Aggregation
Healthcare Supply Chain Analytics Platform

Reads curated layer → computes all operational KPIs → writes to S3 KPI zone
Downstream: loaded into Redshift kpi schema and consumed by Power BI

Job: hsc-kpi-aggregation
Input:  s3://hsc-analytics-curated/{inventory, shipments, procurement}/
Output: s3://hsc-analytics-curated/kpis/
"""

import sys
import logging
from datetime import date
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'run_date'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RUN_DATE = args.get('run_date', str(date.today()))
CURATED  = "s3://hsc-analytics-curated"
KPI_OUT  = f"{CURATED}/kpis"


def compute_inventory_kpis():
    """
    KPIs: Inventory Fill Rate, Stockout %, Critical Shortage Count,
          Total Inventory Value, Avg Restock Urgency
    Source columns used: inventory_id, stock_status, is_critical_shortage,
                         quantity_on_hand, inventory_value, restock_urgency_score,
                         record_date, category, hospital_id
    """
    logger.info("[KPI] Inventory KPIs...")
    inv = spark.read.parquet(f"{CURATED}/inventory/")

    kpi = (inv
        .groupBy("record_date", "category", "hospital_id")
        .agg(
            F.count("inventory_id").alias("total_skus"),
            F.sum(F.when(F.col("stock_status") == "STOCKOUT", 1).otherwise(0))
             .alias("stockout_count"),
            F.sum(F.when(F.col("is_critical_shortage"), 1).otherwise(0))
             .alias("critical_shortage_count"),
            F.sum("quantity_on_hand").alias("total_units_on_hand"),
            F.sum("inventory_value").alias("total_inventory_value"),
            F.avg("restock_urgency_score").alias("avg_urgency_score"),
        )
        .withColumn("inventory_fill_rate_pct",
            F.round((F.col("total_skus") - F.col("stockout_count"))
                    / F.col("total_skus") * 100, 2))
        .withColumn("stockout_pct",
            F.round(F.col("stockout_count") / F.col("total_skus") * 100, 2))
        .withColumn("kpi_category", F.lit("INVENTORY"))
        .withColumn("etl_run_date", F.lit(RUN_DATE))
    )
    return kpi


def compute_shipment_kpis():
    """
    KPIs: Shipment SLA %, Delayed Count, Avg Delivery Days, Avg Delay Days
    Source columns used: shipment_id, is_sla_met, actual_transit_days,
                         delivery_delay_days, ship_date, priority, carrier, shipment_cost
    Note: ship_date is a partition column — read as date from directory structure
    """
    logger.info("[KPI] Shipment KPIs...")
    ship = (spark.read
                 .option("mergeSchema", "true")
                 .parquet(f"{CURATED}/shipments/")
                 .filter(F.col("actual_delivery_date").isNotNull()))

    kpi = (ship
        .groupBy(
            F.date_trunc("month", F.col("ship_date")).alias("month"),
            "priority", "carrier"
        )
        .agg(
            F.count("shipment_id").alias("total_shipments"),
            F.sum(F.when(F.col("is_sla_met") == True,  1).otherwise(0)).alias("on_time_count"),
            F.sum(F.when(F.col("is_sla_met") == False, 1).otherwise(0)).alias("delayed_count"),
            F.avg("actual_transit_days").alias("avg_delivery_days"),
            F.avg("delivery_delay_days").alias("avg_delay_days"),
            F.sum("shipment_cost").alias("total_shipment_cost"),
            F.avg("cost_per_unit").alias("avg_cost_per_unit"),
        )
        .withColumn("shipment_sla_pct",
            F.round(F.col("on_time_count") / F.col("total_shipments") * 100, 2))
        .withColumn("kpi_category", F.lit("SHIPMENT"))
        .withColumn("etl_run_date", F.lit(RUN_DATE))
    )
    return kpi


def compute_supplier_reliability():
    """
    KPI: Supplier Reliability Score
    Formula: (on_time_rate × 0.70) + (max(0, 100 − avg_delay × 10) × 0.30)
    Tiers: PLATINUM ≥90, GOLD 75–89, SILVER 60–74, WATCH_LIST <60
    """
    logger.info("[KPI] Supplier Reliability...")
    ship = (spark.read.parquet(f"{CURATED}/shipments/")
                 .filter(F.col("actual_delivery_date").isNotNull()))

    kpi = (ship
        .groupBy("supplier_id", "supplier_name")
        .agg(
            F.count("shipment_id").alias("total_shipments"),
            F.sum(F.when(F.col("is_sla_met") == True, 1).otherwise(0))
             .alias("on_time_deliveries"),
            F.avg("delivery_delay_days").alias("avg_delay_days"),
            F.avg("cost_per_unit").alias("avg_cost_per_unit"),
            F.sum("freight_cost").alias("total_freight_cost"),
        )
        .withColumn("on_time_rate",
            F.round(F.col("on_time_deliveries") / F.col("total_shipments") * 100, 2))
        .withColumn("reliability_score",
            F.round(
                F.col("on_time_rate") * 0.70
                + F.greatest(F.lit(0.0),
                             F.lit(100.0) - F.coalesce(F.col("avg_delay_days"), F.lit(0.0)) * 10
                             ) * 0.30,
            2))
        .withColumn("reliability_tier",
            F.when(F.col("reliability_score") >= 90, "PLATINUM")
             .when(F.col("reliability_score") >= 75, "GOLD")
             .when(F.col("reliability_score") >= 60, "SILVER")
             .otherwise("WATCH_LIST"))
        .withColumn("kpi_category", F.lit("SUPPLIER"))
        .withColumn("etl_run_date", F.lit(RUN_DATE))
    )
    return kpi


def compute_procurement_kpis():
    """
    KPI: Procurement Cost Variance % by category and month
    Source columns: purchase_order_id, order_date, category,
                    total_order_value, budget_amount, cost_variance_pct
    """
    logger.info("[KPI] Procurement KPIs...")
    proc = spark.read.parquet(f"{CURATED}/procurement/")

    kpi = (proc
        .groupBy(
            F.date_trunc("month", F.col("order_date")).alias("month"),
            "category"
        )
        .agg(
            F.count("purchase_order_id").alias("total_orders"),
            F.sum("total_order_value").alias("actual_spend"),
            F.sum("budget_amount").alias("budget_spend"),
            F.avg("cost_variance_pct").alias("avg_cost_variance_pct"),
            F.sum("quantity_ordered").alias("total_units_ordered"),
        )
        .withColumn("total_cost_variance_pct",
            F.round((F.col("actual_spend") - F.col("budget_spend"))
                    / F.col("budget_spend") * 100, 2))
        .withColumn("kpi_category", F.lit("PROCUREMENT"))
        .withColumn("etl_run_date", F.lit(RUN_DATE))
    )
    return kpi


def main():
    try:
        inv_kpi  = compute_inventory_kpis()
        ship_kpi = compute_shipment_kpis()
        sup_kpi  = compute_supplier_reliability()
        proc_kpi = compute_procurement_kpis()

        inv_kpi.write.mode("overwrite").parquet(f"{KPI_OUT}/inventory_kpis/run_date={RUN_DATE}/")
        ship_kpi.write.mode("overwrite").parquet(f"{KPI_OUT}/shipment_kpis/run_date={RUN_DATE}/")
        sup_kpi.write.mode("overwrite").parquet(f"{KPI_OUT}/supplier_kpis/run_date={RUN_DATE}/")
        proc_kpi.write.mode("overwrite").parquet(f"{KPI_OUT}/procurement_kpis/run_date={RUN_DATE}/")

        logger.info("[DONE] All KPI aggregations complete.")
        job.commit()
    except Exception as e:
        logger.error(f"[FAILED] {str(e)}")
        raise


if __name__ == "__main__":
    main()
