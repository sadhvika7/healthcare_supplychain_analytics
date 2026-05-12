"""
PySpark Data Quality Framework
Healthcare Supply Chain Analytics Platform

Reusable quality engine covering:
  - Null checks          (NULL_CHECK)
  - Duplicate checks     (DUPLICATE)
  - Range validation     (RANGE)
  - Accepted values      (ACCEPTED_VALUES)
  - Freshness checks     (FRESHNESS)

Usage:
    from data_quality.quality_framework import DataQualityEngine, SHIPMENT_RULES

    engine  = DataQualityEngine(spark, df, entity="shipments", run_date="2024-06-15")
    engine.add_rules(SHIPMENT_RULES)
    results = engine.run()           # raises ValueError on ERROR-severity failures
    engine.write_audit_log(results)
"""

import logging
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime, date
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType
)

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# DATA CLASSES
# ──────────────────────────────────────────────
@dataclass
class QualityRule:
    rule_id:   str
    rule_name: str
    rule_type: str               # NULL_CHECK | DUPLICATE | RANGE | ACCEPTED_VALUES | FRESHNESS
    column:    Optional[str] = None
    params:    Dict[str, Any] = field(default_factory=dict)
    severity:  str = "ERROR"     # ERROR | WARN
    is_active: bool = True


@dataclass
class QualityResult:
    rule_id:        str
    rule_name:      str
    rule_type:      str
    column:         Optional[str]
    total_records:  int
    failed_records: int
    pass_rate:      float
    severity:       str
    status:         str          # PASS | FAIL | WARN
    run_date:       str
    entity:         str
    details:        str = ""


# ──────────────────────────────────────────────
# ENGINE
# ──────────────────────────────────────────────
class DataQualityEngine:

    def __init__(
        self,
        spark:           SparkSession,
        df:              DataFrame,
        entity:          str,
        run_date:        str,
        quarantine_path: str = "s3://hsc-analytics-curated/quarantine/",
        audit_path:      str = "s3://hsc-analytics-curated/audit/quality/",
    ):
        self.spark           = spark
        self.df              = df
        self.entity          = entity
        self.run_date        = run_date
        self.quarantine_path = quarantine_path
        self.audit_path      = audit_path
        self.rules: List[QualityRule] = []
        self.total_records   = df.count()

    def add_rules(self, rules: List[QualityRule]):
        self.rules.extend(rules)
        logger.info(f"[QC] {len(rules)} rules added for entity={self.entity}")

    # ── Individual checks ──────────────────────
    def _check_null(self, rule: QualityRule) -> QualityResult:
        col    = rule.column
        failed = self.df.filter(F.col(col).isNull()).count()
        rate   = round((1 - failed / max(self.total_records, 1)) * 100, 2)
        return QualityResult(
            rule_id=rule.rule_id, rule_name=rule.rule_name, rule_type="NULL_CHECK",
            column=col, total_records=self.total_records, failed_records=failed,
            pass_rate=rate, severity=rule.severity,
            status="PASS" if failed == 0 else ("FAIL" if rule.severity == "ERROR" else "WARN"),
            run_date=self.run_date, entity=self.entity,
            details=f"{failed} null values in '{col}'"
        )

    def _check_duplicate(self, rule: QualityRule) -> QualityResult:
        keys   = rule.params.get("key_columns", [rule.column])
        total  = self.df.count()
        unique = self.df.dropDuplicates(keys).count()
        failed = total - unique
        rate   = round((1 - failed / max(total, 1)) * 100, 2)
        return QualityResult(
            rule_id=rule.rule_id, rule_name=rule.rule_name, rule_type="DUPLICATE",
            column=str(keys), total_records=total, failed_records=failed,
            pass_rate=rate, severity=rule.severity,
            status="PASS" if failed == 0 else ("FAIL" if rule.severity == "ERROR" else "WARN"),
            run_date=self.run_date, entity=self.entity,
            details=f"{failed} duplicate rows on keys {keys}"
        )

    def _check_range(self, rule: QualityRule) -> QualityResult:
        col     = rule.column
        min_val = rule.params.get("min_value")
        max_val = rule.params.get("max_value")
        cond    = F.lit(False)
        if min_val is not None:
            cond = cond | (F.col(col) < min_val)
        if max_val is not None:
            cond = cond | (F.col(col) > max_val)
        failed = self.df.filter(cond).count()
        rate   = round((1 - failed / max(self.total_records, 1)) * 100, 2)
        return QualityResult(
            rule_id=rule.rule_id, rule_name=rule.rule_name, rule_type="RANGE",
            column=col, total_records=self.total_records, failed_records=failed,
            pass_rate=rate, severity=rule.severity,
            status="PASS" if failed == 0 else ("FAIL" if rule.severity == "ERROR" else "WARN"),
            run_date=self.run_date, entity=self.entity,
            details=f"{failed} values outside [{min_val}, {max_val}]"
        )

    def _check_accepted_values(self, rule: QualityRule) -> QualityResult:
        col    = rule.column
        values = rule.params.get("values", [])
        failed = self.df.filter(
            F.col(col).isNotNull() & ~F.col(col).isin(values)
        ).count()
        rate = round((1 - failed / max(self.total_records, 1)) * 100, 2)
        return QualityResult(
            rule_id=rule.rule_id, rule_name=rule.rule_name, rule_type="ACCEPTED_VALUES",
            column=col, total_records=self.total_records, failed_records=failed,
            pass_rate=rate, severity=rule.severity,
            status="PASS" if failed == 0 else ("FAIL" if rule.severity == "ERROR" else "WARN"),
            run_date=self.run_date, entity=self.entity,
            details=f"{failed} values not in {values}"
        )

    def _check_freshness(self, rule: QualityRule) -> QualityResult:
        col           = rule.column
        max_age_hours = rule.params.get("max_age_hours", 25)
        max_ts = self.df.agg(F.max(F.col(col))).collect()[0][0]
        if max_ts is None:
            failed, rate, detail = 1, 0.0, "No records — freshness check failed"
        else:
            age   = (datetime.utcnow() - max_ts).total_seconds() / 3600
            failed = 1 if age > max_age_hours else 0
            rate   = 100.0 if failed == 0 else 0.0
            detail = f"Latest record age: {age:.1f}h (threshold: {max_age_hours}h)"
        return QualityResult(
            rule_id=rule.rule_id, rule_name=rule.rule_name, rule_type="FRESHNESS",
            column=col, total_records=self.total_records, failed_records=failed,
            pass_rate=rate, severity=rule.severity,
            status="PASS" if failed == 0 else ("FAIL" if rule.severity == "ERROR" else "WARN"),
            run_date=self.run_date, entity=self.entity, details=detail
        )

    # ── Run all ───────────────────────────────
    def run(self) -> List[QualityResult]:
        dispatch = {
            "NULL_CHECK":      self._check_null,
            "DUPLICATE":       self._check_duplicate,
            "RANGE":           self._check_range,
            "ACCEPTED_VALUES": self._check_accepted_values,
            "FRESHNESS":       self._check_freshness,
        }
        results = []
        for rule in self.rules:
            if not rule.is_active:
                continue
            try:
                fn = dispatch.get(rule.rule_type)
                if fn:
                    r = fn(rule)
                    results.append(r)
                    icon = "✅" if r.status == "PASS" else "❌"
                    logger.info(f"[QC] {icon} [{r.status}] {rule.rule_name}: {r.details}")
            except Exception as e:
                logger.error(f"[QC] Rule {rule.rule_id} exception: {e}")

        fails = [r for r in results if r.status == "FAIL"]
        warns = [r for r in results if r.status == "WARN"]
        logger.info(f"[QC SUMMARY] entity={self.entity} total={len(results)} FAIL={len(fails)} WARN={len(warns)}")

        if fails:
            raise ValueError(f"[QC] {len(fails)} ERROR rules failed: {[r.rule_name for r in fails]}")
        return results

    def write_audit_log(self, results: List[QualityResult]):
        rows = [(
            r.rule_id, r.rule_name, r.rule_type, r.column or "",
            r.total_records, r.failed_records, float(r.pass_rate),
            r.severity, r.status, r.run_date, r.entity, r.details
        ) for r in results]
        schema = StructType([
            StructField("rule_id",        StringType(),  True),
            StructField("rule_name",      StringType(),  True),
            StructField("rule_type",      StringType(),  True),
            StructField("column",         StringType(),  True),
            StructField("total_records",  IntegerType(), True),
            StructField("failed_records", IntegerType(), True),
            StructField("pass_rate",      FloatType(),   True),
            StructField("severity",       StringType(),  True),
            StructField("status",         StringType(),  True),
            StructField("run_date",       StringType(),  True),
            StructField("entity",         StringType(),  True),
            StructField("details",        StringType(),  True),
        ])
        path = f"{self.audit_path}entity={self.entity}/run_date={self.run_date}/"
        self.spark.createDataFrame(rows, schema).write.mode("overwrite").parquet(path)
        logger.info(f"[QC] Audit log written to {path}")


# ──────────────────────────────────────────────
# PRE-BUILT RULE SETS
# ──────────────────────────────────────────────

# Rules for USAID shipment data
SHIPMENT_RULES = [
    QualityRule("SHP_001", "Shipment ID not null",       "NULL_CHECK", "shipment_id",  severity="ERROR"),
    QualityRule("SHP_002", "Supplier ID not null",       "NULL_CHECK", "supplier_id",  severity="ERROR"),
    QualityRule("SHP_003", "Ship date not null",         "NULL_CHECK", "ship_date",    severity="ERROR"),
    QualityRule("SHP_004", "Shipment deduplication",     "DUPLICATE",  "shipment_id",
                params={"key_columns": ["shipment_id"]},                               severity="ERROR"),
    QualityRule("SHP_005", "Priority accepted values",   "ACCEPTED_VALUES", "priority",
                params={"values": ["CRITICAL","HIGH","STANDARD","LOW"]},               severity="ERROR"),
    QualityRule("SHP_006", "Shipment cost non-negative", "RANGE",      "shipment_cost",
                params={"min_value": 0},                                               severity="WARN"),
    QualityRule("SHP_007", "Carrier not null",           "NULL_CHECK", "carrier",      severity="WARN"),
    QualityRule("SHP_008", "Freshness check",            "FRESHNESS",  "etl_load_ts",
                params={"max_age_hours": 25},                                          severity="WARN"),
]

# Rules for hospital inventory data
INVENTORY_RULES = [
    QualityRule("INV_001", "Inventory ID not null",      "NULL_CHECK", "inventory_id",     severity="ERROR"),
    QualityRule("INV_002", "Hospital ID not null",       "NULL_CHECK", "hospital_id",      severity="ERROR"),
    QualityRule("INV_003", "Product ID not null",        "NULL_CHECK", "product_id",       severity="ERROR"),
    QualityRule("INV_004", "Inventory deduplication",    "DUPLICATE",  "inventory_id",
                params={"key_columns": ["inventory_id"]},                               severity="ERROR"),
    QualityRule("INV_005", "Quantity non-negative",      "RANGE",      "quantity_on_hand",
                params={"min_value": 0},                                               severity="ERROR"),
    QualityRule("INV_006", "Unit cost non-negative",     "RANGE",      "unit_cost",
                params={"min_value": 0},                                               severity="WARN"),
    QualityRule("INV_007", "Stock status valid values",  "ACCEPTED_VALUES", "stock_status",
                params={"values": ["STOCKOUT","CRITICAL_LOW","LOW","ADEQUATE","OVERSTOCK"]},
                                                                                       severity="ERROR"),
    QualityRule("INV_008", "Freshness check",            "FRESHNESS",  "etl_load_ts",
                params={"max_age_hours": 25},                                          severity="WARN"),
]

# Rules for DataCo procurement data
PROCUREMENT_RULES = [
    QualityRule("PRC_001", "PO ID not null",             "NULL_CHECK", "purchase_order_id", severity="ERROR"),
    QualityRule("PRC_002", "Order date not null",        "NULL_CHECK", "order_date",        severity="ERROR"),
    QualityRule("PRC_003", "PO deduplication",           "DUPLICATE",  "purchase_order_id",
                params={"key_columns": ["purchase_order_id"]},                          severity="ERROR"),
    QualityRule("PRC_004", "Quantity > 0",               "RANGE",      "quantity_ordered",
                params={"min_value": 1},                                                severity="ERROR"),
    QualityRule("PRC_005", "PO status valid",            "ACCEPTED_VALUES", "po_status",
                params={"values": ["DRAFT","APPROVED","RECEIVED","CANCELLED"]},         severity="WARN"),
    QualityRule("PRC_006", "Freshness check",            "FRESHNESS",  "etl_load_ts",
                params={"max_age_hours": 25},                                           severity="WARN"),
]
