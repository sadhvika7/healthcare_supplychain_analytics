"""Shared constants for AWS Glue ETL jobs — Healthcare Supply Chain Analytics Platform."""

# ── S3 Paths ───────────────────────────────────────────────────────
S3_RAW_BASE     = "s3://hsc-analytics-raw"
S3_CURATED_BASE = "s3://hsc-analytics-curated"
S3_SCRIPTS_BASE = "s3://hsc-analytics-scripts"

RAW_PATHS = {
    "inventory":   f"{S3_RAW_BASE}/inventory/",
    "shipments":   f"{S3_RAW_BASE}/shipments/",
    "suppliers":   f"{S3_RAW_BASE}/suppliers/",
    "procurement": f"{S3_RAW_BASE}/procurement/",
}
CURATED_PATHS = {
    "inventory":   f"{S3_CURATED_BASE}/inventory/",
    "shipments":   f"{S3_CURATED_BASE}/shipments/",
    "suppliers":   f"{S3_CURATED_BASE}/suppliers/",
    "procurement": f"{S3_CURATED_BASE}/procurement/",
    "kpis":        f"{S3_CURATED_BASE}/kpis/",
    "quarantine":  f"{S3_CURATED_BASE}/quarantine/",
    "audit":       f"{S3_CURATED_BASE}/audit/",
}

# ── Business Rules ──────────────────────────────────────────────────
# SLA delivery thresholds in days by priority
SLA_THRESHOLDS = {"CRITICAL": 2, "HIGH": 3, "STANDARD": 5, "LOW": 7}

# USAID product_group → priority mapping
PRODUCT_GROUP_PRIORITY = {
    "ARV":   "CRITICAL",   # Antiretrovirals
    "HRDT":  "HIGH",       # HIV Rapid Diagnostic Tests
    "ANTIM": "HIGH",       # Antimalarials
    "MRDT":  "STANDARD",   # Malaria Rapid Diagnostic Tests
    "ACT":   "STANDARD",   # Artemisinin-based Combination Therapy
    "OTHER": "LOW",
}

# Supplier reliability tier cutoffs
RELIABILITY_TIERS = {"PLATINUM": 90, "GOLD": 75, "SILVER": 60}

# Accepted values
STOCK_STATUSES      = ["STOCKOUT", "CRITICAL_LOW", "LOW", "ADEQUATE", "OVERSTOCK"]
SHIPMENT_PRIORITIES = ["CRITICAL", "HIGH", "STANDARD", "LOW"]
PO_STATUSES         = ["DRAFT", "APPROVED", "RECEIVED", "CANCELLED"]

# ── KPI Targets ─────────────────────────────────────────────────────
KPI_TARGETS = {
    "inventory_fill_rate_pct": 95.0,
    "shipment_sla_pct":        90.0,
    "stockout_pct":             2.0,
    "supplier_reliability":    85.0,
    "cost_variance_pct":        5.0,
    "avg_delivery_days":        5.0,
    "operational_health_score":88.0,
}

# ── Glue Runtime Config ─────────────────────────────────────────────
GLUE_VERSION     = "4.0"        # Glue 4.0 = Spark 3.3, Python 3.10
WORKER_TYPE      = "G.1X"
DEFAULT_WORKERS  = 5
IAM_ROLE         = "arn:aws:iam::ACCOUNT_ID:role/GlueHSCRole"
TEMP_DIR         = f"{S3_CURATED_BASE}/tmp/"
