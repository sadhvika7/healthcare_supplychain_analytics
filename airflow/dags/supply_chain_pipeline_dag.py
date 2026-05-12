"""
Apache Airflow DAG: Healthcare Supply Chain Pipeline
Schedule: Daily @ 03:00 UTC  |  Airflow 2.9.2

Pipeline graph:
  validate_sources
       ├─► glue_inventory ─────────┐
       ├─► glue_shipments ─────────┤
       └─► glue_procurement ───────┤
                               glue_kpi_aggregation
                                   │
                               check_freshness
                                   │
                     ┌─────────────┴────────────┐
              load_redshift_inv        load_redshift_ship
                     └─────────────┬────────────┘
                               dbt_run_and_test
                                   │
                               notify_success ─► end
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
import subprocess, logging

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner":                     "data-engineering",
    "depends_on_past":           False,
    "email":                     ["data-alerts@healthcare.org"],
    "email_on_failure":          True,
    "email_on_retry":            False,
    "retries":                   2,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout":         timedelta(hours=2),
}

ENV             = "prod"
AWS_CONN_ID     = "aws_default"
GLUE_CONN_ID    = "aws_default"
S3_RAW          = "hsc-analytics-raw"
S3_CURATED      = "hsc-analytics-curated"
IAM_ROLE        = "GlueHSCRole"
GLUE_VERSION    = "4.0"
GLUE_WORKERS    = 5
WORKER_TYPE     = "G.1X"
SCRIPT_BASE     = "s3://hsc-analytics-scripts/glue_jobs"

GLUE_KWARGS = dict(
    s3_bucket       = "hsc-analytics-scripts",
    iam_role_name   = IAM_ROLE,
    aws_conn_id     = GLUE_CONN_ID,
    wait_for_completion = True,
    create_job_kwargs = {
        "GlueVersion":    GLUE_VERSION,
        "NumberOfWorkers":GLUE_WORKERS,
        "WorkerType":     WORKER_TYPE,
    },
)


def validate_source_files(**ctx):
    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
    required = ["inventory/", "shipments/", "suppliers/", "procurement/"]
    missing = [p for p in required if not s3.list_keys(bucket_name=S3_RAW, prefix=p)]
    if missing:
        raise ValueError(f"Source validation failed. Missing S3 prefixes: {missing}")
    logger.info("[VALIDATION] All source files present.")


def check_data_freshness(**ctx):
    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
    run_date = ctx["ds"]
    prefix = f"inventory/record_date={run_date}/"
    keys = s3.list_keys(bucket_name=S3_CURATED, prefix=prefix)
    if not keys:
        raise ValueError(f"[FRESHNESS] Curated inventory missing for {run_date}")
    logger.info(f"[FRESHNESS] {len(keys)} curated partition files found.")


def run_dbt(**ctx):
    for cmd in [
        ["dbt", "run",  "--profiles-dir", "/opt/airflow/dbt", "--vars", f"{{run_date: {ctx['ds']}}}"],
        ["dbt", "test", "--profiles-dir", "/opt/airflow/dbt"],
    ]:
        r = subprocess.run(cmd, capture_output=True, text=True, cwd="/opt/airflow/dbt")
        logger.info(r.stdout)
        if r.returncode != 0:
            logger.error(r.stderr)
            raise RuntimeError(f"{cmd[1]} failed: {r.stderr[:500]}")
    logger.info("[DBT] dbt run + test complete.")


def notify_success(**ctx):
    logger.info(f"[NOTIFY] Pipeline succeeded for {ctx['ds']}.")
    # Production: trigger Power BI dataset refresh via REST API


with DAG(
    dag_id           = "supply_chain_pipeline",
    default_args     = DEFAULT_ARGS,
    description      = "Healthcare Supply Chain — Daily ETL Pipeline",
    schedule_interval= "0 3 * * *",
    start_date       = days_ago(1),
    catchup          = False,
    max_active_runs  = 1,
    tags             = ["healthcare","supply-chain","etl","production"],
) as dag:

    start            = EmptyOperator(task_id="start")

    validate_sources = PythonOperator(
        task_id="validate_source_files",
        python_callable=validate_source_files,
        provide_context=True,
    )

    glue_inventory = GlueJobOperator(
        task_id="glue_inventory_processing",
        job_name="hsc-inventory-processing",
        script_location=f"{SCRIPT_BASE}/inventory_processing.py",
        script_args={"--run_date": "{{ ds }}", "--env": ENV},
        **GLUE_KWARGS,
    )

    glue_shipments = GlueJobOperator(
        task_id="glue_shipment_processing",
        job_name="hsc-shipment-processing",
        script_location=f"{SCRIPT_BASE}/shipment_processing.py",
        script_args={"--run_date": "{{ ds }}", "--env": ENV},
        **GLUE_KWARGS,
    )

    glue_procurement = GlueJobOperator(
        task_id="glue_procurement_processing",
        job_name="hsc-procurement-processing",
        script_location=f"{SCRIPT_BASE}/procurement_processing.py",
        script_args={"--run_date": "{{ ds }}", "--env": ENV},
        create_job_kwargs={
            "GlueVersion": GLUE_VERSION,
            "NumberOfWorkers": 3,
            "WorkerType": WORKER_TYPE,
        },
        **{k:v for k,v in GLUE_KWARGS.items() if k != "create_job_kwargs"},
    )

    glue_kpi = GlueJobOperator(
        task_id="glue_kpi_aggregation",
        job_name="hsc-kpi-aggregation",
        script_location=f"{SCRIPT_BASE}/kpi_aggregation.py",
        script_args={"--run_date": "{{ ds }}"},
        create_job_kwargs={
            "GlueVersion": GLUE_VERSION,
            "NumberOfWorkers": 3,
            "WorkerType": WORKER_TYPE,
        },
        **{k:v for k,v in GLUE_KWARGS.items() if k != "create_job_kwargs"},
    )

    freshness_check = PythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
        provide_context=True,
    )

    dbt_run = PythonOperator(
        task_id="dbt_run_and_test",
        python_callable=run_dbt,
        provide_context=True,
        execution_timeout=timedelta(minutes=45),
    )

    notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        provide_context=True,
        trigger_rule="all_success",
    )

    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    # Dependency graph
    start >> validate_sources
    validate_sources >> [glue_inventory, glue_shipments, glue_procurement]
    [glue_inventory, glue_shipments, glue_procurement] >> glue_kpi
    glue_kpi >> freshness_check
    freshness_check >> dbt_run
    dbt_run >> notify >> end
