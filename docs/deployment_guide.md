# Deployment Guide

## Prerequisites
- AWS Account (IAM user with Glue, S3, Redshift, CloudWatch permissions)
- AWS CLI v2 (`aws configure`)
- Python 3.9+
- Apache Airflow 2.9.2
- dbt-redshift 1.8.4

---

## Step 1: S3 Infrastructure

```bash
for bucket in hsc-analytics-raw hsc-analytics-curated hsc-analytics-scripts; do
  aws s3api create-bucket --bucket $bucket --region us-east-1
  aws s3api put-bucket-versioning \
    --bucket $bucket \
    --versioning-configuration Status=Enabled
done
```

## Step 2: IAM Role for Glue

```bash
cat > /tmp/glue-trust.json << 'TRUST'
{
  "Version": "2012-10-17",
  "Statement": [{"Effect":"Allow",
    "Principal":{"Service":"glue.amazonaws.com"},
    "Action":"sts:AssumeRole"}]
}
TRUST

aws iam create-role --role-name GlueHSCRole \
  --assume-role-policy-document file:///tmp/glue-trust.json

aws iam attach-role-policy --role-name GlueHSCRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
aws iam attach-role-policy --role-name GlueHSCRole \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
```

## Step 3: Redshift Cluster

```bash
aws redshift create-cluster \
  --cluster-identifier hsc-analytics \
  --node-type dc2.large \
  --number-of-nodes 2 \
  --master-username admin \
  --master-user-password 'YourSecureP@ssword!' \
  --db-name hsc_dw

# Wait for available
aws redshift wait cluster-available --cluster-identifier hsc-analytics
```

## Step 4: Glue Crawlers

```bash
aws glue create-database \
  --database-input '{"Name":"hsc_raw_catalog"}'

aws glue create-crawler \
  --name hsc-inventory-crawler \
  --role GlueHSCRole \
  --database-name hsc_raw_catalog \
  --targets '{"S3Targets":[{"Path":"s3://hsc-analytics-raw/inventory/"}]}'

aws glue create-crawler \
  --name hsc-shipments-crawler \
  --role GlueHSCRole \
  --database-name hsc_raw_catalog \
  --targets '{"S3Targets":[{"Path":"s3://hsc-analytics-raw/shipments/"}]}'
```

## Step 5: Upload Data & Scripts

```bash
# USAID dataset (no auth)
curl -L "https://data.usaid.gov/api/views/a3rc-nmf6/rows.csv?accessType=DOWNLOAD" \
  -o SCMS.csv
aws s3 cp SCMS.csv s3://hsc-analytics-raw/shipments/

# Kaggle (requires kaggle token ~/.kaggle/kaggle.json)
kaggle datasets download -d vanpatangan/hospital-supply-chain -p ./raw/
kaggle datasets download \
  -d shashwatwork/dataco-smart-supply-chain-for-big-data-analysis -p ./raw/
aws s3 sync ./raw/ s3://hsc-analytics-raw/

# Glue scripts
aws s3 sync glue_jobs/ s3://hsc-analytics-scripts/glue_jobs/
```

## Step 6: Apply Redshift Schema

```bash
psql -h your-cluster.redshift.amazonaws.com -U admin -d hsc_dw \
  -f sql/redshift_schema.sql
```

## Step 7: Configure & Start Airflow

```bash
pip install -r requirements.txt
airflow db init

airflow connections add aws_default --conn-type aws \
  --conn-extra '{"region_name":"us-east-1"}'
airflow connections add redshift_default --conn-type redshift \
  --conn-host your-cluster.redshift.amazonaws.com \
  --conn-port 5439 --conn-schema hsc_dw \
  --conn-login admin --conn-password 'YourSecureP@ssword!'

cp airflow/dags/*.py $AIRFLOW_HOME/dags/
airflow scheduler &
airflow webserver &
airflow dags trigger supply_chain_pipeline
```

## Step 8: Run dbt

```bash
cd dbt/
dbt deps
dbt debug --profiles-dir .
dbt run   --profiles-dir .
dbt test  --profiles-dir .
```

## Step 9: CloudWatch Alerting

```bash
aws cloudwatch put-metric-alarm \
  --alarm-name "HSC-GlueJobFailed" \
  --metric-name "glue.driver.aggregate.numFailedTasks" \
  --namespace "Glue" --statistic Sum --period 300 \
  --threshold 1 --comparison-operator GreaterThanOrEqualToThreshold \
  --alarm-actions arn:aws:sns:us-east-1:ACCOUNT:DataAlerts
```
