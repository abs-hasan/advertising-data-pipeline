# TRENDii Data Pipeline - Setup & Execution Guide


## Architecture
```
GCS Bucket → BigQuery External Tables → dbt Pipeline → Star Schema → Analytics
```

## Prerequisites

### Required Tools
- Python 3.10+
- dbt-bigquery 1.10+
- Google Cloud access
- BigQuery permissions

# Project Setup 

### Google Cloud Storage Structure

```
-- Bucket and folders

trendii_takehome/
├── dimensions/
│   ├── dim_campaign.csv
│   └── dim_product.csv
└── raw/
    └── [31 parquet files with event data]
```

### Set Up BigQuery Dataset
- Dataset created: **`trendii_dataset`**

#### Create Database:
- **`trendii_dataset`** - External tables (source data)

External tables created:
- **`dim_product_ext`** → points to `dimensions/dim_product.csv`
- **`dim_campaign_ext`** → points to `dimensions/dim_campaign.csv`
- **`stg_events_ext`** → points to all parquet event files in `raw/`

### 2nd Database - Development Purpose Only
- **`trendii_dataset_dev`** - Development environment (dbt output)

## Part 2: Local dbt Setup

### 1. Environment Setup (Mac)
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  

# Install dependencies
pip install -r requirements.txt

# Install dbt packages
dbt deps
```

### 2. dbt Authentication with BigQuery

#### Setup Google Cloud Authentication
```bash
# Install Google Cloud SDK (if not already installed)
# Download from: https://cloud.google.com/sdk/docs/install

# Authenticate with your Google account
gcloud auth login

# Set application default credentials for dbt
gcloud auth application-default login

# Set your project
gcloud config set project trendii-data-eng-task

# Verify authentication
gcloud auth list
```

#### Alternative: Service Account (Optional)
For production environments, you can use service account authentication:
1. Create service account in Google Cloud Console
2. Grant BigQuery Data Editor + BigQuery Job User roles  
3. Download JSON key file
4. Reference in profiles.yml `keyfile` parameter

### 3. dbt Configuration

#### Create `~/.dbt/profiles.yml`:
```yaml

trendii_pipeline:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: trendii-data-eng-task
      dataset: trendii_dataset_dev
      location: australia-southeast2
      threads: 4 
      priority: interactive
      job_retries: 1
      job_execution_timeout_seconds: 300
   
      
    prod:
      type: bigquery
      method: oauth
      project: trendii-data-eng-task
      dataset: trendii_dataset
      location: australia-southeast2
      threads: 4 
      priority: batch 
      job_retries: 1
      job_execution_timeout_seconds: 300
   
```

#### Update `sources.yml` (if needed):
Verify the source configuration matches your setup:
```yaml
sources:
  - name: raw_trendii
    database: trendii-data-eng-task
    schema: trendii_dataset  # External tables location
```

### 4. Verify Setup
```bash
# Test connection
dbt debug

# Should show all connections successful
```

## Part 3: Pipeline Execution

### Full Pipeline Build
```bash
# Build everything from scratch
dbt build

# Or run step by step:
dbt run    # Build all models
dbt test   # Run all tests
```

### Incremental Development
```bash
# Run by layer
dbt run --select tag:staging
dbt run --select tag:intermediate  
dbt run --select tag:marts
dbt run --select tag:analytics

# Run specific models
dbt run --select stg_events+  # Run stg_events and all downstream
```

### Key Commands
```bash
# Fresh start (rebuild incremental models)
dbt run --full-refresh

# Run only changed models
dbt run --select state:modified+

# Generate documentation
dbt docs generate
dbt docs serve
```

## Part 4: Analysis Outputs

### Analytics Models
The pipeline creates 5 analysis views in `trendii_dataset_dev`:

| Model | Purpose | Business Question |
|-------|---------|-------------------|
| `q1` | Top 5 articles by traffic per domain | Article performance |
| `q2` | Top 3 clicked products per brand (final week) | Product conversion |  
| `q3` | Most impressed product per campaign | Campaign effectiveness |
| `q4` | Mount rate per domain | Ad inventory utilization |
| `q5` | Unique users advertised to | Reach analysis |

### Query Results
```sql
-- View results in BigQuery
SELECT * FROM `trendii-data-eng-task.trendii_dataset_dev.q1`;
SELECT * FROM `trendii-data-eng-task.trendii_dataset_dev.q2`;
SELECT * FROM `trendii-data-eng-task.trendii_dataset_dev.q3`;
SELECT * FROM `trendii-data-eng-task.trendii_dataset_dev.q4`;
SELECT * FROM `trendii-data-eng-task.trendii_dataset_dev.q5`;
```
**Or Dashboard**
**[View Interactive Dashboard →](https://bubbly-batten.metabaseapp.com/public/dashboard/f8acbd6d-8918-4e24-996b-453307e581ad)**

## Project Structure
```
trendii_pipeline/
├── models/
│   ├── staging/           # Raw data cleaning
│   ├── intermediate/      # Business logic transformations
│   ├── marts/core/       # Star schema (dims + facts)
│   └── analytics/        # Analysis queries (q1-q5)
├── seeds/
│   └── company_standardization.csv
├── dbt_project.yml
├── requirements.txt
├── README.md
└── Solution.md
```

## Data Model Overview

### Star Schema Design
- **Fact Tables**: tagloads, mounts, impressions, clicks
- **Dimensions**: articles, products, campaigns, devices, publishers, dates
- **Bridge Connections**: via page_view_id, article_key, publisher_id

### Key Metrics
- **Traffic**: Unique page views per article
- **Mount Rate**: Ad placements per page load
- **Reach**: 77K+ unique users advertised to
- **Engagement**: 36 avg impressions per user

### Fact Table Connections to Dimensions

| Dimension Table | fact_tagloads | fact_mounts | fact_impressions | fact_clicks | Join Key |
|-----------------|---------------|-------------|------------------|-------------|----------|
| **dim_publishers** | ✅ | ✅ | ✅ | ✅ | publisher_id (PK) |
| **dim_devices** | ✅ | ✅ | ✅ | ✅ | device_id (PK) |
| **dim_articles** | ✅ | ✅ | ✅ | ✅ | article_key (PK) |
| **dim_products** | ❌ | ❌ | ✅ | ✅ | product_id (PK) |
| **dim_campaigns** | ❌ | ❌ | ✅ | ✅ | brand_id |
| **dim_dates** | ✅ | ✅ | ✅ | ✅ | date_actual (PK) |

**Legend:**
- ✅ = Direct connection available
- ❌ = Requires bridge connection
- (PK) = Primary Key in dimension table

## 🌉 Bridge Connections

### Problem
`fact_tagloads` and `fact_mounts` cannot directly connect to `dim_products` and `dim_campaigns`.

### Solution
Use `fact_impressions` as a bridge table via common keys:

#### Bridge Connection Methods

| Connection Goal | Bridge Method | Common Key |
|-----------------|---------------|------------|
| **tagloads → products** | via fact_impressions | `page_view_id` or `article_key` |
| **tagloads → campaigns** | via fact_impressions | `page_view_id` or `article_key` |
| **mounts → products** | via fact_impressions | `page_view_id` or `article_key` |
| **mounts → campaigns** | via fact_impressions | `page_view_id` or `article_key` |


## Troubleshooting

### Common Issues
```bash
# Profile not found
dbt debug --profiles-dir ~/.dbt

# Permissions error
# Ensure BigQuery Data Editor + Job User roles

# Model compilation error  
dbt compile --select model_name

# Test failures
dbt test --store-failures
```

### Performance Tips
- Use `--threads 4` for faster builds
- Run `--select tag:marts` for core tables only
- Use `--exclude tag:analytics` to skip analysis views

## Monitoring & Maintenance

### Daily Operations
```bash
# Incremental refresh (recommended)
dbt run

# Full test suite
dbt test
```


---


## Roadmap

- Automate load from GCS → BigQuery → dbt
- Keep only final marts
- Add Slack alerts for errors


