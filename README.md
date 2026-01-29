# Workflow Example Repository

This is a template repository demonstrating how to define data pipelines for the Serverless Workflow Orchestration Platform.

## Quick Start

1. **Fork this repository** to your GitHub account

2. **Connect to the platform**:
   - Go to https://workflow-orchestrator.example.com
   - Sign in with GitHub
   - Select this repository
   - The platform will automatically deploy your pipelines

3. **Edit your pipeline** in `pipelines/daily_etl.yaml`

4. **Push changes** - the platform automatically redeploys

## Repository Structure

```
workflow-example/
├── pipelines/              # Pipeline definitions (YAML)
│   └── daily_etl.yaml      # Example ETL pipeline
├── functions/              # Python functions
│   ├── extract_data.py     # Data extraction
│   ├── transform_sales.py  # Data transformation
│   └── requirements.txt    # Python dependencies
├── sql/                    # BigQuery SQL files
│   ├── sales_clean.sql     # Data cleaning
│   └── daily_summary.sql   # Aggregations
├── config.yaml             # Platform configuration
└── .env.example            # Secrets template
```

## Pipeline Definition Format

```yaml
name: my_pipeline
schedule: "0 2 * * *"  # Cron expression (2 AM daily)

tasks:
  - name: extract
    type: python
    function: functions/extract_data.py
    memory: "2Gi"
    cpu: "1"

  - name: transform
    type: sql
    file: sql/my_query.sql
    depends_on:
      - extract

alerting:
  on_failure:
    - slack:
        channel: "#alerts"
```

## Task Types

### Python Tasks

Execute Python functions as containerized jobs:

```yaml
- name: my_python_task
  type: python
  function: functions/my_function.py
  entrypoint: main  # Optional, defaults to 'main'
  memory: "4Gi"     # 256Mi to 32Gi
  cpu: "2"          # 0.5 to 8
  timeout: "1h"     # Max 24h
  env:
    MY_VAR: "value"
```

### SQL Tasks

Execute BigQuery SQL queries:

```yaml
- name: my_sql_task
  type: sql
  file: sql/my_query.sql
  destination:
    dataset: my_dataset
    table: my_table
    write_disposition: WRITE_TRUNCATE  # or WRITE_APPEND
```

## Environment Variables

Environment variables can be set at multiple levels:

1. **Pipeline level** - Available to all tasks
2. **Task level** - Override for specific tasks
3. **Secrets** - Stored in `.env` and synced to GCP Secret Manager

### Available Variables

| Variable | Description |
|----------|-------------|
| `GCP_PROJECT_ID` | Your GCP project ID |
| `DATASET` | Default BigQuery dataset |
| `PIPELINE_NAME` | Current pipeline name |
| `TASK_NAME` | Current task name |
| `RUN_ID` | Unique execution ID |

## Alerting

Configure alerts for pipeline events:

```yaml
alerting:
  on_success:
    - slack:
        channel: "#data-pipeline"
        message: "Pipeline completed!"

  on_failure:
    - slack:
        channel: "#alerts"
        mention: "@oncall"
    - email:
        to:
          - team@example.com
```

## Local Development

### Prerequisites

- Python 3.11+
- Google Cloud SDK
- Docker (optional)

### Setup

```bash
# Clone the repository
git clone https://github.com/your-org/workflow-example.git
cd workflow-example

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r functions/requirements.txt

# Set up environment
cp .env.example .env
# Edit .env with your values
```

### Testing Functions Locally

```bash
# Run extraction
python functions/extract_data.py

# Run transformation
python functions/transform_sales.py
```

### Testing SQL Locally

```bash
# Use bq CLI
bq query --use_legacy_sql=false < sql/sales_clean.sql
```

## Cost Estimation

| Component | Typical Usage | Monthly Cost |
|-----------|--------------|--------------|
| Cloud Run Jobs | 2 vCPU, 4GB, 30min/day | ~$15 |
| BigQuery | 10GB processed/day | ~$15 |
| Cloud Workflows | 100 executions/day | ~$3 |
| **Total** | | **~$33/month** |

## Troubleshooting

### Pipeline not deploying

1. Check the platform dashboard for errors
2. Verify YAML syntax: `yamllint pipelines/`
3. Check GitHub webhook delivery

### Function failing

1. Check Cloud Logging in the dashboard
2. Verify environment variables are set
3. Test function locally first

### SQL errors

1. Check query syntax in BigQuery console
2. Verify table and dataset exist
3. Check IAM permissions

## Support

- Documentation: https://docs.workflow-orchestrator.example.com
- Issues: https://github.com/your-org/workflow-orchestrator/issues
- Slack: #workflow-platform

## License

MIT License - see LICENSE file
