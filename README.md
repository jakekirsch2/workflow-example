# Workflow Example Repository

This is a template repository for the Serverless Workflow Orchestration Platform. Define your data pipelines in YAML, write Python functions, and let the platform handle deployment and execution on Dataproc Serverless.

## Quick Start

### 1. Clone this repository

```bash
git clone https://github.com/jakekirsch2/workflow-example.git {my-repo-name}
cd {my-repo-name}
git remote set-url origin https://github.com/{my-username}/{my-repo-name}.git
git push -u origin main
```

### 2. Connect to the platform

- Go to the platform dashboard
- Sign in with GitHub
- Connect your cloned repository
- The platform will automatically deploy your pipelines

### 3. Make changes and push

Edit your pipeline or functions, then push to trigger a redeploy:

```bash
git add .
git commit -m "Update pipeline"
git push
```

### Branch-based Environments

The platform automatically deploys to different environments based on your branch:

| Branch | Environment |
|--------|-------------|
| `main` or `master` | Production |
| `dev` or `develop` | Development |

## Repository Structure

```
workflow-example/
├── config.yaml                # Repo-level config (Python version, schemas)
├── dev.py                     # Local development CLI
├── pipelines/                 # Pipeline definitions (YAML)
│   └── daily_etl.yaml         # Example ETL pipeline
├── functions/                 # Python functions
│   ├── extract_data.py        # Data extraction
│   ├── transform_sales.py     # Data transformation
│   └── requirements.txt       # Python dependencies
└── README.md
```

## Repository Configuration

The `config.yaml` file defines the Python version, Iceberg schemas, and pip packages for your repository.

```yaml
python_version: "3.11"

schemas:
  - sales
  - analytics

pip_packages: []
```

### Config Fields

| Field | Default | Description |
|-------|---------|-------------|
| `python_version` | `"3.11"` | Python version (e.g. `"3.11"`, `"3.12"`) |
| `schemas` | `[]` | Iceberg namespace names for your lakehouse tables |
| `pip_packages` | `[]` | Additional Python packages to install |

Compute resources are managed automatically by Dataproc Serverless — no CPU/memory configuration needed.

## Pipeline Definition

Pipelines are defined in YAML files in the `pipelines/` directory:

```yaml
name: daily-etl
description: Daily ETL pipeline example
schedule: "0 6 * * *"  # 6 AM daily (cron format)

tasks:
  - name: extract
    type: python
    function: functions/extract_data.py
    entrypoint: main
    args:
      - "raw_transactions"
      - "sales_data"

  - name: transform
    type: python
    function: functions/transform_sales.py
    entrypoint: main
    depends_on:
      - extract
```

### Pipeline Fields

| Field | Description |
|-------|-------------|
| `name` | Pipeline identifier (lowercase, hyphens) |
| `description` | Human-readable description |
| `schedule` | Cron expression for scheduled runs (optional) |
| `tasks` | List of tasks to execute |

### Task Fields

| Field | Description |
|-------|-------------|
| `name` | Task identifier |
| `type` | Task type (`python`) |
| `function` | Path to Python file |
| `entrypoint` | Function to call (default: `main`) |
| `args` | Arguments passed to the function (as positional args) |
| `depends_on` | List of tasks that must complete first |

## Writing Python Functions

Functions receive a SparkSession as the first argument, followed by any `args` from the pipeline definition:

```python
# functions/extract_data.py

def main(spark, source_table: str, dataset: str):
    """
    Called with args from pipeline:
      args:
        - "raw_transactions"   -> source_table
        - "sales_data"         -> dataset
    """
    # Write to an Iceberg table
    df = spark.createDataFrame([...], columns=[...])
    df.writeTo(f"sales.{source_table}").createOrReplace()
```

## Environment Variables

Configure environment variables in the platform UI:

1. Go to **Settings** > **Environment Variables**
2. Select your repository and environment
3. Add your variables (e.g., `API_KEY`, `DATABASE_URL`)

Variables are securely stored and injected into your functions at runtime via `os.environ`:

```python
import os

def main(spark):
    api_key = os.environ.get("API_KEY")
    database_url = os.environ.get("DATABASE_URL")
```

## Local Development

The `dev.py` CLI lets you run functions, query data, and browse tables from your local machine. All execution happens on Dataproc Serverless via the platform API — you just need Python and the `requests` library.

### Setup

```bash
pip install requests
```

1. Go to **Settings** > **API Keys** and generate a new key
2. Go to **Settings** > **Environment Variables**, select your repo, and click **Download .env.development**
3. Paste your API key into `.env.development`:

```
WORKFLOW_API_URL=https://your-platform.example.com
WORKFLOW_API_KEY=wf_your_actual_key_here
WORKFLOW_REPO_ID=abc123
WORKFLOW_ENVIRONMENT=development
```

### Commands

```bash
# Run a function against real data (submitted to Dataproc via API)
python dev.py run functions/extract_data.py raw_transactions sales_data

# Run against production data
python dev.py run functions/extract_data.py --env production

# Query data
python dev.py query "SELECT * FROM sales.raw_transactions LIMIT 10"

# Cross-schema queries work naturally
python dev.py query "SELECT * FROM sales.raw_transactions JOIN analytics.metrics USING (id)"

# List schemas and tables
python dev.py tables

# Show environment variables
python dev.py env

# Interactive SQL REPL
python dev.py shell
```

### How it works

- `dev.py run` reads your local Python file and sends it to the platform API, which runs it on Dataproc Serverless with the same Iceberg catalog and environment variables as production pipelines.
- `dev.py query` and `dev.py shell` execute Spark SQL queries via the API. Tables are referenced as `schema.table` (e.g., `sales.raw_transactions`).
- No GCP SDK, no `gcloud`, no Spark installation needed locally. Everything goes through the API.

## Platform Features

- **Auto-deploy**: Push to GitHub and pipelines are automatically redeployed
- **Scheduling**: Run pipelines on a cron schedule
- **Manual triggers**: Run pipelines on-demand from the dashboard
- **Real-time logs**: View execution logs as they happen
- **Cost tracking**: Monitor Dataproc usage and costs per execution
- **Environment variables**: Securely store and inject secrets
- **Local dev CLI**: Test functions and query data without deploying
- **Iceberg lakehouse**: Tables stored in GCS with full schema evolution support
