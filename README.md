# Workflow Example Repository

This is a template repository for the Serverless Workflow Orchestration Platform. Define your data pipelines in YAML, write Python functions, and let the platform handle deployment and execution.

## Quick Start

### 1. Clone this repository

```bash
git clone https://github.com/jakekirsch2/workflow-example.git {my-repo-name}
cd {my-repo-name}
```
Create repo in GitHub

### 2. Connect to the platform

- Go to https://workflow-orchestrator-frontend-gstxypf3sq-uc.a.run.app/
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

This allows you to test changes in development before promoting to production.

## Repository Structure

```
workflow-example/
├── pipelines/              # Pipeline definitions (YAML)
│   └── daily_etl.yaml      # Example ETL pipeline
├── functions/              # Python functions
│   ├── extract_data.py     # Data extraction
│   ├── transform_sales.py  # Data transformation
│   └── requirements.txt    # Python dependencies
└── README.md
```

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
    memory: "2Gi"
    cpu: "1"

  - name: transform
    type: python
    function: functions/transform_sales.py
    entrypoint: main
    depends_on:
      - extract
    memory: "4Gi"
    cpu: "2"
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
| `memory` | Memory allocation (`512Mi`, `2Gi`, etc.) |
| `cpu` | CPU allocation (`1`, `2`, etc.) |

## Writing Python Functions

Your function's entrypoint receives the `args` from the pipeline definition as positional arguments:

```python
# functions/extract_data.py

def main(source_table: str, dataset: str):
    """
    Called with args from pipeline:
      args:
        - "raw_transactions"   -> source_table
        - "sales_data"         -> dataset
    """
    print(f"Extracting from {source_table} to {dataset}")

    # Your extraction logic here

    return {
        "status": "success",
        "records_extracted": 1250
    }
```

The platform calls your function like: `main("raw_transactions", "sales_data")`

## Environment Variables

You can configure environment variables for your pipelines in the platform UI:

1. Go to **Settings** > **Environment Variables**
2. Select your repository
3. Choose the environment (Production or Development)
4. Add your variables (e.g., `API_KEY`, `DATABASE_URL`)

These variables are securely stored and injected into your functions at runtime. Access them in your code:

```python
import os

def main():
    api_key = os.environ.get("API_KEY")
    database_url = os.environ.get("DATABASE_URL")
```

### System Variables

The platform automatically provides these variables:

| Variable | Description |
|----------|-------------|
| `PIPELINE_NAME` | Current pipeline name |
| `TASK_NAME` | Current task name |
| `EXECUTION_ID` | Unique execution ID |
| `ENVIRONMENT` | `production` or `development` |

## Local Development

### Prerequisites

- Python 3.11+
- pyenv (recommended for managing Python versions)

### Setup with pyenv

```bash
# Install pyenv (macOS)
brew install pyenv

# Install Python 3.11
pyenv install 3.11

# Set local Python version
cd workflow-example
pyenv local 3.11

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r functions/requirements.txt
```

### Testing Functions Locally

Test your functions before deploying:

```bash
# Activate virtual environment
source venv/bin/activate

# Test with arguments matching your pipeline definition
python -c "from functions.extract_data import main; main('raw_transactions', 'sales_data')"

# Or run the file directly if it has a __main__ block
python functions/extract_data.py
```

### Adding Dependencies

Add any Python packages your functions need to `functions/requirements.txt`:

```
requests>=2.28.0
pandas>=2.0.0
```

The platform will install these when building your container.

## Platform Features

- **Auto-deploy**: Push to GitHub and pipelines are automatically redeployed
- **Scheduling**: Run pipelines on a cron schedule
- **Manual triggers**: Run pipelines on-demand from the dashboard
- **Real-time logs**: View execution logs as they happen
- **Cost tracking**: Monitor usage and costs per execution
- **Environment variables**: Securely store secrets in the UI

## Support

- Platform: https://workflow-orchestrator-frontend-gstxypf3sq-uc.a.run.app/
- Issues: https://github.com/jakekirsch2/workflow-example/issues
