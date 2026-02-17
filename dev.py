#!/usr/bin/env python3
"""
Workflow Platform - Local Development CLI

Run functions, query data, and manage env vars via the platform API.
Only requires Python 3.7+ and the `requests` library.

Setup:
  1. Generate an API key in Settings > API Keys
  2. Download .env.development from Settings > Environment Variables
  3. Paste your API key into .env.development

Usage:
  python dev.py run functions/extract_data.py raw_transactions sales_data
  python dev.py run functions/extract_data.py --env production
  python dev.py query "SELECT * FROM sales.raw_transactions LIMIT 10"
  python dev.py tables
  python dev.py env
  python dev.py shell
"""

import sys
import os
import json
import time
import argparse

try:
    import requests
except ImportError:
    print("Error: 'requests' library is required. Install with: pip install requests")
    sys.exit(1)


def load_env(env_file):
    """Parse a .env file into a dict. Handles KEY=VALUE lines, ignores comments."""
    env = {}
    if not os.path.exists(env_file):
        return env
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip()
                # Strip surrounding quotes
                if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                    value = value[1:-1]
                env[key] = value
    return env


def get_config(args):
    """Load config from .env file based on environment."""
    environment = getattr(args, "env", None) or "development"
    env_file = f".env.{environment}"

    # Fall back to .env if specific file doesn't exist
    if not os.path.exists(env_file):
        env_file = ".env"

    if not os.path.exists(env_file):
        print(f"Error: No config file found. Expected {env_file}")
        print("Download one from Settings > Environment Variables > Download .env")
        sys.exit(1)

    env = load_env(env_file)

    api_url = env.get("WORKFLOW_API_URL")
    api_key = env.get("WORKFLOW_API_KEY")
    repo_id = env.get("WORKFLOW_REPO_ID")

    if not api_url or not api_key or not repo_id:
        print(f"Error: {env_file} must contain WORKFLOW_API_URL, WORKFLOW_API_KEY, and WORKFLOW_REPO_ID")
        sys.exit(1)

    if api_key == "wf_paste_your_key_here":
        print("Error: Replace the placeholder API key in your .env file with your actual key.")
        print("Generate one at Settings > API Keys")
        sys.exit(1)

    return {
        "api_url": api_url.rstrip("/"),
        "api_key": api_key,
        "repo_id": repo_id,
        "environment": env.get("WORKFLOW_ENVIRONMENT", environment),
    }


def api_request(config, method, path, body=None):
    """Make an authenticated API request."""
    url = f"{config['api_url']}{path}"
    headers = {
        "Authorization": f"Bearer {config['api_key']}",
        "Content-Type": "application/json",
    }

    try:
        resp = requests.request(method, url, headers=headers, json=body, timeout=600)
    except requests.ConnectionError:
        print(f"Error: Could not connect to {config['api_url']}")
        sys.exit(1)

    if resp.status_code == 401:
        print("Error: Authentication failed. Check your API key.")
        sys.exit(1)

    if not resp.ok:
        try:
            error = resp.json().get("error", resp.text)
        except Exception:
            error = resp.text
        print(f"Error ({resp.status_code}): {error}")
        sys.exit(1)

    return resp.json()


def format_table(columns, rows, max_width=40):
    """Format data as a text table."""
    if not rows:
        print("(no rows)")
        return

    # Calculate column widths
    widths = {col: len(col) for col in columns}
    for row in rows:
        for col in columns:
            val = str(row.get(col, ""))
            widths[col] = min(max(widths[col], len(val)), max_width)

    # Header
    header = " | ".join(col.ljust(widths[col])[:widths[col]] for col in columns)
    separator = "-+-".join("-" * widths[col] for col in columns)
    print(header)
    print(separator)

    # Rows
    for row in rows:
        line = " | ".join(
            str(row.get(col, "")).ljust(widths[col])[:widths[col]]
            for col in columns
        )
        print(line)


# ── Commands ────────────────────────────────────────────────────────

def cmd_run(args):
    """Run a function on Dataproc via the API."""
    config = get_config(args)

    file_path = args.file
    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        sys.exit(1)

    with open(file_path) as f:
        function_code = f.read()

    print(f"Submitting {file_path} to Dataproc ({config['environment']})...")
    print()

    result = api_request(config, "POST", f"/api/repos/{config['repo_id']}/dev/run", {
        "functionCode": function_code,
        "entrypoint": args.entrypoint or "main",
        "args": args.args or [],
        "environment": config["environment"],
    })

    # Print logs
    if result.get("logs"):
        print("── Logs ──")
        print(result["logs"])
        print()

    # Print status
    status = result.get("status", "unknown")
    duration = result.get("duration", "?")
    icon = "OK" if status == "success" else "FAIL"
    print(f"[{icon}] Status: {status} | Duration: {duration}")


def cmd_query(args):
    """Execute a SQL query."""
    config = get_config(args)
    sql = args.sql

    print(f"Running query ({config['environment']})...")
    start = time.time()

    result = api_request(config, "POST", f"/api/repos/{config['repo_id']}/database/query", {
        "sql": sql,
        "environment": config["environment"],
        "maxRows": 5000,
    })

    elapsed = time.time() - start

    if result.get("columns") and result.get("rows"):
        format_table(result["columns"], result["rows"])
        print()
        print(f"{result.get('totalRows', len(result['rows']))} rows | {result.get('duration', '?')}ms (Spark) | {elapsed:.1f}s (total)")
    else:
        print("Query returned no results.")


def cmd_tables(args):
    """List schemas and tables."""
    config = get_config(args)

    schemas_data = api_request(config, "GET", f"/api/repos/{config['repo_id']}/database/schemas")
    schemas = schemas_data.get("schemas", [])

    if not schemas:
        print("No schemas defined. Add a 'schemas' list to config.yaml.")
        return

    env = config["environment"]

    for schema in schemas:
        tables_data = api_request(
            config, "GET",
            f"/api/repos/{config['repo_id']}/database/schemas/{schema}/tables?env={env}"
        )
        tables = tables_data.get("tables", [])

        print(f"{schema}/")
        if tables:
            for i, table in enumerate(tables):
                prefix = "  └── " if i == len(tables) - 1 else "  ├── "
                row_count = table.get("rowCount", "0")
                print(f"{prefix}{table['id']} ({row_count} rows)")
        else:
            print("  (no tables)")


def cmd_env(args):
    """Show environment variables."""
    config = get_config(args)

    result = api_request(
        config, "GET",
        f"/api/repos/{config['repo_id']}/env/{config['environment']}"
    )

    variables = result.get("variables", {})
    if not variables:
        print(f"No environment variables defined for {config['environment']}.")
        return

    print(f"Environment variables ({config['environment']}):")
    print()
    for key, data in variables.items():
        value = data.get("value", "****") if isinstance(data, dict) else str(data)
        print(f"  {key} = {value}")


def cmd_shell(args):
    """Interactive SQL REPL."""
    config = get_config(args)

    print(f"Workflow SQL Shell ({config['environment']})")
    print("Type SQL queries, or 'exit' to quit. Queries may take 30-60s to start.")
    print()

    while True:
        try:
            sql = input("sql> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not sql:
            continue
        if sql.lower() in ("exit", "quit", "\\q"):
            break

        # Collect multi-line input if the line ends with a backslash
        while sql.endswith("\\"):
            try:
                sql = sql[:-1] + "\n" + input("  -> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break

        print(f"Running...")
        start = time.time()

        try:
            result = api_request(config, "POST", f"/api/repos/{config['repo_id']}/database/query", {
                "sql": sql,
                "environment": config["environment"],
                "maxRows": 5000,
            })

            elapsed = time.time() - start

            if result.get("columns") and result.get("rows"):
                print()
                format_table(result["columns"], result["rows"])
                print(f"\n{result.get('totalRows', len(result['rows']))} rows | {result.get('duration', '?')}ms (Spark) | {elapsed:.1f}s (total)\n")
            else:
                print("Query returned no results.\n")
        except SystemExit:
            # api_request calls sys.exit on error; catch it in shell mode
            print()
            continue


# ── Main ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Workflow Platform - Local Development CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--env", default=None, help="Environment: development (default) or production")

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # run
    run_parser = subparsers.add_parser("run", help="Run a function on Dataproc")
    run_parser.add_argument("file", help="Path to Python file")
    run_parser.add_argument("args", nargs="*", help="Arguments to pass to the function")
    run_parser.add_argument("--entrypoint", default="main", help="Function name to call (default: main)")

    # query
    query_parser = subparsers.add_parser("query", help="Execute a SQL query")
    query_parser.add_argument("sql", help="SQL query string")

    # tables
    subparsers.add_parser("tables", help="List schemas and tables")

    # env
    subparsers.add_parser("env", help="Show environment variables")

    # shell
    subparsers.add_parser("shell", help="Interactive SQL REPL")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    commands = {
        "run": cmd_run,
        "query": cmd_query,
        "tables": cmd_tables,
        "env": cmd_env,
        "shell": cmd_shell,
    }

    commands[args.command](args)


if __name__ == "__main__":
    main()
