#!/usr/bin/env python3
"""
Extract Data Function (Demo Stub)
Demonstrates the extraction step of the ETL pipeline.

Functions receive arguments from the pipeline YAML definition.
Environment variables are still available for secrets and global config.
"""

import os
import json
import logging
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main(source_table: str = "raw_transactions", dataset: str = "sales_data"):
    """
    Main entry point for the extraction function.

    Args:
        source_table: The source table to extract from
        dataset: The dataset name to use
    """
    logger.info("Starting data extraction (demo mode)")

    # Get system config from environment (set by the platform)
    project_id = os.environ.get('PROJECT_ID', 'demo-project')
    execution_id = os.environ.get('EXECUTION_ID', 'local-run')
    pipeline_name = os.environ.get('PIPELINE_NAME', 'daily_etl')
    task_name = os.environ.get('TASK_NAME', 'extract')

    # Config from environment (via Secret Manager UI)
    batch_size = int(os.environ.get('BATCH_SIZE', '10000'))

    logger.info(f"Configuration:")
    logger.info(f"  Project ID: {project_id} test deploy")
    logger.info(f"  Dataset: {dataset}")
    logger.info(f"  Source Table: {source_table}")
    logger.info(f"  Batch Size: {batch_size}")
    logger.info(f"  Execution ID: {execution_id}")
    logger.info(f"  Pipeline: {pipeline_name}")
    logger.info(f"  Task: {task_name}")

    # Simulate extraction window
    end_time = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(days=1)

    logger.info(f"Extraction window: {start_time.isoformat()} to {end_time.isoformat()}")

    # Simulate extraction steps
    logger.info("Step 1: Connecting to source system...")
    logger.info("Step 2: Querying data for extraction window...")
    logger.info("Step 3: Transforming records...")
    logger.info("Step 4: Loading to staging table...")

    # Simulate some extracted records
    demo_records = 1250

    result = {
        'status': 'success',
        'records_extracted': demo_records,
        'records_loaded': demo_records,
        'duration_seconds': 5.2,
        'extraction_window': {
            'start': start_time.isoformat(),
            'end': end_time.isoformat()
        },
        'execution_id': execution_id,
        'demo_mode': True
    }

    logger.info(f"Extraction completed successfully")
    logger.info(f"Records extracted: {demo_records}")

    print(json.dumps(result, indent=2))
    return result


if __name__ == "__main__":
    main()
