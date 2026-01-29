#!/usr/bin/env python3
"""
Extract Data Function
Extracts raw sales data from source systems and loads into BigQuery staging.
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import requests

from google.cloud import bigquery
from google.cloud import secretmanager
from google.api_core import retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataExtractor:
    """Handles data extraction from various sources."""

    def __init__(self):
        self.project_id = os.environ.get('PROJECT_ID', os.environ.get('GCP_PROJECT_ID'))
        self.dataset = os.environ.get('DATASET', 'sales_data')
        self.source_table = os.environ.get('SOURCE_TABLE', 'raw_transactions')
        self.batch_size = int(os.environ.get('BATCH_SIZE', '10000'))

        # Initialize clients
        self.bq_client = bigquery.Client(project=self.project_id)
        self.secret_client = secretmanager.SecretManagerServiceClient()

        logger.info(f"Initialized DataExtractor for project: {self.project_id}")

    def get_secret(self, secret_name: str) -> str:
        """Retrieve a secret from Secret Manager."""
        name = f"projects/{self.project_id}/secrets/{secret_name}/versions/latest"
        response = self.secret_client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")

    def get_extraction_window(self) -> tuple:
        """Calculate the time window for data extraction."""
        end_time = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        start_time = end_time - timedelta(days=1)
        return start_time, end_time

    def extract_from_api(self, api_config: Dict[str, Any]) -> List[Dict]:
        """Extract data from an external API."""
        logger.info(f"Extracting data from API: {api_config.get('name', 'unknown')}")

        api_key = self.get_secret(api_config.get('api_key_secret', 'api_key'))
        base_url = api_config.get('base_url')

        start_time, end_time = self.get_extraction_window()

        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }

        all_records = []
        page = 1

        while True:
            params = {
                'start_date': start_time.isoformat(),
                'end_date': end_time.isoformat(),
                'page': page,
                'limit': self.batch_size
            }

            response = requests.get(
                f"{base_url}/transactions",
                headers=headers,
                params=params,
                timeout=60
            )
            response.raise_for_status()

            data = response.json()
            records = data.get('data', [])

            if not records:
                break

            all_records.extend(records)
            logger.info(f"Extracted {len(records)} records from page {page}")

            if len(records) < self.batch_size:
                break

            page += 1

        logger.info(f"Total records extracted from API: {len(all_records)}")
        return all_records

    def extract_from_database(self, db_config: Dict[str, Any]) -> List[Dict]:
        """Extract data from a source database via federated query."""
        logger.info("Extracting data from source database")

        start_time, end_time = self.get_extraction_window()

        # Example: Using BigQuery external connection
        query = f"""
        SELECT
            transaction_id,
            customer_id,
            product_id,
            quantity,
            unit_price,
            total_amount,
            transaction_date,
            store_id,
            payment_method,
            created_at
        FROM `{self.project_id}.{self.dataset}.{self.source_table}`
        WHERE transaction_date >= @start_time
            AND transaction_date < @end_time
        ORDER BY transaction_date
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", start_time),
                bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", end_time),
            ]
        )

        query_job = self.bq_client.query(query, job_config=job_config)
        results = query_job.result()

        records = [dict(row) for row in results]
        logger.info(f"Extracted {len(records)} records from database")

        return records

    def transform_records(self, records: List[Dict]) -> List[Dict]:
        """Apply basic transformations to extracted records."""
        transformed = []

        for record in records:
            transformed_record = {
                'transaction_id': str(record.get('transaction_id', '')),
                'customer_id': str(record.get('customer_id', '')),
                'product_id': str(record.get('product_id', '')),
                'quantity': int(record.get('quantity', 0)),
                'unit_price': float(record.get('unit_price', 0.0)),
                'total_amount': float(record.get('total_amount', 0.0)),
                'transaction_date': record.get('transaction_date'),
                'store_id': str(record.get('store_id', '')),
                'payment_method': str(record.get('payment_method', 'unknown')),
                'extracted_at': datetime.utcnow().isoformat(),
                'source_system': 'daily_etl'
            }
            transformed.append(transformed_record)

        return transformed

    @retry.Retry(predicate=retry.if_exception_type(Exception))
    def load_to_staging(self, records: List[Dict]) -> int:
        """Load extracted records to BigQuery staging table."""
        if not records:
            logger.warning("No records to load")
            return 0

        table_id = f"{self.project_id}.{self.dataset}.staging_{self.source_table}"

        # Define schema
        schema = [
            bigquery.SchemaField("transaction_id", "STRING"),
            bigquery.SchemaField("customer_id", "STRING"),
            bigquery.SchemaField("product_id", "STRING"),
            bigquery.SchemaField("quantity", "INTEGER"),
            bigquery.SchemaField("unit_price", "FLOAT"),
            bigquery.SchemaField("total_amount", "FLOAT"),
            bigquery.SchemaField("transaction_date", "TIMESTAMP"),
            bigquery.SchemaField("store_id", "STRING"),
            bigquery.SchemaField("payment_method", "STRING"),
            bigquery.SchemaField("extracted_at", "TIMESTAMP"),
            bigquery.SchemaField("source_system", "STRING"),
        ]

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        # Load in batches
        total_loaded = 0
        for i in range(0, len(records), self.batch_size):
            batch = records[i:i + self.batch_size]

            load_job = self.bq_client.load_table_from_json(
                batch,
                table_id,
                job_config=job_config
            )
            load_job.result()  # Wait for completion

            total_loaded += len(batch)
            logger.info(f"Loaded batch {i // self.batch_size + 1}: {len(batch)} records")

        logger.info(f"Total records loaded to staging: {total_loaded}")
        return total_loaded

    def run(self) -> Dict[str, Any]:
        """Execute the extraction pipeline."""
        logger.info("Starting data extraction")
        start_time = datetime.utcnow()

        try:
            # Extract from source
            records = self.extract_from_database({})

            # Transform records
            transformed = self.transform_records(records)

            # Load to staging
            loaded_count = self.load_to_staging(transformed)

            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()

            result = {
                'status': 'success',
                'records_extracted': len(records),
                'records_loaded': loaded_count,
                'duration_seconds': duration,
                'extraction_window': {
                    'start': self.get_extraction_window()[0].isoformat(),
                    'end': self.get_extraction_window()[1].isoformat()
                }
            }

            logger.info(f"Extraction completed: {json.dumps(result)}")
            return result

        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            raise


def main():
    """Main entry point for the extraction function."""
    extractor = DataExtractor()
    result = extractor.run()

    # Write result to stdout for logging
    print(json.dumps(result, indent=2))

    return result


if __name__ == "__main__":
    main()
