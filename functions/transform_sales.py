#!/usr/bin/env python3
"""
Transform Sales Function
Applies business logic transformations to cleaned sales data.
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from decimal import Decimal, ROUND_HALF_UP

import pandas as pd
import numpy as np
from google.cloud import bigquery

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SalesTransformer:
    """Handles business logic transformations for sales data."""

    # Tax rates by region
    TAX_RATES = {
        'US-CA': 0.0725,
        'US-NY': 0.08,
        'US-TX': 0.0625,
        'US-WA': 0.065,
        'US-DEFAULT': 0.07,
        'EU-DE': 0.19,
        'EU-FR': 0.20,
        'EU-UK': 0.20,
        'EU-DEFAULT': 0.20,
    }

    # Product category mappings
    CATEGORY_MAPPING = {
        'ELEC': 'Electronics',
        'CLTH': 'Clothing',
        'FOOD': 'Food & Beverage',
        'HOME': 'Home & Garden',
        'SPRT': 'Sports & Outdoors',
        'BOOK': 'Books & Media',
        'TOYS': 'Toys & Games',
        'OTHER': 'Other'
    }

    def __init__(self):
        self.project_id = os.environ.get('PROJECT_ID', os.environ.get('GCP_PROJECT_ID'))
        self.dataset = os.environ.get('DATASET', 'sales_data')
        self.input_table = os.environ.get('INPUT_TABLE', 'cleaned_transactions')
        self.output_table = os.environ.get('OUTPUT_TABLE', 'transformed_sales')

        self.bq_client = bigquery.Client(project=self.project_id)

        logger.info(f"Initialized SalesTransformer for project: {self.project_id}")

    def load_data(self) -> pd.DataFrame:
        """Load cleaned data from BigQuery."""
        logger.info(f"Loading data from {self.dataset}.{self.input_table}")

        query = f"""
        SELECT *
        FROM `{self.project_id}.{self.dataset}.{self.input_table}`
        """

        df = self.bq_client.query(query).to_dataframe()
        logger.info(f"Loaded {len(df)} records")

        return df

    def calculate_tax(self, row: pd.Series) -> float:
        """Calculate tax amount based on region."""
        region = row.get('region', 'US-DEFAULT')
        amount = float(row.get('total_amount', 0))

        # Get tax rate for region
        tax_rate = self.TAX_RATES.get(region, self.TAX_RATES.get('US-DEFAULT', 0.07))

        # Calculate tax
        tax_amount = amount * tax_rate

        # Round to 2 decimal places
        return round(tax_amount, 2)

    def categorize_product(self, product_id: str) -> str:
        """Map product ID to category."""
        if not product_id:
            return 'Other'

        # Extract category prefix (first 4 chars)
        prefix = product_id[:4].upper()

        return self.CATEGORY_MAPPING.get(prefix, 'Other')

    def calculate_discount_tier(self, quantity: int, total_amount: float) -> str:
        """Determine discount tier based on purchase amount and quantity."""
        if total_amount >= 1000 or quantity >= 50:
            return 'PLATINUM'
        elif total_amount >= 500 or quantity >= 25:
            return 'GOLD'
        elif total_amount >= 100 or quantity >= 10:
            return 'SILVER'
        else:
            return 'BRONZE'

    def calculate_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """Calculate derived metrics for a transaction."""
        quantity = int(row.get('quantity', 0))
        unit_price = float(row.get('unit_price', 0))
        total_amount = float(row.get('total_amount', 0))

        return {
            'average_item_price': round(total_amount / quantity, 2) if quantity > 0 else 0,
            'price_variance': round(abs((unit_price * quantity) - total_amount), 2),
            'is_bulk_order': quantity >= 10,
            'is_high_value': total_amount >= 500,
        }

    def transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all transformations to the dataframe."""
        logger.info("Applying transformations")

        # Create a copy to avoid modifying original
        transformed = df.copy()

        # Add region column if not present (derive from store_id)
        if 'region' not in transformed.columns:
            transformed['region'] = transformed['store_id'].apply(
                lambda x: f"US-{str(x)[:2].upper()}" if x else 'US-DEFAULT'
            )

        # Calculate tax
        transformed['tax_amount'] = transformed.apply(self.calculate_tax, axis=1)
        transformed['total_with_tax'] = transformed['total_amount'] + transformed['tax_amount']

        # Categorize products
        transformed['product_category'] = transformed['product_id'].apply(self.categorize_product)

        # Calculate discount tier
        transformed['discount_tier'] = transformed.apply(
            lambda row: self.calculate_discount_tier(
                row.get('quantity', 0),
                row.get('total_amount', 0)
            ),
            axis=1
        )

        # Calculate additional metrics
        metrics = transformed.apply(self.calculate_metrics, axis=1)
        metrics_df = pd.DataFrame(metrics.tolist())

        transformed['average_item_price'] = metrics_df['average_item_price']
        transformed['price_variance'] = metrics_df['price_variance']
        transformed['is_bulk_order'] = metrics_df['is_bulk_order']
        transformed['is_high_value'] = metrics_df['is_high_value']

        # Add timestamp columns
        transformed['transformed_at'] = datetime.utcnow()
        transformed['transform_version'] = '1.0.0'

        # Extract date components for partitioning
        if 'transaction_date' in transformed.columns:
            transformed['transaction_date'] = pd.to_datetime(transformed['transaction_date'])
            transformed['transaction_year'] = transformed['transaction_date'].dt.year
            transformed['transaction_month'] = transformed['transaction_date'].dt.month
            transformed['transaction_day'] = transformed['transaction_date'].dt.day
            transformed['transaction_weekday'] = transformed['transaction_date'].dt.day_name()
            transformed['transaction_hour'] = transformed['transaction_date'].dt.hour

        # Calculate customer metrics
        customer_stats = transformed.groupby('customer_id').agg({
            'transaction_id': 'count',
            'total_amount': 'sum'
        }).rename(columns={
            'transaction_id': 'customer_order_count',
            'total_amount': 'customer_total_spend'
        })

        transformed = transformed.merge(
            customer_stats,
            left_on='customer_id',
            right_index=True,
            how='left'
        )

        # Classify customer segment
        def classify_customer(row):
            spend = row.get('customer_total_spend', 0)
            orders = row.get('customer_order_count', 0)

            if spend >= 10000 or orders >= 50:
                return 'VIP'
            elif spend >= 5000 or orders >= 25:
                return 'LOYAL'
            elif spend >= 1000 or orders >= 10:
                return 'REGULAR'
            else:
                return 'NEW'

        transformed['customer_segment'] = transformed.apply(classify_customer, axis=1)

        logger.info(f"Transformation complete. Output records: {len(transformed)}")

        return transformed

    def validate_output(self, df: pd.DataFrame) -> bool:
        """Validate transformed data meets quality requirements."""
        logger.info("Validating transformed data")

        validations = []

        # Check for null values in key columns
        key_columns = ['transaction_id', 'customer_id', 'total_amount', 'total_with_tax']
        for col in key_columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    logger.warning(f"Found {null_count} null values in {col}")
                    validations.append(False)
                else:
                    validations.append(True)

        # Check tax calculations are reasonable
        if 'tax_amount' in df.columns and 'total_amount' in df.columns:
            invalid_tax = df[df['tax_amount'] > df['total_amount'] * 0.3]
            if len(invalid_tax) > 0:
                logger.warning(f"Found {len(invalid_tax)} records with unusually high tax")
                validations.append(False)
            else:
                validations.append(True)

        # Check for duplicate transaction IDs
        if 'transaction_id' in df.columns:
            duplicate_count = df['transaction_id'].duplicated().sum()
            if duplicate_count > 0:
                logger.warning(f"Found {duplicate_count} duplicate transaction IDs")
                validations.append(False)
            else:
                validations.append(True)

        # Check data freshness
        if 'transaction_date' in df.columns:
            max_date = pd.to_datetime(df['transaction_date']).max()
            days_old = (datetime.utcnow() - max_date).days
            if days_old > 2:
                logger.warning(f"Data appears stale. Most recent transaction is {days_old} days old")

        is_valid = all(validations)
        logger.info(f"Validation {'passed' if is_valid else 'failed'}")

        return is_valid

    def save_to_bigquery(self, df: pd.DataFrame) -> int:
        """Save transformed data to BigQuery."""
        logger.info(f"Saving to {self.dataset}.{self.output_table}")

        table_id = f"{self.project_id}.{self.dataset}.{self.output_table}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )

        job = self.bq_client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        job.result()  # Wait for completion

        table = self.bq_client.get_table(table_id)
        logger.info(f"Saved {table.num_rows} rows to {table_id}")

        return table.num_rows

    def run(self) -> Dict[str, Any]:
        """Execute the transformation pipeline."""
        logger.info("Starting sales transformation")
        start_time = datetime.utcnow()

        try:
            # Load data
            df = self.load_data()

            if df.empty:
                logger.warning("No data to transform")
                return {
                    'status': 'success',
                    'records_input': 0,
                    'records_output': 0,
                    'message': 'No data to transform'
                }

            # Transform
            transformed_df = self.transform_dataframe(df)

            # Validate
            is_valid = self.validate_output(transformed_df)
            if not is_valid:
                logger.warning("Validation failed but continuing with save")

            # Save
            records_saved = self.save_to_bigquery(transformed_df)

            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()

            result = {
                'status': 'success',
                'records_input': len(df),
                'records_output': records_saved,
                'validation_passed': is_valid,
                'duration_seconds': duration,
                'metrics': {
                    'total_revenue': float(transformed_df['total_with_tax'].sum()),
                    'total_tax': float(transformed_df['tax_amount'].sum()),
                    'unique_customers': int(transformed_df['customer_id'].nunique()),
                    'bulk_orders': int(transformed_df['is_bulk_order'].sum()),
                    'high_value_orders': int(transformed_df['is_high_value'].sum()),
                }
            }

            logger.info(f"Transformation completed: {json.dumps(result)}")
            return result

        except Exception as e:
            logger.error(f"Transformation failed: {str(e)}")
            raise


def main():
    """Main entry point for the transformation function."""
    transformer = SalesTransformer()
    result = transformer.run()

    # Write result to stdout for logging
    print(json.dumps(result, indent=2, default=str))

    return result


if __name__ == "__main__":
    main()
