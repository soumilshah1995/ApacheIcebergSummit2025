import os
import logging
import json
import boto3
import pyarrow as pa
import pyiceberg
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration Constants
# Environment variables for AWS and Catalog configuration

REGION = os.getenv("AWS_DEFAULT_REGION")
CATALOG = "s3tablescatalog"
DATABASE = 's3tables'
TABLE_BUCKET = os.getenv("BUCKET_NAME")
TABLE_NAME = 'invoices'


def initialize_catalog(account_id):
    """
    Initialize the Iceberg catalog using AWS Glue REST API.

    Args:
        account_id (str): AWS Account ID for warehouse path configuration.

    Returns:
        pyiceberg.catalog.Catalog: Configured Iceberg catalog or None if initialization fails.
    """
    try:
        # Configuration for REST-based Iceberg catalog using AWS Glue
        catalog_config = {
            "type": "rest",
            "warehouse": f"{account_id}:{CATALOG}/{TABLE_BUCKET}",
            "uri": f"https://glue.{REGION}.amazonaws.com/iceberg",
            "rest.sigv4-enabled": "true",
            "rest.signing-name": "glue",
            "rest.signing-region": REGION,
        }
        print(catalog_config)

        rest_catalog = load_catalog(CATALOG, **catalog_config)
        logger.info("Catalog loaded successfully!")
        return rest_catalog
    except Exception as e:
        logger.error(f"Error loading catalog: {e}", exc_info=True)
        return None


def load_table(catalog, database, table_name):
    """
    Load an Iceberg table from the specified catalog and database.

    Args:
        catalog (pyiceberg.catalog.Catalog): Initialized Iceberg catalog.
        database (str): Name of the database containing the table.
        table_name (str): Name of the table to load.

    Returns:
        pyiceberg.table.Table: Loaded Iceberg table or None if loading fails.
    """
    try:
        table = catalog.load_table(f"{database}.{table_name}")
        logger.info(f"Table {table_name} schema: {table.schema()}")
        return table
    except Exception as e:
        logger.error(f"Error loading the table: {e}", exc_info=True)
        return None


def read_table_data(table):
    """
    Read and print all data from the Iceberg table.

    Args:
        table (pyiceberg.table.Table): Iceberg table to read data from.
    """
    try:
        logger.info("Reading data from the table...")
        all_data = table.scan().to_pandas()
        logger.info("\nData in the table:")
        print(all_data)
    except Exception as e:
        logger.error(f"Error reading data from the table: {e}", exc_info=True)


def main():
    """
    Main function to orchestrate Iceberg table operations.
    Retrieves AWS account ID, initializes catalog, loads table,
    reads data, performs deletion, and reads data again.
    """
    try:
        # Get AWS Account ID
        account_id = boto3.client('sts').get_caller_identity().get('Account')

        # Initialize catalog
        catalog = initialize_catalog(account_id)
        if not catalog:
            logger.error("Catalog initialization failed. Exiting.")
            return

        # Load table
        table = load_table(catalog, DATABASE, TABLE_NAME)
        if not table:
            logger.error("Table loading failed. Exiting.")
            return

        # Read initial table data
        read_table_data(table)

    except Exception as e:
        logger.error(f"Unexpected error in main function: {e}", exc_info=True)


if __name__ == "__main__":
    main()
