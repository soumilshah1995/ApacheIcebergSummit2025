import argparse
import time
import boto3
import os
import json
import uuid
from pyspark.sql import SparkSession
from datetime import datetime


def parse_args():
    """
    Parse command line arguments for the script.

    Returns:
        argparse.Namespace: The parsed arguments
    """
    parser = argparse.ArgumentParser(description='Process files from S3 with Spark')
    parser.add_argument('--bucket_name', required=True, help='S3 bucket name')
    parser.add_argument('--data_path', required=True, help='S3 path to raw data (s3://bucket/prefix)')
    parser.add_argument('--max_files', type=int, default=5, help='Maximum number of files to process')
    parser.add_argument('--archived_prefix', default='archived/', help='Prefix for archived files')
    parser.add_argument('--error_prefix', default='error/', help='Prefix for error files')
    parser.add_argument('--pending_prefix', default='pending/', help='Prefix for pending files')

    return parser.parse_args()


import os


def create_spark_session():
    return SparkSession.builder.getOrCreate()


def parse_s3_path(s3_path):
    """
    Parse an S3 path into bucket name and prefix.

    Args:
        s3_path: Full S3 path

    Returns:
        tuple: (bucket_name, prefix)
    """
    path_without_scheme = s3_path.split("://", 1)[-1]
    parts = path_without_scheme.split("/", 1)
    return parts[0], parts[1] if len(parts) > 1 else ""


def create_pending_manifest(s3_client, bucket_name, raw_prefix, pending_prefix, uri_scheme, max_files):
    """
    List files in the raw prefix and create a manifest file in the pending folder.

    Args:
        s3_client: Boto3 S3 client
        bucket_name: S3 bucket name
        raw_prefix: Prefix for raw files
        pending_prefix: Prefix for pending files
        uri_scheme: 's3://' or 's3a://'
        max_files: Maximum number of files to include

    Returns:
        str: Path to the manifest file, or None if no files found
    """
    print(f"Listing files in {uri_scheme}{bucket_name}/{raw_prefix}")

    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=raw_prefix)

    files = []
    for page in pages:
        for obj in page.get('Contents', []):
            files.append(f"{uri_scheme}{bucket_name}/{obj['Key']}")
            if len(files) >= max_files:
                break
        if len(files) >= max_files:
            break

    if not files:
        print("No files found in the raw prefix. Skipping manifest creation.")
        return None

    manifest_content = '\n'.join(files)
    unix_ts = int(time.time())
    manifest_key = f"{pending_prefix}{unix_ts}.pending"

    # Write manifest file to S3
    s3_client.put_object(Bucket=bucket_name, Key=manifest_key, Body=manifest_content)

    manifest_path = f"{uri_scheme}{bucket_name}/{manifest_key}"
    print(f"Manifest file created: {manifest_path}")

    return manifest_path


def process_data_with_spark(spark, manifest_path):
    """
    Read and process files listed in the manifest using Spark.

    Args:
        spark: SparkSession
        manifest_path: Path to the manifest file

    Returns:
        DataFrame: Processed Spark DataFrame
    """
    print(f"Processing data from manifest: {manifest_path}")

    # Read the manifest file as text
    manifest_df = spark.read.text(manifest_path)
    manifest_df.show()

    # Extract file paths from the manifest and read them as Parquet
    file_paths = manifest_df.rdd.map(lambda r: r[0]).collect()
    print(f"file_paths | Found {len(file_paths)} files to process")

    # Process the data
    data_df = spark.read.csv(file_paths, sep='\t', header=True, inferSchema=True)

    # Display information about the data
    row_count = data_df.count()
    print(f"Number of rows: {row_count}")
    print("Schema:")
    data_df.printSchema()

    # Perform additional processing as needed
    # For example: data_df = data_df.filter(...).select(...)

    return data_df


def archive_processed_files(s3_client, bucket_name, manifest_path, archived_prefix):
    """
    Archive processed files by moving them from raw to archived prefix.

    Args:
        s3_client: Boto3 S3 client
        bucket_name: S3 bucket name
        manifest_path: Path to the manifest file
        archived_prefix: Prefix for archived files
    """
    print(f"Archiving processed files from manifest: {manifest_path}")

    # Get the manifest key
    _, manifest_key = parse_s3_path(manifest_path)

    # Read the manifest content
    response = s3_client.get_object(Bucket=bucket_name, Key=manifest_key)
    content = response['Body'].read().decode('utf-8')
    file_paths = [path.strip() for path in content.split('\n') if path.strip()]

    # Archive each file
    for file_path in file_paths:
        old_key = parse_s3_path(file_path)[1]
        new_key = f"{archived_prefix}{os.path.basename(old_key)}"

        # Copy to archive and delete from raw
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': old_key},
            Key=new_key
        )
        s3_client.delete_object(Bucket=bucket_name, Key=old_key)
        print(f"Archived: {old_key} -> {new_key}")

    # Delete the manifest file
    s3_client.delete_object(Bucket=bucket_name, Key=manifest_key)
    print(f"Deleted manifest: {manifest_key}")


def create_iceberg_table(spark, catalog, namespace, table):
    """
    Create Iceberg table if not exists.

    Args:
        spark: SparkSession
        target_table: Target Iceberg table path
    """
    try:
        print(f"Creating Iceberg table if not exists: {table}")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{catalog}`.`{namespace}`")

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS `{catalog}`.`{namespace}`.`{table}`  (
            invoiceid INT,
            itemid INT,
            category STRING,
            price FLOAT,
            quantity INT,
            orderdate STRING,
            destinationstate STRING,
            shippingtype STRING,
            referral STRING
        )
        USING iceberg
        PARTITIONED BY (destinationstate)
        """)
        print("Iceberg table created ")
    except Exception as e:
        print("Table Already Exists")



def merge_data_into_iceberg_table(spark, catalog, namespace, table, df):
    df.createOrReplaceTempView("source_table")

    spark.sql(f"""
      MERGE INTO `{catalog}`.`{namespace}`.`{table}` AS target
        USING (
            SELECT
                invoiceid,
                itemid,
                category,
                price,
                quantity,
                orderdate,
                destinationstate,
                shippingtype,
                referral
            FROM (
                     SELECT *,
                            ROW_NUMBER() OVER (
                       PARTITION BY invoiceid, itemid
                       ORDER BY replicadmstimestamp DESC
                   ) AS row_num
                     FROM source_table
                 ) AS deduped_source
            WHERE row_num = 1
        ) AS source
        ON target.invoiceid = source.invoiceid AND target.itemid = source.itemid
        WHEN MATCHED THEN
            UPDATE SET
                target.category = source.category,
                target.price = source.price,
                target.quantity = source.quantity,
                target.orderdate = source.orderdate,
                target.destinationstate = source.destinationstate,
                target.shippingtype = source.shippingtype,
                target.referral = source.referral
        WHEN NOT MATCHED THEN
            INSERT (
                    invoiceid, itemid, category, price, quantity, orderdate, destinationstate, shippingtype, referral
                )
                VALUES (
                           source.invoiceid, source.itemid,
                           source.category, source.price,
                           source.quantity, source.orderdate,
                           source.destinationstate, source.shippingtype,
                           source.referral
                       );
        """)
    df.drop("source_table")
    print("----------- MERGE COMPLETE -----------")


def move_to_error(s3_client, bucket_name, manifest_path, error_prefix):
    """
    Move manifest file to error folder.

    Args:
        s3_client: Boto3 S3 client
        bucket_name: S3 bucket name
        manifest_path: Path to the manifest file
        error_prefix: Prefix for error files

    Returns:
        str: Path to the error manifest file
    """
    print(f"Moving manifest to error folder: {manifest_path}")

    # Get the manifest key
    _, manifest_key = parse_s3_path(manifest_path)
    filename = os.path.basename(manifest_key)
    error_key = f"{error_prefix}{filename}"

    # Copy to error folder
    s3_client.copy_object(
        Bucket=bucket_name,
        CopySource={'Bucket': bucket_name, 'Key': manifest_key},
        Key=error_key
    )

    # Delete from pending location
    s3_client.delete_object(Bucket=bucket_name, Key=manifest_key)

    error_path = f"s3://{bucket_name}/{error_key}"
    print(f"Moved failed manifest to: {error_path}")
    return error_path


def main():
    # Parse arguments
    args = parse_args()

    # Initialize AWS client
    s3_client = boto3.client('s3')

    # Initialize variables
    bucket_name = args.bucket_name
    bucket_name, raw_prefix = parse_s3_path(args.data_path)
    uri_scheme = "s3a://"

    # Initialize Spark session
    spark = create_spark_session()

    try:
        # Step 1: Create pending manifest
        manifest_path = create_pending_manifest(
            s3_client, bucket_name, raw_prefix,
            args.pending_prefix, uri_scheme, args.max_files
        )

        if manifest_path is None:
            print("No files to process. Exiting.")
            return

        # Step 2: Process data with Spark
        processed_data_df = process_data_with_spark(spark, manifest_path)
        processed_data_df.show()

        """Create iceberg Tables"""

        create_iceberg_table(spark=spark,
                             catalog="ManagedIcebergCatalog",
                             namespace="s3tables",
                             table='invoices')

        print("Create table complete ")

        """MERGE INTO"""
        merge_data_into_iceberg_table(
            spark=spark,
            catalog="ManagedIcebergCatalog",
            namespace="s3tables",
            table='invoices',
            df=processed_data_df
        )
        print("MERGE COMPLETE")

        # Step 3: Archive processed files
        archive_processed_files(s3_client, bucket_name, manifest_path, args.archived_prefix)

        print(f"Processing completed successfully at {datetime.now().isoformat()}")

    except Exception as e:
        print(f"Error during processing: {str(e)}")

        # Move manifest to error folder if it exists
        if manifest_path:
            move_to_error(s3_client, bucket_name, manifest_path, args.error_prefix)

        # Re-raise the exception to mark the job as failed
        raise

    finally:
        # Always stop Spark session
        if spark:
            spark.stop()
            print("Spark session stopped")


if __name__ == "__main__":
    main()
