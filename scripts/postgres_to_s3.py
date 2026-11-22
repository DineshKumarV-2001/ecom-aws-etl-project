
import os
import csv
import json
import logging
import boto3
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from psycopg2 import sql


# 1. CONFIGURATION

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

S3_BUCKET = os.getenv("BUCKET_NAME")
s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION"))

TABLES = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id"
}

BATCH_SIZE = 50
CHECKPOINT_FILE = "checkpoints/watermarks.json"

# 2. LOGGING

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# 3. WATERMARK FUNCTIONS

def load_watermarks():

    if not os.path.exists(CHECKPOINT_FILE):
        return {}

    with open(CHECKPOINT_FILE, "r") as file:
        return json.load(file)


def save_watermarks(watermarks):

    os.makedirs("checkpoints", exist_ok=True)

    with open(CHECKPOINT_FILE, "w") as file:
        json.dump(watermarks, file, indent=4)


# 4. EXTRACT AND UPLOAD

def extract_table(conn, table, primary_key, watermark):

    run_started_at = datetime.now(timezone.utc)

    query = sql.SQL(
        "SELECT * FROM {} WHERE updated_at <= %s"
    ).format(sql.Identifier(table))

    params = [run_started_at]

    if watermark:
        previous_time = datetime.fromisoformat(watermark)
        lower_bound = previous_time - timedelta(minutes=5)

        query += sql.SQL(" AND updated_at >= %s")
        params.append(lower_bound)

    query += sql.SQL(
        " ORDER BY updated_at, {}"
    ).format(sql.Identifier(primary_key))

    cursor = conn.cursor()

    try:
        cursor.execute(query, params)

        header = [desc[0] for desc in cursor.description]
        updated_index = header.index("updated_at")

        batch_number = 0
        total_records = 0
        latest_timestamp = None

        while True:

            records = cursor.fetchmany(BATCH_SIZE)

            if not records:
                break

            batch_number += 1
            total_records += len(records)

            batch_max = max(
                row[updated_index] for row in records
            )

            if latest_timestamp is None or batch_max > latest_timestamp:
                latest_timestamp = batch_max

            file_path = f"data/temporary/{table}_{batch_number}.csv"

            os.makedirs("data/temporary", exist_ok=True)

            with open(file_path, "w", newline="", encoding="utf-8") as file:

                writer = csv.writer(file)
                writer.writerow(header)
                writer.writerows(records)

            s3_key = (
                f"bronze/{table}/"
                f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}/"
                f"{table}_{batch_number}.csv"
            )

            s3.upload_file(file_path, S3_BUCKET, s3_key)

            os.remove(file_path)

            logger.info(
                "%s | Batch %s | Rows %s uploaded",
                table, batch_number, len(records)
            )

        # Update watermark only after successful uploads
        new_watermark = latest_timestamp or run_started_at

        return new_watermark, total_records

    finally:
        cursor.close()


# 5. MAIN PIPELINE

def main():

    conn = None

    try:
        logger.info("Starting PostgreSQL to S3 ingestion")

        conn = psycopg2.connect(**DB_CONFIG)

        watermarks = load_watermarks()

        for table, primary_key in TABLES.items():

            logger.info("Processing table: %s", table)

            watermark = watermarks.get(table)

            new_watermark, total = extract_table(
                conn, table, primary_key, watermark
            )

            # Save checkpoint after this table succeeds
            watermarks[table] = new_watermark.isoformat()
            save_watermarks(watermarks)

            conn.commit()

            logger.info(
                "Completed %s | Total rows: %s",
                table, total
            )

        logger.info("All tables processed successfully")

    except Exception:
        if conn:
            conn.rollback()

        logger.exception("Pipeline failed")
        raise

    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    main()
