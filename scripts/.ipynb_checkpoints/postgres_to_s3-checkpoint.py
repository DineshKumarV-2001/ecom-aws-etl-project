import os
import csv
import io
import logging

import boto3
import psycopg2

from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from psycopg2 import sql

load_dotenv()

# Postgres Configuration
db_config = {
        "host" :"localhost",
        "port" : os.getenv("PORT"),
        "user" : os.getenv("USER1"),
        "password" : os.getenv("DB_PASSWORD"),
        "dbname" : os.getenv("DB_NAME")
}

# AWS Configuration
S3_BUCKET   = os.getenv("BUCKET_NAME")
REGION      = os.getenv("AWS_REGION")

# Basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# Table configuration
TABLES = ["customers","order_items","orders","products","payments"]
BATCH_SIZE = 10000 

# Extraction

try:
    logger.info("Starting PostgreSQL extraction")
    conn = psycopg2.connect(**db_config)
    
    logger.info("Connected to PostgreSQL database")
    table_cursor = None
    
    for table in TABLES:
        logger.info(f"Extracting Data from table: {table}")
        table_cursor = conn.cursor(name=f"{table}_cursor")
        table_cursor.execute(f"SELECT * FROM {table}")

        total_records = 0
        batch_number = 0

        while True:
            records = table_cursor.fetchmany(BATCH_SIZE)

            if not records:
                break
            
            batch_number = +1
            total_records += len(records)
            
            logger.info(
                "Table: %s | Batch: %s | Records: %s",
                table,batch_number,len(records)
            )
            
            cursor.close()
            cursor = None
        
        logger.info("All Table Extraction Completed Successfully!")

        
except Exception as e:
    logger.exception("EXtraction Failed!")
   
finally:
    if cursor:
        cursor.close()
        
    if conn:
        conn.close()
        logger.info("Database connection closed.")