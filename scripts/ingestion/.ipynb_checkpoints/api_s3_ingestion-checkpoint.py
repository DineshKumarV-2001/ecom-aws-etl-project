import json
import pandas as pd
import boto3
import requests

# 1. Configuration
API_URL = "https://dummyjson.com/products?limit=50"
BUCKET_NAME = "my-ecom-aws-etl-project"
S3_KEY = "bronze/api-data/raw_products.json"

# 2. Extraction From API
response = requests.get(API_URL, timeo5ut=1)
print(response.json(products))
