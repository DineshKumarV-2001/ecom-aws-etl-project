import json
import boto3
import requests
import pandas as pd

# 1.Configuration
API_URL = "https://dummyjson.com/products?limit=50"
Bucket_Name = "my-ecom-aws-etl-project"
S3_Key = "bronze/api-data/"

# 2. Extraction
response = requests.get(API_URL)

try:
    if response.status_code == 200:
        data = response.json()
        
        df = pd.DataFrame(data)
        print(df)

except Exception as e:
    print(e)        

