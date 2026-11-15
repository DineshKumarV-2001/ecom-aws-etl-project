import json
import boto3
import requests
import pandas as pd
from datetime import datetime

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# 1.Configuration
API_URL = "https://dummyjson.com/products?limit=50"
Bucket_Name = "my-ecom-aws-etl-project"
S3_Key = f"bronze/api-data/products_{timestamp}.csv"
csv_path = '/home/dinesh/data-engineering-project/project/data/api_products.csv'

print("Extraction Process has Started...")
# 2. Extraction
try:
    response = requests.get(API_URL)
    response.raise_for_status()
    
    data = response.json()
    df = pd.DataFrame(data["products"])
    df.to_csv(csv_path,index=False)
    print(f"Data is Extracted Successfully")    
    print(f"Saving it on Local: {csv_path}")
    
# 3. Load to S3
    s3_client = boto3.client("s3")
    s3_client.upload_file(csv_path,Bucket_Name,S3_Key)
    print(f"Successfully Uploaded to S3//{Bucket_Name}//{S3_Key}")

except Exception as e:
    print(f"ETL Failed:{e}")        
    

