import json
import os
import boto3
import requests
import pandas as pd

# 1. Configuration
API_URL = "https://dummyjson.com/products?limit=50"


print("Extracting Data from API")
response = requests.get(API_URL)

if response.status_code == 200
    data = response.json()

print("Printing DataFrame")
df = pd.DataFrame(data)
print(df)


print("writing to Local")
df.to_csv("data.csv",index=False)  