
import requests
import pandas as pd

# 1. Configuration
API_URL = "https://dummyjson.com/products?limit=50"

print("Extracting Data from API")

try:
    # 2. API request
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()

    # 3. Extract JSON
    data = response.json()

    # 4. Convert to DataFrame
    df = pd.DataFrame(data['products'])

    print("\nAvailable Columns:")
    print(df.columns.tolist())

    # 5. Select required columns
    specific_columns = [
        'id', 'title', 'price',
        'rating', 'brand', 'category'
    ]

    final_df = df[specific_columns]

    print("\nSelected Data:")
    print(final_df.head())

    # 6. Export to CSV
    final_df.to_csv('/home/dinesh/data-engineering-project/project/data/api_products.csv', index=False)

    print("\nCSV created successfully!")
    print("Total records:", len(final_df))

except requests.exceptions.RequestException as e:
    print("API extraction failed:", e)

except (KeyError, ValueError) as e:
    print("Data processing failed:", e)
