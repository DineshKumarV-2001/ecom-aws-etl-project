import os
import psycopg2
from dotenv import load_dotenv


load_dotenv()

try:
    conn = psycopg2.connect(
        host = "localhost",
        port = "5432",
        user = "dinesh",
        password = os.getenv("DB_PASSWORD"),
        database = os.getenv("DB_NAME")
    )
    print("Postgres Connection Successfull!")

    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM customers")
    print("customer data",cursor.fetchall())
            
except Exception as e:
    print("Error occurred:", e)
  
finally:
    if cursor:
        cursor.close()
    if conn:    
        conn.close()    