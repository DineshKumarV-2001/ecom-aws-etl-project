import psycopg2

try:
    conn = psycopg2.connect(
        host = "localhost",
        port = "5432",
        user = "dinesh",
        password = "Dinesh2109",
        database = "ecom_db"
    )
    print("Postgres Connection Successfull!")

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM customers")
    print("customer data:", cursor.fetchall())
    
    cursor.close
    conn.close
        
except Exception as e:
    print("Error occurred:", e)
    