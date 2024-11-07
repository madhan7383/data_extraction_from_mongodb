import pandas as pd
import pymysql
import psycopg2
import pymongo

def connect_mysql():
    """Connect to MySQL Database."""
    return pymysql.connect(
        host='localhost',
        user='your_mysql_user',
        password='your_mysql_password',
        database='your_mysql_db'
    )

def connect_postgresql():
    """Connect to PostgreSQL Database."""
    return psycopg2.connect(
        host='localhost',
        user='your_pg_user',
        password='your_pg_password',
        database='your_pg_db'
    )

def connect_mongodb():
    """Connect to MongoDB Database."""
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["your_mongo_db"]
    return db

def extract_from_mysql(connection):
    """Extract complex data using joins from MySQL."""
    query = """
    SELECT 
        c.id AS customer_id, 
        c.name AS customer_name, 
        o.order_id, 
        o.amount 
    FROM 
        customers c
    INNER JOIN 
        orders o 
    ON 
        c.id = o.customer_id 
    WHERE 
        o.amount > 100
    ORDER BY 
        o.amount DESC;
    """
    df = pd.read_sql(query, connection)
    return df

def extract_from_postgresql(connection):
    """Extract complex data using joins from PostgreSQL."""
    query = """
    SELECT 
        c.name AS customer_name, 
        COALESCE(SUM(o.amount), 0) AS total_amount
    FROM 
        customers c
    LEFT JOIN 
        orders o 
    ON 
        c.id = o.customer_id
    GROUP BY 
        c.name
    HAVING 
        SUM(o.amount) > 200
    ORDER BY 
        total_amount DESC;
    """
    df = pd.read_sql(query, connection)
    return df

def extract_from_mongodb(db):
    """Extract complex data using MongoDB aggregation."""
    pipeline = [
        {
            "$lookup": {
                "from": "orders",
                "localField": "_id",  # assuming _id is customer_id
                "foreignField": "customer_id",
                "as": "order_data"
            }
        },
        {
            "$unwind": "$order_data"
        },
        {
            "$match": {
                "order_data.amount": {"$gt": 100}
            }
        },
        {
            "$project": {
                "_id": 0,
                "customer_name": "$name",
                "order_id": "$order_data._id",
                "amount": "$order_data.amount"
            }
        },
        {
            "$sort": {"amount": -1}
        }
    ]
    result = db.customers.aggregate(pipeline)
    df = pd.DataFrame(list(result))
    return df

def save_to_csv(df, filename):
    """Save DataFrame to CSV."""
    df.to_csv(filename, index=False)

def main():
    try:
        # Connect to databases
        mysql_conn = connect_mysql()
        pg_conn = connect_postgresql()
        mongo_db = connect_mongodb()

        # Extract data from each database
        mysql_data = extract_from_mysql(mysql_conn)
        print("MySQL Data:")
        print(mysql_data)

        pg_data = extract_from_postgresql(pg_conn)
        print("\nPostgreSQL Data:")
        print(pg_data)

        mongo_data = extract_from_mongodb(mongo_db)
        print("\nMongoDB Data:")
        print(mongo_data)

        # Save results to CSV
        save_to_csv(mysql_data, 'mysql_data.csv')
        save_to_csv(pg_data, 'postgresql_data.csv')
        save_to_csv(mongo_data, 'mongodb_data.csv')

        print("\nData extraction complete. Results saved to CSV files.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close connections
        if 'mysql_conn' in locals():
            mysql_conn.close()
        if 'pg_conn' in locals():
            pg_conn.close()

if __name__ == "__main__":
    main()
