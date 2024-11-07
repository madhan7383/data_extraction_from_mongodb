import schedule
import time

def job():
    mysql_conn = connect_mysql()
    pg_conn = connect_postgresql()
    mongo_db = connect_mongodb()

    # Extract data
    mysql_data = extract_from_mysql(mysql_conn)
    pg_data = extract_from_postgresql(pg_conn)
    mongo_data = extract_from_mongodb(mongo_db)

    # Transform and save
    save_to_csv(transform_data(mysql_data), 'mysql_data.csv')
    save_to_csv(transform_data(pg_data), 'pg_data.csv')
    save_to_csv(transform_data(mongo_data), 'mongo_data.csv')

# Schedule the job every day at 9 AM
schedule.every().day.at("09:00").do(job)

# Keep the script running
while True:
    schedule.run_pending()
    time.sleep(1)
