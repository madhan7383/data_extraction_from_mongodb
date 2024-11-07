import pymysql  # MySQL
import psycopg2  # PostgreSQL
import pymongo  # MongoDB

# MySQL connection
def connect_mysql():
    connection = pymysql.connect(
        host='localhost',
        user='username',
        password='password',
        database='database_name'
    )
    return connection

# PostgreSQL connection
def connect_postgresql():
    connection = psycopg2.connect(
        host='localhost',
        user='username',
        password='password',
        database='database_name'
    )
    return connection

# MongoDB connection
def connect_mongodb():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["database_name"]
    return db
