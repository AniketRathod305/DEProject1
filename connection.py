from dotenv import load_dotenv
import os
import mysql.connector

load_dotenv()


def mysql_connect():

    try:
        conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DB")
        )
        print('Connected successfully to MySQL DB', conn)
        return conn
    except Exception as e:
        print('Error while connecting to SQL DB', e)
        return -1


# mysql_connect()
