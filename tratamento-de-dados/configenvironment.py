import boto3
import os
# from dotenv import load_dotenv, find_dotenv
import mysql.connector

try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv())
except ImportError:
    pass

bucket_bronze = os.getenv("AWS_BUCKET_BRONZE")
bucket_silver = os.getenv("AWS_BUCKET_SILVER")
bucket_gold = os.getenv("AWS_BUCKET_GOLD")

session = boto3.Session()
s3 = session.client("s3")


cnx = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "root"),
    "database": os.getenv("DB_NAME", "cryptosight"),
    "autocommit" : True,
}

conexao = mysql.connector.connect(**cnx)
cursor = conexao.cursor()