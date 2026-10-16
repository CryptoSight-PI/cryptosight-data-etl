import boto3
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import os
from datetime import datetime

bucket_bronze = os.getenv('AWS_BUCKET_BRONZE')
bucket_silver = os.getenv('AWS_BUCKET_SILVER')
bucket_gold = os.getenv('AWS_BUCKET_GOLD')


session = boto3.Session()

s3 = session.client("s3")

def mandars3(data , arquivo):
    data = datetime.strptime(data, '%Y-%m-%d_%H-%M-%S')
    ano = data.strftime("%Y")
    mes = data.strftime("%m")
    dia = data.strftime("%d")

    caminho = f"{ano}/{mes}/{dia}/{arquivo}"
    
    s3.upload_file(arquivo , bucket_bronze , caminho)