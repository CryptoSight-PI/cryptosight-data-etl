import boto3
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(ROOT_DIR / ".env")

bucket_bronze = os.getenv("AWS_BUCKET_RAW") or os.getenv("AWS_BUCKET_BRONZE")
bucket_silver = os.getenv('AWS_BUCKET_SILVER')
bucket_gold = os.getenv('AWS_BUCKET_GOLD')

s3 = boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION") or None)

def mandars3(data , arquivo):
    data = datetime.strptime(data, '%Y-%m-%d_%H-%M-%S')
    ano = data.strftime("%Y")
    mes = data.strftime("%m")
    dia = data.strftime("%d")

    if not bucket_bronze:
        raise ValueError("Configure AWS_BUCKET_RAW ou AWS_BUCKET_BRONZE no .env.")

    nome_arquivo = os.path.basename(arquivo)
    camada = "processes" if "_processos_" in nome_arquivo else "metrics"
    caminho = f"{camada}/{ano}/{mes}/{dia}/{nome_arquivo}"
    s3.upload_file(arquivo, bucket_bronze, caminho)
    print(f"Enviado para s3://{bucket_bronze}/{caminho}")
    return caminho