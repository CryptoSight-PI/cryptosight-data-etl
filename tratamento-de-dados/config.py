import os
from pathlib import Path, PurePosixPath

import boto3
from dotenv import load_dotenv
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv()) 

# PASTA_CSV = ROOT_DIR / "csv"
# PASTA_RAW = ROOT_DIR / "raw"
# PASTA_SILVER = ROOT_DIR / "silver"
# PASTA_GOLD = ROOT_DIR / "gold"

PREFIXO_RAW_METRICS = "metrics"
PREFIXO_RAW_PROCESSES = "processes"


def particao(dia):
    """date -> '2026/10/21'"""
    return f"{dia:%Y/%m/%d}"

BUCKET_ENVIRONMENTS = {
    "raw": ("AWS_BUCKET_RAW", "AWS_BUCKET_BRONZE"),
    "trusted": ("AWS_BUCKET_TRUSTED", "AWS_BUCKET_SILVER"),
    "gold": ("AWS_BUCKET_GOLD",),
}


def _bucket_da_camada(camada):
    nomes_env = BUCKET_ENVIRONMENTS.get(camada)
    if nomes_env is None:
        raise ValueError(f"Camada S3 inválida: {camada}")

    bucket = next((os.getenv(nome) for nome in nomes_env if os.getenv(nome)), None)
    if not bucket:
        raise ValueError(
            f"Configure uma destas variáveis para a camada {camada}: "
            f"{', '.join(nomes_env)}"
        )
    return bucket


def _cliente_s3():
    return boto3.client(
        "s3",
        region_name=os.getenv("AWS_DEFAULT_REGION") or None,
    )


def enviar_para_s3(caminho_local, chave_s3, camada="raw"):
    caminho_local = Path(caminho_local)
    if not caminho_local.is_file():
        raise FileNotFoundError(f"Arquivo local não encontrado: {caminho_local}")

    bucket = _bucket_da_camada(camada)
    _cliente_s3().upload_file(str(caminho_local), bucket, chave_s3)
    print(f"Enviado para s3://{bucket}/{chave_s3}")


def baixar_arquivos_s3(camada, prefixo, pasta_destino):
    bucket = _bucket_da_camada(camada)
    pasta_destino = Path(pasta_destino)
    cliente = _cliente_s3()
    arquivos = []

    paginas = cliente.get_paginator("list_objects_v2").paginate(
        Bucket=bucket,
        Prefix=prefixo,
    )
    for pagina in paginas:
        for objeto in pagina.get("Contents", []):
            chave = objeto["Key"]
            if not chave.lower().endswith(".csv"):
                continue

            caminho_relativo = PurePosixPath(chave)
            if caminho_relativo.is_absolute() or ".." in caminho_relativo.parts:
                raise ValueError(f"Chave S3 inválida: {chave}")

            caminho_local = pasta_destino.joinpath(*caminho_relativo.parts)
            caminho_local.parent.mkdir(parents=True, exist_ok=True)
            cliente.download_file(bucket, chave, str(caminho_local))
            arquivos.append(caminho_local)

    return arquivos