
import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import os
import subprocess

from raw_to_trusted import main as raw_to_trusted
from trusted_to_curated import main as trusted_to_curated
from configenvironment import s3 , bucket_silver , bucket_gold , bucket_bronze , cursor
from raw_to_trusted import particao
from raw_to_trusted_rede import main as rede_raw_to_trusted
from CURATED import rede as rede_curated

DEFAULT_EVENTS = 5



def agrupar_por_empresa(arquivos):
    arquivos_por_empresa = {}
    arquivos_sem_id = []

    for chave in arquivos:
        nome = chave.rsplit("/", 1)[-1]
        resultado = re.match(r"^(\d+)[-_]", nome)

        if resultado:
            id_empresa = int(resultado.group(1))
            arquivos_por_empresa.setdefault(id_empresa, []).append(chave)
        else:
            arquivos_sem_id.append(nome)

    if arquivos_sem_id:
        raise ValueError(
            "Não foi possível identificar o ID da empresa nestes CSVs: "
            + ", ".join(arquivos_sem_id)
        )

    if not arquivos_por_empresa:
        raise ValueError("Nenhum ID de empresa foi encontrado nos CSVs.")

    return arquivos_por_empresa



PREFIXO_RAW_METRICS = "metrics"

# se for do dia coloca prefixo dia
def listar_arquivos_raw():
    # prefixo = f"{PREFIXO_RAW_METRICS}/{particao(dia)}/"
    prefixo = "metrics/"

    paginator = s3.get_paginator("list_objects_v2")
    arquivos = []

    for pagina in paginator.paginate(
        Bucket=bucket_bronze,
        Prefix=prefixo,
    ):
        for objeto in pagina.get("Contents", []):
            chave = objeto["Key"]
            nome = chave.rsplit("/", 1)[-1]

            if not nome.lower().endswith(".csv"):
                continue

            if "_processos_" in nome.lower():
                continue

            arquivos.append(chave)

    return arquivos


def arquivar_arquivo(chave):

    s3.copy_object(
        Bucket=bucket_bronze,
        CopySource={
            "Bucket": bucket_bronze,
            "Key": chave,
        },
        Key=f"processados/{chave}",
    )

    s3.delete_object(
        Bucket=bucket_bronze,
        Key=chave,
    )




# agrupamento por data ou seja nao é do dia
def main_metricas():
    arquivos = listar_arquivos_raw()

    if not arquivos:
        print("Nenhum arquivo pendente encontrado.")
        return

    
    arquivos_por_data = {}

    for chave in arquivos:
        partes = chave.split("/")

        # metrics/AAAA/MM/DD/arquivo.csv
        if len(partes) < 5:
            print(f"Caminho ignorado por formato inválido: {chave}")
            continue

        try:
            dia = date(int(partes[1]), int(partes[2]), int(partes[3])
            )
        except ValueError:
            print(f"Data inválida no caminho: {chave}")
            continue

        arquivos_por_data.setdefault(dia, []).append(chave)

    for dia, arquivos_dia in sorted(arquivos_por_data.items()):
        try:
            arquivos_por_empresa = agrupar_por_empresa(arquivos_dia)
        except ValueError as erro:
            print(f"Dia {dia} ignorado: {erro}")
            continue    

        for id_empresa, arquivos_empresa in sorted(
            arquivos_por_empresa.items()
        ):
            try:
                raw_to_trusted(id_empresa, arquivos_empresa, dia)

                for chave in arquivos_empresa:
                    arquivar_arquivo(chave)
                

                trusted_to_curated(id_empresa, dia)

                
            except Exception as erro:
                print(
                    f"Erro na empresa {id_empresa}, "
                    f"data {dia}: {erro}"
                )
                continue

    print("Pipeline finalizado.")

def main_rede():
    rede_raw_to_trusted()
    rede_curated.main()
    

def main():
    try:
        main_metricas()
    except Exception as erro:
        print(f"Erro nas métricas: {erro}")

    main_rede()

if __name__ == "__main__":
    main()