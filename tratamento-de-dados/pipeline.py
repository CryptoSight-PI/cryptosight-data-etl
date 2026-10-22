"""Orquestra: Csv local -> Raw -> Trusted -> Curated.

É o pipeline.py ORIGINAL com o csv_bind.py fundido aqui dentro.
  [REUSADO]  copiado do original | [ALTERADO]  mudou (motivo no comentário)
  [NOVO]     não existia

ARQUIVOS DO PROJETO (depois da fusão):
  pipeline.py                 <- este (inclui o antigo csv_bind.py)
  etl/config.py               <- pastas, .env e funções do S3 (antigo s3_utils.py)
  etl/raw_to_trusted.py       <- antigo treatment-clean-data.py + silver.py (JSON)
  etl/trusted_to_curated.py   <- antigo gold_file.py

[COMO FUNCIONA]
  1. enviar_csvs_locais(): sobe os CSVs de Csv/ para o bucket raw, particionados por dia
  2. baixa os CSVs de métricas do raw e agrupa por empresa (ID no início do nome)
  3. para cada empresa: consolida em Csv/all-collected.csv, depois
       raw_to_trusted     (limpeza + JSON -> trusted)
       trusted_to_curated (nome no MySQL + pasta da empresa -> gold)
  A pasta Silver é compartilhada, por isso cada empresa faz o caminho completo
  antes da próxima começar.
"""
import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

# from config import (
#     PASTA_CSV,
#     PASTA_RAW,
#     PREFIXO_RAW_METRICS,
#     PREFIXO_RAW_PROCESSES,
#     baixar_arquivos_s3,
#     enviar_para_s3,
#     particao,
# )
from raw_to_trusted import main as raw_to_trusted
from trusted_to_curated import main as trusted_to_curated
from configenvironment import s3 , bucket_silver , bucket_gold , bucket_bronze , cursor
from raw_to_trusted import particao


# def enviar_csvs_locais(dia):

#     PASTA_CSV.mkdir(parents=True, exist_ok=True)
#     arquivos = sorted(
#         a for a in PASTA_CSV.glob("*.csv") if a.name != "all-collected.csv"
#     )

#     if not arquivos:
#         print("Nenhum CSV local para enviar; será usada a camada raw existente no S3.")
#         return

#     for arquivo in arquivos:
#         base = (
#             PREFIXO_RAW_PROCESSES
#             if "_processos_" in arquivo.name
#             else PREFIXO_RAW_METRICS
#         )
#         enviar_para_s3(arquivo, f"{base}/{particao(dia)}/{arquivo.name}", camada="raw")



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



# def main():
#     dia = date.today()
#     data_particao = particao(dia)

#     print(
#         f"Iniciando pipeline Raw -> Trusted -> Curated "
#         f"para {data_particao}."
#     )

#     arquivos_raw = listar_arquivos_raw(dia)

#     if not arquivos_raw:
#         print(
#             f"Nenhum CSV de métricas encontrado em "
#             f"{PREFIXO_RAW_METRICS}/{data_particao}/ "
#             f"no bucket Bronze."
#         )
#         return

#     arquivos_por_empresa = agrupar_por_empresa(arquivos_raw)

#     if not arquivos_por_empresa:
#         print(
#             "Nenhuma empresa válida encontrada nos nomes dos CSVs."
#         )
#         return

#     for id_empresa, arquivos_empresa in sorted(
#         arquivos_por_empresa.items()
#     ):
#         print(
#             f"\nEmpresa {id_empresa}: "
#             f"{len(arquivos_empresa)} CSV(s) encontrados."
#         )

#         try:
#             raw_to_trusted(
#                 id_empresa=id_empresa,
#                 arquivos_empresa=arquivos_empresa,
#                 dia=dia,
#             )

#             trusted_to_curated(
#                 id_empresa=id_empresa,
#                 dia=dia,
#             )

#             for chave in arquivos_empresa:
#                 arquivar_arquivo(chave)

#             print(
#                 f"Empresa {id_empresa} concluída para "
#                 f"{data_particao}."
#             )

#         except Exception as erro:
#             print(
#                 f"Erro ao processar empresa {id_empresa}: {erro}"
#             )

#     print("\nPipeline finalizado.")



# agrupamento por data ou seja nao é do dia
def main():
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
        arquivos_por_empresa = agrupar_por_empresa(arquivos_dia)

        for id_empresa, arquivos_empresa in sorted(
            arquivos_por_empresa.items()
        ):
            try:
                raw_to_trusted(id_empresa, arquivos_empresa, dia)
                trusted_to_curated(id_empresa, dia)

                for chave in arquivos_empresa:
                    arquivar_arquivo(chave)

            except Exception as erro:
                print(
                    f"Erro na empresa {id_empresa}, "
                    f"data {dia}: {erro}"
                )
                continue

    print("Pipeline finalizado.")

if __name__ == "__main__":
    main()