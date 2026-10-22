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

from etl.config import (
    PASTA_CSV,
    PASTA_RAW,
    PREFIXO_RAW_METRICS,
    PREFIXO_RAW_PROCESSES,
    baixar_arquivos_s3,
    enviar_para_s3,
    particao,
)
from etl.raw_to_trusted import main as raw_to_trusted
from etl.trusted_to_curated import main as trusted_to_curated


def enviar_csvs_locais(dia):

    PASTA_CSV.mkdir(parents=True, exist_ok=True)
    arquivos = sorted(
        a for a in PASTA_CSV.glob("*.csv") if a.name != "all-collected.csv"
    )

    if not arquivos:
        print("Nenhum CSV local para enviar; será usada a camada raw existente no S3.")
        return

    for arquivo in arquivos:
        base = (
            PREFIXO_RAW_PROCESSES
            if "_processos_" in arquivo.name
            else PREFIXO_RAW_METRICS
        )
        enviar_para_s3(arquivo, f"{base}/{particao(dia)}/{arquivo.name}", camada="raw")



def _agrupar_por_empresa(arquivos):
    arquivos_por_empresa = {}
    arquivos_sem_id = []
    for arquivo in arquivos:
        resultado = re.match(r"^(\d+)[-_]", arquivo.name)
        if resultado:
            id_empresa = int(resultado.group(1))
            arquivos_por_empresa.setdefault(id_empresa, []).append(arquivo)
        else:
            arquivos_sem_id.append(arquivo.name)

    if arquivos_sem_id:
        raise ValueError(
            "Não foi possível identificar o ID da empresa nestes CSVs raw: "
            + ", ".join(arquivos_sem_id)
        )
    if not arquivos_por_empresa:
        raise ValueError("Nenhum ID de empresa foi encontrado nos CSVs raw.")
    return arquivos_por_empresa


def main():
    enviar_csvs_locais(date.today())

    arquivos_raw = baixar_arquivos_s3(
        "raw",
        "metrics/",
        PASTA_RAW,
    )
    if not arquivos_raw:
        raise RuntimeError(
            "Nenhum CSV de métricas foi encontrado em raw/metrics/ no bucket Raw."
        )

    PASTA_CSV.mkdir(parents=True, exist_ok=True)
    arquivos_por_empresa = _agrupar_por_empresa(arquivos_raw)
    for id_empresa, arquivos_empresa in sorted(arquivos_por_empresa.items()):

        dataframes = [pd.read_csv(arquivo, sep=";") for arquivo in arquivos_empresa]
        caminho_consolidado = PASTA_CSV / "all-collected.csv"
        pd.concat(dataframes, ignore_index=True).to_csv(
            caminho_consolidado,
            sep=";",
            index=False,
        )
        print(
            f"Empresa {id_empresa}: {len(arquivos_empresa)} CSVs raw "
            f"consolidados em {caminho_consolidado}."
        )

        raw_to_trusted(id_empresa)       # = treatment-clean-data.py + silver.py (JSON)
        trusted_to_curated(id_empresa)   # = gold_file.py

    print("Pipeline concluído: Raw -> Trusted -> Gold para todas as empresas.")


if __name__ == "__main__":
    main()