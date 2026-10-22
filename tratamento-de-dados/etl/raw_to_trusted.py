"""Raw -> Trusted.

Este arquivo junta três pedaços que antes estavam espalhados:
  - pipeline.py (versão antiga)  -> baixar do raw, agrupar por empresa, consolidar CSVs
  - etl/silver.py                -> limpeza: separar linhas válidas das inválidas
  - silver.py (versão antiga)    -> csv_to_json (gera o treated-data.json)

LEGENDA DOS COMENTÁRIOS
  [REUSADO]  código copiado do original, sem mudança de lógica
  [ALTERADO] código do original com uma mudança (explicada no comentário)
  [NOVO]     código que não existia no original
  [COMO FUNCIONA]  explicação do que o trecho faz

FLUXO (por empresa):
  raw (S3) --baixar_raw--> CSVs locais --consolidar--> Csv/all-collected.csv
  --separar_validos--> Silver/clean_data.csv + Silver/dados_nulos.csv
  --csv_to_json--> Silver/treated-data.json  --> tudo enviado ao bucket trusted
"""
import csv
import json
import re
from datetime import date

import pandas as pd

from .config import (
    PASTA_CSV,
    PASTA_RAW,
    PASTA_SILVER,
    PREFIXO_RAW_METRICS,
    particao,
)

from .config import baixar_arquivos_s3, enviar_para_s3


NUMERICAS = [
    "cpu_percent",
    "cpu_frequency",
    "ram_percent",
    "swap_memory_total",
    "swap_memory_used",
    "swap_memory_percent",
    "upload_speed",
    "download_speed",
]
OBRIGATORIAS = NUMERICAS + ["timestamp"]


NAO_ZERO = ["cpu_percent", "cpu_frequency", "ram_percent"]


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


def baixar_raw(dia):
    """[COMO FUNCIONA] Baixa do bucket raw os CSVs de métricas e devolve
    {id_empresa: [arquivos]}."""
    arquivos_raw = baixar_arquivos_s3(
        "raw",
        f"{PREFIXO_RAW_METRICS}/{particao(dia)}/",
        PASTA_RAW,
    )
    if not arquivos_raw:
        raise RuntimeError(
            "Nenhum CSV de métricas foi encontrado em raw/metrics/ no bucket Raw."
        )
    return _agrupar_por_empresa(arquivos_raw)



def consolidar(id_empresa, arquivos_empresa):
    """Junta os CSVs da empresa em Csv/all-collected.csv"""
    PASTA_CSV.mkdir(parents=True, exist_ok=True)

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


def separar_validos(df):

    ausentes = [c for c in OBRIGATORIAS if c not in df.columns]
    if ausentes:
        raise ValueError(
            "O CSV de entrada não contém as colunas esperadas: " + ", ".join(ausentes)
        )

    df = df.copy()

    df[NUMERICAS] = df[NUMERICAS].apply(pd.to_numeric, errors="coerce")
    df["timestamp"] = df["timestamp"].replace("", pd.NA)

    invalida = df[OBRIGATORIAS].isnull().any(axis=1) | (df[NAO_ZERO] == 0).any(axis=1)
    return df.loc[~invalida], df.loc[invalida]


def csv_to_json(csv_file, json_file, id_empresa):

    data = []
    with open(csv_file, encoding="utf-8", newline="") as arquivo:
        reader = csv.DictReader(arquivo, delimiter=";")
        for row in reader:
            linha = {"id_empresa": id_empresa}
            linha.update(row)
            data.append(linha)

    with open(json_file, "w", encoding="utf-8") as arquivo:
        json.dump(data, arquivo, ensure_ascii=False, indent=4)



def main(id_empresa, arquivos_empresa, dia):
    consolidar(id_empresa, arquivos_empresa) 

    arquivo_csv = PASTA_CSV / "all-collected.csv"
    if not arquivo_csv.is_file():
        raise FileNotFoundError(f"CSV raw consolidado não encontrado: {arquivo_csv}")

    PASTA_SILVER.mkdir(parents=True, exist_ok=True)
    limpos, nulos = separar_validos(pd.read_csv(arquivo_csv, sep=";"))

    caminho_limpos = PASTA_SILVER / "clean_data.csv"
    caminho_nulos = PASTA_SILVER / "null_data.csv"
    limpos.to_csv(caminho_limpos, sep=";", index=False)
    nulos.to_csv(caminho_nulos, sep=";", index=False)
    print(f"Silver: {len(limpos)} linhas limpas, {len(nulos)} descartadas.")

    caminho_json = PASTA_SILVER / "treated-data.json"
    csv_to_json(caminho_limpos, caminho_json, id_empresa)

    prefixo = f"{id_empresa}/{particao(dia)}"
    enviar_para_s3(caminho_limpos, f"{prefixo}/clean_data.csv", camada="trusted")
    enviar_para_s3(caminho_nulos, f"{prefixo}/null_data.csv", camada="trusted")
    enviar_para_s3(caminho_json, f"{prefixo}/treated-data.json", camada="trusted")


if __name__ == "__main__":
    dia = date.today()
    for id_empresa, arquivos in sorted(baixar_raw(dia).items()):
        main(id_empresa, arquivos, dia)