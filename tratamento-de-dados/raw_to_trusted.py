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
from configenvironment import s3, bucket_bronze, bucket_silver , cursor
import io

import pandas as pd

# from .config import (
#     PASTA_CSV,
#     PASTA_RAW,
#     PASTA_SILVER,
#     PREFIXO_RAW_METRICS,
#     particao,
# )

# from .config import baixar_arquivos_s3, enviar_para_s3


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

def particao(dia):
    """date -> '2026/10/21'"""
    return f"{dia:%Y/%m/%d}"

def ler_csv_s3(chave):
    resposta = s3.get_object(
        Bucket=bucket_bronze,
        Key=chave
    )

    conteudo = resposta["Body"].read()

    return pd.read_csv(
        io.BytesIO(conteudo),
        sep=";"
    )

def csv_em_memoria(df):
    buffer = io.StringIO()

    df.to_csv(
        buffer,
        sep=";",
        index=False
    )

    return buffer.getvalue().encode("utf-8")

def buscar_empresa(id_empresa):
    cursor.execute(
        """
        SELECT id, razao_social, nome_fantasia
        FROM empresa
        WHERE id = %s
        """,
        (id_empresa,),
    )

    empresa = cursor.fetchone()

    if empresa is None:
        raise ValueError(
            f"Nenhuma empresa encontrada com ID {id_empresa}"
        )

    nome_empresa = empresa[2] or empresa[1]

    if not nome_empresa:
        raise ValueError(
            f"A empresa {id_empresa} não tem nome cadastrado"
        )

    return nome_empresa

# def baixar_raw(dia):
#     """[COMO FUNCIONA] Baixa do bucket raw os CSVs de métricas e devolve
#     {id_empresa: [arquivos]}."""
#     arquivos_raw = baixar_arquivos_s3(
#         "raw",
#         f"{PREFIXO_RAW_METRICS}/{particao(dia)}/",
#         PASTA_RAW,
#     )
#     if not arquivos_raw:
#         raise RuntimeError(
#             "Nenhum CSV de métricas foi encontrado em raw/metrics/ no bucket Raw."
#         )
#     return _agrupar_por_empresa(arquivos_raw)



# def consolidar(id_empresa, arquivos_empresa):
#     """Junta os CSVs da empresa em Csv/all-collected.csv"""
#     PASTA_CSV.mkdir(parents=True, exist_ok=True)

#     dataframes = [pd.read_csv(arquivo, sep=";") for arquivo in arquivos_empresa]
#     caminho_consolidado = PASTA_CSV / "all-collected.csv"
#     pd.concat(dataframes, ignore_index=True).to_csv(
#         caminho_consolidado,
#         sep=";",
#         index=False,
#     )
#     print(
#         f"Empresa {id_empresa}: {len(arquivos_empresa)} CSVs raw "
#         f"consolidados em {caminho_consolidado}."
#     )


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


def csv_to_json(conteudo_csv, id_empresa):
    texto = conteudo_csv.decode("utf-8")

    reader = csv.DictReader(
        io.StringIO(texto, newline=""),
        delimiter=";",
    )

    dados = []

    for row in reader:
        linha = {"id_empresa": id_empresa}
        linha.update(row)
        dados.append(linha)

    return json.dumps(
        dados,
        ensure_ascii=False,
        indent=4,
    ).encode("utf-8")



def salvar_s3(chave, conteudo, content_type):
    s3.put_object(
        Bucket=bucket_silver,
        Key=chave,
        Body=conteudo,
        ContentType=content_type,
    )


def main(id_empresa, arquivos_empresa, dia):
    if not bucket_bronze or not bucket_silver:
        raise ValueError(
            "Configure AWS_BUCKET_BRONZE/AWS_BUCKET_RAW e "
            "AWS_BUCKET_SILVER/AWS_BUCKET_TRUSTED."
        )

    if not arquivos_empresa:
        raise ValueError(
            f"Nenhum CSV encontrado para a empresa {id_empresa}."
        )

    dataframes = [
        ler_csv_s3(chave)
        for chave in arquivos_empresa
    ]

    dados = pd.concat(
        dataframes,
        ignore_index=True,
    )

    limpos, nulos = separar_validos(dados)

    prefixo = f"{id_empresa}/{particao(dia)}"

    conteudo_limpo = csv_em_memoria(limpos)
    conteudo_nulos = csv_em_memoria(nulos)

    conteudo_json = csv_to_json(
        conteudo_limpo,
        id_empresa,
    )

    salvar_s3(
        f"{prefixo}/clean_data.csv",
        conteudo_limpo,
        "text/csv",
    )

    salvar_s3(
        f"{prefixo}/dados_nulos.csv",
        conteudo_nulos,
        "text/csv",
    )

    salvar_s3(
        f"{prefixo}/treated-data.json",
        conteudo_json,
        "application/json",
    )

    print(
        f"Empresa {id_empresa} | Data {particao(dia)} | "
        f"Silver concluído: {len(limpos)} linhas limpas, "
        f"{len(nulos)} descartadas."
    )

# if __name__ == "__main__":
#     dia = date.today()
#     for id_empresa, arquivos in sorted(baixar_raw(dia).items()):
#         main(id_empresa, arquivos, dia)