import argparse
from pathlib import Path

import pandas as pd

from s3_utils import enviar_para_s3


ROOT_DIR = Path(__file__).resolve().parent.parent
PASTA_CSV = ROOT_DIR / "Csv"
PASTA_SILVER = ROOT_DIR / "Silver"
CLEAN_COLUMNS = [
    "cpu_percent",
    "cpu_frequency",
    "ram_percent",
    "swap_memory_total",
    "swap_memory_used",
    "swap_memory_percent",
    "upload_speed",
    "download_speed",
    "timestamp",
]


def main(id_empresa):
    arquivo_csv = PASTA_CSV / "all-collected.csv"
    if not arquivo_csv.is_file():
        raise FileNotFoundError(f"CSV raw consolidado não encontrado: {arquivo_csv}")

    PASTA_SILVER.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(arquivo_csv, sep=";")
    colunas_ausentes = [coluna for coluna in CLEAN_COLUMNS if coluna not in df.columns]
    if colunas_ausentes:
        raise ValueError(
            "O CSV de entrada não contém as colunas esperadas: "
            + ", ".join(colunas_ausentes)
        )

    filtro_nulas = (
        df[CLEAN_COLUMNS].isnull()
        | (df[CLEAN_COLUMNS] == 0)
    ).any(axis=1)

    caminho_nulos = PASTA_SILVER / "dados_nulos.csv"
    df.loc[filtro_nulas].to_csv(caminho_nulos, sep=";", index=False)

    caminho_limpos = PASTA_SILVER / "clean_data.csv"
    df.loc[~filtro_nulas].to_csv(caminho_limpos, sep=";", index=False)

    print(f"Tratamento concluído. Arquivos salvos em {PASTA_SILVER}.")
    prefixo = f"trusted/{id_empresa}/"
    enviar_para_s3(
        caminho_nulos,
        f"{prefixo}dados_nulos.csv",
        camada="trusted",
    )
    enviar_para_s3(
        caminho_limpos,
        f"{prefixo}clean_data.csv",
        camada="trusted",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--company-id", type=int, required=True)
    argumentos = parser.parse_args()
    main(argumentos.company_id)