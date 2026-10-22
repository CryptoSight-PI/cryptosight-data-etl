"""Raw consolidado -> Silver (trusted): separa linhas válidas e inválidas."""
import pandas as pd

from .config import PASTA_CSV, PASTA_SILVER, particao
from .s3_utils import enviar_para_s3

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

# Só nestas colunas o valor 0 indica leitura falha.
# Em swap_*, upload e download, 0 é um valor legítimo.
NAO_ZERO = ["cpu_percent", "cpu_frequency", "ram_percent"]


def separar_validos(df):
    ausentes = [c for c in OBRIGATORIAS if c not in df.columns]
    if ausentes:
        raise ValueError(
            "O CSV de entrada não contém as colunas esperadas: " + ", ".join(ausentes)
        )

    df = df.copy()
    # valores não numéricos viram NaN e caem em "nulos"
    df[NUMERICAS] = df[NUMERICAS].apply(pd.to_numeric, errors="coerce")
    df["timestamp"] = df["timestamp"].replace("", pd.NA)

    invalida = df[OBRIGATORIAS].isnull().any(axis=1) | (df[NAO_ZERO] == 0).any(axis=1)
    return df.loc[~invalida], df.loc[invalida]


def main(id_empresa, dia):
    arquivo_csv = PASTA_CSV / "all-collected.csv"
    if not arquivo_csv.is_file():
        raise FileNotFoundError(f"CSV raw consolidado não encontrado: {arquivo_csv}")

    PASTA_SILVER.mkdir(parents=True, exist_ok=True)
    limpos, nulos = separar_validos(pd.read_csv(arquivo_csv, sep=";"))

    caminho_limpos = PASTA_SILVER / "clean_data.csv"
    caminho_nulos = PASTA_SILVER / "dados_nulos.csv"
    limpos.to_csv(caminho_limpos, sep=";", index=False)
    nulos.to_csv(caminho_nulos, sep=";", index=False)
    print(f"Silver: {len(limpos)} linhas limpas, {len(nulos)} descartadas.")

    prefixo = f"{id_empresa}/{particao(dia)}"
    enviar_para_s3(caminho_limpos, f"{prefixo}/clean_data.csv", camada="trusted")
    enviar_para_s3(caminho_nulos, f"{prefixo}/dados_nulos.csv", camada="trusted")