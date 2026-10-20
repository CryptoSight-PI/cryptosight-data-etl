import pandas as pd
import os
from s3_utils import enviar_para_s3  # função que envia arquivos ao S3

PASTA_CSV = os.path.join(".", "Csv")
PASTA_SILVER = os.path.join(".", "Silver")

os.makedirs(PASTA_SILVER, exist_ok=True)

arquivo_csv = os.path.join(PASTA_CSV, "all-collected.csv")

clean_columns = [
    "cpu_percent",
    "cpu_frequency",
    "ram_percent",
    "swap_memory_total",
    "swap_memory_used",
    "swap_memory_percent",
    "upload_speed",
    "download_speed",
    "timestamp"
]

df = pd.read_csv(arquivo_csv, sep=";")

filtro_nulas = (
    df[clean_columns].isnull()
    | (df[clean_columns] == 0)
).any(axis=1)

lines_null = df.loc[filtro_nulas]

caminho_nulos = os.path.join(PASTA_SILVER, "dados_nulos.csv")

lines_null.to_csv(
    caminho_nulos,
    sep=";",
    index=False
)

df_filtrado = df.loc[~filtro_nulas].copy()

caminho_limpos = os.path.join(PASTA_SILVER, "clean_data.csv")

df_filtrado.to_csv(
    caminho_limpos,
    sep=";",
    index=False
)

print("Tratamento concluído. Arquivos salvos na pasta Silver.")

# Envia os dois arquivos para a camada silver do bucket
enviar_para_s3(caminho_nulos, "silver/dados_nulos.csv")
enviar_para_s3(caminho_limpos, "silver/clean_data.csv")