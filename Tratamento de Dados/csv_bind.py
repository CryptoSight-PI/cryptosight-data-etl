import pandas as pd
import glob
import time
import os
from s3_utils import enviar_para_s3  # função que envia arquivos ao S3

PASTA_CSV = os.path.join(".", "Csv")

while True:
    arquivos_csv = glob.glob(os.path.join(PASTA_CSV, "*.csv"))

    
    arquivos_csv = [
        arquivo for arquivo in arquivos_csv
        if os.path.basename(arquivo) != "all-collected.csv"
    ]

    if not arquivos_csv:
        print("Nenhum arquivo CSV encontrado!")

    else:
        dataframes = [
            pd.read_csv(arquivo, sep=";")
            for arquivo in arquivos_csv
        ]

        final_csv = pd.concat(dataframes, ignore_index=True)

        caminho_final = os.path.join(PASTA_CSV, "all-collected.csv")

        final_csv.to_csv(
            caminho_final,
            sep=";",
            index=False
        )

        print(f"{len(arquivos_csv)} arquivos unificados.")

        # Envia o CSV unificado para a camada bronze do bucket
        try:
            enviar_para_s3(caminho_final, "bronze/all-collected.csv")
        except Exception as erro:
            # se falhar, mostra o erro mas o loop continua rodando
            print(f"Erro ao enviar para o S3: {erro}")

    time.sleep(60)