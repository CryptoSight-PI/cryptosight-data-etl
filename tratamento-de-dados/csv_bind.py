from pathlib import Path

import pandas as pd

from s3_utils import enviar_para_s3


ROOT_DIR = Path(__file__).resolve().parent.parent
PASTA_CSV = ROOT_DIR / "Csv"


def main():
    PASTA_CSV.mkdir(parents=True, exist_ok=True)
    arquivos_csv = sorted(
        arquivo
        for arquivo in PASTA_CSV.glob("*.csv")
        if arquivo.name != "all-collected.csv"
    )

    if not arquivos_csv:
        print("Nenhum CSV local para enviar; será usada a camada raw existente no S3.")
        return

    dataframes = []
    for arquivo in arquivos_csv:
        if "_processos_" in arquivo.name:
            prefixo = "raw/processes/"
        else:
            prefixo = "raw/metrics/"
            dataframes.append(pd.read_csv(arquivo, sep=";"))

        enviar_para_s3(arquivo, f"{prefixo}{arquivo.name}", camada="raw")

    if dataframes:
        final_csv = pd.concat(dataframes, ignore_index=True)
        caminho_final = PASTA_CSV / "all-collected.csv"
        final_csv.to_csv(caminho_final, sep=";", index=False)
        print(f"{len(dataframes)} arquivos de métricas unificados em {caminho_final}.")
    else:
        print("Nenhum CSV de métricas encontrado para unificação.")


if __name__ == "__main__":
    main()