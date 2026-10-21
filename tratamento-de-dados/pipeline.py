import re
import subprocess
import sys
from pathlib import Path

import pandas as pd

from csv_bind import main as enviar_csvs_locais
from s3_utils import baixar_arquivos_s3


ROOT_DIR = Path(__file__).resolve().parent.parent
PASTA_CSV = ROOT_DIR / "Csv"
PASTA_RAW = ROOT_DIR / "Raw"
PASTA_TRATAMENTO = Path(__file__).resolve().parent


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
    enviar_csvs_locais()

    arquivos_raw = baixar_arquivos_s3(
        "raw",
        "raw/metrics/",
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

        etapas = (
            ["treatment-clean-data.py", "--company-id", str(id_empresa)],
            ["silver.py", "--company-id", str(id_empresa)],
            ["gold_file.py", "--company-id", str(id_empresa)],
        )
        for etapa in etapas:
            subprocess.run(
                [sys.executable, str(PASTA_TRATAMENTO / etapa[0]), *etapa[1:]],
                cwd=ROOT_DIR,
                check=True,
            )

    print("Pipeline concluído: Raw -> Trusted -> Gold para todas as empresas.")


if __name__ == "__main__":
    main()
