from .config import (
    PASTA_CSV,
    PREFIXO_RAW_METRICS,
    PREFIXO_RAW_PROCESSES,
    particao,
)
from .s3_utils import enviar_para_s3


def main(dia):
    """Envia os CSVs locais (Csv/*.csv) para o bucket raw, particionados por dia."""
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