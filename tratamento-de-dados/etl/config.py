from pathlib import Path

from dotenv import load_dotenv

# tratamento-de-dados/etl/config.py -> raiz do projeto (onde ficam Csv/, Raw/, ...)
ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")

PASTA_CSV = ROOT_DIR / "csv"
PASTA_RAW = ROOT_DIR / "raw"
PASTA_SILVER = ROOT_DIR / "silver"
PASTA_GOLD = ROOT_DIR / "gold"

PREFIXO_RAW_METRICS = "metrics"
PREFIXO_RAW_PROCESSES = "processes"


def particao(dia):
    """date -> '2026/10/21'"""
    return f"{dia:%Y/%m/%d}"