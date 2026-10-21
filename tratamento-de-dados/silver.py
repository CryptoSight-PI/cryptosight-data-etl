import argparse
import csv
import json
from pathlib import Path

from s3_utils import enviar_para_s3


ROOT_DIR = Path(__file__).resolve().parent.parent
PASTA_SILVER = ROOT_DIR / "Silver"


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


def main(id_empresa):
    if id_empresa is None:
        raise ValueError("Informe o ID da empresa para gerar os dados trusted.")

    csv_entrada = PASTA_SILVER / "clean_data.csv"
    json_saida = PASTA_SILVER / "treated-data.json"
    if not csv_entrada.is_file():
        raise FileNotFoundError(f"CSV trusted não encontrado: {csv_entrada}")

    csv_to_json(csv_entrada, json_saida, id_empresa)
    print(f"JSON gerado em {json_saida}.")
    enviar_para_s3(
        json_saida,
        f"trusted/{id_empresa}/treated-data.json",
        camada="trusted",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--company-id", type=int, required=True)
    argumentos = parser.parse_args()
    main(argumentos.company_id)
