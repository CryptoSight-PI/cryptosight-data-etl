import csv
import json
import mysql.connector
from dotenv import load_dotenv
import os
import re
from s3_utils import enviar_para_s3  # função que envia arquivos ao S3

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE")
}


PASTA_SILVER = os.path.join(".", "Silver")

def extrair_id(nome_arquivo):
    nome = os.path.basename(nome_arquivo)
    resultado = re.match(r"^(\d+)", nome)

    if resultado:
        return int(resultado.group(1))

    raise ValueError(
        f"Não foi possível encontrar o ID no arquivo: {nome}"
    )


def buscar_empresa(id_empresa):
    conexao = mysql.connector.connect(**DB_CONFIG)

    try:
        cursor = conexao.cursor()

        consulta = """
            SELECT id, razao_social, nome_fantasia
            FROM empresa
            WHERE id = %s
        """

        cursor.execute(consulta, (id_empresa,))
        empresa = cursor.fetchone()

        if empresa is None:
            raise ValueError(
                f"Nenhuma empresa encontrada com ID {id_empresa}"
            )

        razao_social = empresa[1]
        nome_fantasia = empresa[2]

        nome_empresa = nome_fantasia or razao_social

        if not nome_empresa:
            raise ValueError(
                f"A empresa {id_empresa} não tem nome cadastrado"
            )

        return nome_empresa

    finally:
        if 'cursor' in locals():
            cursor.close()

        conexao.close()


# Transforma o CSV dos dados tratados em JSON
def csv_to_json(csv_file, json_file, id_empresa):
    data = []

    with open(csv_file, encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')

        for row in reader:
            linha = {"id_empresa": id_empresa}
            linha.update(row)
            data.append(linha)

    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


# Arquivo usado para extrair o ID da empresa
arquivo_origem = "1-all-collected.csv"

id_empresa = extrair_id(arquivo_origem)

# Caminhos de entrada e saída
csv_entrada = os.path.join(PASTA_SILVER, "clean_data.csv")
json_saida = os.path.join(PASTA_SILVER, "treated-data.json")

csv_to_json(
    csv_entrada,
    json_saida,
    id_empresa
)

print("JSON gerado com sucesso!")

# Envia o JSON tratado para a camada silver do bucket
enviar_para_s3(json_saida, "silver/treated-data.json")
