"""Trusted -> Curated (antes chamado Gold).

Este arquivo é o antigo etl/gold_file.py com poucas mudanças.

LEGENDA DOS COMENTÁRIOS
  [REUSADO]  código copiado do original, sem mudança de lógica
  [ALTERADO] código do original com uma mudança (explicada no comentário)
  [COMO FUNCIONA]  explicação do que o trecho faz

FLUXO (por empresa):
  Silver/{clean_data.csv, dados_nulos.csv, treated-data.json}
    --> Gold/<id>-<empresa>/  (cópia local)  --> bucket gold: <id>/<empresa>/<arquivo>
O nome da empresa vem do MySQL (tabela empresa).
"""
import argparse
import os
import re
import shutil

import mysql.connector

# from .config import PASTA_GOLD, PASTA_SILVER
# from .config import enviar_para_s3
from configenvironment import s3 , bucket_bronze , bucket_gold , bucket_silver , cursor
from raw_to_trusted import particao 

# DB_CONFIG = {
#     "host": os.getenv("DB_HOST"),
#     "user": os.getenv("DB_USER"),
#     "password": os.getenv("DB_PASSWORD"),
#     "database": os.getenv("DB_DATABASE"),
# }

ARQUIVOS = ("clean_data.csv", "dados_nulos.csv", "treated-data.json")


def buscar_empresa(id_empresa):
    try:
        cursor.execute(
            """
            SELECT id, razao_social, nome_fantasia
            FROM empresa
            WHERE id = %s
            """,
            (id_empresa,),
        )
        empresa = cursor.fetchone()
        if empresa is None:
            raise ValueError(f"Nenhuma empresa encontrada com ID {id_empresa}")

        nome_empresa = empresa[2] or empresa[1]
        if not nome_empresa:
            raise ValueError(f"A empresa {id_empresa} não tem nome cadastrado")
        return nome_empresa
    finally:
        if cursor is not None:
            cursor.close()


# def criar_pasta(id_empresa, nome_empresa):

#     nome_sanitizado = re.sub(r'[<>:"/\\|?*]', "_", nome_empresa).strip()
#     if not nome_sanitizado:
#         raise ValueError("O nome da empresa não pode ser usado como pasta Gold.")

#     nome_pasta = f"{id_empresa}-{nome_sanitizado}"
#     pasta_destino = PASTA_GOLD / nome_pasta
#     pasta_destino.mkdir(parents=True, exist_ok=True)

#     for nome_arquivo in ARQUIVOS:
#         origem = PASTA_SILVER / nome_arquivo
#         if not origem.is_file():
#             raise FileNotFoundError(f"Arquivo trusted não encontrado: {origem}")

#         destino = pasta_destino / nome_arquivo
#         shutil.copy2(origem, destino)
#         enviar_para_s3(
#             destino,
#             f"{id_empresa}/{nome_sanitizado}/{nome_arquivo}",
#             camada="gold",  # nome da camada no s3_utils (variável AWS_BUCKET_GOLD)
#         )

#     return pasta_destino

def sanitizar_nome(nome_empresa):
    """Remove caracteres inválidos do nome usado no caminho S3."""
    nome = re.sub(r'[<>:"/\\|?*]', "_", nome_empresa).strip(" .")

    if not nome:
        raise ValueError("Nome inválido para a empresa.")

    return nome 


# def main(id_empresa):
#     if id_empresa is None:
#         raise ValueError("Informe o ID da empresa para gerar a camada Gold.")

#     nome_empresa = buscar_empresa(id_empresa)
#     pasta_destino = criar_pasta(id_empresa, nome_empresa)
#     print(f"Camada Gold gerada para {nome_empresa}: {pasta_destino}")

def main(id_empresa, dia):
    """
    Silver -> Gold para uma empresa e uma data.

    Origem:
        bucket Silver / ID_EMPRESA / AAAA/MM/DD / arquivo

    Destino:
        bucket Gold / ID_EMPRESA / NOME_EMPRESA / arquivo
    """
    if not bucket_silver or not bucket_gold:
        raise ValueError(
            "Configure AWS_BUCKET_SILVER/AWS_BUCKET_TRUSTED "
            "e AWS_BUCKET_GOLD."
        )

    nome_empresa = buscar_empresa(id_empresa)
    nome_sanitizado = sanitizar_nome(nome_empresa)

    prefixo_silver = f"{id_empresa}/{particao(dia)}"

    for nome_arquivo in ARQUIVOS:
        chave_origem = f"{prefixo_silver}/{nome_arquivo}"

        # Confirma que o arquivo de origem existe e lê seu conteúdo.
        resposta = s3.get_object(
            Bucket=bucket_silver,
            Key=chave_origem,
        )

        conteudo = resposta["Body"].read()

        chave_destino = (
            f"{id_empresa}/{nome_sanitizado}/{nome_arquivo}"
        )

        content_type = (
            "application/json"
            if nome_arquivo.endswith(".json")
            else "text/csv"
        )

        s3.put_object(
            Bucket=bucket_gold,
            Key=chave_destino,
            Body=conteudo,
            ContentType=content_type,
        )

    print(
        f"Gold concluído para empresa {nome_empresa} "
        f"(ID {id_empresa})."
    )


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--company-id", type=int, required=True)
#     argumentos = parser.parse_args()
#     main(argumentos.company_id)