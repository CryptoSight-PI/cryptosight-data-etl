
import argparse
import os
import re
import shutil

import mysql.connector

from configenvironment import s3 , bucket_bronze , bucket_gold , bucket_silver , cursor
from raw_to_trusted import particao 


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



def sanitizar_nome(nome_empresa):
    nome = re.sub(r'[<>:"/\\|?*]', "_", nome_empresa).strip(" .")

    if not nome:
        raise ValueError("Nome inválido para a empresa.")

    return nome 


def main(id_empresa, dia):
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
