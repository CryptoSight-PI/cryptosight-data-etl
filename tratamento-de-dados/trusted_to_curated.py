
import argparse
import os
import re
import shutil

import mysql.connector

from configenvironment import s3 , bucket_bronze , bucket_gold , bucket_silver , cursor
from raw_to_trusted import particao , buscar_empresa


from CURATED.financeiro import main as financeiro
from CURATED.operacional import main as operacional
from CURATED.rede import main as rede

ARQUIVOS = ("clean_data.csv", "dados_nulos.csv", "treated-data.json")



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

    financeiro(id_empresa , dia)
    operacional(id_empresa , dia)
    rede(id_empresa , dia)
    # nome_sanitizado = sanitizar_nome(nome_empresa)

    # prefixo_silver = f"{id_empresa}/{particao(dia)}"

    # for nome_arquivo in ARQUIVOS:
    #     chave_origem = f"{prefixo_silver}/{nome_arquivo}"

    #     resposta = s3.get_object(
    #         Bucket=bucket_silver,
    #         Key=chave_origem,
    #     )

    #     conteudo = resposta["Body"].read()

    #     chave_destino = (
    #         f"{id_empresa}/{nome_sanitizado}/{nome_arquivo}"
    #     )

    #     content_type = (
    #         "application/json"
    #         if nome_arquivo.endswith(".json")
    #         else "text/csv"
    #     )

    #     s3.put_object(
    #         Bucket=bucket_gold,
    #         Key=chave_destino,
    #         Body=conteudo,
    #         ContentType=content_type,
    #     )

    print(
        f"Gold concluído para empresa {nome_empresa} "
        f"(ID {id_empresa})."
    )
