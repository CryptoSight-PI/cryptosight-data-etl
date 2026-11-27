# from configenvironment import s3 , bucket_silver , bucket_gold , bucket_bronze , cursor
import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE")
}

def extrair_pagamentos(id_empresa):
    conexao = mysql.connector.connect(**DB_CONFIG)

    try:
        cursor = conexao.cursor()

        consulta = """
SELECT f.nome, u.nome, p.valor, p.categoria, p.data_hora_pagamento FROM pagamento p
JOIN usuario u on p.id_usuario = u.id
JOIN farm f on p.id_farm = f.id
WHERE f.id_empresa = %s
        """

        cursor.execute(consulta, (id_empresa,))
        pagamentos = cursor.fetchall()

        return pagamentos

    finally:
        if 'cursor' in locals():
            cursor.close()

        conexao.close()

def extrair_aluguel(id_empresa):
    conexao = mysql.connector.connect(**DB_CONFIG)

    try:
        cursor = conexao.cursor()

        consulta = """
SELECT id, aluguel, dia_pagamento_aluguel FROM farm WHERE id_empresa = %s
        """

        cursor.execute(consulta, (id_empresa,))
        alugueis = cursor.fetchall()

        return alugueis

    finally:
        if 'cursor' in locals():
            cursor.close()

        conexao.close()

def montarJSON(empresa_id):
    jsons = []
    dados = extrair_pagamentos(empresa_id)
    alugueis = extrair_aluguel(empresa_id)
    for dado in dados:
        json = {
            "farm": dado[0],
            "responsavel":dado[1],
            "valor":dado[2],
            "categoria":dado[3],
            "dataHora":dado[4]
        }
        jsons.append(json)

    for aluguel in alugueis:
        json = {
            "farm": aluguel[0],
            "aluguel": aluguel[1],
            "diaPagamento": aluguel[2]
        }

    return jsons

print(montarJSON(1))

# from trusted_to_curated import sanitizar_nome, ARQUIVOS
# from CURATED.utils import sanitizar_nome , ARQUIVOS
# from raw_to_trusted import buscar_empresa , particao

# def main(idempresa, dia):
#     nome_sanitizado = sanitizar_nome(buscar_empresa(idempresa))
#     prefixo_silver = f"{idempresa}/{particao(dia)}/"

#     paginator = s3.get_paginator("list_objects_v2")
#     page_iterator = paginator.paginate(Bucket=bucket_silver, Prefix=prefixo_silver)

#     for page in page_iterator:

#         if 'Contents' in page:

#             for i in page['Contents']:

#                 key = i["Key"]
#                 nome_arquivo = key.rsplit("/", 1)[-1]

#                 conteudo = s3.get_object(Bucket=bucket_silver, Key=key)["Body"].read()

#                 chave_destino = f"{idempresa}/{nome_sanitizado}/financeiro/{particao(dia)}/{nome_arquivo}"

#                 content_type = "application/json" if nome_arquivo.endswith(".json") else "text/csv"

#                 s3.put_object(
#                     Bucket=bucket_gold,
#                     Key=chave_destino,
#                     Body=conteudo,
#                     ContentType=content_type,
#                 ) 
#     print("teste")