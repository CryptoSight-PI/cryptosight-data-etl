from configenvironment import s3 , bucket_silver , bucket_gold , bucket_bronze , cursor
# from trusted_to_curated import sanitizar_nome, ARQUIVOS
from CURATED.utils import sanitizar_nome , ARQUIVOS
from raw_to_trusted import buscar_empresa , particao

def main(idempresa, dia):
    nome_sanitizado = sanitizar_nome(buscar_empresa(idempresa))
    prefixo_silver = f"{idempresa}/{particao(dia)}/"

    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket_silver, Prefix=prefixo_silver)

    for page in page_iterator:

        if 'Contents' in page:

            for i in page['Contents']:

                key = i["Key"]
                nome_arquivo = key.rsplit("/", 1)[-1]

                conteudo = s3.get_object(Bucket=bucket_silver, Key=key)["Body"].read()

                chave_destino = f"{idempresa}/{nome_sanitizado}/financeiro/{particao(dia)}/{nome_arquivo}"

                content_type = "application/json" if nome_arquivo.endswith(".json") else "text/csv"

                s3.put_object(
                    Bucket=bucket_gold,
                    Key=chave_destino,
                    Body=conteudo,
                    ContentType=content_type,
                ) 
    print("teste")