from configenvironment import s3 , bucket_silver , bucket_gold , bucket_bronze , cursor
# from trusted_to_curated import sanitizar_nome, ARQUIVOS
from CURATED.utils import sanitizar_nome, buscar_farm , buscar_sessoes , buscar_maquinas_sessao
from raw_to_trusted import buscar_empresa, particao
import pandas as pd
import io
import json

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

                if not nome_arquivo.endswith(".csv"):
                    continue

                conteudo = s3.get_object(Bucket=bucket_silver, Key=key)["Body"].read()

                df = pd.read_csv(io.BytesIO(conteudo), sep=";")

                if "user" not in df.columns:
                    print(f"Coluna user não encontrada em {nome_arquivo}: {df.columns.tolist()}")
                    continue

                for id_farm in buscar_farm(idempresa):
                    for id_sessao in buscar_sessoes(id_farm):
                        for maquina in buscar_maquinas_sessao(id_sessao):

                            id_maquina = maquina["id_maquina"]
                            mac = maquina["mac_address"]

                            dados = df[
                            df["user"].astype(str).str.strip().str.lower()
                            .str.replace("-", ":", regex=False) == mac]

                            chave_destino = f"{idempresa}/{nome_sanitizado}/operacional/farm={id_farm}/sessao={id_sessao}/maquina={maquina['id_maquina']}/{particao(dia)}/{nome_arquivo.replace('.csv' , '.json')}"

                            registros = dados.to_dict(orient = "records")

                            content_type = "application/json" if nome_arquivo.endswith(".json") else "text/csv"

                            s3.put_object(
                                Bucket=bucket_gold,
                                Key=chave_destino,
                                Body=json.dumps(registros, ensure_ascii=False, indent = 4),
                                ContentType=content_type,
                            ) 
    print("teste")