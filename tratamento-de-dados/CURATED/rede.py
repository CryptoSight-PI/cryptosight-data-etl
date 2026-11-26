import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from configenvironment import s3, bucket_silver, bucket_gold, cursor, conexao, garantir_conexao

PREFIXO_GOLD = "rede/"
TRABALHADORES = 16


def listar_chaves(bucket, prefixo):
    chaves = []

    paginator = s3.get_paginator("list_objects_v2")
    for pagina in paginator.paginate(Bucket=bucket, Prefix=prefixo):
        for objeto in pagina.get("Contents", []):
            if objeto["Key"].endswith(".json"):
                chaves.append(objeto["Key"])

    return chaves


def processar(chave):
    chave_gold = PREFIXO_GOLD + chave

    try:
        resposta = s3.get_object(Bucket=bucket_silver, Key=chave)
        registro = json.loads(resposta["Body"].read().decode("utf-8"))

        if "timestamp-medicao" in registro:
            registro["timestamp_medicao"] = registro.pop("timestamp-medicao")

        s3.put_object(
            Bucket=bucket_gold,
            Key=chave_gold,
            Body=json.dumps(registro).encode("utf-8"),
            ContentType="application/json",
        )

        return registro

    except Exception as erro:
        print(f"Erro em {chave}: {erro}")
        return None


def inserir_no_banco(registros):
    linhas = []

    for registro in registros:
        momento = datetime.strptime(registro["timestamp_medicao"], "%Y-%m-%dT%H:%M:%SZ")
        linhas.append((
            registro["id_farm"],
            registro["latencia_pool"],
            registro["latencia_dns"],
            registro["latencia_gateway"],
            momento,
        ))

    if not linhas:
        return

    cursor.executemany(
        "insert into medicao_rede (id_farm, latencia_pool, latencia_dns, latencia_gateway, timestamp_medicao) values (%s, %s, %s, %s, %s)",
        linhas,
    )
    conexao.commit()


def main():
    arquivos = listar_chaves(bucket_silver, "medicoes/")
    print(f"{len(arquivos)} arquivos de rede no silver")

    no_gold = set(listar_chaves(bucket_gold, PREFIXO_GOLD + "medicoes/"))

    pendentes = [c for c in arquivos if PREFIXO_GOLD + c not in no_gold]

    with ThreadPoolExecutor(TRABALHADORES) as executor:
        resultados = list(executor.map(processar, pendentes))

    registros = [r for r in resultados if r is not None]

    try:
        inserir_no_banco(registros)
    except Exception as erro:
        print(f"Erro ao inserir no banco: {erro}")


def lambda_handler(event=None, context=None):
    garantir_conexao()
    main()
    return {"status": "ok"}


if __name__ == "__main__":
    main()