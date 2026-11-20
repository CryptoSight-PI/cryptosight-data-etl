"""
Promove registros do bucket Trusted (silver) para Curated (gold) e insere
os registros limpos no banco de dados MySQL quando possível.

Usage:
  Configure variáveis de ambiente: BUCKET_TRUSTED (ou BUCKET_SILVER),
  BUCKET_CURATED (ou BUCKET_GOLD), S3_ENDPOINT opcional, ou deixe
O script tenta inserir em uma tabela `medicao` com colunas
  (id_farm, latencia_pool, latencia_dns, latencia_gateway, timestamp_medicao)
Se a tabela não existir ele continuará sem falhar.
"""
import json
from datetime import datetime

from configenvironment import s3, bucket_silver, bucket_gold, cursor, conexao

PREFIXO_GOLD = "rede/"


def listar_arquivos():
    arquivos = []

    paginator = s3.get_paginator("list_objects_v2")
    for pagina in paginator.paginate(Bucket=bucket_silver, Prefix="medicoes/"):
        for objeto in pagina.get("Contents", []):
            if objeto["Key"].endswith(".json"):
                arquivos.append(objeto["Key"])

    return arquivos


def ja_existe_no_gold(chave):
    try:
        s3.head_object(Bucket=bucket_gold, Key=chave)
        return True
    except Exception:
        return False


def inserir_no_banco(registro):
    momento = datetime.strptime(registro["timestamp_medicao"], "%Y-%m-%dT%H:%M:%SZ")

    cursor.execute(
        "insert into medicao_rede (id_farm, latencia_pool, latencia_dns, latencia_gateway, timestamp_medicao) values (%s, %s, %s, %s, %s)",
        (
            registro["id_farm"],
            registro["latencia_pool"],
            registro["latencia_dns"],
            registro["latencia_gateway"],
            momento,
        ),
    )
    conexao.commit()


def main():
    arquivos = listar_arquivos()
    print(f"{len(arquivos)} arquivos de rede no silver")

    for chave in arquivos:
        chave_gold = PREFIXO_GOLD + chave

        # se já está no gold, já foi pro banco também
        if ja_existe_no_gold(chave_gold):
            continue

        try:
            resposta = s3.get_object(Bucket=bucket_silver, Key=chave)
            registro = json.loads(resposta["Body"].read().decode("utf-8"))

            # o gerador manda "timestamp-medicao", o banco usa "timestamp_medicao"
            if "timestamp-medicao" in registro:
                registro["timestamp_medicao"] = registro.pop("timestamp-medicao")

            s3.put_object(
                Bucket=bucket_gold,
                Key=chave_gold,
                Body=json.dumps(registro).encode("utf-8"),
                ContentType="application/json",
            )

            inserir_no_banco(registro)

        except Exception as erro:
            print(f"Erro em {chave}: {erro}")


def lambda_handler(event=None, context=None):
    main()
    return {"status": "ok"}


if __name__ == "__main__":
    main()