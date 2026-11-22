"""
Exemplo de LEITURA do bucket Bronze e INSERÇÃO no bucket Silver.
Usa as mesmas variáveis de ambiente do gerador + BUCKET_SILVER.

"""

import json
import os

from configenvironment import s3 , bucket_bronze , bucket_silver, garantir_conexao

# (adaptado) os buckets e o cliente S3 agora vêm da configenvironment do projeto,
# ou seja: o MESMO bucket bronze/silver do resto da pipeline
BUCKET_BRONZE = bucket_bronze
BUCKET_SILVER = bucket_silver
BUCKET_LOCAL_DIR = os.environ.get("LOCAL_BUCKET_DIR", "./bucket_raw")

use_local = not BUCKET_BRONZE  # if no bronze/raw bucket configured, read local dir



def listar_chaves(bucket, prefixo):
    """Lista o nome (chave) de todos os arquivos que começam com o prefixo.

    Se o processo estiver em modo local (sem S3), lista arquivos no diretório local
    que reproduz a estrutura de chaves (p.ex. bucket_raw/medicoes/...).
    """
    chaves = []
    if use_local:
        base = os.path.join(BUCKET_LOCAL_DIR or "./bucket_raw", prefixo)
        if not os.path.exists(base):
            return chaves
        for root, _, files in os.walk(base):
            for f in files:
                full = os.path.join(root, f)
                # compute key relative to base directory
                chave = os.path.relpath(full, start=BUCKET_LOCAL_DIR).replace(os.path.sep, "/")
                chaves.append(chave)
        return chaves

    paginador = s3.get_paginator("list_objects_v2")  # a cada chamada vêm até 1000
    for pagina in paginador.paginate(Bucket=bucket, Prefix=prefixo):
        for objeto in pagina.get("Contents", []):  # "Contents" não existe se vazio
            chaves.append(objeto["Key"])
    return chaves


def ler_json(bucket, chave):
    """Baixa 1 arquivo e converte de bytes para dict.

    Em modo local, abre o arquivo no diretório que espelha as chaves do S3.
    """
    if use_local:
        caminho = os.path.join(BUCKET_LOCAL_DIR, chave)
        with open(caminho, "r", encoding="utf-8") as f:
            return json.load(f)

    resposta = s3.get_object(Bucket=bucket, Key=chave)
    return json.loads(resposta["Body"].read().decode("utf-8"))


def gravar_json(bucket, chave, dados):
    """Grava 1 dict como arquivo JSON."""
    if use_local:
        caminho = os.path.join(BUCKET_LOCAL_DIR, chave)
        pasta = os.path.dirname(caminho)
        os.makedirs(pasta, exist_ok=True)
        with open(caminho, "w", encoding="utf-8") as f:
            json.dump(dados, f, ensure_ascii=False)
        return

    s3.put_object(
        Bucket=bucket,
        Key=chave,
        Body=json.dumps(dados).encode("utf-8"),
        ContentType="application/json",
    )


def limpar(registro):
    """Silver (exemplo): devolve o registro limpo ou None se for descartado."""
    campos = ["latencia_pool", "latencia_dns", "latencia_gateway"]
    for campo in campos:
        valor = registro.get(campo)
        if isinstance(valor, str):  # "212ms" -> 212
            valor = int(valor.replace("ms", "").strip())
        if valor is None or valor < 0 or valor > 5000:
            return None  # nulo ou absurdo: descarta
        registro[campo] = valor
    return registro

def main():
    if not BUCKET_SILVER:
        raise RuntimeError("BUCKET_SILVER / BUCKET_TRUSTED não configurado. Defina a variável de ambiente.")

    chaves = listar_chaves(BUCKET_BRONZE or BUCKET_LOCAL_DIR, "medicoes/")
    print(f"{len(chaves)} arquivos no bronze")

    for chave in chaves:
        try:
            registro = ler_json(BUCKET_BRONZE or BUCKET_LOCAL_DIR, chave)
        except Exception as e:
            print("falha lendo:", chave, e)
            continue
        limpo = limpar(registro)
        if limpo is None:
            print("descartado:", chave)
            continue
        try:
            gravar_json(BUCKET_SILVER, chave, limpo)  # mesma chave, outro bucket
            print("promovido p/ silver:", chave)
        except Exception as e:
            print("falha gravando no silver:", chave, e)


def lambda_handler(event=None, context=None):
    garantir_conexao()
    main()
    return {"status": "ok"}


if __name__ == "__main__":
    main()