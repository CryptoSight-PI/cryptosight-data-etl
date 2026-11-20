"""
Gerador simples de medições -> grava JSONs no bucket Bronze.

Fluxo:
  1. Lê as configurações das variáveis de ambiente
  2. Busca os ids das farms no banco (uma vez, no início)
  3. Repete para sempre: gera 1 registro -> (às vezes suja) -> grava no bucket -> espera 1s

"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from config import conexao,  cursor
from s3config import bucket_bronze as BUCKET_RAW , s3

import boto3  
import mysql.connector 

# PASSO 1 - Configurações 
# DB_HOST = os.getenv("DB_HOST")
# DB_PORT = os.getenv("DB_PORT", "3306")
# DB_NAME = os.getenv("DB_NAME")  
# DB_USER = os.getenv("DB_USER")
# DB_PASSWORD = os.getenv("DB_PASSWORD")

# BUCKET_RAW = os.getenv("BUCKET_RAW") or os.getenv("AWS_BUCKET_RAW") or os.getenv("AWS_BUCKET_BRONZE")
# S3_ENDPOINT = os.getenv("S3_ENDPOINT")
# AWS_REGION = os.getenv("AWS_DEFAULT_REGION")
# optional: number of events to emit and exit (0 = run forever)
NUM_EVENTS = int(os.getenv("NUM_EVENTS", "0"))
# If BUCKET_RAW is not set we fallback to a local directory to allow running
# the generator without an S3 bucket. Set LOCAL_BUCKET_DIR to change location.
LOCAL_BUCKET_DIR = os.getenv("LOCAL_BUCKET_DIR", "./bucket_raw")

DIRTY_RATE = float(os.getenv("DIRTY_RATE", "0.2")) 
INTERVALO = float(os.getenv("INTERVALO_SEGUNDOS", "1"))  

CAMPOS_LATENCIA = ["latencia_pool", "latencia_dns", "latencia_gateway"]


# PASSO 2 - Buscar no banco
def buscar_ids_farm():
    cursor.execute("SELECT id FROM farm")
    linhas = cursor.fetchall()
    return [linha[0] for linha in linhas]


# PASSO 3 - Gerar um registro limpo (aleatório, menos id_farm)
def gerar_registro_limpo(id_farm):
    agora = datetime.now(timezone.utc)
    return {
        "id_farm": id_farm,
        "latencia_pool": random.randint(100, 400),
        "latencia_dns": random.randint(5, 60),
        "latencia_gateway": random.randint(5, 50),
        "timestamp-medicao": agora.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# PASSO 4 - Sujar o registro
def sujar_registro(registro):
    campo = random.choice(CAMPOS_LATENCIA) 
    tipo = random.choice(["nulo", "texto", "outlier"])

    if tipo == "nulo":
        registro[campo] = None  
    elif tipo == "texto":
        registro[campo] = f"{registro[campo]}ms"
    else:
        registro[campo] = random.choice([-5, 99999])  

    return registro


# PASSO 5 - Gravar no bucket
def salvar_no_bucket(s3, registro, use_local=False, local_dir=LOCAL_BUCKET_DIR):
    agora = datetime.now(timezone.utc)
    # "pastas" no bucket são só parte do nome (chave) do arquivo
    chave = (
        f"medicoes/ano={agora:%Y}/mes={agora:%m}/dia={agora:%d}/"
        f"{agora:%H%M%S}_{uuid.uuid4().hex[:8]}.json"
    )
    if use_local:
        # write the JSON to a local file mirroring the S3 key structure
        caminho = os.path.join(local_dir, chave)
        pasta = os.path.dirname(caminho)
        os.makedirs(pasta, exist_ok=True)
        with open(caminho, "wb") as f:
            f.write(json.dumps(registro).encode("utf-8"))
    else:
        # write to temp file and upload using upload_file
        tmp = None
        try:
            import tempfile

            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as tf:
                tmp = tf.name
                json.dump(registro, tf, ensure_ascii=False)
            s3.upload_file(tmp, BUCKET_RAW, chave)
        finally:
            try:
                if tmp and os.path.exists(tmp):
                    os.remove(tmp)
            except Exception:
                pass
    return chave


# PASSO 6 - Loop
def main():
    # validação das variáveis de ambiente mais comuns para falhas óbvias
    # BUCKET_RAW is optional: if missing we fallback to a local directory.
    # obrigatorios = {
    #     "DB_HOST": DB_HOST,
    #     "DB_USER": DB_USER,
    #     "DB_NAME": DB_NAME,
    #     "DB_PASSWORD": DB_PASSWORD,
    # }
    # faltando = [k for k, v in obrigatorios.items() if not v]
    # if faltando:
    #     raise RuntimeError(
    #         "Variáveis de ambiente faltando ou vazias: " + ", ".join(faltando)
    #     )
    ids_farm = buscar_ids_farm()

    use_local = not BUCKET_RAW
    destino = LOCAL_BUCKET_DIR if use_local else BUCKET_RAW
    origem_descr = f"diretório local '{destino}'" if use_local else f"bucket '{destino}'"
    print(f"{len(ids_farm)} farms carregadas. Gravando em {origem_descr}...")

    # create s3 client using region_name when provided; ignore custom endpoint unless explicitly set
    # if use_local:
    #     s3 = None
    # else:
    #     region = AWS_REGION or None
    #     if S3_ENDPOINT:
    #         s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT, region_name=region)
    #     else:
    #         s3 = boto3.client("s3", region_name=region)

    try:
        count = 0
        limit = NUM_EVENTS if NUM_EVENTS > 0 else None
        while True:
            try:
                registro = gerar_registro_limpo(random.choice(ids_farm))
                if random.random() < DIRTY_RATE:
                    registro = sujar_registro(registro)
                chave = salvar_no_bucket(s3, registro, use_local=use_local, local_dir=LOCAL_BUCKET_DIR)
                print("gravado:", chave)
            except Exception as erro: 
                print("erro:", erro)

            count += 1
            if limit is not None and count >= limit:
                break

            time.sleep(INTERVALO)
    except KeyboardInterrupt:
        print("Encerrado.")


if __name__ == "__main__":
    main()