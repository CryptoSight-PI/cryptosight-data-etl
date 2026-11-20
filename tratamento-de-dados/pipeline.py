
from configenvironment import s3, bucket_bronze

from TRUSTED import raw_to_trusted, raw_to_trusted_rede
from CURATED import operacional, rede, financeiro


def evento_s3(chaves):
    return {"Records": [{"s3": {"object": {"key": c}}} for c in chaves]}


def listar_bronze(prefixo):
    paginator = s3.get_paginator("list_objects_v2")
    chaves = []
    for pagina in paginator.paginate(Bucket=bucket_bronze, Prefix=prefixo):
        for objeto in pagina.get("Contents", []):
            chaves.append(objeto["Key"])
    return chaves


def rodar_curated(handler, nome, chaves_silver):
    if not chaves_silver:
        return
    if handler is None:
        print(f"[{nome}] sem lambda_handler ainda, pulando.")
        return
    print(f"[{nome}]", handler(evento_s3(chaves_silver)))


def main_metricas():
    chaves = [c for c in listar_bronze("metrics/") if c.lower().endswith(".csv")]
    if not chaves:
        print("Métricas: nada pendente no bronze.")
        return

    resultado = raw_to_trusted.lambda_handler(evento_s3(chaves))
    print("[trusted métricas]", resultado)

    silver = resultado["silver_keys"]
    rodar_curated(operacional.lambda_handler, "curated operacional", silver)
    rodar_curated(getattr(financeiro, "lambda_handler", None), "curated financeiro", silver)


def main_rede():
    print("[trusted rede]", raw_to_trusted_rede.lambda_handler({}))
    print("[curated rede]", rede.lambda_handler({}))


def main():
    for etapa in (main_metricas, main_rede):
        try:
            etapa()
        except Exception as erro:
            print(f"Erro em {etapa.__name__}: {erro}")
    print("Pipeline finalizado.")


if __name__ == "__main__":
    main()