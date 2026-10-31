from configenvironment import s3 , bucket_silver , bucket_gold , bucket_bronze , cursor
# from trusted_to_curated import sanitizar_nome, ARQUIVOS
from CURATED.utils import sanitizar_nome, buscar_farm , buscar_sessoes , buscar_maquinas_sessao
from raw_to_trusted import buscar_empresa, particao, NUMERICAS, COLUNAS_VETOR
import pandas as pd
import io
import json


def gravar_s3(chave, conteudo):
    s3.put_object(
        Bucket=bucket_gold,
        Key=chave,
        Body=json.dumps(conteudo, ensure_ascii=False, indent=2),
        ContentType="application/json",
    )


def calcular_kpis(dados, metricas , idmaquina):
    kpis = {}
    for c in metricas:
        serie = dados[c].dropna()
        if serie.empty:
            continue
 
        pico = serie.idxmax()
 
        kpis[c] = {
            "sum": float(serie.sum()),
            "count": int(serie.count()),
            "avg": float(serie.mean()),
            "min": float(serie.min()),
            "max": float(serie.max()),
            "max_em": dados.loc[pico, "timestamp"].strftime("%Y-%m-%dT%H:%M:%S"),
            "max_maquina_id": int(idmaquina) if idmaquina is not None else int(dados.loc[pico , "_id_maquina"]),
        }
    return kpis


def calcular_series(dados, metricas , idmaquina):
    listaserie = []
    for hora, df in dados.groupby(dados["timestamp"].dt.floor("60min")):
        listaserie.append({
            "t": hora.strftime("%Y-%m-%dT%H:%M:%S"),
            "amostras": len(df),
            "metricas": calcular_kpis(df, metricas , idmaquina),
        })
    return listaserie



def main(idempresa, dia):
    nome_sanitizado = sanitizar_nome(buscar_empresa(idempresa))
    prefixo_silver = f"{idempresa}/{particao(dia)}/"

    maquinas = []
    for id_farm in buscar_farm(idempresa):
        for id_sessao in buscar_sessoes(id_farm):
            for maquina in buscar_maquinas_sessao(id_sessao):
                mac = str(maquina["mac_address"]).strip().lower().replace("-", ":")
                maquinas.append((id_farm, id_sessao, maquina["id_maquina"], mac))


    dados_maquinas = {}
    dados_sessoes = {}
    dados_farms = {}
    dados_empresa = []

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_silver, Prefix=prefixo_silver):
        for i in page.get("Contents", []):
            key = i["Key"]
            nome_arquivo = key.rsplit("/", 1)[-1]

            
            # if not (nome_arquivo.startswith("clean_data") and nome_arquivo.endswith(".csv")):
            if not nome_arquivo.endswith(".csv"):
                continue

            conteudo = s3.get_object(Bucket=bucket_silver, Key=key)["Body"].read()
            df = pd.read_csv(io.BytesIO(conteudo), sep=";")

            if "user" not in df.columns:
                print(f"Coluna user não encontrada em {nome_arquivo}: {df.columns.tolist()}")
                continue

            usuarios = df["user"].astype(str).str.strip().str.lower().str.replace("-", ":", regex=False)

            for id_farm, id_sessao, id_maquina, mac in maquinas:
                dados = df[usuarios == mac].copy()
                if dados.empty:
                    continue
                dados["_id_maquina"] = id_maquina
                

                chave = (id_farm, id_sessao, id_maquina)
                if chave not in dados_maquinas:
                    dados_maquinas[chave] = []
                # gravar_s3(chave_destino, dados)

                dados_maquinas[chave].append(dados)


                dados_sessoes.setdefault((id_farm, id_sessao), []).append(dados)
                dados_farms.setdefault(id_farm, []).append(dados)
                dados_empresa.append(dados)


    for chave in dados_maquinas:
        id_farm = chave[0]
        id_sessao = chave[1]
        id_maquina = chave[2]

        dados = pd.concat(dados_maquinas[chave], ignore_index=True)

        dados["timestamp"] = pd.to_datetime(dados["timestamp"], utc=True)

        metricas = [c for c in dados.columns if c in NUMERICAS]

        kpis = calcular_kpis(dados, metricas , id_maquina)
        series = calcular_series(dados, metricas , id_maquina)

        destinoespecificodia = (f"{idempresa}/{nome_sanitizado}/operacional/especifico/farm={id_farm}/sessao={id_sessao}/maquina={id_maquina}/dias/{particao(dia).replace('/' , '-')}")

                    
        # destinogeralfarmdia = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/dias/{particao(dia).replace('/' , '-')}")

        # destinogeralempresadia = (f"{idempresa}/{nome_sanitizado}/operacional/geral/dias/{particao(dia).replace('/' , '-')}")

        gravar_s3(f"{destinoespecificodia}/kpis.json", kpis)

        gravar_s3(f"{destinoespecificodia}/series.json", {
            "resolucao": "1h",
            "pontos": series
        })


                
    for chave in dados_sessoes:

        id_farm = chave[0]
        id_sessao = chave[1]

        dados = pd.concat(dados_sessoes[chave], ignore_index=True)

        dados["timestamp"] = pd.to_datetime(dados["timestamp"], utc=True)

        metricas = [c for c in dados.columns if c in NUMERICAS]

        kpis = calcular_kpis(dados, metricas, None)
        series = calcular_series(dados, metricas, None)

        destinogeralsessaodia = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/sessao={id_sessao}/dias/{particao(dia).replace('/' , '-')}")
                    

        gravar_s3(f"{destinogeralsessaodia}/kpis.json", kpis)

        gravar_s3(f"{destinogeralsessaodia}/series.json", {
            "resolucao": "1h",
            "pontos": series
        })



    print("teste")