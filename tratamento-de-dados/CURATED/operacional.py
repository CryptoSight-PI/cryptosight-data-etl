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


def gravar_janelas(destino, kpis, series):

    for janela in ["24h", "7d", "30d"]:

        gravar_s3(f"{destino}/janelas/{janela}.json", {
            "kpis": kpis,
            "resolucao": "1h",
            "pontos": series
        })



def main(idempresa, dia):
    nome_sanitizado = sanitizar_nome(buscar_empresa(idempresa))
    data = pd.to_datetime(dia)

    ano = data.strftime("%Y")
    mes = data.strftime("%m")
    dia_numero = data.strftime("%d")

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

        destinoespecificodia = (f"{idempresa}/{nome_sanitizado}/operacional/especifico/farm={id_farm}/sessao={id_sessao}/maquina={id_maquina}/ano={ano}/mes={mes}/dia={dia_numero}")
     
        destinoespecificomes = (f"{idempresa}/{nome_sanitizado}/operacional/especifico/farm={id_farm}/sessao={id_sessao}/maquina={id_maquina}/ano={ano}/mes={mes}")

        destinoespecificoano = (f"{idempresa}/{nome_sanitizado}/operacional/especifico/farm={id_farm}/sessao={id_sessao}/maquina={id_maquina}/ano={ano}")

        destinoespecifico = (f"{idempresa}/{nome_sanitizado}/operacional/especifico/farm={id_farm}/sessao={id_sessao}/maquina={id_maquina}")

         
                

                    
        # destinogeralfarmdia = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/{particao(dia)}")

        # destinogeralempresadia = (f"{idempresa}/{nome_sanitizado}/operacional/geral")

        gravar_s3(f"{destinoespecificodia}/kpis.json", kpis)

        gravar_s3(f"{destinoespecificodia}/series.json", {
            "resolucao": "1h",
            "pontos": series
        })


        #Apenas para TESTE nao esta funcionando REALMENTE ->


        gravar_s3(f"{destinoespecificomes}/kpis.json", kpis)
        
        gravar_s3(f"{destinoespecificomes}/series.json", {
            "resolucao": "1h",
            "pontos": series
        })


        gravar_s3(f"{destinoespecificoano}/kpis.json", kpis)
        
        gravar_s3(f"{destinoespecificoano}/series.json", {
            "resolucao": "1h",
            "pontos": series
        })



        gravar_janelas(destinoespecifico, kpis, series)
        
        


                
    for chave in dados_sessoes:

        id_farm = chave[0]
        id_sessao = chave[1]

        dados = pd.concat(dados_sessoes[chave], ignore_index=True)

        dados["timestamp"] = pd.to_datetime(dados["timestamp"], utc=True)

        metricas = [c for c in dados.columns if c in NUMERICAS]

        kpis = calcular_kpis(dados, metricas, None)
        series = calcular_series(dados, metricas, None)

        destinogeralsessaodia = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/sessao={id_sessao}/ano={ano}/mes={mes}/dia={dia_numero}")
        destinogeralsessaomes = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/sessao={id_sessao}/ano={ano}/mes={mes}")  
        destinogeralsessaoano = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/sessao={id_sessao}/ano={ano}") 
        destinogeralsessao = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/sessao={id_sessao}")     

        destinogeralfarmdia = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/ano={ano}/mes={mes}/dia={dia_numero}")
        destinogeralfarmmes = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/ano={ano}/mes={mes}")  
        destinogeralfarmano = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/ano={ano}") 
        destinogeralfarm = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}") 
        

        destinogeralempresadia = (f"{idempresa}/{nome_sanitizado}/operacional/geral/ano={ano}/mes={mes}/dia={dia_numero}")
        destinogeralempresames = (f"{idempresa}/{nome_sanitizado}/operacional/geral/ano={ano}/mes={mes}")  
        destinogeralempresaano = (f"{idempresa}/{nome_sanitizado}/operacional/geral/ano={ano}")
        destinogeralempresa = (f"{idempresa}/{nome_sanitizado}/operacional/geral")                  
                          

        gravar_s3(f"{destinogeralsessaodia}/kpis.json", kpis)

        gravar_s3(f"{destinogeralsessaodia}/series.json", {
            "resolucao": "1h",
            "pontos": series
        })


        #apenas para TESTE nao esta funcionando REALMENTE

        gravar_s3(f"{destinogeralsessaomes}/kpis.json", kpis)
        
        gravar_s3(f"{destinogeralsessaomes}/series.json", {
            "resolucao": "1h",
            "pontos": series
        })


        gravar_s3(f"{destinogeralsessaoano}/kpis.json", kpis)
                
        gravar_s3(f"{destinogeralsessaoano}/series.json", {
            "resolucao": "1h",
            "pontos": series
        })

        gravar_janelas(destinogeralsessao, kpis , series)

        gravar_s3(f"{destinogeralfarmdia}/kpis.json", kpis)
                        
        gravar_s3(f"{destinogeralfarmdia}/series.json", {
            "resolucao": "1h",
            "pontos": series
        })


        gravar_s3(f"{destinogeralfarmmes}/kpis.json", kpis)
                                
        gravar_s3(f"{destinogeralfarmmes}/series.json", {
            "resolucao": "1h",
            "pontos": series
        })


        gravar_s3(f"{destinogeralfarmano}/kpis.json", kpis)
                                        
        gravar_s3(f"{destinogeralfarmano}/series.json", {
            "resolucao": "1h",
            "pontos": series
        })

        gravar_janelas(destinogeralfarm, kpis , series)


        gravar_s3(f"{destinogeralempresadia}/kpis.json", kpis)
                                                
        gravar_s3(f"{destinogeralempresadia}/series.json", {
            "resolucao": "1h",
            "pontos": series
        })


        gravar_s3(f"{destinogeralempresames}/kpis.json", kpis)
                                                        
        gravar_s3(f"{destinogeralempresames}/series.json", {
            "resolucao": "1h",
            "pontos": series
        })


        gravar_s3(f"{destinogeralempresaano}/kpis.json", kpis)
                                                                
        gravar_s3(f"{destinogeralempresaano}/series.json", {
            "resolucao": "1h",
            "pontos": series
        })


        gravar_janelas(destinogeralempresa, kpis , series)

#TODOS OS ARQUIVOS QUE GRAVEI NO S3 SAO PRA TESTE POR ENQUANTO SO PRA VER O PARTICIONAMENTO

    print("teste")