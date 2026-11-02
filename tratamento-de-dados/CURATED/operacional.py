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


def gravar_janelas(destino, dados, metricas, idmaquina, agora):

    janelas = {
        "24h": agora - pd.Timedelta(hours=24),
        "7d": agora - pd.Timedelta(days=7),
        "30d": agora - pd.Timedelta(days=30)
    }

    for nome, limite in janelas.items():

        dados_filtrados = dados[
            (dados["timestamp"] >= limite) &
            (dados["timestamp"] <= agora)
        ]

        if dados_filtrados.empty:
            continue

        kpis = calcular_kpis(dados_filtrados, metricas, idmaquina)
        series = calcular_series(dados_filtrados, metricas, idmaquina)

        gravar_s3(f"{destino}/{nome}/kpis.json", kpis)

        gravar_s3(f"{destino}/{nome}/series.json", {
            "resolucao": "1h",
            "pontos": series
        })



def gerar_atual(dados):
    # o sort serve pra organizar pelo timestamp
    #o iloc -1 pega o ultimo
    ultima = dados.sort_values("timestamp").iloc[-1]

    atual = {
        "timestamp": ultima["timestamp"].strftime("%Y-%m-%dT%H:%M:%S"),
        "metricas": ultima[NUMERICAS].to_dict()
    }

    return atual


def gerar_atual_geral(dados):

    maquinas = []

    for id_maquina, grupo in dados.groupby("_id_maquina"):

        atual = gerar_atual(grupo)

        atual["id_maquina"] = int(id_maquina)

        maquinas.append(atual)

    return {
        "total_maquinas": len(maquinas),
        "maquinas": maquinas
    }


def vetor_para_numero(texto, modo):
    if not isinstance(texto, str):
        return None

    valores = []
    for parte in texto.strip("[] ").split(","):
        try:
            numero = float(parte)
        except ValueError:
            return None
        if numero != numero:
            return None
        valores.append(numero)

    if not valores:
        return None

    if modo == "max":
        return max(valores)
    if modo == "sum":
        return sum(valores)
    return sum(valores) / len(valores)



def carregar_historico_janelas(idempresa, maquinas, agora):

    inicio = agora - pd.Timedelta(days=30)

    meses = pd.period_range(
        start=inicio.strftime("%Y-%m"),
        end=agora.strftime("%Y-%m"),
        freq="M"
    )

    dados_maquinas = {}
    dados_sessoes = {}
    dados_farms = {}
    dados_empresa = []

    paginator = s3.get_paginator("list_objects_v2")

    for mes_historico in meses:

        prefixo = (
            f"{idempresa}/"
            f"ano={mes_historico.year}/"
            f"mes={mes_historico.month:02d}/"
        )

        for page in paginator.paginate(
            Bucket=bucket_silver,
            Prefix=prefixo
        ):

            for i in page.get("Contents", []):

                key = i["Key"]
                nome_arquivo = key.rsplit("/", 1)[-1]

                if not nome_arquivo.endswith(".csv"):
                    continue

                conteudo = s3.get_object(
                    Bucket=bucket_silver,
                    Key=key
                )["Body"].read()

                df = pd.read_csv(
                    io.BytesIO(conteudo),
                    sep=";"
                )

                for coluna in COLUNAS_VETOR:

                    if coluna not in df.columns:
                        continue

                    if coluna == "gpu_temp":
                        modo = "max"
                    elif coluna == "gpu_energy":
                        modo = "sum"
                    else:
                        modo = "mean"

                    numeros = []

                    for texto in df[coluna]:
                        numeros.append(
                            vetor_para_numero(texto, modo)
                        )

                    df[coluna] = numeros

                df["timestamp"] = pd.to_datetime(
                    df["timestamp"],
                    utc=True,
                    format="mixed"
                )

                if "user" not in df.columns:
                    continue

                usuarios = (df["user"].astype(str).str.strip().str.lower().str.replace("-", ":", regex=False))

                for id_farm, id_sessao, id_maquina, mac in maquinas:

                    dados = df[usuarios == mac].copy()

                    if dados.empty:
                        continue

                    dados["_id_maquina"] = id_maquina

                    chave = (id_farm, id_sessao, id_maquina)

                    dados_maquinas.setdefault(chave, []).append(dados)

                    dados_sessoes.setdefault(
                        (id_farm, id_sessao), []
                    ).append(dados)

                    dados_farms.setdefault(
                        id_farm, []
                    ).append(dados)

                    dados_empresa.append(dados)

    return dados_maquinas, dados_sessoes, dados_farms, dados_empresa



def main(idempresa, dia):
    nome_sanitizado = sanitizar_nome(buscar_empresa(idempresa))
    data = pd.to_datetime(dia)

    ano = data.strftime("%Y")
    mes = data.strftime("%m")
    dia_numero = data.strftime("%d")

    prefixo_silver = f"{idempresa}/{particao(dia)}/"
    prefixo_silver_mes = f"{idempresa}/ano={ano}/mes={mes}/"
    prefixo_silver_ano = f"{idempresa}/ano={ano}/"

    maquinas = []
    
    for id_farm in buscar_farm(idempresa):
        for id_sessao in buscar_sessoes(id_farm):
            for maquina in buscar_maquinas_sessao(id_sessao):
                mac = str(maquina["mac_address"]).strip().lower().replace("-", ":")
                maquinas.append((id_farm, id_sessao, maquina["id_maquina"], mac))

    agora = pd.Timestamp.now(tz="UTC")
                
    historico_maquinas, historico_sessoes, historico_farms, historico_empresa = (carregar_historico_janelas(idempresa, maquinas, agora))
                


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
            for coluna in COLUNAS_VETOR:
                if coluna not in df.columns:
                    continue

                if coluna == "gpu_temp":
                    modo = "max"
                elif coluna == "gpu_energy":
                    modo = "sum"
                else:
                    modo = "mean"

                numeros = []
                for texto in df[coluna]:
                    numeros.append(vetor_para_numero(texto, modo))
                df[coluna] = numeros

            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="mixed")

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

        atual = gerar_atual(dados)

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

        gravar_s3(f"{destinoespecificodia}/atual.json", atual)


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



        # gravar_janelas(destinoespecifico, kpis, series)
        # gravar_janelas(destinoespecifico, dados, metricas, id_maquina)
        
        


                
    for chave in dados_sessoes:

        id_farm = chave[0]
        id_sessao = chave[1]

        dados = pd.concat(dados_sessoes[chave], ignore_index=True)

        dados["timestamp"] = pd.to_datetime(dados["timestamp"], utc=True)

        atual = gerar_atual_geral(dados)

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

        gravar_s3(f"{destinogeralsessaodia}/atual.json", atual)


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

        # gravar_janelas(destinogeralsessao, kpis , series)
        # gravar_janelas(destinogeralsessao, dados, metricas, None)


    for chave in dados_farms:

        id_farm = chave

        dados = pd.concat(dados_farms[chave], ignore_index=True)

        dados["timestamp"] = pd.to_datetime(dados["timestamp"], utc=True)

        atual = gerar_atual_geral(dados)

        metricas = [c for c in dados.columns if c in NUMERICAS]

        kpis = calcular_kpis(dados, metricas, None)
        series = calcular_series(dados, metricas, None)

        destinogeralfarmdia = (
            f"{idempresa}/{nome_sanitizado}/operacional/geral/"
            f"farm={id_farm}/ano={ano}/mes={mes}/dia={dia_numero}"
        )

        destinogeralfarmmes = (
            f"{idempresa}/{nome_sanitizado}/operacional/geral/"
            f"farm={id_farm}/ano={ano}/mes={mes}"
        )

        destinogeralfarmano = (
            f"{idempresa}/{nome_sanitizado}/operacional/geral/"
            f"farm={id_farm}/ano={ano}"
        )

        destinogeralfarm = (
            f"{idempresa}/{nome_sanitizado}/operacional/geral/"
            f"farm={id_farm}"
        )

        
        gravar_s3(f"{destinogeralfarmdia}/kpis.json", kpis)

        gravar_s3(f"{destinogeralfarmdia}/series.json", {
            "resolucao": "1h",
            "pontos": series
        })

        gravar_s3(f"{destinogeralfarmdia}/atual.json", atual)

        
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

        
        # gravar_janelas(destinogeralfarm, dados, metricas, None)





        

    if dados_empresa:

        dados = pd.concat(dados_empresa, ignore_index=True)

        dados["timestamp"] = pd.to_datetime(dados["timestamp"], utc=True)

        atual = gerar_atual_geral(dados)

        metricas = [c for c in dados.columns if c in NUMERICAS]

        kpis = calcular_kpis(dados, metricas, None)
        series = calcular_series(dados, metricas, None)

        destinogeralempresadia = (
            f"{idempresa}/{nome_sanitizado}/operacional/geral/"
            f"ano={ano}/mes={mes}/dia={dia_numero}"
        )

        destinogeralempresames = (
            f"{idempresa}/{nome_sanitizado}/operacional/geral/"
            f"ano={ano}/mes={mes}"
        )

        destinogeralempresaano = (
            f"{idempresa}/{nome_sanitizado}/operacional/geral/"
            f"ano={ano}"
        )

        destinogeralempresa = (
            f"{idempresa}/{nome_sanitizado}/operacional/geral"
        )

        
        gravar_s3(f"{destinogeralempresadia}/kpis.json", kpis)

        gravar_s3(f"{destinogeralempresadia}/series.json", {
            "resolucao": "1h",
            "pontos": series
        })

        gravar_s3(f"{destinogeralempresadia}/atual.json", atual)

        
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

        
        # gravar_janelas(destinogeralempresa, kpis, series)
        # gravar_janelas(destinogeralempresa, dados, metricas, None)





    #ESTRUTURA PRA SEPARAR POR MES / IDENTICA AO DIARIA, SO MUDA O CAMINHO QUE O PAGINATOR PEGA DO SILVER

    dados_maquinas_mes = {}
    dados_sessoes_mes = {}
    dados_farms_mes = {}
    dados_empresa_mes = []


    for page in paginator.paginate(Bucket=bucket_silver, Prefix=prefixo_silver_mes):
        for i in page.get("Contents", []):
            key = i["Key"]
            nome_arquivo = key.rsplit("/", 1)[-1]
    
                
            # if not (nome_arquivo.startswith("clean_data") and nome_arquivo.endswith(".csv")):
            if not nome_arquivo.endswith(".csv"):
                continue
    
            conteudo = s3.get_object(Bucket=bucket_silver, Key=key)["Body"].read()
            df = pd.read_csv(io.BytesIO(conteudo), sep=";")
            for coluna in COLUNAS_VETOR:
                if coluna not in df.columns:
                    continue
    
                if coluna == "gpu_temp":
                    modo = "max"
                elif coluna == "gpu_energy":
                    modo = "sum"
                else:
                    modo = "mean"
    
                numeros = []
                for texto in df[coluna]:
                    numeros.append(vetor_para_numero(texto, modo))
                df[coluna] = numeros
    
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="mixed")
    
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

                dados_maquinas_mes.setdefault(chave, []).append(dados)
                dados_sessoes_mes.setdefault((id_farm, id_sessao), []).append(dados)
                dados_farms_mes.setdefault(id_farm, []).append(dados)
                dados_empresa_mes.append(dados)

    for chave in dados_maquinas_mes:
        id_farm = chave[0]
        id_sessao = chave[1]
        id_maquina = chave[2]

        dados = pd.concat(dados_maquinas_mes[chave], ignore_index=True)

        # atual = gerar_atual(dados)

        dados["timestamp"] = pd.to_datetime(dados["timestamp"], utc=True)

        metricas = [c for c in dados.columns if c in NUMERICAS]

        kpis = calcular_kpis(dados, metricas, id_maquina)
        series = calcular_series(dados, metricas, id_maquina)

        destinoespecificomes = (f"{idempresa}/{nome_sanitizado}/operacional/especifico/farm={id_farm}/sessao={id_sessao}/maquina={id_maquina}/ano={ano}/mes={mes}")
        

        gravar_s3(f"{destinoespecificomes}/kpis.json", kpis)

        gravar_s3(f"{destinoespecificomes}/series.json", {
            "resolucao": "1h",
            "pontos": series
        })

        # gravar_s3(f"{destinoespecificomes}/atual.json" , atual)


    for chave in dados_sessoes_mes:

        id_farm = chave[0]
        id_sessao = chave[1]

        dados = pd.concat(dados_sessoes_mes[chave], ignore_index=True)

        atual = gerar_atual_geral(dados)

        dados["timestamp"] = pd.to_datetime(dados["timestamp"], utc=True)

        metricas = [c for c in dados.columns if c in NUMERICAS]

        kpis = calcular_kpis(dados, metricas, None)
        series = calcular_series(dados, metricas, None)

        destinogeralsessaomes = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/sessao={id_sessao}/ano={ano}/mes={mes}")  

        gravar_s3(f"{destinogeralsessaomes}/kpis.json", kpis)

        gravar_s3(f"{destinogeralsessaomes}/series.json", {
            "resolucao": "1h",
            "pontos": series
        })

        gravar_s3(f"{destinogeralsessaomes}/atual.json" , atual)

    for chave in dados_farms_mes:

        id_farm = chave

        dados = pd.concat(dados_farms_mes[chave], ignore_index=True)

        atual = gerar_atual_geral(dados)

        dados["timestamp"] = pd.to_datetime(dados["timestamp"], utc=True)

        metricas = [c for c in dados.columns if c in NUMERICAS]

        kpis = calcular_kpis(dados, metricas, None)
        series = calcular_series(dados, metricas, None)

        destinogeralfarmmes = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/ano={ano}/mes={mes}")

        gravar_s3(f"{destinogeralfarmmes}/kpis.json", kpis)

        gravar_s3(f"{destinogeralfarmmes}/series.json", {
            "resolucao": "1h",
            "pontos": series
        })

        gravar_s3(f"{destinogeralfarmmes}/atual.json" , atual)


    if dados_empresa_mes:

        dados = pd.concat(dados_empresa_mes, ignore_index=True)

        atual = gerar_atual_geral(dados)

        dados["timestamp"] = pd.to_datetime(dados["timestamp"], utc=True)

        metricas = [c for c in dados.columns if c in NUMERICAS]

        kpis = calcular_kpis(dados, metricas, None)
        series = calcular_series(dados, metricas, None)

        destinogeralempresames = (f"{idempresa}/{nome_sanitizado}/operacional/geral/ano={ano}/mes={mes}") 

        gravar_s3(f"{destinogeralempresames}/kpis.json", kpis)

        gravar_s3(f"{destinogeralempresames}/series.json", {
            "resolucao": "1h",
            "pontos": series
        })

        gravar_s3(f"{destinogeralempresames}/atual.json" , atual)






    # ESTRUTURA PRO ANO

    dados_maquinas_ano = {}
    dados_sessoes_ano = {}
    dados_farms_ano = {}
    dados_empresa_ano = []



    for page in paginator.paginate(Bucket=bucket_silver, Prefix=prefixo_silver_ano):
        for i in page.get("Contents", []):
            key = i["Key"]
            nome_arquivo = key.rsplit("/", 1)[-1]
        
                    
            # if not (nome_arquivo.startswith("clean_data") and nome_arquivo.endswith(".csv")):
            if not nome_arquivo.endswith(".csv"):
                continue
        
            conteudo = s3.get_object(Bucket=bucket_silver, Key=key)["Body"].read()
            df = pd.read_csv(io.BytesIO(conteudo), sep=";")
            for coluna in COLUNAS_VETOR:
                if coluna not in df.columns:
                    continue
        
                if coluna == "gpu_temp":
                    modo = "max"
                elif coluna == "gpu_energy":
                    modo = "sum"
                else:
                    modo = "mean"
        
                numeros = []
                for texto in df[coluna]:
                    numeros.append(vetor_para_numero(texto, modo))
                df[coluna] = numeros
        
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="mixed")
        
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
    
                dados_maquinas_ano.setdefault(chave, []).append(dados)
                dados_sessoes_ano.setdefault((id_farm, id_sessao), []).append(dados)
                dados_farms_ano.setdefault(id_farm, []).append(dados)
                dados_empresa_ano.append(dados)

    for chave in dados_maquinas_ano:
        id_farm = chave[0]
        id_sessao = chave[1]
        id_maquina = chave[2]
    
        dados = pd.concat(dados_maquinas_ano[chave], ignore_index=True)

        # atual = gerar_atual(dados)
    
        dados["timestamp"] = pd.to_datetime(dados["timestamp"], utc=True)
    
        metricas = [c for c in dados.columns if c in NUMERICAS]
    
        kpis = calcular_kpis(dados, metricas, id_maquina)
        series = calcular_series(dados, metricas, id_maquina)
    
        destinoespecificoano = (f"{idempresa}/{nome_sanitizado}/operacional/especifico/farm={id_farm}/sessao={id_sessao}/maquina={id_maquina}/ano={ano}")
            
    
        gravar_s3(f"{destinoespecificoano}/kpis.json", kpis)
    
        gravar_s3(f"{destinoespecificoano}/series.json", {
            "resolucao": "1h",
            "pontos": series
        })

        # gravar_s3(f"{destinoespecificoano}/atual.json" , atual)
    
    
            
    for chave in dados_sessoes_ano:
    
        id_farm = chave[0]
        id_sessao = chave[1]
    
        dados = pd.concat(dados_sessoes_ano[chave], ignore_index=True)

        atual = gerar_atual_geral(dados)
    
        dados["timestamp"] = pd.to_datetime(dados["timestamp"], utc=True)
    
        metricas = [c for c in dados.columns if c in NUMERICAS]
    
        kpis = calcular_kpis(dados, metricas, None)
        series = calcular_series(dados, metricas, None)
    
        destinogeralsessaoano = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/sessao={id_sessao}/ano={ano}")

        gravar_s3(f"{destinogeralsessaoano}/kpis.json", kpis)
    
        gravar_s3(f"{destinogeralsessaoano}/series.json", {
            "resolucao": "1h",
            "pontos": series
        })

        gravar_s3(f"{destinogeralsessaoano}/atual.json" , atual)
    
    for chave in dados_farms_ano:
    
        id_farm = chave
    
        dados = pd.concat(dados_farms_ano[chave], ignore_index=True)

        atual = gerar_atual_geral(dados)
    
        dados["timestamp"] = pd.to_datetime(dados["timestamp"], utc=True)
    
        metricas = [c for c in dados.columns if c in NUMERICAS]
    
        kpis = calcular_kpis(dados, metricas, None)
        series = calcular_series(dados, metricas, None)
    
        destinogeralfarmano = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/ano={ano}") 
    
        gravar_s3(f"{destinogeralfarmano}/kpis.json", kpis)
    
        gravar_s3(f"{destinogeralfarmano}/series.json", {
            "resolucao": "1h",
            "pontos": series
        })

        gravar_s3(f"{destinogeralfarmano}/atual.json" , atual)
    
    
    if dados_empresa_ano:
    
        dados = pd.concat(dados_empresa_ano, ignore_index=True)
    
        dados["timestamp"] = pd.to_datetime(dados["timestamp"], utc=True)

        atual = gerar_atual_geral(dados)
    
        metricas = [c for c in dados.columns if c in NUMERICAS]
    
        kpis = calcular_kpis(dados, metricas, None)
        series = calcular_series(dados, metricas, None)
    
        destinogeralempresaano = (f"{idempresa}/{nome_sanitizado}/operacional/geral/ano={ano}") 
    
        gravar_s3(f"{destinogeralempresaano}/kpis.json", kpis)
    
        gravar_s3(f"{destinogeralempresaano}/series.json", {
            "resolucao": "1h",
            "pontos": series
        })

        gravar_s3(f"{destinogeralempresaano}/atual.json" , atual)
    
    


    # JANELAS de 24  7  30 - segue a mesma logica do resto

    for chave in historico_maquinas:

        id_farm, id_sessao, id_maquina = chave

        dados = pd.concat(
            historico_maquinas[chave],
            ignore_index=True
        )

        metricas = [c for c in dados.columns if c in NUMERICAS]

        destino = (
            f"{idempresa}/{nome_sanitizado}/operacional/especifico/"
            f"farm={id_farm}/sessao={id_sessao}/maquina={id_maquina}"
        )

        gravar_janelas(destino, dados, metricas, id_maquina, agora)


    for chave in historico_sessoes:

        id_farm, id_sessao = chave

        dados = pd.concat(
            historico_sessoes[chave],
            ignore_index=True
        )

        metricas = [c for c in dados.columns if c in NUMERICAS]

        destino = (
            f"{idempresa}/{nome_sanitizado}/operacional/geral/"
            f"farm={id_farm}/sessao={id_sessao}"
        )

        gravar_janelas(destino, dados, metricas, None, agora)


    for id_farm in historico_farms:

        dados = pd.concat(
            historico_farms[id_farm],
            ignore_index=True
        )

        metricas = [c for c in dados.columns if c in NUMERICAS]

        destino = (
            f"{idempresa}/{nome_sanitizado}/operacional/geral/"
            f"farm={id_farm}"
        )

        gravar_janelas(destino, dados, metricas, None, agora)


    if historico_empresa:

        dados = pd.concat(
            historico_empresa,
            ignore_index=True
        )

        metricas = [c for c in dados.columns if c in NUMERICAS]

        destino = (
            f"{idempresa}/{nome_sanitizado}/operacional/geral"
        )

        gravar_janelas(destino, dados, metricas, None, agora)


        



            
        


#TODOS OS ARQUIVOS QUE GRAVEI NO S3 SAO PRA TESTE POR ENQUANTO SO PRA VER O PARTICIONAMENTO

    print("teste")