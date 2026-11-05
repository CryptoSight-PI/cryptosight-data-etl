from configenvironment import s3 , bucket_silver , bucket_gold , cursor
from CURATED.utils import sanitizar_nome, buscar_farm , buscar_sessoes , buscar_maquinas_sessao
from raw_to_trusted import buscar_empresa, particao, NUMERICAS
import pandas as pd
import io
import json


FUSO_LOCAL = "America/Sao_Paulo"


def formatar_timestamp(timestamp):
    return timestamp.tz_convert(FUSO_LOCAL).isoformat(timespec="seconds")

def gravar_s3(chave, conteudo):
    s3.put_object(
        Bucket=bucket_gold,
        Key=chave,
        Body=json.dumps({"gerado_em": formatar_timestamp(pd.Timestamp.now(tz="UTC")), "dados": conteudo}, ensure_ascii=False, indent=2),
        ContentType="application/json",
    )


def calcular_kpis(dados, metricas , idmaquina):
    kpis = {}
    for i in metricas:
        serie = dados[i].dropna()
        if serie.empty:
            kpis[i] = {
                        "sum": None,
                        "count": 0,
                        "avg": None,
                        "min": None,
                        "max": None,
                        "desvio_padrao": None,
                        "max_em": None,
                        "max_maquina_id": None,
                    }
            continue


        #indice do max (maior valor)
        pico = serie.idxmax()
 
        kpis[i] = {
            "sum": float(serie.sum()),
            "count": int(serie.count()),
            "avg": float(serie.mean()),
            "min": float(serie.min()),
            "max": float(serie.max()),
            "desvio_padrao": float(serie.std()) if serie.count() > 1 else None,
            "max_em": formatar_timestamp(dados.loc[pico, "timestamp"]),
            "max_maquina_id": int(idmaquina) if idmaquina is not None else int(dados.loc[pico , "_id_maquina"]),
        }
    return kpis


def calcular_series(dados, metricas , idmaquina):

    dados = dados.copy()

    dados["timestamp_local"] = (
        dados["timestamp"].dt.tz_convert(FUSO_LOCAL)
    )

    listaserie = []
    for hora, df in dados.groupby(dados["timestamp_local"].dt.floor("60min")):
        listaserie.append({
            "t": formatar_timestamp(hora),
            "amostras": len(df),
            "metricas": calcular_kpis(df, metricas , idmaquina),
        })
    return listaserie

def juntar_metricas(destino, kpis, series):
    gravar_s3(f"{destino}/metricas.json", {
        "kpis": kpis,
        "serie": {
            "resolucao": "1h",
            "pontos": series
        }
    })


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

        # gravar_s3(f"{destino}/janelas/{nome}/kpis.json", kpis)

        # gravar_s3(f"{destino}/janelas/{nome}/series.json", {
        #     "resolucao": "1h",
        #     "pontos": series
        # })
        juntar_metricas(f"{destino}/janelas/{nome}", kpis , series)



def gerar_atual(dados):
    # o sort serve pra organizar pelo timestamp
    #o iloc -1 pega o ultimo
    ultima = dados.sort_values("timestamp").iloc[-1]
    metricas = ultima[NUMERICAS]
    # isso para evitar o nan no json, pq quebra no front dps, dai bota none
    # where é tipo where(condicao , valor se falso)
    metricas = metricas.astype(object).where(metricas.notna(), None)


    atual = {
        "timestamp": formatar_timestamp(ultima["timestamp"]),
        "metricas": metricas[NUMERICAS].to_dict()
    }

    return atual


def gerar_atual_geral(dados):

    maquinas = []

    for id_maquina, grupo in dados.groupby("_id_maquina"):
        # o group by vai separar todos os dados por maquina , em que id_maquina é esse id e grupo é os dados dessa maquina esepcifica

        atual = gerar_atual(grupo)
        # vai pegar os dados dessa maquina especifica e gerar o atual

        atual["id_maquina"] = int(id_maquina)
        # agora colocando o id da maquina que nao tinha no dicionario que vinha de gerar atual

        maquinas.append(atual)

    return {
        "total_maquinas": len(maquinas),
        "maquinas": maquinas
    }





def ler_trusted(prefixo, maquinas):

    dados_maquinas = {}
    dados_sessoes = {}
    dados_farms = {}
    dados_empresa = []
    
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_silver, Prefix=prefixo):
        for i in page.get("Contents", []):
            key = i["Key"]
            nome_arquivo = key.rsplit("/", 1)[-1]
    
                
            if not (nome_arquivo.endswith(".csv")):
                continue
    
            conteudo = s3.get_object(Bucket=bucket_silver, Key=key)["Body"].read()
            df = pd.read_csv(io.BytesIO(conteudo), sep=";")

            df["timestamp"] = pd.to_datetime(df["timestamp"],utc=True,format="mixed")
            
            if "user" not in df.columns:
                print(f"Coluna user não encontrada em {nome_arquivo}: {df.columns.tolist()}")
                continue
    
            usuarios = df["user"].astype(str).str.strip().str.lower().str.replace("-", ":", regex=False)
    
            for id_farm, id_sessao, id_maquina, mac in maquinas:
                # cada loop filtra pelo mac / maquina
                dados = df[usuarios == mac].copy()
                if dados.empty:
                    continue
                dados["_id_maquina"] = id_maquina
                # adiciona o id da maquina pra conseguir saber qual maquina que é, tipo em dados_sessoes vai ter varios df nesse dicionario, e oq separa os df é o id maquina 
                    
                dados_maquinas.setdefault((id_farm, id_sessao , id_maquina), []).append(dados)
                dados_sessoes.setdefault((id_farm, id_sessao), []).append(dados)
                # se nao tiver no dicionario, adiciona e atribui um vetor vazio a essa tupla, tipo, (1 , 4) : []
                #e se ja tiver essa sessao, ou seja, ja tem maquina nessa sessao, apenas adiciona no vetor
                # tipo assim {
                    # (1, 10, 100): [],
                    # (1, 10, 101): []
                            # }
                dados_farms.setdefault(id_farm, []).append(dados)
                dados_empresa.append(dados)
    

    return dados_maquinas, dados_sessoes, dados_farms, dados_empresa




def carregar_historico_janelas(idempresa, maquinas, agora):

    inicio = agora - pd.Timedelta(days=30)
    # pega os ultimos 30 dias

    meses = pd.period_range(
        start=inicio.strftime("%Y-%m"),
        end=agora.strftime("%Y-%m"),
        freq="M"
    )
    # todos os meses desse periodo dos ultimos 30 dias


    # nao usa logica do ler_trusted pq iria sobrescrever, ja que muda o prefixo e o ler trusted zera o dicionario quando tem prefixo novo

    #mas na pratica segue a mesma logica do ler trusted, mas o dicionario é criado antes entao nao sobrescreve, le o s3 pelo prefixo

    dados_maquinas = {}
    dados_sessoes = {}
    dados_farms = {}
    dados_empresa = []

    paginator = s3.get_paginator("list_objects_v2")

    for mes in meses:

        prefixo = (f"{idempresa}/{mes.year}/{mes.month:02d}/")
        # o 02d serve para garantir que o mes vai ter dois digitos , tipo 09 ao inves de 9

        for page in paginator.paginate(
            Bucket=bucket_silver,
            Prefix=prefixo
        ):

            for i in page.get("Contents", []):

                key = i["Key"]
                nome_arquivo = key.rsplit("/", 1)[-1]

                if not (nome_arquivo.endswith(".csv")):
                    continue

                conteudo = s3.get_object(
                    Bucket=bucket_silver,
                    Key=key
                )["Body"].read()

                df = pd.read_csv(io.BytesIO(conteudo), sep=";")


                df["timestamp"] = pd.to_datetime(df["timestamp"],utc=True,format="mixed")

                if "user" not in df.columns:
                    continue

                usuarios = (df["user"].astype(str).str.strip().str.lower().str.replace("-", ":", regex=False))

                for id_farm, id_sessao, id_maquina, mac in maquinas:

                    dados = df[usuarios == mac].copy()

                    if dados.empty:
                        continue

                    dados["_id_maquina"] = id_maquina

                    dados_maquinas.setdefault((id_farm, id_sessao, id_maquina), []).append(dados)

                    dados_sessoes.setdefault((id_farm, id_sessao), []).append(dados)

                    dados_farms.setdefault(id_farm, []).append(dados)

                    dados_empresa.append(dados)

    return dados_maquinas, dados_sessoes, dados_farms, dados_empresa



def gravar_catalogo(idempresa):
    cursor.execute("""select f.id, f.nome, s.id, s.nome, m.id, m.modelo , m.tipo from empresa e join farm f on f.id_empresa = e.id join sessao s on s.id_farm = f.id join maquina m on m.id_sessao = s.id where e.id = %s
        """, (idempresa,))
    maquinas = []
    for farmid , farmnome, sessaoid , sessaonome , maqid , maqmodelo , maqtipo in cursor.fetchall():
        maquinas.append({"id_farm" : farmid , "farm" : farmnome , "id_sessao" : sessaoid , "sessao" : sessaonome , "id_maquina" : maqid , "modelo" : maqmodelo , "tipo" : maqtipo})
    
    gravar_s3(f"{idempresa}/{sanitizar_nome(idempresa)}/catalogo/estrutura.json", {"maquinas": maquinas})



    cursor.execute("""select mc.id ,  mc.id_maquina, c.nome, mc.limite_min, mc.limite_max, mc.monitorado from empresa e join farm f on f.id_empresa = e.id join sessao s on s.id_farm = f.id join maquina m on m.id_sessao = s.id join maquina_componente mc on mc.id_maquina = m.id join componente c on c.id = mc.id_componente where e.id = %s
        """, (idempresa,))

    limites = []
    for mcid, id_maquina , nomecomponente , limite_min , limite_max , monitorado in cursor.fetchall():
        limites.append({"id" : mcid , "id_maquina" : id_maquina , "componente" : nomecomponente , "limite_min" : limite_min , "limite_max" : limite_max , "monitorado" : monitorado})

    gravar_s3(f"{idempresa}/{sanitizar_nome(idempresa)}/catalogo/limites.json", {"limites": limites})


def main(idempresa, dia):
    nome_sanitizado = sanitizar_nome(buscar_empresa(idempresa))
    data = pd.to_datetime(dia)

    ano = data.strftime("%Y")
    mes = data.strftime("%m")
    dia_numero = data.strftime("%d")

    prefixo_silver = f"{idempresa}/{particao(dia)}/"
    prefixo_silver_mes = f"{idempresa}/{ano}/{mes}/"
    prefixo_silver_ano = f"{idempresa}/{ano}/"

    maquinas = []
    
    for id_farm in buscar_farm(idempresa):
        for id_sessao in buscar_sessoes(id_farm):
            for maquina in buscar_maquinas_sessao(id_sessao):
                mac = str(maquina["mac_address"]).strip().lower().replace("-", ":")
                maquinas.append((id_farm, id_sessao, maquina["id_maquina"], mac))
                # organiza maquinas em tupla, de forma que farm , sessao, maquina . -- (1 , 2 , 5)

    agora = pd.Timestamp.now(tz="UTC")
    
                
    historico_maquinas, historico_sessoes, historico_farms, historico_empresa = (carregar_historico_janelas(idempresa, maquinas, agora))
                


    dados_maquinas , dados_sessoes , dados_farms , dados_empresa = ler_trusted(prefixo_silver , maquinas)

    

    for id_farm , id_sessao, id_maquina in dados_maquinas:
      
        dados = pd.concat(dados_maquinas[(id_farm, id_sessao, id_maquina)], ignore_index=True)

        # atual = gerar_atual(dados)

        # vai juntar os dfs do vetor em um so
        # tipo se tem (1 , 10 , 1) : [df1 , df2 , df3] -> vai criar um df final com os 3 juntos

        metricas = [c for c in dados.columns if c in NUMERICAS]

        kpis = calcular_kpis(dados, metricas , id_maquina)
        series = calcular_series(dados, metricas , id_maquina)

        destinoespecificodia = (f"{idempresa}/{nome_sanitizado}/operacional/especifico/farm={id_farm}/sessao={id_sessao}/maquina={id_maquina}/historico/ano={ano}/mes={mes}/dia={dia_numero}")
     

        # gravar_s3(f"{destinoespecificodia}/kpis.json", kpis)

        # gravar_s3(f"{destinoespecificodia}/series.json", {
        #     "resolucao": "1h",
        #     "pontos": series
        # })

        juntar_metricas(f"{destinoespecificodia}" , kpis , series)

        # gravar_s3(f"{destinoespecificodia}/atual.json", atual)


    
                
    for id_farm , id_sessao in dados_sessoes:

        dados = pd.concat(dados_sessoes[(id_farm , id_sessao)], ignore_index=True)

        atual = gerar_atual_geral(dados)
        # aqui sua geral pq tem varias maquinas na mesma sessao e no  geral separa por maquina 

        metricas = [c for c in dados.columns if c in NUMERICAS]

        kpis = calcular_kpis(dados, metricas, None)
        series = calcular_series(dados, metricas, None)

        destinogeralsessaodia = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/sessao={id_sessao}/historico/ano={ano}/mes={mes}/dia={dia_numero}")             
                          
        # gravar_s3(f"{destinogeralsessaodia}/kpis.json", kpis)

        # gravar_s3(f"{destinogeralsessaodia}/series.json", {
        #     "resolucao": "1h",
        #     "pontos": series
        # })

        juntar_metricas(f"{destinogeralsessaodia}" , kpis , series)

        # gravar_s3(f"{destinogeralsessaodia}/atual.json", atual)



    for id_farm in dados_farms:


        dados = pd.concat(dados_farms[(id_farm)], ignore_index=True)

        atual = gerar_atual_geral(dados)

        metricas = [c for c in dados.columns if c in NUMERICAS]

        kpis = calcular_kpis(dados, metricas, None)
        series = calcular_series(dados, metricas, None)

        destinogeralfarmdia = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/historico/ano={ano}/mes={mes}/dia={dia_numero}")

        
        
        # gravar_s3(f"{destinogeralfarmdia}/kpis.json", kpis)

        # gravar_s3(f"{destinogeralfarmdia}/series.json", {
        #     "resolucao": "1h",
        #     "pontos": series
        # })

        juntar_metricas(f"{destinogeralfarmdia}", kpis , series)

        # gravar_s3(f"{destinogeralfarmdia}/atual.json", atual)

        

    if dados_empresa:

        dados = pd.concat(dados_empresa, ignore_index=True)

        atual = gerar_atual_geral(dados)

        metricas = [c for c in dados.columns if c in NUMERICAS]

        kpis = calcular_kpis(dados, metricas, None)
        series = calcular_series(dados, metricas, None)

        destinogeralempresadia = (f"{idempresa}/{nome_sanitizado}/operacional/geral/historico/ano={ano}/mes={mes}/dia={dia_numero}")


        # gravar_s3(f"{destinogeralempresadia}/kpis.json", kpis)

        # gravar_s3(f"{destinogeralempresadia}/series.json", {
        #     "resolucao": "1h",
        #     "pontos": series
        # })

        juntar_metricas(f"{destinogeralempresadia}", kpis , series)

        # gravar_s3(f"{destinogeralempresadia}/atual.json", atual)

        

    #ESTRUTURA PRA SEPARAR POR MES / IDENTICA AO DIARIA, SO MUDA O CAMINHO QUE O PAGINATOR PEGA DO SILVER

    dados_maquinas_mes , dados_sessoes_mes , dados_farms_mes , dados_empresa_mes = ler_trusted(prefixo_silver_mes , maquinas)

    for id_farm, id_sessao, id_maquina in dados_maquinas_mes:

        dados = pd.concat(dados_maquinas_mes[(id_farm, id_sessao, id_maquina)], ignore_index=True)

        metricas = [c for c in dados.columns if c in NUMERICAS]

        kpis = calcular_kpis(dados, metricas, id_maquina)
        series = calcular_series(dados, metricas, id_maquina)

        destinoespecificomes = (f"{idempresa}/{nome_sanitizado}/operacional/especifico/farm={id_farm}/sessao={id_sessao}/maquina={id_maquina}/historico/ano={ano}/mes={mes}")
        

        # gravar_s3(f"{destinoespecificomes}/kpis.json", kpis)

        # gravar_s3(f"{destinoespecificomes}/series.json", {
        #     "resolucao": "1h",
        #     "pontos": series
        # })

        juntar_metricas(f"{destinoespecificomes}", kpis , series)

        # gravar_s3(f"{destinoespecificomes}/atual.json" , atual)


    for id_farm , id_sessao in dados_sessoes_mes:

        dados = pd.concat(dados_sessoes_mes[(id_farm, id_sessao)], ignore_index=True)

        atual = gerar_atual_geral(dados)

        metricas = [c for c in dados.columns if c in NUMERICAS]

        kpis = calcular_kpis(dados, metricas, None)
        series = calcular_series(dados, metricas, None)

        destinogeralsessaomes = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/sessao={id_sessao}/historico/ano={ano}/mes={mes}")  

        # gravar_s3(f"{destinogeralsessaomes}/kpis.json", kpis)

        # gravar_s3(f"{destinogeralsessaomes}/series.json", {
        #     "resolucao": "1h",
        #     "pontos": series
        # })

        juntar_metricas(f"{destinogeralsessaomes}", kpis  , series)

        # gravar_s3(f"{destinogeralsessaomes}/atual.json" , atual)

    for id_farm in dados_farms_mes:

        dados = pd.concat(dados_farms_mes[(id_farm)], ignore_index=True)

        atual = gerar_atual_geral(dados)

        metricas = [c for c in dados.columns if c in NUMERICAS]

        kpis = calcular_kpis(dados, metricas, None)
        series = calcular_series(dados, metricas, None)

        destinogeralfarmmes = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/historico/ano={ano}/mes={mes}")

        # gravar_s3(f"{destinogeralfarmmes}/kpis.json", kpis)

        # gravar_s3(f"{destinogeralfarmmes}/series.json", {
        #     "resolucao": "1h",
        #     "pontos": series
        # })

        juntar_metricas(f"{destinogeralfarmmes}", kpis , series)

        # gravar_s3(f"{destinogeralfarmmes}/atual.json" , atual)


    if dados_empresa_mes:

        dados = pd.concat(dados_empresa_mes, ignore_index=True)

        atual = gerar_atual_geral(dados)

        metricas = [c for c in dados.columns if c in NUMERICAS]

        kpis = calcular_kpis(dados, metricas, None)
        series = calcular_series(dados, metricas, None)

        destinogeralempresames = (f"{idempresa}/{nome_sanitizado}/operacional/geral/historico/ano={ano}/mes={mes}") 

        # gravar_s3(f"{destinogeralempresames}/kpis.json", kpis)

        # gravar_s3(f"{destinogeralempresames}/series.json", {
        #     "resolucao": "1h",
        #     "pontos": series
        # })

        juntar_metricas(f"{destinogeralempresames}", kpis , series)

        # gravar_s3(f"{destinogeralempresames}/atual.json" , atual)






    # ESTRUTURA PRO ANO

    dados_maquinas_ano , dados_sessoes_ano , dados_farms_ano , dados_empresa_ano = ler_trusted(prefixo_silver_ano , maquinas)

    for id_farm , id_sessao, id_maquina in dados_maquinas_ano:
    
        dados = pd.concat(dados_maquinas_ano[(id_farm , id_sessao , id_maquina)], ignore_index=True)
    
        metricas = [c for c in dados.columns if c in NUMERICAS]
    
        kpis = calcular_kpis(dados, metricas, id_maquina)
        series = calcular_series(dados, metricas, id_maquina)
    
        destinoespecificoano = (f"{idempresa}/{nome_sanitizado}/operacional/especifico/farm={id_farm}/sessao={id_sessao}/maquina={id_maquina}/historico/ano={ano}")
            
    
        # gravar_s3(f"{destinoespecificoano}/kpis.json", kpis)
    
        # gravar_s3(f"{destinoespecificoano}/series.json", {
        #     "resolucao": "1h",
        #     "pontos": series
        # })

        juntar_metricas(f"{destinoespecificoano}", kpis , series)

        # gravar_s3(f"{destinoespecificoano}/atual.json" , atual)
    
    
            
    for id_farm , id_sessao in dados_sessoes_ano:
    
        dados = pd.concat(dados_sessoes_ano[(id_farm , id_sessao)], ignore_index=True)

        atual = gerar_atual_geral(dados)
    
        metricas = [c for c in dados.columns if c in NUMERICAS]
    
        kpis = calcular_kpis(dados, metricas, None)
        series = calcular_series(dados, metricas, None)
    
        destinogeralsessaoano = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/sessao={id_sessao}/historico/ano={ano}")

        # gravar_s3(f"{destinogeralsessaoano}/kpis.json", kpis)
    
        # gravar_s3(f"{destinogeralsessaoano}/series.json", {
        #     "resolucao": "1h",
        #     "pontos": series
        # })

        juntar_metricas(f"{destinogeralsessaoano}", kpis , series)

        # gravar_s3(f"{destinogeralsessaoano}/atual.json" , atual)
    
    for id_farm in dados_farms_ano:
    
        dados = pd.concat(dados_farms_ano[(id_farm)], ignore_index=True)

        atual = gerar_atual_geral(dados)
    
        metricas = [c for c in dados.columns if c in NUMERICAS]
    
        kpis = calcular_kpis(dados, metricas, None)
        series = calcular_series(dados, metricas, None)
    
        destinogeralfarmano = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/historico/ano={ano}") 
    
        # gravar_s3(f"{destinogeralfarmano}/kpis.json", kpis)
    
        # gravar_s3(f"{destinogeralfarmano}/series.json", {
        #     "resolucao": "1h",
        #     "pontos": series
        # })

        juntar_metricas(f"{destinogeralfarmano}", kpis , series)

        # gravar_s3(f"{destinogeralfarmano}/atual.json" , atual)
    
    
    if dados_empresa_ano:
    
        dados = pd.concat(dados_empresa_ano, ignore_index=True)

        atual = gerar_atual_geral(dados)
    
        metricas = [c for c in dados.columns if c in NUMERICAS]
    
        kpis = calcular_kpis(dados, metricas, None)
        series = calcular_series(dados, metricas, None)
    
        destinogeralempresaano = (f"{idempresa}/{nome_sanitizado}/operacional/geral/historico/ano={ano}") 
    
        # gravar_s3(f"{destinogeralempresaano}/kpis.json", kpis)
    
        # gravar_s3(f"{destinogeralempresaano}/series.json", {
        #     "resolucao": "1h",
        #     "pontos": series
        # })

        juntar_metricas(f"{destinogeralempresaano}", kpis , series)

        # gravar_s3(f"{destinogeralempresaano}/atual.json" , atual)
    
    


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

        gravar_s3(f"{destino}/tempo_real/atual.json", gerar_atual(dados))
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

        gravar_s3(f"{destino}/tempo_real/atual.json", gerar_atual_geral(dados))
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

        gravar_s3(f"{destino}/tempo_real/atual.json", gerar_atual_geral(dados))
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

        gravar_s3(f"{destino}/tempo_real/atual.json", gerar_atual_geral(dados))
        gravar_janelas(destino, dados, metricas, None, agora)


    #JA CHAMANDO AQUI
    gravar_catalogo(idempresa)



    print("teste")