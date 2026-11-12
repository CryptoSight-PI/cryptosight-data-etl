from configenvironment import s3 , bucket_silver , bucket_gold , cursor
from CURATED.utils import sanitizar_nome, buscar_farm , buscar_sessoes , buscar_maquinas_sessao
from raw_to_trusted import buscar_empresa, particao, NUMERICAS
import pandas as pd
import io
import json


FUSO_LOCAL = "America/Sao_Paulo"

# ver se vai definir isso como os limites ou vai pode escolher frequencia e outras metricas adicionais
# ou seja provavelmente vai mudar isso
MAPA_COMPONENTES = {
    "cpu": "cpu_percent",
    "ram": "ram_percent",
    "swap": "swap_memory_percent",
    # "rede": "download_speed",
    # fica pra persona de rede
    "temperatura": "temperature",
    "ventoinha": "fans_speed",
    "disco": "disk",
    "gpu": "gpu_usage"
}

MINUTOS_OFFLINE = 10

MARGEM_ATENCAO = 0.10


def formatar_timestamp(timestamp):
    return timestamp.tz_convert(FUSO_LOCAL).isoformat(timespec="seconds")

def gravar_s3(chave, conteudo):
    s3.put_object(
        Bucket=bucket_gold,
        Key=chave,
        Body=json.dumps({"gerado_em": formatar_timestamp(pd.Timestamp.now(tz="UTC")), "dados": conteudo}, ensure_ascii=False, default=float, indent=2),
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


def gravar_janelas(destino, dados, metricas, idmaquina, agora, limites):

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
        picos = gerar_picos(dados_filtrados , limites)

        juntar_metricas(f"{destino}/janelas/{nome}", kpis , series)
        gravar_s3(f"{destino}/janelas/{nome}/picos.json", picos)



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

    vetor = []

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_silver, Prefix=prefixo):
        for i in page.get("Contents", []):
            key = i["Key"]
            nome_arquivo = key.rsplit("/", 1)[-1]

            if not (nome_arquivo.endswith(".csv")):
                continue

            conteudo = s3.get_object(Bucket=bucket_silver, Key=key)["Body"].read()
            df = pd.read_csv(io.BytesIO(conteudo), sep=";")

            if df.empty:
                continue

            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="mixed")

            if "user" not in df.columns:
                print(f"Coluna user não encontrada em {nome_arquivo}: {df.columns.tolist()}")
                continue

            df["mac"] = df["user"].astype(str).str.strip().str.lower().str.replace("-", ":", regex=False)

            df = df.merge(maquinas, on="mac")

            if df.empty:
                continue

            vetor.append(df)

        if not vetor:
            vazio = pd.DataFrame(columns=["timestamp", "id_farm", "id_sessao", "_id_maquina"])
            vazio["timestamp"] = pd.to_datetime(vazio["timestamp"], utc=True)
            return vazio

        return pd.concat(vetor, ignore_index=True)


def gravar_catalogo(idempresa , nomesanitizado):
    cursor.execute("""select f.id, f.nome, s.id, s.nome, m.id, m.modelo , m.tipo from empresa e join farm f on f.id_empresa = e.id join sessao s on s.id_farm = f.id join maquina m on m.id_sessao = s.id where e.id = %s
        """, (idempresa,))
    maquinas = []
    for farmid , farmnome, sessaoid , sessaonome , maqid , maqmodelo , maqtipo in cursor.fetchall():
        maquinas.append({"id_farm" : farmid , "farm" : farmnome , "id_sessao" : sessaoid , "sessao" : sessaonome , "id_maquina" : maqid , "modelo" : maqmodelo , "tipo" : maqtipo})
    
    gravar_s3(f"{idempresa}/{nomesanitizado}/operacional/catalogo/estrutura.json", {"maquinas": maquinas})

    cursor.execute("""select mc.id ,  mc.id_maquina, c.nome, mc.limite_min, mc.limite_max, mc.monitorado from empresa e join farm f on f.id_empresa = e.id join sessao s on s.id_farm = f.id join maquina m on m.id_sessao = s.id join maquina_componente mc on mc.id_maquina = m.id join componente c on c.id = mc.id_componente where e.id = %s
        """, (idempresa,))

    limites = []
    for mcid, id_maquina , nomecomponente , limite_min , limite_max , monitorado in cursor.fetchall():
        limites.append({"id" : mcid , "id_maquina" : id_maquina , "componente" : nomecomponente , "limite_min" : limite_min , "limite_max" : limite_max , "monitorado" : monitorado})

    gravar_s3(f"{idempresa}/{nomesanitizado}/operacional/catalogo/limites.json", {"limites": limites})
    return maquinas, limites



def classificar(valor, limitemin, limitemax):
    if pd.isna(valor):
        return "sem_dados"

    if limitemax is not None:
        limitemax = float(limitemax)
        if valor > limitemax:
            return "critico"

    if limitemin is not None:
        limitemin = float(limitemin)
        if valor < limitemin:
            return "critico"

    if limitemax is not None and valor > limitemax * (1 - MARGEM_ATENCAO):
        return "atencao"

    if limitemin is not None and valor < limitemin * (1 + MARGEM_ATENCAO):
        return "atencao"

    return "normal"



def gerar_status_geral(dados, limites, agora):

    vetor = []

    for idmaq in dados["_id_maquina"].unique():

        maquina_dados = dados[dados["_id_maquina"] == idmaq]
        df = maquina_dados.sort_values("timestamp").iloc[-1]

        offline = (agora - df["timestamp"]) > pd.Timedelta(minutes=MINUTOS_OFFLINE)

        componentes = []

        for i in limites:

            if i["componente"].lower() not in MAPA_COMPONENTES:
                continue

            if i["id_maquina"] == idmaq and i["monitorado"]:

                limitemin = i["limite_min"]
                limitemax = i["limite_max"]

                valor = df[MAPA_COMPONENTES[i["componente"].lower()]]

                if offline:
                    status = "offline"
                else:
                    status = classificar(valor , limitemin , limitemax)

                if pd.isna(valor):
                    valor = None

                componentes.append({
                    "componente": i["componente"].lower(),
                    "valor": valor,
                    "status": status
                })

        vetor.append({
            "id_maquina": idmaq,
            "timestamp": formatar_timestamp(df["timestamp"]),
             "status_geral": (
                "offline" if offline
                    else "critico" if any(c["status"] == "critico" for c in componentes)
                    else "atencao" if any(c["status"] == "atencao" for c in componentes)
                    else "sem_dados" if componentes and all(c["status"] == "sem_dados" for c in componentes)
                    else "normal"),
            "componentes": componentes
        })

    return vetor



def gerar_picos(dados, limites):

    picos = []

    dados = dados.copy()

    dados["hora"] = dados["timestamp"].dt.tz_convert(FUSO_LOCAL).dt.floor("60min")

    for i in NUMERICAS:

        if i not in dados.columns:
            continue

        for j in limites:

            componente = j["componente"].lower()

            if MAPA_COMPONENTES.get(componente) != i:
                continue

            if not j["monitorado"]:
                continue

            maquina = dados[dados["_id_maquina"] == j["id_maquina"]]

            if maquina.empty:
                continue

    
            fora = pd.Series(False, index=maquina.index)
            # cria uma coluna tudo false

            if j["limite_max"] is not None:
                fora = fora | (maquina[i] > float(j["limite_max"]))
                # fala que ou vai ser false essa coluna ou true se passar do limite

            if j["limite_min"] is not None:
                fora = fora | (maquina[i] < float(j["limite_min"]))

            estouros = maquina[fora]
            # cria um df estouros em que é a maquina filtrada pelos picos / estouros

            for hora, grupo in estouros.groupby("hora"):
                picos.append({
                    "id_maquina": int(j["id_maquina"]),
                    "componente": componente,
                    "timestamp" : hora,
                    "pico": float(grupo[i].max()),
                    "qtd": int(len(grupo))
                })

    picos = pd.DataFrame(picos)

    if picos.empty:
        return []

    picos = picos.sort_values("timestamp")
    picos["timestamp"] = picos["timestamp"].apply(formatar_timestamp)

    picos = picos.to_dict(orient="records")

    return picos


def main(idempresa, dia):
    nome_sanitizado = sanitizar_nome(buscar_empresa(idempresa))
    catalogomaq , limites = gravar_catalogo(idempresa , nome_sanitizado)
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
                maquinas.append({
                    "id_farm": id_farm,
                    "id_sessao": id_sessao,
                    "_id_maquina": maquina["id_maquina"],
                    "mac": mac
                })

    maquinas = pd.DataFrame(maquinas, columns=["id_farm", "id_sessao", "_id_maquina", "mac"])

    agora = pd.Timestamp.now(tz="UTC")

    inicio = agora - pd.Timedelta(days=30)
        # pega os ultimos 30 dias

    meses = pd.period_range(
        start=inicio.strftime("%Y-%m"),
        end=agora.strftime("%Y-%m"),
        freq="M"
    )

   
    historico_vetor = []

    for mes2 in meses:
        historico_vetor.append(ler_trusted(f"{idempresa}/{mes2.year}/{mes2.month:02d}/", maquinas))

    historico = pd.concat(historico_vetor, ignore_index=True)


    dados_dia = ler_trusted(prefixo_silver, maquinas)

    if not dados_dia.empty:

        metricas = [c for c in dados_dia.columns if c in NUMERICAS]

        kpis = calcular_kpis(dados_dia, metricas, None)
        series = calcular_series(dados_dia, metricas, None)

        destinogeralempresadia = (f"{idempresa}/{nome_sanitizado}/operacional/geral/historico/ano={ano}/mes={mes}/dia={dia_numero}")

        juntar_metricas(f"{destinogeralempresadia}", kpis , series)

        for id_farm in dados_dia["id_farm"].unique():

            dados_farm = dados_dia[dados_dia["id_farm"] == id_farm]

            kpis = calcular_kpis(dados_farm, metricas, None)
            series = calcular_series(dados_farm, metricas, None)

            destinogeralfarmdia = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/historico/ano={ano}/mes={mes}/dia={dia_numero}")

            juntar_metricas(f"{destinogeralfarmdia}", kpis , series)

            for id_sessao in dados_farm["id_sessao"].unique():

                dados_sessao = dados_farm[dados_farm["id_sessao"] == id_sessao]

                kpis = calcular_kpis(dados_sessao, metricas, None)
                series = calcular_series(dados_sessao, metricas, None)

                destinogeralsessaodia = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/sessao={id_sessao}/historico/ano={ano}/mes={mes}/dia={dia_numero}")

                juntar_metricas(f"{destinogeralsessaodia}" , kpis , series)

                for id_maquina in dados_sessao["_id_maquina"].unique():

                    dados = dados_sessao[dados_sessao["_id_maquina"] == id_maquina]

                    kpis = calcular_kpis(dados, metricas, id_maquina)
                    series = calcular_series(dados, metricas, id_maquina)

                    destinoespecificodia = (f"{idempresa}/{nome_sanitizado}/operacional/especifico/farm={id_farm}/sessao={id_sessao}/maquina={id_maquina}/historico/ano={ano}/mes={mes}/dia={dia_numero}")

                    juntar_metricas(f"{destinoespecificodia}" , kpis , series)

    dados_mes = ler_trusted(prefixo_silver_mes, maquinas)

    if not dados_mes.empty:

        metricas = [c for c in dados_mes.columns if c in NUMERICAS]

        kpis = calcular_kpis(dados_mes, metricas, None)
        series = calcular_series(dados_mes, metricas, None)

        destinogeralempresames = (f"{idempresa}/{nome_sanitizado}/operacional/geral/historico/ano={ano}/mes={mes}")

        juntar_metricas(f"{destinogeralempresames}", kpis , series)

        for id_farm in dados_mes["id_farm"].unique():

            dados_farm = dados_mes[dados_mes["id_farm"] == id_farm]

            kpis = calcular_kpis(dados_farm, metricas, None)
            series = calcular_series(dados_farm, metricas, None)

            destinogeralfarmmes = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/historico/ano={ano}/mes={mes}")

            juntar_metricas(f"{destinogeralfarmmes}", kpis , series)

            for id_sessao in dados_farm["id_sessao"].unique():

                dados_sessao = dados_farm[dados_farm["id_sessao"] == id_sessao]

                kpis = calcular_kpis(dados_sessao, metricas, None)
                series = calcular_series(dados_sessao, metricas, None)

                destinogeralsessaomes = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/sessao={id_sessao}/historico/ano={ano}/mes={mes}")

                juntar_metricas(f"{destinogeralsessaomes}", kpis , series)

                for id_maquina in dados_sessao["_id_maquina"].unique():

                    dados = dados_sessao[dados_sessao["_id_maquina"] == id_maquina]

                    kpis = calcular_kpis(dados, metricas, id_maquina)
                    series = calcular_series(dados, metricas, id_maquina)

                    destinoespecificomes = (f"{idempresa}/{nome_sanitizado}/operacional/especifico/farm={id_farm}/sessao={id_sessao}/maquina={id_maquina}/historico/ano={ano}/mes={mes}")

                    juntar_metricas(f"{destinoespecificomes}", kpis , series)

    dados_ano = ler_trusted(prefixo_silver_ano, maquinas)

    if not dados_ano.empty:

        metricas = [c for c in dados_ano.columns if c in NUMERICAS]

        kpis = calcular_kpis(dados_ano, metricas, None)
        series = calcular_series(dados_ano, metricas, None)

        destinogeralempresaano = (f"{idempresa}/{nome_sanitizado}/operacional/geral/historico/ano={ano}")

        juntar_metricas(f"{destinogeralempresaano}", kpis , series)

        for id_farm in dados_ano["id_farm"].unique():

            dados_farm = dados_ano[dados_ano["id_farm"] == id_farm]

            kpis = calcular_kpis(dados_farm, metricas, None)
            series = calcular_series(dados_farm, metricas, None)

            destinogeralfarmano = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/historico/ano={ano}")

            juntar_metricas(f"{destinogeralfarmano}", kpis , series)

            for id_sessao in dados_farm["id_sessao"].unique():

                dados_sessao = dados_farm[dados_farm["id_sessao"] == id_sessao]

                kpis = calcular_kpis(dados_sessao, metricas, None)
                series = calcular_series(dados_sessao, metricas, None)

                destinogeralsessaoano = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/sessao={id_sessao}/historico/ano={ano}")

                juntar_metricas(f"{destinogeralsessaoano}", kpis , series)

                for id_maquina in dados_sessao["_id_maquina"].unique():

                    dados = dados_sessao[dados_sessao["_id_maquina"] == id_maquina]

                    kpis = calcular_kpis(dados, metricas, id_maquina)
                    series = calcular_series(dados, metricas, id_maquina)

                    destinoespecificoano = (f"{idempresa}/{nome_sanitizado}/operacional/especifico/farm={id_farm}/sessao={id_sessao}/maquina={id_maquina}/historico/ano={ano}")

                    juntar_metricas(f"{destinoespecificoano}", kpis , series)



    if not historico.empty:

        metricas = [c for c in historico.columns if c in NUMERICAS]

        destino = (f"{idempresa}/{nome_sanitizado}/operacional/geral")

        gravar_s3(f"{destino}/tempo_real/atual.json", gerar_atual_geral(historico))
        gravar_janelas(destino, historico, metricas, None, agora, limites)
        gravar_s3(f"{destino}/tempo_real/status.json", gerar_status_geral(historico, limites, agora))

        for id_farm in historico["id_farm"].unique():

            dados_farm = historico[historico["id_farm"] == id_farm]

            destino = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}")

            gravar_s3(f"{destino}/tempo_real/atual.json", gerar_atual_geral(dados_farm))
            gravar_janelas(destino, dados_farm, metricas, None, agora, limites)
            gravar_s3(f"{destino}/tempo_real/status.json", gerar_status_geral(dados_farm, limites, agora))

            for id_sessao in dados_farm["id_sessao"].unique():

                dados_sessao = dados_farm[dados_farm["id_sessao"] == id_sessao]

                destino = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/sessao={id_sessao}")

                gravar_s3(f"{destino}/tempo_real/atual.json", gerar_atual_geral(dados_sessao))
                gravar_janelas(destino, dados_sessao, metricas, None, agora, limites)
                gravar_s3(f"{destino}/tempo_real/status.json", gerar_status_geral(dados_sessao, limites, agora))

                for id_maquina in dados_sessao["_id_maquina"].unique():

                    dados = dados_sessao[dados_sessao["_id_maquina"] == id_maquina]

                    destino = (f"{idempresa}/{nome_sanitizado}/operacional/especifico/farm={id_farm}/sessao={id_sessao}/maquina={id_maquina}")

                    gravar_s3(f"{destino}/tempo_real/atual.json", gerar_atual(dados))
                    gravar_janelas(destino, dados, metricas, id_maquina, agora, limites)

                    status = gerar_status_geral(dados, limites, agora)
                    gravar_s3(f"{destino}/tempo_real/status.json", status[0])
                    
    print("teste")

    