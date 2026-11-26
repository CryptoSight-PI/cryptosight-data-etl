from configenvironment import s3 , bucket_silver , bucket_gold , cursor, garantir_conexao
# from CURATED.utils import sanitizar_nome, buscar_farm , buscar_sessoes , buscar_maquinas_sessao
# from raw_to_trusted import buscar_empresa, particao, NUMERICAS
import pandas as pd
import io
import json
import re
from datetime import date
from sklearn.linear_model import LinearRegression
from urllib.parse import unquote_plus
from concurrent.futures import ThreadPoolExecutor


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

LIMITE_DANO = {
    "temperatura": 95,
    "cpu": 100,
    "ram": 100,
    "swap": 100,
    "disco": 100,
    "gpu": 100
}


NUMERICAS = [
    "cpu_percent", "cpu_frequency", "ram_percent",
    "swap_memory_total", "swap_memory_used", "swap_memory_percent",
    "upload_speed", "download_speed", "temperature", "fans_speed", "disk",
    "gpu_usage", "gpu_energy", "gpu_temp", "gpu_fan_speed",
]

CACHE_CSV = {}
PENDENTES = []
TRABALHADORES = 16


def particao(dia):
    """date -> '2026/10/21'"""
    return f"{dia:%Y/%m/%d}"


def buscar_empresa(id_empresa):
    cursor.execute(
        """
        SELECT id, razao_social, nome_fantasia
        FROM empresa
        WHERE id = %s
        """,
        (id_empresa,),
    )

    empresa = cursor.fetchone()

    if empresa is None:
        raise ValueError(
            f"Nenhuma empresa encontrada com ID {id_empresa}"
        )

    nome_empresa = empresa[2] or empresa[1]

    if not nome_empresa:
        raise ValueError(
            f"A empresa {id_empresa} não tem nome cadastrado"
        )

    return nome_empresa


def sanitizar_nome(nome_empresa):
    nome = re.sub(r'[<>:"/\\|?*]', "_", nome_empresa).strip(" .")

    if not nome:
        raise ValueError("Nome inválido para a empresa.")

    return nome 

def buscar_farm(idempresa):
    cursor.execute("select id from farm where id_empresa = %s" , (idempresa ,) , )

    farms = []

    for i in cursor.fetchall():
        farms.append(i[0])

    # print(farms)

    return farms

def buscar_sessoes(idfarm):
    cursor.execute("select id from sessao where id_farm = %s" , (idfarm ,) ,)

    sessoes = []

    for i in cursor.fetchall():
        sessoes.append(i[0])

    # print(sessoes)
    return sessoes


def buscar_maquinas_sessao(idsessao):
    cursor.execute("select id, mac_address from maquina where id_sessao = %s" , (idsessao ,) , )

    maquinas = []

    for i in cursor.fetchall():
        maquinas.append({"id_maquina" : i[0] , "mac_address" : str(i[1]).strip().lower().replace("-", ":")})

    # print(maquinas)
    return maquinas



#deixa o timestamp para o fuso local e chega ate segundos (tira o milisegundos)
def formatar_timestamp(timestamp):
    return timestamp.tz_convert(FUSO_LOCAL).isoformat(timespec="seconds")

#coloca o conteudo no s3, com base na chave / prefixo
# def gravar_s3(chave, conteudo):
#     s3.put_object(
#         Bucket=bucket_gold,
#         Key=chave,
#         Body=json.dumps({"gerado_em": formatar_timestamp(pd.Timestamp.now(tz="UTC")), "dados": conteudo}, ensure_ascii=False, default=float, indent=2),
#         ContentType="application/json",
#     )

def gravar_s3(chave, conteudo):
    corpo = json.dumps({"gerado_em": formatar_timestamp(pd.Timestamp.now(tz="UTC")), "dados": conteudo}, ensure_ascii=False, default=float, indent=2)
    PENDENTES.append((chave, corpo))


def enviar_pendentes():
    def enviar(item):
        s3.put_object(
            Bucket=bucket_gold,
            Key=item[0],
            Body=item[1],
            ContentType="application/json",
        )

    with ThreadPoolExecutor(TRABALHADORES) as executor:
        list(executor.map(enviar, PENDENTES))

    PENDENTES.clear()


#calculo de kpis de um df com base nas metricas (usada para nao fazer calculo de timestamp, user...)
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
            #aqui deixa tudo none, pq se nao vim dado, coloca como none, para nao bugar logica de grafico etc...
            continue


        #indice do max (maior valor), ou seja, vai, no df espeficicado retornar o index do maior valor dessa serie(df na coluna da vez)
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
            #o .loc usa como parametro, index e nome da coluna, ou seja,vai naquele index e naquela coluna retornar o valor
        }
    return kpis

#calculo de series, progressao do tempo
def calcular_series(dados, metricas , idmaquina , resolucao="1h"):

    dados = dados.copy()

    dados["timestamp_local"] = (dados["timestamp"].dt.tz_convert(FUSO_LOCAL))
    # dados["timestamp_local"] = formatar_timestamp(dados["timestamp"].dt)

    listaserie = []
    for hora, df in dados.groupby(dados["timestamp_local"].dt.floor(resolucao)):
    #aqui ele agrupa por resolucao(hora, dia , sla, depende do retorno, o padrao é 1 hora, caso nao declare), ou seja, o primeira retorno é a hora e o df daquela hora, tipo no mysql mesmo, exemplo, 1h : df1 ; 2h: df2
        listaserie.append({
            "t": formatar_timestamp(hora),
            "amostras": len(df),
            "metricas": calcular_kpis(df, metricas , idmaquina),
        })
    return listaserie

def juntar_metricas(destino, kpis, series, resolucao="1h"):
    #junta as kpis e as series num metrica.json
    gravar_s3(f"{destino}/metricas.json", {
        "kpis": kpis,
        "serie": {
            "resolucao": resolucao,
            "pontos": series
        }
    })


def gravar_janelas(destino, dados, metricas, idmaquina, agora, limites):
    #aqui é essencial, pois tem as 3 janelas que serao usadas no front
    #nesse dicionario é passado o nome (horario) e as configs dele, em que inicio é onde inicia esse intervalo, ou seja, pra 24 horas, é 1 dia atras
    #e a resolucao que sera usada para melhorar a plotagem em grafico usando a resolucao em kpis e series
    janelas = {
        "24h": {
            "inicio": agora - pd.Timedelta(hours=24),
            "resolucao": "1h"
        },
        "7d": {
            "inicio": agora - pd.Timedelta(days=7),
            "resolucao": "6h"
        },
        "30d": {
            "inicio": agora - pd.Timedelta(days=30),
            "resolucao": "1D"
        }
    }

    for nome, limite in janelas.items():

        #aqui os dados sao filtrados para retornar so oq a janela pede, ou seja 24 horas, so os dados das 24 horas
        dados_filtrados = dados[ (dados["timestamp"] >= limite["inicio"]) & (dados["timestamp"] <= agora)]

        if dados_filtrados.empty:
            continue

        resolucao = limite["resolucao"]

        kpis = calcular_kpis(dados_filtrados, metricas, idmaquina)
        series = calcular_series(dados_filtrados, metricas, idmaquina, resolucao)
        picos = gerar_picos(dados_filtrados , limites, resolucao)

        juntar_metricas(f"{destino}/janelas/{nome}", kpis , series, resolucao)
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


# def ler_trusted(prefixo, maquinas):

#     vetor = []

#     paginator = s3.get_paginator("list_objects_v2")
#     for page in paginator.paginate(Bucket=bucket_silver, Prefix=prefixo):
#         for i in page.get("Contents", []):
#             key = i["Key"]
#             nome_arquivo = key.rsplit("/", 1)[-1]

#             # só dados limpos; dados_nulos_*.csv (descartados) não entram nas métricas
#             if not (nome_arquivo.startswith("clean_data_") and nome_arquivo.endswith(".csv")):
#                 continue

#             if key not in CACHE_CSV:
#                 conteudo = s3.get_object(Bucket=bucket_silver, Key=key)["Body"].read()
#                 CACHE_CSV[key] = pd.read_csv(io.BytesIO(conteudo), sep=";")

#             df = CACHE_CSV[key].copy()

#             if df.empty:
#                 continue

#             df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="mixed")

#             if "user" not in df.columns:
#                 print(f"Coluna user não encontrada em {nome_arquivo}: {df.columns.tolist()}")
#                 continue

#             df["mac"] = df["user"].astype(str).str.strip().str.lower().str.replace("-", ":", regex=False)

#             df = df.merge(maquinas, on="mac")

#             if df.empty:
#                 continue

#             vetor.append(df)

#     if not vetor:
#         vazio = pd.DataFrame(columns=["timestamp", "id_farm", "id_sessao", "_id_maquina"])
#         vazio["timestamp"] = pd.to_datetime(vazio["timestamp"], utc=True)
#         return vazio

#     return pd.concat(vetor, ignore_index=True)

def carregar_csv(key, maquinas):
    conteudo = s3.get_object(Bucket=bucket_silver, Key=key)["Body"].read()
    df = pd.read_csv(io.BytesIO(conteudo), sep=";")

    if df.empty:
        return None

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="mixed")

    if "user" not in df.columns:
        print(f"Coluna user não encontrada em {key}: {df.columns.tolist()}")
        return None

    df["mac"] = df["user"].astype(str).str.strip().str.lower().str.replace("-", ":", regex=False)

    df = df.merge(maquinas, on="mac")

    if df.empty:
        return None

    return df


def ler_trusted(prefixo, maquinas):

    chaves = []

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_silver, Prefix=prefixo):
        for i in page.get("Contents", []):
            key = i["Key"]
            nome_arquivo = key.rsplit("/", 1)[-1]

            if nome_arquivo.startswith("clean_data_") and nome_arquivo.endswith(".csv"):
                chaves.append(key)

    novas = [k for k in chaves if k not in CACHE_CSV]

    with ThreadPoolExecutor(TRABALHADORES) as executor:
        resultados = list(executor.map(carregar_csv, novas, [maquinas] * len(novas)))

    for key, df in zip(novas, resultados):
        CACHE_CSV[key] = df

    vetor = [CACHE_CSV[k] for k in chaves if CACHE_CSV[k] is not None]

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
        maior_z = 0
        maior_comp = None
        maior_valor = None

        for i in limites:

            if i["componente"].lower() not in MAPA_COMPONENTES:
                continue

            if i["id_maquina"] == idmaq and i["monitorado"]:

                limitemin = i["limite_min"]
                limitemax = i["limite_max"]

                valor = df[MAPA_COMPONENTES[i["componente"].lower()]]
                serie = maquina_dados[MAPA_COMPONENTES[i["componente"].lower()]]
                desvio = serie.std()

                if desvio > 0 and not pd.isna(valor):
                    z = (valor - serie.mean()) / desvio
                    if abs(z) > abs(maior_z):
                        maior_z = z
                        maior_comp = i["componente"].lower()
                        maior_valor = float(valor)

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
            "id_maquina": int(idmaq),
            "timestamp": formatar_timestamp(df["timestamp"]),
            "status_geral": (
                "offline" if offline
                    else "critico" if any(c["status"] == "critico" for c in componentes)
                    else "atencao" if any(c["status"] == "atencao" for c in componentes)
                    else "sem_dados" if componentes and all(c["status"] == "sem_dados" for c in componentes)
                    else "normal"),
            "leitura_mais_fora_media" : {
                "componente" : maior_comp,
                "z_score" : round(float(maior_z) , 2),
                "valor" : maior_valor
            },
            "componentes": componentes
        })

    return vetor



def gerar_picos(dados, limites, resolucao="1h"):

    picos = []

    dados = dados.copy()

    dados["hora"] = dados["timestamp"].dt.tz_convert(FUSO_LOCAL).dt.floor(resolucao)

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



def gerar_previsoes(dados, limites, agora):

    previsoes = []

    dados = dados.copy()
    dados["hora"] = dados["timestamp"].dt.tz_convert(FUSO_LOCAL).dt.floor("60min")

    for id_maquina in dados["_id_maquina"].unique():

        maquina = dados[dados["_id_maquina"] == id_maquina].sort_values("timestamp")
        componentes = []

        for j in limites:

            if j["id_maquina"] != id_maquina or not j["monitorado"]:
                continue

            componente = j["componente"].lower()
            coluna = MAPA_COMPONENTES.get(componente)

            if coluna is None:
                continue

            if j["limite_min"] is None and j["limite_max"] is None:
                continue

            if coluna not in maquina.columns:
                continue

            ultima_leitura = maquina["timestamp"].max()

            if (agora - ultima_leitura) > pd.Timedelta(hours=2):
                continue

            lim_max = float(j["limite_max"]) if j["limite_max"] is not None else None
            lim_min = float(j["limite_min"]) if j["limite_min"] is not None else None

            leituras = maquina[coluna].dropna()
            serie = maquina.groupby("hora")[coluna].mean().dropna().tail(24)

            if serie.empty:
                componentes.append({
                    "componente": componente,
                    "estado_atual": None,
                    "valor_atual": None,
                    "valor_limite": None,
                    "direcao": "sem_dados",
                    "subida_por_hora": None,
                    "valores_previstos": {},
                    "horas_ate_limite": None,
                    "horas_ate_atencao": None,
                    "horas_ate_critico": None,
                    "horario_critico": None,
                    "horario_estimado": None,
                    "nivel_proximo_risco": None,
                    "horas_em_critico": None,
                    "critico_desde": None,
                    "excesso": None,
                    "excesso_pct": None,
                    "r2": None,
                    "limite_dano": None,
                    "tendencia_critica": None,
                    "horas_ate_dano": None,
                    "horario_dano": None,
                    "horas_ate_normalizar": None,
                    "margem_restante_pct": None,
                    "qualidade_tendencia": "baixa"
                })
                continue

            valor_atual = float(leituras.iloc[-1])
            estado_atual = classificar(valor_atual, lim_min, lim_max)

            subida = 0.0
            r2 = None

            if len(serie) >= 4:
                x = (
                    (serie.index - serie.index[0]).total_seconds() / 3600
                ).values.reshape(-1, 1)

                y = serie.values

                modelo = LinearRegression().fit(x, y)

                subida = float(modelo.coef_[0])
                r2 = float(modelo.score(x, y))

            limite_atencao_max = (lim_max * (1 - MARGEM_ATENCAO) if lim_max is not None else None)

            limite_atencao_min = (lim_min * (1 + MARGEM_ATENCAO) if lim_min is not None else None)

            horas_atencao = None
            horas_critico = None

            if subida > 0 and lim_max is not None:

                if valor_atual < limite_atencao_max:
                    horas_atencao = (limite_atencao_max - valor_atual) / subida
                else:
                    horas_atencao = 0

                if valor_atual < lim_max:
                    horas_critico = (lim_max - valor_atual) / subida
                else:
                    horas_critico = 0

            elif subida < 0 and lim_min is not None:

                if valor_atual > limite_atencao_min:
                    horas_atencao = (limite_atencao_min - valor_atual) / subida
                else:
                    horas_atencao = 0

                if valor_atual > lim_min:
                    horas_critico = (lim_min - valor_atual) / subida
                else:
                    horas_critico = 0

            if horas_atencao is not None and not 0 <= horas_atencao <= 168:
                horas_atencao = None

            if horas_critico is not None and not 0 <= horas_critico <= 168:
                horas_critico = None

            if estado_atual == "critico":
                horas_critico = 0
                horas_atencao = 0
            elif estado_atual == "atencao":
                horas_atencao = 0

            valor_limite = lim_max if subida >= 0 else lim_min

            horas_em_critico = None
            critico_desde = None
            excesso = None
            excesso_pct = None

            if estado_atual == "critico":
                inicio = None

                for hora in reversed(serie.index):
                    v = serie.loc[hora]
                    fora = (lim_max is not None and v > lim_max) or (lim_min is not None and v < lim_min)

                    if not fora:
                        break

                    inicio = hora

                if inicio is None:
                    inicio = ultima_leitura

                horas_em_critico = max(0.0, (ultima_leitura - inicio).total_seconds() / 3600)
                critico_desde = formatar_timestamp(inicio)

                if lim_max is not None and valor_atual > lim_max:
                    valor_limite = lim_max
                    excesso = valor_atual - lim_max
                else:
                    valor_limite = lim_min
                    excesso = lim_min - valor_atual

                if valor_limite:
                    excesso_pct = round(excesso / valor_limite * 100, 1)

            limite_dano = None
            tendencia_critica = None
            horas_ate_dano = None
            horario_dano = None
            horas_ate_normalizar = None
            margem_restante_pct = None

            if estado_atual == "critico":
                if lim_max is not None and valor_atual > lim_max:
                    sentido = 1
                    limite_dano = LIMITE_DANO.get(componente)

                    if limite_dano is None or limite_dano <= lim_max:
                        limite_dano = lim_max * 1.15
                else:
                    sentido = -1
                    limite_dano = lim_min * 0.3

                margem = (limite_dano - valor_atual) / (limite_dano - valor_limite)
                margem_restante_pct = round(max(0, min(1, margem)) * 100, 1)

                recente = serie.tail(6)

                if margem <= 0:
                    tendencia_critica = "dano_atingido"
                    horas_ate_dano = 0
                elif len(recente) < 3:
                    tendencia_critica = "sem_tendencia"
                else:
                    x = ((recente.index - recente.index[0]).total_seconds() / 3600).values.reshape(-1, 1)
                    piora = float(LinearRegression().fit(x, recente.values).coef_[0]) * sentido

                    if piora > abs(valor_limite) * 0.005:
                        tendencia_critica = "agravando"
                        horas_ate_dano = (limite_dano - valor_atual) * sentido / piora
                    elif piora < -abs(valor_limite) * 0.005:
                        tendencia_critica = "recuperando"
                        horas_ate_normalizar = (valor_atual - valor_limite) * sentido / -piora
                    else:
                        tendencia_critica = "estavel"

                if horas_ate_dano is not None and not 0 <= horas_ate_dano <= 168:
                    horas_ate_dano = None

                if horas_ate_normalizar is not None and not 0 <= horas_ate_normalizar <= 168:
                    horas_ate_normalizar = None

                if horas_ate_dano is not None:
                    horario_dano = formatar_timestamp(ultima_leitura + pd.Timedelta(hours=horas_ate_dano))

            if horas_critico is not None:
                nivel = "critico"
            elif horas_atencao is not None:
                nivel = "atencao"
            else:
                nivel = None

            if r2 is None:
                qualidade_tendencia = "baixa"
            elif r2 >= 0.7:
                qualidade_tendencia = "boa"
            elif r2 >= 0.4:
                qualidade_tendencia = "moderada"
            else:
                qualidade_tendencia = "baixa"

            if r2 is None:
                direcao = "dados_insuficientes"
            elif subida > 0:
                direcao = "subindo"
            elif subida < 0:
                direcao = "descendo"
            else:
                direcao = "estavel"

            componentes.append({
                "componente": componente,
                "estado_atual": estado_atual,
                "valor_atual": round(valor_atual, 2),
                "valor_limite": valor_limite,
                "direcao": direcao,
                "subida_por_hora": round(subida, 3),
                "valores_previstos": {
                    "6h": round(max(0, valor_atual + subida * 6), 2),
                    "12h": round(max(0, valor_atual + subida * 12), 2),
                    "24h": round(max(0, valor_atual + subida * 24), 2),
                    "48h": round(max(0, valor_atual + subida * 48), 2)
                },
                "horas_ate_limite": round(horas_critico, 1) if horas_critico is not None else None,
                "horas_ate_atencao": round(horas_atencao, 1) if horas_atencao is not None else None,
                "horas_ate_critico": round(horas_critico, 1) if horas_critico is not None else None,
                "horario_critico": (formatar_timestamp(ultima_leitura + pd.Timedelta(hours=horas_critico)) if horas_critico is not None else None),
                "horario_estimado": (formatar_timestamp(ultima_leitura + pd.Timedelta(hours=horas_critico)) if horas_critico is not None else None),
                "nivel_proximo_risco": nivel,
                "horas_em_critico": round(horas_em_critico, 1) if horas_em_critico is not None else None,
                "critico_desde": critico_desde,
                "excesso": round(excesso, 2) if excesso is not None else None,
                "excesso_pct": excesso_pct,
                "r2": round(r2, 3) if r2 is not None else None,
                "limite_dano": round(limite_dano, 2) if limite_dano is not None else None,
                "tendencia_critica": tendencia_critica,
                "horas_ate_dano": round(horas_ate_dano, 1) if horas_ate_dano is not None else None,
                "horario_dano": horario_dano,
                "horas_ate_normalizar": round(horas_ate_normalizar, 1) if horas_ate_normalizar is not None else None,
                "margem_restante_pct": margem_restante_pct,
                "qualidade_tendencia": qualidade_tendencia
            })

    
        if componentes:
            tabela = pd.DataFrame(componentes).sort_values("horas_ate_critico", ascending=True, na_position="last")
            tabela = tabela.astype(object).where(tabela.notna(), None)
            componentes = tabela.to_dict(orient="records")

        if componentes:
            em_critico = []
            futuras = []

            for c in componentes:
                if c["estado_atual"] == "critico":
                    em_critico.append(c)
                elif c["horas_ate_critico"] is not None:
                    futuras.append(c)

            critico_atual = None
            for c in em_critico:
                if critico_atual is None:
                    critico_atual = c
                    continue

                horas_c = c["horas_ate_dano"] if c["horas_ate_dano"] is not None else 9999
                horas_a = critico_atual["horas_ate_dano"] if critico_atual["horas_ate_dano"] is not None else 9999

                if horas_c < horas_a or (horas_c == horas_a and (c["excesso_pct"] or 0) > (critico_atual["excesso_pct"] or 0)):
                    critico_atual = c

            proxima_falha = futuras[0] if futuras else None

            if critico_atual is not None:
                proxima_horas = 0
                principal = critico_atual["componente"]
            elif proxima_falha is not None:
                proxima_horas = proxima_falha["horas_ate_critico"]
                principal = proxima_falha["componente"]
            else:
                proxima_horas = None
                principal = None

            previsoes.append({
                "id_maquina": int(id_maquina),
                "critico_atual": critico_atual,
                "proxima_falha": proxima_falha,
                "proxima_ocorrencia_em_horas": proxima_horas,
                "componente_com_maior_risco": principal,
                "horas_ate_dano": critico_atual["horas_ate_dano"] if critico_atual is not None else None,
                "componentes": componentes
            })

    if not previsoes:
        return []

    tabela = pd.DataFrame(previsoes)
    tabela["ordem_critico"] = tabela["critico_atual"].apply(lambda c: 0 if c is not None else 1)
    tabela["ordem_horas"] = tabela.apply(lambda p: p["horas_ate_dano"] if p["critico_atual"] is not None else p["proxima_ocorrencia_em_horas"], axis=1)
    tabela = tabela.sort_values(["ordem_critico", "ordem_horas"], ascending=True, na_position="last")
    tabela = tabela.drop(columns=["ordem_critico", "ordem_horas"])
    tabela = tabela.astype(object).where(tabela.notna(), None)
    return tabela.to_dict(orient="records")




def main(idempresa, dia):
    CACHE_CSV.clear()
    PENDENTES.clear()

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
        series = calcular_series(dados_mes, metricas, None, "1D")

        destinogeralempresames = (f"{idempresa}/{nome_sanitizado}/operacional/geral/historico/ano={ano}/mes={mes}")

        juntar_metricas(f"{destinogeralempresames}", kpis , series, "1D")

        for id_farm in dados_mes["id_farm"].unique():

            dados_farm = dados_mes[dados_mes["id_farm"] == id_farm]

            kpis = calcular_kpis(dados_farm, metricas, None)
            series = calcular_series(dados_farm, metricas, None, "1D")

            destinogeralfarmmes = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/historico/ano={ano}/mes={mes}")

            juntar_metricas(f"{destinogeralfarmmes}", kpis , series, "1D")

            for id_sessao in dados_farm["id_sessao"].unique():

                dados_sessao = dados_farm[dados_farm["id_sessao"] == id_sessao]

                kpis = calcular_kpis(dados_sessao, metricas, None)
                series = calcular_series(dados_sessao, metricas, None, "1D")

                destinogeralsessaomes = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/sessao={id_sessao}/historico/ano={ano}/mes={mes}")

                juntar_metricas(f"{destinogeralsessaomes}", kpis , series, "1D")

                for id_maquina in dados_sessao["_id_maquina"].unique():

                    dados = dados_sessao[dados_sessao["_id_maquina"] == id_maquina]

                    kpis = calcular_kpis(dados, metricas, id_maquina)
                    series = calcular_series(dados, metricas, id_maquina, "1D")

                    destinoespecificomes = (f"{idempresa}/{nome_sanitizado}/operacional/especifico/farm={id_farm}/sessao={id_sessao}/maquina={id_maquina}/historico/ano={ano}/mes={mes}")

                    juntar_metricas(f"{destinoespecificomes}", kpis , series, "1D")

    dados_ano = ler_trusted(prefixo_silver_ano, maquinas)

    if not dados_ano.empty:

        metricas = [c for c in dados_ano.columns if c in NUMERICAS]

        kpis = calcular_kpis(dados_ano, metricas, None)
        series = calcular_series(dados_ano, metricas, None, "1D")

        destinogeralempresaano = (f"{idempresa}/{nome_sanitizado}/operacional/geral/historico/ano={ano}")

        juntar_metricas(f"{destinogeralempresaano}", kpis , series, "1D")

        for id_farm in dados_ano["id_farm"].unique():

            dados_farm = dados_ano[dados_ano["id_farm"] == id_farm]

            kpis = calcular_kpis(dados_farm, metricas, None)
            series = calcular_series(dados_farm, metricas, None, "1D")

            destinogeralfarmano = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/historico/ano={ano}")

            juntar_metricas(f"{destinogeralfarmano}", kpis , series, "1D")

            for id_sessao in dados_farm["id_sessao"].unique():

                dados_sessao = dados_farm[dados_farm["id_sessao"] == id_sessao]

                kpis = calcular_kpis(dados_sessao, metricas, None)
                series = calcular_series(dados_sessao, metricas, None, "1D")

                destinogeralsessaoano = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/sessao={id_sessao}/historico/ano={ano}")

                juntar_metricas(f"{destinogeralsessaoano}", kpis , series, "1D")

                for id_maquina in dados_sessao["_id_maquina"].unique():

                    dados = dados_sessao[dados_sessao["_id_maquina"] == id_maquina]

                    kpis = calcular_kpis(dados, metricas, id_maquina)
                    series = calcular_series(dados, metricas, id_maquina, "1D")

                    destinoespecificoano = (f"{idempresa}/{nome_sanitizado}/operacional/especifico/farm={id_farm}/sessao={id_sessao}/maquina={id_maquina}/historico/ano={ano}")

                    juntar_metricas(f"{destinoespecificoano}", kpis , series, "1D")



    if not historico.empty:

        metricas = [c for c in historico.columns if c in NUMERICAS]

        destino = (f"{idempresa}/{nome_sanitizado}/operacional/geral")

        status_todos = gerar_status_geral(historico, limites, agora)
        previsoes_todas = gerar_previsoes(historico, limites, agora)

        gravar_s3(f"{destino}/tempo_real/atual.json", gerar_atual_geral(historico))
        gravar_janelas(destino, historico, metricas, None, agora, limites)
        gravar_s3(f"{destino}/tempo_real/status.json", status_todos)
        gravar_s3(f"{destino}/previsoes/previsoes.json", previsoes_todas)

        for id_farm in historico["id_farm"].unique():

            dados_farm = historico[historico["id_farm"] == id_farm]
            ids_farm = [int(x) for x in dados_farm["_id_maquina"].unique()]

            destino = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}")

            gravar_s3(f"{destino}/tempo_real/atual.json", gerar_atual_geral(dados_farm))
            gravar_janelas(destino, dados_farm, metricas, None, agora, limites)
            gravar_s3(f"{destino}/tempo_real/status.json", [s for s in status_todos if s["id_maquina"] in ids_farm])

            for id_sessao in dados_farm["id_sessao"].unique():

                dados_sessao = dados_farm[dados_farm["id_sessao"] == id_sessao]
                ids_sessao = [int(x) for x in dados_sessao["_id_maquina"].unique()]

                destino = (f"{idempresa}/{nome_sanitizado}/operacional/geral/farm={id_farm}/sessao={id_sessao}")

                gravar_s3(f"{destino}/tempo_real/atual.json", gerar_atual_geral(dados_sessao))
                gravar_janelas(destino, dados_sessao, metricas, None, agora, limites)
                gravar_s3(f"{destino}/tempo_real/status.json", [s for s in status_todos if s["id_maquina"] in ids_sessao])

                for id_maquina in dados_sessao["_id_maquina"].unique():

                    dados = dados_sessao[dados_sessao["_id_maquina"] == id_maquina]

                    destino = (f"{idempresa}/{nome_sanitizado}/operacional/especifico/farm={id_farm}/sessao={id_sessao}/maquina={id_maquina}")

                    gravar_s3(f"{destino}/tempo_real/atual.json", gerar_atual(dados))
                    gravar_janelas(destino, dados, metricas, id_maquina, agora, limites)

                    status_maquina = [s for s in status_todos if s["id_maquina"] == int(id_maquina)]
                    previsoes_maquina = [p for p in previsoes_todas if p["id_maquina"] == int(id_maquina)]

                    gravar_s3(f"{destino}/tempo_real/status.json", status_maquina[0])
                    gravar_s3(f"{destino}/previsoes/previsoes.json", previsoes_maquina)

    enviar_pendentes()
                    
    print(f"Gold concluído: empresa {idempresa}, dia {dia}")

def lambda_handler(event, context=None):
    garantir_conexao()
     # o event vem assim, se vier de um gatilho do s3:
        # {"Records": [{"s3": {"bucket": {"name": "..."},
        #                  "object": {"key": "metrics/2026/11/22/2-2026-11-22_10-00-00-aa-bb.csv"}}}]}

    # se nao tiver gatilho, entra em eventbridge (agendamento) e retorna assim {
#   "version": "0",
#   "id": "a1b2c3...",
#   "detail-type": "Scheduled Event",
#   "source": "aws.events",
#   "account": "123456789012",
#   "time": "2026-11-22T13:05:00Z",
#   "region": "us-east-1",
#   "resources": ["arn:aws:events:...:rule/gold-5min"],
#   "detail": {}
# }
    hoje = pd.Timestamp.now(tz=FUSO_LOCAL).date()

    alvos = set()

    if "Records" in event:

        for r in event["Records"]:
            partes = unquote_plus(r["s3"]["object"]["key"]).split("/")

            if not partes[-1].startswith("clean_data"):
                continue

            try:
                alvos.add((int(partes[0]), date(int(partes[1]), int(partes[2]), int(partes[3]))))
            except ValueError:
                print(f"Chave ignorada (fora do padrão): {r['s3']['object']['key']}")
    else:
        paginator = s3.get_paginator("list_objects_v2")
        for pagina in paginator.paginate(Bucket=bucket_silver, Delimiter="/"):
            for pasta in pagina.get("CommonPrefixes", []):
                nome = pasta["Prefix"].strip("/")
                #esse nome é o idempresa
                if nome.isdigit():
                    #so se for numero
                    alvos.add((int(nome), hoje))

    #no fim nos dois casos, alvos ficou com idempreasa e data

    erros = []

    for idempresa, dia in sorted(alvos):
        try:
            main(idempresa, dia)
        except Exception as erro:
            print(f"Erro na empresa {idempresa}, dia {dia}: {erro}")
            erros.append(f"{idempresa}/{dia}: {erro}")

    if erros and "Records" in event:
        raise RuntimeError("; ".join(erros))

    return {"statusCode": 200, "erros": erros}