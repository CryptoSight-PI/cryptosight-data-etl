from configenvironment import s3 , bucket_silver , bucket_gold , cursor
# from CURATED.utils import sanitizar_nome, buscar_farm , buscar_sessoes , buscar_maquinas_sessao
# from raw_to_trusted import buscar_empresa, particao, NUMERICAS
import pandas as pd
import io
import json
import re
from datetime import date
from sklearn.linear_model import LinearRegression
from urllib.parse import unquote_plus


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


NUMERICAS = [
    "cpu_percent", "cpu_frequency", "ram_percent",
    "swap_memory_total", "swap_memory_used", "swap_memory_percent",
    "upload_speed", "download_speed", "temperature", "fans_speed", "disk",
    "gpu_usage", "gpu_energy", "gpu_temp", "gpu_fan_speed",
]


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
def gravar_s3(chave, conteudo):
    s3.put_object(
        Bucket=bucket_gold,
        Key=chave,
        Body=json.dumps({"gerado_em": formatar_timestamp(pd.Timestamp.now(tz="UTC")), "dados": conteudo}, ensure_ascii=False, default=float, indent=2),
        ContentType="application/json",
    )


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
        maior_z = 0
        maior_comp = None

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
                "z_score" : round(float(maior_z) , 2)
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

        maquina = dados[dados["_id_maquina"] == id_maquina]
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

            if (agora - ultima_leitura) > pd.Timedelta(hours=1):
                continue

            serie = maquina.groupby("hora")[coluna].mean().dropna().tail(48)

            

            if serie.empty:
                componentes.append({
                    "componente": componente,
                    "valor_atual": None,
                    "valor_limite": None,
                    "direcao": "sem_dados",
                    "subida_por_hora": None,
                    "valores_previstos": {},
                    "horas_ate_limite": None,
                    "horario_estimado": None,
                    "r2": None,
                    "qualidade_tendencia": "baixa"
                })
                continue

            valor_atual = float(serie.iloc[-1])

            if len(serie) < 6:
                componentes.append({
                    "componente": componente,
                    "valor_atual": round(valor_atual, 2),
                    "valor_limite": None,
                    "direcao": "dados_insuficientes",
                    "subida_por_hora": None,
                    "valores_previstos": {},
                    "horas_ate_limite": None,
                    "horario_estimado": None,
                    "r2": None,
                    "qualidade_tendencia": "baixa"
                })
                continue

            #cmc da regressao e da previsao por si so
            x = (
                (serie.index - serie.index[0]).total_seconds() / 3600
            ).values.reshape(-1, 1)

            y = serie.values

            modelo = LinearRegression().fit(x, y)

            subida = float(modelo.coef_[0])
            r2 = float(modelo.score(x, y))

            
            limite_atencao_max = (float(j["limite_max"]) * (1 - MARGEM_ATENCAO) if j["limite_max"] is not None else None)

            limite_atencao_min = (float(j["limite_min"]) * (1 + MARGEM_ATENCAO) if j["limite_min"] is not None else None)

           

            horas_atencao = None
            horas_critico = None

            if j["limite_max"] is not None and valor_atual >= float(j["limite_max"]):
                horas_critico = 0
            
            elif j["limite_min"] is not None and valor_atual <= float(j["limite_min"]):
                horas_critico = 0

            if subida > 0 and j["limite_max"] is not None:

                if valor_atual < limite_atencao_max:
                    horas_atencao = (limite_atencao_max - valor_atual) / subida
                    #subida é quanto aumenta por hora
                    #ou seja pega quanto falta ate o limite e divide por quanto aumenta por hora
                    #ex: limite= 90  valor = 70    subida: 5 por hora
                    #90 - 70 = 20;   20 / 5 = 4;     falta 4 horas pra atingir o limite
                else:
                    horas_atencao = 0

                if valor_atual < float(j["limite_max"]):
                    horas_critico = (float(j["limite_max"]) - valor_atual) / subida
                else:
                    horas_critico = 0

    
            elif subida < 0 and j["limite_min"] is not None:

                if valor_atual > limite_atencao_min:
                    horas_atencao = (limite_atencao_min - valor_atual) / subida
                else:
                    horas_atencao = 0

                if valor_atual > float(j["limite_min"]):
                    horas_critico = (float(j["limite_min"]) - valor_atual) / subida
                else:
                    horas_critico = 0

            
            if horas_atencao is not None and not 0 <= horas_atencao <= 72:
                horas_atencao = None

            if horas_critico is not None and not 0 <= horas_critico <= 72:
                horas_critico = None


            #declara tudo no nivel de atencao
            horas = horas_atencao
            limite = limite_atencao_max if subida > 0 else limite_atencao_min
            nivel = "atencao"

            #se as condicoes do critico baterem =, vira critco
            if horas_critico == 0 or (horas_critico is not None and(horas is None or horas_critico < horas)):
                horas = horas_critico
                limite = j["limite_max"] if subida > 0 else j["limite_min"]
                nivel = "critico"

            if horas is None:
                limite = None
            elif limite is not None:
                limite = float(limite)

            if r2 >= 0.7:
                qualidade_tendencia = "boa"
            elif r2 >= 0.4:
                qualidade_tendencia = "moderada"
            else:
                qualidade_tendencia = "baixa"


            componentes.append({
                "componente": componente,
                "valor_atual": round(valor_atual, 2),
                "valor_limite": limite,
                "direcao": "subindo" if subida > 0 else "descendo" if subida < 0 else "estavel",
                "subida_por_hora": round(subida, 3),
                "valores_previstos": {
                    "6h": round(max(0, valor_atual + subida * 6), 2),
                    "12h": round(max(0, valor_atual + subida * 12), 2),
                    "24h": round(max(0, valor_atual + subida * 24), 2),
                    "48h": round(max(0, valor_atual + subida * 48), 2)
                },
                "horas_ate_limite": round(horas, 1) if horas is not None else None,
                "horario_estimado": (formatar_timestamp(ultima_leitura + pd.Timedelta(hours=horas)) if horas is not None else None),
                "horas_ate_atencao": round(horas_atencao, 1) if horas_atencao is not None else None,
                "horas_ate_critico": round(horas_critico, 1) if horas_critico is not None else None,
                "nivel_proximo_risco": nivel if horas is not None else None,
                "r2": round(r2, 3),
                "qualidade_tendencia": qualidade_tendencia
            })

    
        if componentes:
            tabela = pd.DataFrame(componentes).sort_values("horas_ate_limite", ascending=True, na_position="last")
            tabela = tabela.astype(object).where(tabela.notna(), None)
            componentes = tabela.to_dict(orient="records")

        if componentes:
            previsoes_validas = []
            for c in componentes:
                if c["horas_ate_limite"] is not None:
                    previsoes_validas.append(c)
         
            previsoes.append({
                "id_maquina": int(id_maquina),
                "proxima_ocorrencia_em_horas": ( previsoes_validas[0]["horas_ate_limite"] if previsoes_validas else None),
                "componente_com_maior_risco": (previsoes_validas[0]["componente"] if previsoes_validas else None),
                "componentes": componentes
            })

    if not previsoes:
        return []

    tabela = pd.DataFrame(previsoes).sort_values("proxima_ocorrencia_em_horas", ascending=True, na_position="last")
    tabela = tabela.astype(object).where(tabela.notna(), None)
    return tabela.to_dict(orient="records")




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
        previsoes = gerar_previsoes(historico, limites, agora)
        gravar_s3(f"{destino}/previsoes/previsoes.json", previsoes)

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
                    previsoes = gerar_previsoes(dados, limites, agora)
                    gravar_s3(f"{destino}/previsoes/previsoes.json", previsoes)
                    
    print("teste")


PADRAO_CHAVE = re.compile(r"^(\d+)/(\d{4})/(\d{2})/(\d{2})/clean_data_[^/]+\.csv$")


def chave_controle(idempresa):
    return f"{idempresa}/_controle/operacional_lidos.json"


def ler_controle(idempresa):
    try:
        resposta = s3.get_object(Bucket=bucket_gold, Key=chave_controle(idempresa))
    except s3.exceptions.NoSuchKey:
        return set()
    return set(json.loads(resposta["Body"].read().decode("utf-8")))


def salvar_controle(idempresa, lidos):
    s3.put_object(
        Bucket=bucket_gold,
        Key=chave_controle(idempresa),
        Body=json.dumps(sorted(lidos)).encode("utf-8"),
        ContentType="application/json",
    )


def listar_clean_data(idempresa):
    encontrados = {}
    paginator = s3.get_paginator("list_objects_v2")
    for pagina in paginator.paginate(Bucket=bucket_silver, Prefix=f"{idempresa}/"):
        for objeto in pagina.get("Contents", []):
            m = PADRAO_CHAVE.match(objeto["Key"])
            if m:
                _, a, me, d = m.groups()
                encontrados[objeto["Key"]] = date(int(a), int(me), int(d))
    return encontrados


def processar_empresa(idempresa):
    lidos = ler_controle(idempresa)
    pendentes = {
        chave: dia
        for chave, dia in listar_clean_data(idempresa).items()
        if chave not in lidos
    }

    if not pendentes:
        print(f"Empresa {idempresa}: nenhum arquivo novo no silver.")
        return [], []

    por_dia = {}
    for chave, dia in pendentes.items():
        por_dia.setdefault(dia, []).append(chave)

    processados, erros = [], []

    for dia, chaves in sorted(por_dia.items()):
        try:
            main(idempresa, dia)
        except Exception as erro:
            print(f"Erro operacional empresa {idempresa}, dia {dia}: {erro}")
            erros.append(f"{idempresa}/{dia}: {erro}")
            continue  

        lidos.update(chaves)
        salvar_controle(idempresa, lidos)
        processados += chaves

    return processados, erros


def lambda_handler(event, context=None):
    empresas = set()
    for r in event.get("Records", []):
        chave = unquote_plus(r["s3"]["object"]["key"])
        m = PADRAO_CHAVE.match(chave)
        if m:
            empresas.add(int(m.group(1)))

    processados, erros = [], []
    for idempresa in sorted(empresas):
        p, e = processar_empresa(idempresa)
        processados += p
        erros += e

    return {"processados": processados, "erros": erros}