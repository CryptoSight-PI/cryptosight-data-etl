import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression


FUSO_LOCAL = "America/Sao_Paulo"

MAPA_COMPONENTES = {
    "temperatura": "temperature",
    "ram": "ram_percent",
    "gpu": "gpu_usage"
}


def formatar_timestamp(timestamp):
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")

    return timestamp.tz_convert(FUSO_LOCAL).isoformat(timespec="seconds")


def criar_dados_mock(agora):
    """
    Cria 72 horas de dados fictícios para uma rig.

    Temperatura: tendência de subida.
    RAM: praticamente estável.
    GPU: oscila, sem tendência forte.
    """

    timestamps = pd.date_range(
        end=agora,
        periods=72,
        freq="1h"
    )

    np.random.seed(42)

    dados = []

    for hora, timestamp in enumerate(timestamps):
        dados.append({
            "timestamp": timestamp,
            "_id_maquina": 101,
            "temperature": 55 + (hora * 0.35) + np.random.normal(0, 1.2),
            "ram_percent": 65 + np.random.normal(0, 2),
            "gpu_usage": 85 + np.random.normal(0, 3)
        })

    return pd.DataFrame(dados)


def criar_limites_mock():
    """
    Simula os limites que viriam do catálogo do Gold/MySQL.
    """

    return [
        {
            "id_maquina": 101,
            "componente": "temperatura",
            "limite_min": None,
            "limite_max": 90,
            "monitorado": True
        },
        {
            "id_maquina": 101,
            "componente": "ram",
            "limite_min": None,
            "limite_max": 90,
            "monitorado": True
        },
        {
            "id_maquina": 101,
            "componente": "gpu",
            "limite_min": None,
            "limite_max": 100,
            "monitorado": True
        }
    ]


def gerar_previsoes(dados, limites, agora):

    previsoes = []

    dados = dados.copy()
    dados["hora"] = dados["timestamp"].dt.floor("60min")

    for id_maquina in dados["_id_maquina"].unique():

        maquina = dados[dados["_id_maquina"] == id_maquina]
        componentes = []

        for j in limites:

            if j["id_maquina"] != id_maquina or not j["monitorado"]:
                continue

            componente = j["componente"].lower()
            coluna = MAPA_COMPONENTES.get(componente)

            if coluna is None or j["limite_max"] is None:
                continue

            #agrupei por hora e calculei a media nessa hora
            #ou seja, serie é o df agrupado por hora, as ultimas 48 horas = vai virar uma variavel
            serie = maquina.groupby("hora")[coluna].mean().dropna().tail(48)

            #limite para prever historico ne, no script final vai virar uma variavel esse 12
            if len(serie) < 12:
                continue

            # O eixo X representa quantas horas se passaram desde a primeira amostra.
            x = (
                (serie.index - serie.index[0]).total_seconds() / 3600
            ).values.reshape(-1, 1)
            # aqui ele pega a diferenca de horario desde a primeira hora, essencial para um grafico de tempo ne
            #ja que independe do horario, pq das 14 pras 15 e das 22 pras 23, abos passam 1 hora
            #mostra a esvolucao em si
            #o reshape deixa no formato de matriz pro scikit learn
            #como se fosse um eixo x mesmo
            #lembrando que o pandas ja tinha deixado ordenado por timestamp

            y = serie.values
            #aqui simplesmente pega os valores desse medicao

            modelo = LinearRegression().fit(x, y)
            #reta entre hora (tempo) e valor

            limite = float(j["limite_max"])
            valor_atual = float(y[-1])
            subida = float(modelo.coef_[0])
            #inclinacao da reta, ou seja quanto isso tende a aumentar ali


            if valor_atual >= limite:
                continue
            #tipo se ja ta em alerta, nao faz sentido prever falha

        
            if subida <= 0:
                continue
            #so sobe se tiver tendencia de subir

            horas = (limite - valor_atual) / subida
            #quantas horas ate atingir, pensa assim, subida é o quanto aumenta por hora, dai vira uma regra de 3 simples isso

            
            if horas > 72:
                continue
            #nao deixa prever coisa muito longe


            r2 = float(modelo.score(x, y))
            #o r quadrado em relacao ao historico

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
                "subida_por_hora": round(subida, 3),
                "valor_previsto_24h": round(valor_atual + subida * 24, 2),
                "horas_ate_limite": round(horas, 1),
                "horario_estimado": formatar_timestamp(
                    agora + pd.Timedelta(hours=horas)
                ),
                "r2": round(r2, 3),
                "qualidade_tendencia": qualidade_tendencia
            })

        #o componente que pode atingir o limite primeiro fica na frente.
        componentes = sorted(
            componentes,
            key=lambda c: c["horas_ate_limite"]
        )

        if componentes:
            previsoes.append({
                "id_maquina": int(id_maquina),
                "proxima_ocorrencia_em_horas": componentes[0]["horas_ate_limite"],
                "componente_com_maior_risco": componentes[0]["componente"],
                "componentes": componentes
            })

    #a maquina com o limite estimado mais próximo fica na frente.
    return sorted(
        previsoes,
        key=lambda p: p["proxima_ocorrencia_em_horas"]
    )


def main():

    agora = pd.Timestamp.now(tz="UTC")

    dados = criar_dados_mock(agora)
    limites = criar_limites_mock()

    previsoes = gerar_previsoes(dados, limites, agora)

    print("\n========== DADOS MAIS RECENTES ==========")
    print(dados.tail(5).to_string(index=False))

    print("\n========== PREVISÕES ==========")

    if not previsoes:
        print("Nenhuma tendência de estouro encontrada.")
        return

    for previsao in previsoes:

        print(f"\nMáquina: {previsao['id_maquina']}")
        print(
            "Próximo limite estimado em: "
            f"{previsao['proxima_ocorrencia_em_horas']} horas"
        )
        print(
            "Componente com maior risco: "
            f"{previsao['componente_com_maior_risco']}"
        )

        for componente in previsao["componentes"]:
            print(f"\n  Componente: {componente['componente']}")
            print(f"  Valor atual: {componente['valor_atual']}")
            print(f"  Limite: {componente['valor_limite']}")
            print(f"  Subida por hora: {componente['subida_por_hora']}")
            print(f"  Valor estimado em 24h: {componente['valor_previsto_24h']}")
            print(f"  Horas até o limite: {componente['horas_ate_limite']}")
            print(f"  Horário estimado: {componente['horario_estimado']}")
            print(f"  R²: {componente['r2']}")
            print(f"  Qualidade da tendência: {componente['qualidade_tendencia']}")


if __name__ == "__main__":
    main()
