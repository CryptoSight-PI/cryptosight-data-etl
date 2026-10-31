
import csv
import json
import re
from datetime import date, datetime, timezone
from configenvironment import s3, bucket_bronze, bucket_silver , cursor
import io

import pandas as pd


COMPONENTE_COLUNAS = {
    "cpu":         ["cpu_percent", "cpu_frequency"],
    "ram":         ["ram_percent"],
    "swap":        ["swap_memory_total", "swap_memory_used", "swap_memory_percent"],
    "rede":        ["upload_speed", "download_speed"],
    "temperatura": ["temperature"],
    "ventoinha":   ["fans_speed"],
    "disco":       ["disk"],
    "gpu":         ["gpu_usage", "gpu_energy", "gpu_temp", "gpu_fan_speed"],
}

NUMERICAS = [
    "cpu_percent", "cpu_frequency", "ram_percent",
    "swap_memory_total", "swap_memory_used", "swap_memory_percent",
    "upload_speed", "download_speed", "temperature", "fans_speed", "disk",
    "gpu_usage", "gpu_energy", "gpu_temp", "gpu_fan_speed",
]
OBRIGATORIAS = ["timestamp"]


NAO_ZERO = ["cpu_percent", "cpu_frequency", "ram_percent"]
VETOR_NAO_ZERO = ["gpu_temp"]

COLUNAS_VETOR = ["gpu_usage", "gpu_energy", "gpu_temp", "gpu_fan_speed"]

def buscar_monitorados(id_empresa):
    cursor.execute(""" select m.mac_address, c.nome, mc.monitorado from empresa e join farm f on f.id_empresa = e.id join sessao s on s.id_farm = f.id join maquina m on m.id_sessao = s.id join maquina_componente mc on mc.id_maquina = m.id join componente c on c.id = mc.id_componente
        where e.id = %s
        """,
        (id_empresa,),
    )

    monitorados = {}
    for mac, nome, monitorado in cursor.fetchall():
        status = 1 if monitorado in (1, True) else 0
        maccerto = str(mac).strip().lower().replace("-", ":")
        if maccerto not in monitorados:
            monitorados[maccerto] = {}

        monitorados[maccerto][nome.lower()] = status

    return monitorados   


def particao(dia):
    """date -> '2026/10/21'"""
    return f"{dia:%Y/%m/%d}"

def ler_csv_s3(chave):
    resposta = s3.get_object(
        Bucket=bucket_bronze,
        Key=chave
    )

    conteudo = resposta["Body"].read()

    return pd.read_csv(
        io.BytesIO(conteudo),
        sep=";"
    )

def csv_em_memoria(df):
    buffer = io.StringIO()

    df.to_csv(
        buffer,
        sep=";",
        index=False
    )

    return buffer.getvalue().encode("utf-8")

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

def vetor_valido(texto):
    for i in str(texto).strip("[] ").split(","):
        try:
            numero = float(i)
        except ValueError:
            return False
        if numero != numero:   
            return False
    return True

def vetor_sem_zero(texto):
    for i in str(texto).strip("[] ").split(","):
        if i.strip() in ("0" , "0.0"):
            return False
    return True


def separar_validos(df, monitorados):
    if "timestamp" not in df.columns:
        raise ValueError("O CSV de entrada não contém a coluna timestamp")

    df = df.copy()

    for coluna in NUMERICAS: 
        if coluna not in df.columns:
            df[coluna] = pd.NA

    escalares = [c for c in NUMERICAS if c not in COLUNAS_VETOR]
    df[escalares] = df[escalares].apply(pd.to_numeric, errors="coerce")

    df["timestamp"] = df["timestamp"].replace("", pd.NA)

    mask = pd.DataFrame(False, index=df.index, columns=NUMERICAS)
    for mac, grupo in df.groupby("user"):
        comps = monitorados.get(str(mac).strip().lower().replace("-", ":") , {})
        colunas = []
    
        for componente , colunas_do_componente in COMPONENTE_COLUNAS.items():
            if comps.get(componente) == 1:
                for i in colunas_do_componente:
                    colunas.append(i)
        mask.loc[grupo.index , colunas] = True

    for i in COLUNAS_VETOR:
        novos = []
        for j in df.index:
            valor = df.loc[j , i]
            if mask.loc[j , i] and not vetor_valido(valor):
                novos.append(pd.NA)
            else:
                novos.append(valor)
        df[i] = novos

    for i in VETOR_NAO_ZERO:
        novos = []
        for j in df.index:
            valor = df.loc[j, i]
            if mask.loc[j, i] and not vetor_sem_zero(valor):
                novos.append(pd.NA)
            else:
                novos.append(valor)
        df[i] = novos

    

    
            

    nulo_monitorado = (df[NUMERICAS].isnull() & mask).any(axis=1)
    zero_monitorado = ((df[NAO_ZERO] == 0) & mask[NAO_ZERO]).any(axis=1)

    print("Descartadas por nulo:", nulo_monitorado.sum())
    print("Descartadas por zero:", zero_monitorado.sum())
    print("\nNulas e monitoradas, por coluna:")
    print((df[NUMERICAS].isnull() & mask).sum())
    print("\nValores de ram_percent:", df["ram_percent"].unique())
    invalida = nulo_monitorado | zero_monitorado | df["timestamp"].isnull()
    return df.loc[~invalida], df.loc[invalida]

def csv_to_json(conteudo_csv, id_empresa):
    texto = conteudo_csv.decode("utf-8")

    reader = csv.DictReader(
        io.StringIO(texto, newline=""),
        delimiter=";",
    )

    dados = []

    for row in reader:
        linha = {"id_empresa": id_empresa}
        linha.update(row)
        dados.append(linha)

    return json.dumps(
        dados,
        ensure_ascii=False,
        indent=4,
    ).encode("utf-8")



def salvar_s3(chave, conteudo, content_type):
    s3.put_object(
        Bucket=bucket_silver,
        Key=chave,
        Body=conteudo,
        ContentType=content_type,
    )


def main(id_empresa, arquivos_empresa, dia):
    if not bucket_bronze or not bucket_silver:
        raise ValueError(
            "Configure AWS_BUCKET_BRONZE/AWS_BUCKET_RAW e "
            "AWS_BUCKET_SILVER/AWS_BUCKET_TRUSTED."
        )

    if not arquivos_empresa:
        raise ValueError(
            f"Nenhum CSV encontrado para a empresa {id_empresa}."
        )
    iddoarquivo = datetime.now().strftime("%Y%m%dT%H%M%S%fZ")

    dataframes = [
        ler_csv_s3(chave)
        for chave in arquivos_empresa
    ]

    dados = pd.concat(
        dataframes,
        ignore_index=True,
    )

    monitorados = buscar_monitorados(id_empresa)

    limpos, nulos = separar_validos(dados , monitorados)

    prefixo = f"{id_empresa}/{particao(dia)}"

    conteudo_limpo = csv_em_memoria(limpos)
    conteudo_nulos = csv_em_memoria(nulos)

    conteudo_json = csv_to_json(
        conteudo_limpo,
        id_empresa,
    )

    salvar_s3(
        f"{prefixo}/clean_data_{iddoarquivo}.csv",
        conteudo_limpo,
        "text/csv",
    )

    salvar_s3(
        f"{prefixo}/dados_nulos_{iddoarquivo}.csv",
        conteudo_nulos,
        "text/csv",
    )

    salvar_s3(
        f"{prefixo}/treated-data_{iddoarquivo}.json",
        conteudo_json,
        "application/json",
    )

    print(
        f"Empresa {id_empresa} | Data {particao(dia)} | "
        f"Silver concluído: {len(limpos)} linhas limpas, "
        f"{len(nulos)} descartadas."
    )

