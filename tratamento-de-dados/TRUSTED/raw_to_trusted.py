import re
from datetime import date, datetime, timezone
from urllib.parse import unquote_plus
from configenvironment import s3, bucket_bronze, bucket_silver , cursor
import io
import numpy as np

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


# NAO_ZERO = ["cpu_percent", "cpu_frequency", "ram_percent"]
NAO_ZERO = ["cpu_frequency"]
VETOR_NAO_ZERO = ["gpu_temp"]

COLUNAS_VETOR = ["gpu_usage", "gpu_energy", "gpu_temp", "gpu_fan_speed"]


MODO_VETOR = {
    "gpu_usage": "mean",
    "gpu_energy": "sum",
    "gpu_temp": "max",
    "gpu_fan_speed": "mean",
}


FUSO_COLETOR = "America/Sao_Paulo"

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

def vetor_para_numero(valor, modo):
    valores = []

    for parte in str(valor).strip("[] ").split(","):
        parte = parte.strip()

        if parte in ("", "None", "nan", "<NA>"):
            continue

        try:
            numero = float(parte)
        except ValueError:
            return np.nan

        if not np.isfinite(numero):
            return np.nan

        valores.append(numero)

    if not valores:
        return np.nan

    if modo == "max":
        return max(valores)
    if modo == "sum":
        return sum(valores)
    return sum(valores) / len(valores)




def separar_validos(df, monitorados):
    if "user" not in df.columns:
        raise ValueError("O CSV de entrada não contém a coluna user")

    if "timestamp" not in df.columns:
        raise ValueError("O CSV de entrada não contém a coluna timestamp")

    df = df.copy()

    df["user"] = (df["user"].astype("string").str.strip().str.lower().str.replace("-", ":", regex=False))

    for coluna in NUMERICAS: 
        if coluna not in df.columns:
            df[coluna] = pd.NA

    escalares = [c for c in NUMERICAS if c not in COLUNAS_VETOR]
    df[escalares] = df[escalares].apply(pd.to_numeric, errors="coerce")

    df[escalares] = df[escalares].replace([np.inf, -np.inf],np.nan)

    for coluna in COLUNAS_VETOR:
        numeros = []
        for texto in df[coluna]:
            numeros.append(vetor_para_numero(texto, MODO_VETOR[coluna]))
        df[coluna] = numeros

    df["timestamp"] = pd.to_datetime(df["timestamp"],errors="coerce", format="mixed")
    df["timestamp"] = df["timestamp"].dt.tz_localize(FUSO_COLETOR, ambiguous="NaT", nonexistent="NaT").dt.tz_convert("UTC")


    mask = pd.DataFrame(False, index=df.index, columns=NUMERICAS)
    for mac, grupo in df.groupby("user"):
        comps = monitorados.get(mac , {})
        colunas = []
    
        for componente , colunas_do_componente in COMPONENTE_COLUNAS.items():
            if comps.get(componente) == 1:
                for i in colunas_do_componente:
                    colunas.append(i)
        mask.loc[grupo.index , colunas] = True

    df[NUMERICAS] = df[NUMERICAS].where(mask, np.nan)
    # se nao monitora valor é removido 


    nulo_monitorado = (df[NUMERICAS].isnull() & mask).any(axis=1)
    zero_monitorado = ((df[NAO_ZERO] == 0) & mask[NAO_ZERO]).any(axis=1)

    futuro = df["timestamp"] > pd.Timestamp.now(tz="UTC") + pd.Timedelta(minutes=5)

    motivos = pd.DataFrame({
        "nulo_monitorado": nulo_monitorado,
        "zero_monitorado": zero_monitorado,
        "timestamp_invalido": df["timestamp"].isnull() | futuro,
        "user_invalido": df["user"].isna() | df["user"].eq("") | ~df["user"].isin(monitorados),
    }).astype(bool)

    invalida =( nulo_monitorado | zero_monitorado | df["timestamp"].isnull() | df["user"].isna() | df["user"].eq("") | ~df["user"].isin(monitorados) | futuro )

    texto_motivos = []
    for linha in motivos.to_numpy():
        texto_motivos.append(";".join(motivos.columns[linha]))
    texto_motivos = pd.Series(texto_motivos, index=df.index)


    limpos = df.loc[~invalida].drop_duplicates(subset=["user", "timestamp"], keep="last")  
    descartados = df.loc[invalida].assign(motivo_descarte=texto_motivos[invalida])

    print("Descartadas por motivo:")
    print(motivos.sum())
    print("\nNulas e monitoradas, por coluna:")
    print((df[NUMERICAS].isnull() & mask).sum())

    return limpos, descartados

def csv_to_json(conteudo_csv, id_empresa):
    df = pd.read_csv(
        io.BytesIO(conteudo_csv),
        sep=";"
    )

    df["id_empresa"] = id_empresa

    return df.to_json(
        orient="records",
        force_ascii=False,
        indent=4
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
    iddoarquivo = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")

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

    return [
        f"{prefixo}/clean_data_{iddoarquivo}.csv",
        f"{prefixo}/dados_nulos_{iddoarquivo}.csv",
        f"{prefixo}/treated-data_{iddoarquivo}.json",
    ]


def agrupar_por_empresa(arquivos):
    arquivos_por_empresa = {}
    arquivos_sem_id = []

    for chave in arquivos:
        nome = chave.rsplit("/", 1)[-1]
        resultado = re.match(r"^(\d+)[-_]", nome)

        if resultado:
            id_empresa = int(resultado.group(1))
            arquivos_por_empresa.setdefault(id_empresa, []).append(chave)
        else:
            arquivos_sem_id.append(nome)

    if arquivos_sem_id:
        raise ValueError(
            "Não foi possível identificar o ID da empresa nestes CSVs: "
            + ", ".join(arquivos_sem_id)
        )

    if not arquivos_por_empresa:
        raise ValueError("Nenhum ID de empresa foi encontrado nos CSVs.")

    return arquivos_por_empresa


def arquivar_arquivo(chave):

    s3.copy_object(
        Bucket=bucket_bronze,
        CopySource={
            "Bucket": bucket_bronze,
            "Key": chave,
        },
        Key=f"processados/{chave}",
    )

    s3.delete_object(
        Bucket=bucket_bronze,
        Key=chave,
    )


def chaves_do_evento(event):
    return [
        unquote_plus(r["s3"]["object"]["key"])
        for r in event.get("Records", [])
    ]


def lambda_handler(event, context=None):
    silver_keys = []
    erros = []

    arquivos = []
    for chave in chaves_do_evento(event):
        nome = chave.rsplit("/", 1)[-1]

        if not chave.startswith("metrics/"):
            continue

        if not nome.lower().endswith(".csv"):
            continue

        if "_processos_" in nome.lower():
            continue

        arquivos.append(chave)

    if not arquivos:
        print("Nenhum arquivo pendente encontrado.")
        return {"silver_keys": silver_keys, "erros": erros}

    arquivos_por_data = {}

    for chave in arquivos:
        partes = chave.split("/")

        # metrics/AAAA/MM/DD/arquivo.csv
        if len(partes) < 5:
            print(f"Caminho ignorado por formato inválido: {chave}")
            continue

        try:
            dia = date(int(partes[1]), int(partes[2]), int(partes[3])
            )
        except ValueError:
            print(f"Data inválida no caminho: {chave}")
            continue

        arquivos_por_data.setdefault(dia, []).append(chave)

    for dia, arquivos_dia in sorted(arquivos_por_data.items()):
        try:
            arquivos_por_empresa = agrupar_por_empresa(arquivos_dia)
        except ValueError as erro:
            print(f"Dia {dia} ignorado: {erro}")
            continue

        for id_empresa, arquivos_empresa in sorted(
            arquivos_por_empresa.items()
        ):
            try:
                silver_keys += main(id_empresa, arquivos_empresa, dia)

                for chave in arquivos_empresa:
                    arquivar_arquivo(chave)

            except Exception as erro:
                print(
                    f"Erro na empresa {id_empresa}, "
                    f"data {dia}: {erro}"
                )
                erros.append(f"{id_empresa}/{dia}: {erro}")
                continue

    return {"silver_keys": silver_keys, "erros": erros}