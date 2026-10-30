from configenvironment import s3 , bucket_silver , bucket_gold , bucket_bronze , cursor
# from trusted_to_curated import sanitizar_nome, ARQUIVOS
from CURATED.utils import sanitizar_nome , ARQUIVOS
from raw_to_trusted import buscar_empresa, particao

def main(idempresa , dia):
    nome_empresa = buscar_empresa(idempresa)
    nome_sanitizado = sanitizar_nome(nome_empresa)
    
    prefixo_silver = f"{idempresa}/{particao(dia)}"
    
    for nome_arquivo in ARQUIVOS:
        chave_origem = f"{prefixo_silver}/{nome_arquivo}"
    
        resposta = s3.get_object(
            Bucket=bucket_silver,
            Key=chave_origem,
        )
    
        conteudo = resposta["Body"].read()
    
        chave_destino = (
            f"{idempresa}/{nome_sanitizado}/rede/{nome_arquivo}"
        )
    
        content_type = (
            "application/json"
            if nome_arquivo.endswith(".json")
            else "text/csv"
        )
    
        s3.put_object(
            Bucket=bucket_gold,
            Key=chave_destino,
            Body=conteudo,
            ContentType=content_type,
        )
    
    print("teste")