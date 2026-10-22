import os
import re
import shutil
import mysql.connector
from dotenv import load_dotenv
from s3_utils import enviar_para_s3  # função que envia arquivos ao S3

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE")
}

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PASTA_SILVER = os.path.join(BASE_DIR, "Silver")

ARQUIVOS = [
    "clean_data.csv",
    "dados_nulos.csv",
    "treated-data.json"
]


def extrair_id(nome_arquivo):
    nome = os.path.basename(nome_arquivo)
    resultado = re.match(r"^(\d+)", nome)

    if resultado:
        return int(resultado.group(1))

    raise ValueError(
        f"Não foi possível encontrar o ID no arquivo: {nome}"
    )


def buscar_empresa(id_empresa):
    conexao = mysql.connector.connect(**DB_CONFIG)

    try:
        cursor = conexao.cursor()

        consulta = """
            SELECT id, razao_social, nome_fantasia
            FROM empresa
            WHERE id = %s
        """

        cursor.execute(consulta, (id_empresa,))
        empresa = cursor.fetchone()

        if empresa is None:
            raise ValueError(
                f"Nenhuma empresa encontrada com ID {id_empresa}"
            )

        razao_social = empresa[1]
        nome_fantasia = empresa[2]
        nome_empresa = nome_fantasia or razao_social

        if not nome_empresa:
            raise ValueError(
                f"A empresa {id_empresa} não tem nome cadastrado"
            )

        return nome_empresa

    finally:
        if 'cursor' in locals():
            cursor.close()

        conexao.close()


def criar_pasta(nome_empresa):
    nome_pasta = re.sub(r'[<>:"/\\|?*]', "_", nome_empresa).strip()

    pasta_destino = os.path.join(BASE_DIR, nome_pasta)
    os.makedirs(pasta_destino, exist_ok=True)

    for arquivo in ARQUIVOS:
        origem = os.path.join(PASTA_SILVER, arquivo)

        if os.path.isfile(origem):
            destino = os.path.join(pasta_destino, arquivo)
            shutil.move(origem, destino)
            print(f"Arquivo movido: {origem} -> {destino}")

            # Envia para a camada gold do bucket, dentro da pasta da empresa
            enviar_para_s3(destino, f"{nome_pasta}/{arquivo}")

            # Só chega aqui se o envio deu certo (se falhar, o erro para antes)
            os.remove(destino)  # apaga o arquivo da máquina local
            print(f"Arquivo apagado da máquina local: {destino}")
        else:
            print(f"Arquivo não encontrado: {origem}")

    # Se a pasta da empresa ficou vazia, apaga ela também
    if not os.listdir(pasta_destino):
        os.rmdir(pasta_destino)

    print(f"Pasta da empresa: {pasta_destino}")


def main():
    arquivo_origem = "1-all-collected.csv"

    try:
        id_empresa = extrair_id(arquivo_origem)
        nome_empresa = buscar_empresa(id_empresa)

        print(f"ID da empresa: {id_empresa}")
        print(f"Nome da empresa: {nome_empresa}")

        criar_pasta(nome_empresa)

        print("Processo finalizado.")

    except mysql.connector.Error as erro:
        print(f"Erro no MySQL: {erro}")

    except (ValueError, OSError) as erro:
        print(f"Erro: {erro}")


if __name__ == "__main__":
    main()