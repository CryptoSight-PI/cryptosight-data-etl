
import os
import re
import shutil
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE")
}

ARQUIVOS = [
    "clean_data.csv",
    "dados_nulos.csv",
    "formated-data.json"
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

    os.makedirs(nome_pasta, exist_ok=True)

    for arquivo in ARQUIVOS:
        if os.path.isfile(arquivo):
            destino = os.path.join(nome_pasta, os.path.basename(arquivo))
            shutil.move(arquivo, destino)
            print(f"Arquivo movido: {arquivo} -> {destino}")
        else:
            print(f"Arquivo não encontrado: {arquivo}")

    print(f"Pasta da empresa: {nome_pasta}")


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
