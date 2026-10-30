import re

ARQUIVOS = ("clean_data.csv", "dados_nulos.csv", "treated-data.json")



def sanitizar_nome(nome_empresa):
    nome = re.sub(r'[<>:"/\\|?*]', "_", nome_empresa).strip(" .")

    if not nome:
        raise ValueError("Nome inválido para a empresa.")

    return nome 