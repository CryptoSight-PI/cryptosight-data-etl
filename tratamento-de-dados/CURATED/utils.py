import os
import re
import sys
from getmac import get_mac_address
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from configenvironment import cursor
ARQUIVOS = ("clean_data.csv", "dados_nulos.csv", "treated-data.json")



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


# buscar_farm(1)
# buscar_sessoes(1)
# buscar_maquinas_sessao(1)