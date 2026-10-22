from configenvironment import cursor
from getmac import get_mac_address
import json

id = 1
mac = get_mac_address()


query = "select mc.monitorado , c.nome from empresa e join farm f on f.id_empresa = e.id join sessao s on s.id_farm = f.id join maquina m join maquina_componente mc  on mc.id_maquina = m.id join componente c on mc.id_componente = c.id where m.mac_address = %s and e.id = %s;"
cursor.execute(query , (mac , id))
resultado = cursor.fetchall()
print(resultado)

lista = {}

for i in resultado:
    status = 1 if i[0] in (1, True) else 0
    lista[i[1]] = status

json1 = json.dumps(lista, indent=4, ensure_ascii=False)

# print(json1)
#     volta assim :
#     {
#     "CPU": 1,
#     "Rede": 1,
#     "RAM": 0,
#     "Disco": 1,
#     "GPU": 1,
#     "Swap": 1,
#     "Ventoinha": 1,
#     "Temperatura": 1
# }
