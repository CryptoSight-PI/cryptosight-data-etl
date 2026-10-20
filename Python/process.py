import psutil
import pandas as pd


def capturarprocesso():
    lista = []
    for proc in psutil.process_iter(['pid', 'name','username', 'cpu_percent', 'memory_percent']):
        print(proc.info)
        lista.append({
            'pid' : proc.info['pid'],
            'name' : proc.info['name'],
            'username' : proc.info['username'],
            'cpu_percent' : proc.info['cpu_percent'],
            'memory_percent' : proc.info['memory_percent'],
        })
    return pd.DataFrame(lista)

