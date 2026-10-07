
import pandas as pd
import glob
import time

# Junta os csvs a cada x periodo de tempo
# estou usando  minuto so por simulação/teste
while True :

    #Acha os arquivos csvs
    find_csv = glob.glob("1-*.csv")

    if not find_csv:
        print("Nenhum arquivo CSV encontrado!")
    else:

     # Faz uma lista de dataframes com os csvs coletados
        dataframe_list = [pd.read_csv(files) for files in find_csv]

     # Junta os dataframes 
        final_csv = pd.concat(dataframe_list, ignore_index=True)

     # junsta todos os csvs em um
        final_csv.to_csv("all-collected.csv", index=False)
     
        print(f" {len(find_csv)} arquivos unificados.")
        
    time.sleep(60)
