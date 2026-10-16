import pandas as pd
import numpy as np

# Cria um dataframe com o csv gerado pelo arquivo csv_bind.py
df = pd.read_csv("all-collected.csv", sep=';')

# vetor com nome das colunas que serão apagadas
clean_cloumns = [
    'cpu_percent', 'cpu_frequency', 'ram_percent', 
    'swap_memory_total', 'swap_memory_used', 'swap_memory_percent', 
    'upload_speed', 'download_speed', 'timestamp'
]


# se as colunas que estão no vetor vierm zeradas apaga as linha
df[clean_cloumns] = df[clean_cloumns].replace(0, np.nan)

# cria um data frame para armazenar os dados filtrados 
df_filtrado = df.dropna(subset=clean_cloumns)

# coloca os dados filtrados em um csv
df_filtrado.to_csv("clean_data.csv", sep=';', index=False)

