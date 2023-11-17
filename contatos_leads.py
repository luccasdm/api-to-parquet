import requests
import json
import pandas as pd
from refreshtoken import *
import os

access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJodHRwczovL2FwaS5yZC5zZXJ2aWNlcyIsInN1YiI6Ilh4WGZuY294TEt2UjRSeVhUckZIaDRxeThTUzI1cktBeHU0T2JYVjBwVE1AY2xpZW50cyIsImF1ZCI6Imh0dHBzOi8vYXBwLnJkc3RhdGlvbi5jb20uYnIvYXBpL3YyLyIsImFwcF9uYW1lIjoiQVBJIEF6dXJlIiwiZXhwIjoxNjk4MzY4NDQ1LCJpYXQiOjE2OTgyODIwNDUsInNjb3BlIjoiIn0.gYLl4slMP5ENNgorzC-V-IFZtaD6qvg_NTHsdE_EEhZAfuPbj4Z_Kkch3gqpPQLHmfGUbjjDsAx54PE9PQq9QyITAVrtHO69XtZ9B4v0ehLY3hMF726ou38DwmvnrGMKR1xa2ZDdNYDQFUl-AdUuO04iQhgNU7pnBEIEaoIi_tsuFQjEwqP0bPCUZQig7XxHl2YLTiQsgyZXuQ9gXgSLigbAzQOijOb8nYLtPS8CoyglBfhzUgjqwtAeyyo9JRMyU9N6YJb6Qy7fSupDpHA5x0_Qm_5Mfyz6GC4fgu0iEa2FF7eKSBFQ4o4wOzbsqGZ6hasyEcuvSbfjhbt5eZVqog"

# URL da API
url = "https://api.rd.services/platform/segmentations/4603878/contacts"


headers = {"accept": "application/json",
           "authorization": access_token
        }

# Parâmetros da solicitação
params = { 
            "page": "1",
            "page_size": "125"
        }

data_sem_links = []
data_links = []
data_all = []
data_uuid = []


while True:
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()

        if data['contacts'] == [] or response.status_code == 429:
            print(f"Fim do arquivo: {data}")
            ultima_pagina = params["page"]
            add_ultima = [f"total_de_paginas={ultima_pagina}"]
            schema = {'uuid': str}
            df_ultima = pd.DataFrame(add_ultima, columns=schema)
            data_all.append(df_ultima)
            break  # Nenhum dos dados foram retornados

        df = pd.DataFrame(data['contacts'])

        data_all.append(df)
               
        # Atualiza o número da página
        params["page"] = str(int(params["page"]) + 1)

        # Exibe páginação e status
        print(f"Página atual: {params['page']}")
        print(f"Status de retorno: {response.status_code}")
    else:
        print(f"Erro na solicitação: {response.status_code}")
        break


# Concatena todos os DataFrames em um único DataFrame
final_all_data = pd.concat(data_all, ignore_index=True)

# Dataframe com apenas UUID
df_uuid = final_all_data['uuid']

# Separa campos aninhados de LINKS
df_links = final_all_data['links'].explode().apply(pd.Series)

df_links = pd.merge(df_uuid, df_links, left_index=True, right_index=True)

# Selecionando apenas as colunas corretas
df_links = df_links[['uuid', 'rel', 'href', 'media', 'type']]

# Dataframe sem o campo de links
new_df = final_all_data.drop(columns=['links'])

# Converta a série em um DataFrame com uma única coluna chamada 'uuid'.
# df_uuid_final = pd.DataFrame({'uuid': final_data_uuid})

# Altera caminho para pastas de "arquivos"
pasta_atual = os.getcwd()

if pasta_atual != f"{pasta_atual}\\arquivos":
    try:
        pasta_desejada = f"{pasta_atual}\\arquivos"
        os.chdir(pasta_desejada)
    except OSError:
        print(f"Não foi possível ou Já está no diretório desejado")
else:
    print(f"Já está no diretório desejado")

# Salva o DataFrame em formato Parquet
new_df.to_parquet("contatos-leads-api.parquet", index=False)

df_uuid.to_frame().to_parquet("uuids-for-events-leads.parquet", index=False)

df_links.to_parquet("contatos-leads-links.parquet", index=False)
