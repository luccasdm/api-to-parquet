import requests
import json
import pandas as pd
from refreshtoken import *
import os

access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJodHRwczovL2FwaS5yZC5zZXJ2aWNlcyIsInN1YiI6Ilh4WGZuY294TEt2UjRSeVhUckZIaDRxeThTUzI1cktBeHU0T2JYVjBwVE1AY2xpZW50cyIsImF1ZCI6Imh0dHBzOi8vYXBwLnJkc3RhdGlvbi5jb20uYnIvYXBpL3YyLyIsImFwcF9uYW1lIjoiQVBJIEF6dXJlIiwiZXhwIjoxNjk5MzE4OTY4LCJpYXQiOjE2OTkyMzI1NjgsInNjb3BlIjoiIn0.EDeWtnOOJVHbMeIlLgMmyArFtXAbLsyWQ8Co2v1NJJASIOlHfIQxFfTd3RL-_yHuR4DVcwpH168j-PFhyww9c-f4xuUEEBYXGPtUJmBRzO8JC8id9UYyoSdfmINdX0hEe6QxrfH5xiwrPm6XLClSTAUZV2MSnZk5yxBc74gz9x1qLeh4v6AIuUzFjPyIWeRLy-s2-kdXqjLS-28DKjCl13lsTkFDY5nH37TK02u0DBi6_7JNjp0HwWFCeUqSWeNm0Yav7zKPdg6_GR5NDZJmqtT2nB8vQiJmuC76PBFf6hLKVUM_hM-MQWuFYCmE3FRbfOnA9qvx0Tfv_X6JOT-jFw"

# URL da API
url = "https://api.rd.services/platform/embeddables"

# Verifica a validade do token
#verificar_validade_token(access_token, url)

headers = {"accept": "application/json",
           "authorization": access_token
        }

# Parâmetros da solicitação
params = { 
            "page_size": "100",
            "page": "1"
        }

# Lista para armazenar os dados de todas as páginas
all_data = []

while True:
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        data = response.json()
        if not data:
            print(f"Fim do arquivo: {data}")
            ultima_pagina = params["page"]
            add_ultima = [f"total_de_paginas={ultima_pagina}"]
            schema = {'title': str}
            df_ultima = pd.DataFrame(add_ultima, columns=schema)
            all_data.append(df_ultima)
            break  # Nenhum dos dados foram retornados
        df = pd.DataFrame(data)
        all_data.append(df)
        
        # Atualiza o número da página
        params["page"] = str(int(params["page"]) + 1)

        # Exibe páginação e status
        print(f"Página atual: {params['page']}")
        print(f"Status de retorno: {response.status_code}")
    else:
        print(f"Erro na solicitação: {response.status_code}")
        break

# Altera caminho para pastas de "arquivos"
pasta_atual = os.getcwd()

if pasta_atual != f"{pasta_atual}\\arquivos":
    try:
        pasta_desejada = f"{pasta_atual}\\arquivos"
        os.chdir(pasta_desejada)
    except OSError:
        print(f"")
else:
    print(f"Já está no diretório desejado")

# Concatena todos os DataFrames em um único DataFrame
final_data = pd.concat(all_data, ignore_index=True)

# Salva o DataFrame em formato Parquet
final_data.to_parquet("formularios-api.parquet", index=False)