import requests
import json
import pandas as pd
from refreshtoken import *
import os
import uuid
import time

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

access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJodHRwczovL2FwaS5yZC5zZXJ2aWNlcyIsInN1YiI6Ilh4WGZuY294TEt2UjRSeVhUckZIaDRxeThTUzI1cktBeHU0T2JYVjBwVE1AY2xpZW50cyIsImF1ZCI6Imh0dHBzOi8vYXBwLnJkc3RhdGlvbi5jb20uYnIvYXBpL3YyLyIsImFwcF9uYW1lIjoiQVBJIEF6dXJlIiwiZXhwIjoxNjk4MjgyMDQyLCJpYXQiOjE2OTgxOTU2NDIsInNjb3BlIjoiIn0.fGLnD4zS0ULmY8XPkZotyntrTSW2cRT0jV-IegNkloLdDNcZFApAwaVaWvCFeYbffXyG6_I3ZtYmny9r1CNcbT_t4wa4esND01IBGOJrn8Iu8m7k0OdA5C-3cb0gjNiYn18S41bGIaXVrksX0smKJOUa9YVtvuTHZWKWvPPBBdhtHmgqioVU-SoGCXDBfnXppk76P5rv8DD7Zxi3YJNTvCTvoZAL_HhNZ88c9ZLgAa3SWrVXshrp_9VOIA1aeQ0dPorYPojDYAFUNB3xekkpsGvaFm6vOuF0aRpAHVqx0_Eq1Bluc1hSRUw0nzb3xUbiGpjasdj_o4TIZhN8R3dNGw"

# URL da API 
url = "https://api.rd.services/platform/contacts/"

# Verifica a validade do token
# verificar_validade_token(access_token, url)

headers = {"accept": "application/json",
           "authorization": access_token
        }

string_to_check = '"message":"API rate limit exceeded"'
data_all = []
total_count = 0
loop = 0

# Lista com uuids para Eventos de Leads
df = pd.read_parquet('uuids-for-events-clients.parquet')

# Nome do campo que contém os UUIDs
uuid_field = "uuid"

# Crie um conjunto para armazenar os UUIDs já consultados
uuids_consultados = set()

# Itere sobre os dados e consulte apenas UUIDs não consultados
for value in df[uuid_field]:

    # Verifique se o UUID já foi consultado
    if value not in uuids_consultados:
        # if loop <= 10: # (apenas para testes)

        # Faça a consulta para este UUID
        print(f"START: Consultando UUID: {value}")

        # Marque o UUID como consultado
        uuids_consultados.add(value)

        # Constroi a URL completa 
        url_completa = f"{url}{value}/events?event_type=CONVERSION"

        # Faça a solicitação HTTP
        response = requests.get(url_completa, headers=headers)

        print(response.text)

        response_text_to_check = response.text
        
        # Verifica se o limite de requisições foi atendido e aplica um sleep de 30 segundos
        if string_to_check in response_text_to_check:
            print("PAUSE: Limite de requests atingido, aguardando 30 segundos")
            time.sleep(30)

            # Continua com consulta para o UUID pausado
            response = requests.get(url_completa, headers=headers)

            print(f"Consultando UUID {value} após pausa")
            print(response.text)

            data = json.loads(response.text)

            df = pd.DataFrame(data)
        
            data_all.append(df)

            # Acumula o total de registros
            total_count += len(data)
            print(f"Total de registros = {total_count}")

            # Exibe status
            print(f"DONE: Consulta bem-sucedida, próximo.")
            print(f"")

        elif response.status_code == 200:
            print(f"Status de retorno: {response.status_code} e o limite de requests ainda não foi atingido")
           
            # print(f"URL completa: {response.url}")
            
            data = json.loads(response.text)

            df = pd.DataFrame(data)
        
            data_all.append(df)

            # Acumula o total de registros
            total_count += len(data)
            print(f"Total de registros = {total_count}")

            # Exibe status
            print(f"DONE: Consulta bem-sucedida, próximo.")
            print(f"")

            # # Atualiza loop (apenas para testes)
            # loop = loop + 1
            # print(f"Execuçäo: {loop}")          
              
        else:
            print(f"Erro na solicitação: {response.status_code}")

    else:
        # Este UUID já foi consultado, ignore-o
        print(f"UUID já consultado: {value}")

# Concatena todos 
final_all_data = pd.concat(data_all, ignore_index=True)

df_final = pd.DataFrame(final_all_data)

# Separa campo event-identifier
df_identifier = df_final['event_identifier']

# Separa campo aninhado em novo dataframe
df_payload = pd.json_normalize(df_final['payload'])

# Merge dos dataframes
df_payload_final = pd.merge(df_identifier, df_payload, left_index=True, right_index=True)

# Selecionando apenas as colunas corretas
df_payload_final = df_payload_final[['event_identifier', 'email', 'event_timestamp', 'conversion_identifier', 'traffic_source', 'name']]

# Conta quantidades de registros no dataframe final
df_count_registros = df_payload_final.count()

with open('count_registros_eventos_clientes.txt', 'w') as file:
        file.write(f"Total de registros: {total_count}")

# Salva o DataFrame em formato Parquet
df_payload_final.to_parquet("eventos-contatos-clientes-api.parquet", index=False)

